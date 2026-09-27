#!/usr/bin/env python3

"""Run independent HiGHS jobs on a pool of already-running containers."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Protocol, Sequence


EXIT_OK = 0
EXIT_RUNTIME_ERROR = 2
EXIT_CONFLICT = 3
EXIT_INPUT_ERROR = 64
EXIT_INTERRUPTED = 130

CONTAINER_COLUMNS = ["id", "container_name"]
JOB_COLUMNS = [
    "problem_id",
    "job_id",
    "model",
    "options",
    "order",
    "timeout_seconds",
]
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
CONTAINER_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
UINT64_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
MAX_UINT64 = (1 << 64) - 1
WINNING_STATUSES = {"OPTIMAL", "INFEASIBLE", "UNBOUNDED"}
FORCED_OPTIONS = (
    ("output_flag", "true"),
    ("log_to_console", "true"),
    ("threads", "1"),
    ("parallel", "off"),
)

LP_CONSTRAINT_SECTIONS = {"subject to", "such that", "st", "s.t."}
LP_END_SECTIONS = {
    "bounds",
    "bound",
    "binary",
    "binaries",
    "bin",
    "general",
    "generals",
    "gen",
    "integer",
    "integers",
    "semi-continuous",
    "semi",
    "semis",
    "sos",
    "end",
}
LP_CONSTRAINT_NAME_RE = re.compile(r"[ \t]*([^\s:]+)[ \t]*:")
LP_COMPARISON_RE = re.compile(r"<=|>=|(?<![<>])=(?!=)|(?<!<)<(?!=)|(?<!>)>(?!=)")
LP_VARIABLE_RE = re.compile(r"(?<![A-Za-z0-9_.])[A-Za-z_][A-Za-z0-9_.]*")
LP_RESERVED = {"inf", "infinity"}

HIGHS_MODEL_STATUS_RE = re.compile(
    r"^\s*Model status\s*:\s*(.*?)\s*$", re.MULTILINE | re.IGNORECASE
)
HIGHS_REPORT_STATUS_RE = re.compile(
    r"^\s*Status\s+(?::\s*)?(.*?)\s*$", re.MULTILINE | re.IGNORECASE
)
HIGHS_OBJECTIVE_RE = re.compile(
    r"^\s*Objective value\s*:\s*(\S+)", re.MULTILINE | re.IGNORECASE
)
HIGHS_PRIMAL_BOUND_RE = re.compile(
    r"^\s*Primal bound\s+(?::\s*)?(\S+)", re.MULTILINE | re.IGNORECASE
)
HIGHS_TIMING_RE = re.compile(
    r"^\s*(?:Timing|(?:HiGHS\s+)?Run Time)\s+(?::\s*)?(\S+)",
    re.MULTILINE | re.IGNORECASE,
)


class InputError(Exception):
    """Invalid command line or manifest data."""


class BackendError(Exception):
    """The execution backend could not perform a requested operation."""


class RunnerArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise InputError(message)


@dataclass(frozen=True)
class ContainerSpec:
    container_id: str
    container_name: str


@dataclass(frozen=True)
class OrderSpec:
    kind: str
    seed: int | None = None

    def text(self) -> str:
        return f"shuffle:{self.seed}" if self.kind == "shuffle" else self.kind


@dataclass(frozen=True)
class JobSpec:
    input_index: int
    problem_id: str
    job_id: str
    model_path: Path
    options_path: Path
    order: OrderSpec
    timeout_seconds: float
    model_format: str
    solver: str = "highs"


@dataclass(frozen=True)
class RunConfig:
    containers_path: Path
    jobs_path: Path
    mode: str
    output_dir: Path
    containers: tuple[ContainerSpec, ...]
    jobs: tuple[JobSpec, ...]


@dataclass(frozen=True)
class PreparedContent:
    job: JobSpec
    model_bytes: bytes
    options_text: str


@dataclass(frozen=True)
class PreparedJob:
    job: JobSpec
    artifact_dir: Path
    model_path: Path
    options_path: Path
    stdout_path: Path
    stderr_path: Path


@dataclass(frozen=True)
class ContainerProbe:
    runtime_id: str
    inspect_text: str
    solver_version: str


@dataclass(frozen=True)
class RawExecution:
    exit_code: int | None
    cancel_reason: str = ""
    backend_error: str = ""
    reusable_container: bool = True


@dataclass(frozen=True)
class JobResult:
    input_index: int
    problem_id: str
    job_id: str
    container_id: str
    container_name: str
    solver: str
    order: str
    timeout_seconds: float
    status: str
    objective: str | None
    objective_value: float | None
    solver_seconds: float | None
    container_seconds: float
    exit_code: int | None
    cancel_reason: str


@dataclass(frozen=True)
class Conflict:
    problem_id: str
    kind: str
    details: str


@dataclass(frozen=True)
class ScheduleOutcome:
    results: tuple[JobResult, ...]
    winners: dict[str, JobResult]
    interrupted: bool


@dataclass(frozen=True)
class MpsLayout:
    lines: tuple[str, ...]
    constraint_positions: tuple[int, ...]
    arities: tuple[int, ...]


@dataclass(frozen=True)
class LpLayout:
    prefix: tuple[str, ...]
    constraint_blocks: tuple[tuple[str, ...], ...]
    arities: tuple[int, ...]
    suffix: tuple[str, ...]


@dataclass
class ActiveJob:
    container: ContainerSpec
    prepared: PreparedJob
    handle: object
    started_at: float


class RunnerBackend(Protocol):
    def probe(self, container: ContainerSpec) -> ContainerProbe:
        ...

    def start(self, container: ContainerSpec, job: PreparedJob) -> object:
        ...

    def poll(self, handle: object) -> int | None:
        ...

    def complete(self, handle: object) -> RawExecution:
        ...

    def cancel(self, handle: object, reason: str) -> RawExecution:
        ...


def build_parser() -> RunnerArgumentParser:
    parser = RunnerArgumentParser(
        description="Параллельный запуск HiGHS job в готовых Docker-контейнерах"
    )
    parser.add_argument("--containers", required=True, type=Path)
    parser.add_argument("--jobs", required=True, type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--mode",
        choices=("benchmark", "portfolio"),
        help="явно выбрать режим; по умолчанию portfolio",
    )
    mode.add_argument(
        "--wait-all",
        action="store_true",
        help="выполнить все job (эквивалент --mode benchmark)",
    )
    parser.add_argument("--output", required=True, type=Path)
    return parser


def require_readable_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise InputError(f"{label} не найден: {path}")
    if not os.access(path, os.R_OK):
        raise InputError(f"{label} недоступен для чтения: {path}")


def read_tsv(path: Path, expected_columns: list[str], label: str) -> list[dict[str, str]]:
    require_readable_file(path, label)
    try:
        source = path.open("r", encoding="utf-8-sig", newline="")
    except (OSError, UnicodeError) as error:
        raise InputError(f"не удалось прочитать {label}: {error}") from error

    rows: list[dict[str, str]] = []
    with source:
        reader = csv.DictReader(source, delimiter="\t")
        if reader.fieldnames != expected_columns:
            expected = "\t".join(expected_columns)
            actual = "\t".join(reader.fieldnames or [])
            raise InputError(
                f"в {label} ожидался заголовок {expected!r}, получен {actual!r}"
            )
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise InputError(f"{label}, строка {line_number}: слишком много колонок")
            clean = {key: (value or "").strip() for key, value in row.items()}
            clean["__line__"] = str(line_number)
            rows.append(clean)
    return rows


def parse_identifier(value: str, label: str, line_number: int) -> str:
    if not ID_RE.fullmatch(value):
        raise InputError(
            f"строка {line_number}: {label} должен соответствовать {ID_RE.pattern!r}, "
            f"получено {value!r}"
        )
    return value


def load_containers(path: Path) -> tuple[ContainerSpec, ...]:
    rows = read_tsv(path, CONTAINER_COLUMNS, "containers.tsv")
    if not rows:
        raise InputError("containers.tsv не содержит контейнеров")
    result: list[ContainerSpec] = []
    ids: set[str] = set()
    names: set[str] = set()
    for row in rows:
        line = int(row["__line__"])
        container_id = parse_identifier(row["id"], "id", line)
        name = row["container_name"]
        if not CONTAINER_NAME_RE.fullmatch(name):
            raise InputError(f"строка {line}: недопустимое container_name {name!r}")
        if container_id in ids:
            raise InputError(f"строка {line}: повторяющийся id {container_id!r}")
        if name in names:
            raise InputError(f"строка {line}: контейнер {name!r} указан повторно")
        ids.add(container_id)
        names.add(name)
        result.append(ContainerSpec(container_id, name))
    return tuple(result)


def resolve_manifest_path(manifest: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = manifest.parent / path
    return path.resolve()


def parse_order(value: str, line_number: int) -> OrderSpec:
    if value == "original":
        return OrderSpec("original")
    if value == "low-arity-first":
        return OrderSpec("low-arity-first")
    if value.startswith("shuffle:"):
        seed_text = value.removeprefix("shuffle:")
        if not UINT64_RE.fullmatch(seed_text):
            raise InputError(
                f"строка {line_number}: seed в order должен быть uint64"
            )
        seed = int(seed_text)
        if seed > MAX_UINT64:
            raise InputError(
                f"строка {line_number}: seed в order превышает uint64"
            )
        return OrderSpec("shuffle", seed)
    raise InputError(
        f"строка {line_number}: order должен быть original, shuffle:<uint64> "
        "или low-arity-first"
    )


def detect_model_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".lp", ".mps"}:
        return suffix[1:]
    raise InputError(f"модель должна иметь расширение .lp или .mps: {path}")


def parse_timeout(value: str, line_number: int) -> float:
    try:
        timeout = float(value)
    except ValueError as error:
        raise InputError(
            f"строка {line_number}: timeout_seconds должен быть числом"
        ) from error
    if not math.isfinite(timeout) or timeout <= 0:
        raise InputError(
            f"строка {line_number}: timeout_seconds должен быть конечным числом > 0"
        )
    return timeout


def load_jobs(path: Path) -> tuple[JobSpec, ...]:
    rows = read_tsv(path, JOB_COLUMNS, "jobs.tsv")
    if not rows:
        raise InputError("jobs.tsv не содержит job")
    result: list[JobSpec] = []
    job_ids: set[str] = set()
    for input_index, row in enumerate(rows):
        line = int(row["__line__"])
        problem_id = parse_identifier(row["problem_id"], "problem_id", line)
        job_id = parse_identifier(row["job_id"], "job_id", line)
        if job_id in job_ids:
            raise InputError(f"строка {line}: повторяющийся job_id {job_id!r}")
        model_path = resolve_manifest_path(path, row["model"])
        options_path = resolve_manifest_path(path, row["options"])
        require_readable_file(model_path, f"модель в строке {line}")
        require_readable_file(options_path, f"options в строке {line}")
        order = parse_order(row["order"], line)
        timeout = parse_timeout(row["timeout_seconds"], line)
        result.append(
            JobSpec(
                input_index=input_index,
                problem_id=problem_id,
                job_id=job_id,
                model_path=model_path,
                options_path=options_path,
                order=order,
                timeout_seconds=timeout,
                model_format=detect_model_format(model_path),
            )
        )
        job_ids.add(job_id)
    return tuple(result)


def load_config(arguments: argparse.Namespace) -> RunConfig:
    containers_path = arguments.containers.expanduser().resolve()
    jobs_path = arguments.jobs.expanduser().resolve()
    output_dir = arguments.output.expanduser().resolve()
    if output_dir.exists() or output_dir.is_symlink():
        raise InputError(f"выходной каталог уже существует: {output_dir}")
    if output_dir.parent.exists() and not output_dir.parent.is_dir():
        raise InputError(
            f"родитель выходного каталога не является каталогом: {output_dir.parent}"
        )
    mode = getattr(arguments, "mode", None) or (
        "benchmark" if getattr(arguments, "wait_all", False) else "portfolio"
    )
    return RunConfig(
        containers_path=containers_path,
        jobs_path=jobs_path,
        mode=mode,
        output_dir=output_dir,
        containers=load_containers(containers_path),
        jobs=load_jobs(jobs_path),
    )


def normalize_lp_section(line: str) -> str:
    return " ".join(line.strip().lower().split())


def lp_block_arity(block: tuple[str, ...], name: str) -> int:
    text = "".join(line.split("\\", maxsplit=1)[0] for line in block)
    text = LP_CONSTRAINT_NAME_RE.sub("", text, count=1)
    match = LP_COMPARISON_RE.search(text)
    expression = text[: match.start()] if match else text
    variables = {
        token
        for token in LP_VARIABLE_RE.findall(expression)
        if token.lower() not in LP_RESERVED and token != name
    }
    return len(variables)


def parse_lp_layout(source: str) -> LpLayout:
    lines = tuple(source.splitlines(keepends=True))
    masked = tuple(line.split("\\", maxsplit=1)[0] for line in lines)
    headers = [
        index
        for index, line in enumerate(masked)
        if normalize_lp_section(line) in LP_CONSTRAINT_SECTIONS
    ]
    if len(headers) != 1:
        raise InputError("LP должен содержать ровно одну секцию Subject To")
    section_start = headers[0]
    section_end = len(lines)
    for index in range(section_start + 1, len(lines)):
        if normalize_lp_section(masked[index]) in LP_END_SECTIONS:
            section_end = index
            break

    starts: list[int] = []
    names: list[str] = []
    for index in range(section_start + 1, section_end):
        match = LP_CONSTRAINT_NAME_RE.match(masked[index])
        if match:
            starts.append(index)
            names.append(match.group(1))
    if not starts:
        raise InputError("секция Subject To не содержит именованных ограничений")
    if len(names) != len(set(names)):
        raise InputError("имена ограничений LP должны быть уникальными")
    if "".join(masked[section_start + 1 : starts[0]]).strip():
        raise InputError("обнаружено безымянное ограничение в начале Subject To")

    blocks: list[tuple[str, ...]] = []
    arities: list[int] = []
    for number, start in enumerate(starts):
        end = starts[number + 1] if number + 1 < len(starts) else section_end
        block = lines[start:end]
        comparisons = LP_COMPARISON_RE.findall("".join(masked[start:end]))
        if len(comparisons) != 1:
            raise InputError(
                f"ограничение {names[number]!r} неоднозначно: ожидался один "
                f"оператор сравнения, найдено {len(comparisons)}"
            )
        blocks.append(block)
        arities.append(lp_block_arity(block, names[number]))
    return LpLayout(
        prefix=lines[: starts[0]],
        constraint_blocks=tuple(blocks),
        arities=tuple(arities),
        suffix=lines[section_end:],
    )


def mps_section(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("*"):
        return None
    fields = stripped.split()
    if len(fields) != 1:
        return None
    section = fields[0].upper()
    if section in {"ROWS", "COLUMNS", "RHS", "RANGES", "BOUNDS", "ENDATA"}:
        return section
    return None


def parse_mps_layout(source: str) -> MpsLayout:
    lines = tuple(source.splitlines(keepends=True))
    rows_headers = [i for i, line in enumerate(lines) if mps_section(line) == "ROWS"]
    if len(rows_headers) != 1:
        raise InputError("MPS должен содержать ровно одну секцию ROWS")
    rows_start = rows_headers[0]
    columns_headers = [
        i for i in range(rows_start + 1, len(lines)) if mps_section(lines[i]) == "COLUMNS"
    ]
    if not columns_headers:
        raise InputError("после ROWS в MPS не найдена секция COLUMNS")
    rows_end = columns_headers[0]

    positions: list[int] = []
    names: list[str] = []
    all_names: set[str] = set()
    for index in range(rows_start + 1, rows_end):
        stripped = lines[index].strip()
        if not stripped or stripped.startswith("*"):
            continue
        fields = stripped.split()
        if len(fields) != 2 or fields[0].upper() not in {"N", "E", "L", "G"}:
            raise InputError(f"неоднозначная строка {index + 1} секции ROWS в MPS")
        name = fields[1]
        if name in all_names:
            raise InputError(f"повторяющееся имя строки {name!r} в MPS")
        all_names.add(name)
        if fields[0].upper() != "N":
            positions.append(index)
            names.append(name)

    arity_sets = {name: set() for name in names}
    constraint_names = set(names)
    columns_start = rows_end
    columns_end = len(lines)
    for index in range(columns_start + 1, len(lines)):
        if mps_section(lines[index]) in {"RHS", "RANGES", "BOUNDS", "ENDATA"}:
            columns_end = index
            break
    for index in range(columns_start + 1, columns_end):
        stripped = lines[index].strip()
        if not stripped or stripped.startswith("*"):
            continue
        fields = stripped.split()
        if "MARKER" in {field.strip("'").upper() for field in fields}:
            continue
        if len(fields) not in {3, 5}:
            raise InputError(f"неоднозначная строка {index + 1} секции COLUMNS в MPS")
        column = fields[0]
        pairs = fields[1:]
        for pair_index in range(0, len(pairs), 2):
            row_name = pairs[pair_index]
            if row_name in constraint_names:
                arity_sets[row_name].add(column)
    return MpsLayout(
        lines=lines,
        constraint_positions=tuple(positions),
        arities=tuple(len(arity_sets[name]) for name in names),
    )


def ordered_indices(count: int, arities: tuple[int, ...], order: OrderSpec) -> list[int]:
    indices = list(range(count))
    if order.kind == "shuffle":
        assert order.seed is not None
        random.Random(order.seed).shuffle(indices)
    elif order.kind == "low-arity-first":
        indices.sort(key=lambda index: arities[index])
    elif order.kind != "original":
        raise InputError(f"неизвестный порядок ограничений: {order.kind}")
    return indices


def render_lp(layout: LpLayout, order: OrderSpec) -> str:
    indices = ordered_indices(len(layout.constraint_blocks), layout.arities, order)
    body = [line for index in indices for line in layout.constraint_blocks[index]]
    return "".join((*layout.prefix, *body, *layout.suffix))


def render_mps(layout: MpsLayout, order: OrderSpec) -> str:
    result = list(layout.lines)
    indices = ordered_indices(len(layout.constraint_positions), layout.arities, order)
    reordered = [layout.lines[layout.constraint_positions[index]] for index in indices]
    for position, line in zip(layout.constraint_positions, reordered):
        result[position] = line
    return "".join(result)


def option_name(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        return None
    name, _ = line.split("=", maxsplit=1)
    return name.strip(" \t\r\n\"'")


def make_effective_options(source: str) -> str:
    forced_names = {name for name, _ in FORCED_OPTIONS}
    lines = [line for line in source.splitlines() if option_name(line) not in forced_names]
    if lines and lines[-1] != "":
        lines.append("")
    lines.append("# Задано parallel_runner.py")
    lines.extend(f"{name} = {value}" for name, value in FORCED_OPTIONS)
    return "\n".join(lines) + "\n"


def read_utf8(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InputError(f"не удалось прочитать {label} {path}: {error}") from error


def prepare_content(job: JobSpec) -> PreparedContent:
    try:
        original = job.model_path.read_bytes()
    except OSError as error:
        raise InputError(f"не удалось прочитать модель {job.model_path}: {error}") from error
    if job.order.kind == "original":
        model_bytes = original
    else:
        try:
            source = original.decode("utf-8")
        except UnicodeError as error:
            raise InputError(
                f"для изменения порядка модель должна быть UTF-8: {job.model_path}"
            ) from error
        if job.model_format == "lp":
            rendered = render_lp(parse_lp_layout(source), job.order)
        elif job.model_format == "mps":
            rendered = render_mps(parse_mps_layout(source), job.order)
        else:
            raise InputError(f"неизвестный формат модели: {job.model_format}")
        model_bytes = rendered.encode("utf-8")
    options = make_effective_options(read_utf8(job.options_path, "options"))
    return PreparedContent(job, model_bytes, options)


def prepare_all_content(jobs: tuple[JobSpec, ...]) -> tuple[PreparedContent, ...]:
    return tuple(prepare_content(job) for job in jobs)


def create_output(config: RunConfig, content: tuple[PreparedContent, ...]) -> tuple[PreparedJob, ...]:
    try:
        config.output_dir.mkdir(parents=True, exist_ok=False)
        inputs_dir = config.output_dir / "inputs"
        jobs_dir = config.output_dir / "jobs"
        containers_dir = config.output_dir / "containers"
        inputs_dir.mkdir()
        jobs_dir.mkdir()
        containers_dir.mkdir()
        shutil.copyfile(config.containers_path, inputs_dir / "containers.tsv")
        shutil.copyfile(config.jobs_path, inputs_dir / "jobs.tsv")
        prepared: list[PreparedJob] = []
        for item in content:
            artifact_dir = jobs_dir / item.job.job_id
            artifact_dir.mkdir()
            model_path = artifact_dir / f"model.{item.job.model_format}"
            options_path = artifact_dir / "effective.options"
            model_path.write_bytes(item.model_bytes)
            options_path.write_text(item.options_text, encoding="utf-8")
            stdout_path = artifact_dir / "stdout.log"
            stderr_path = artifact_dir / "stderr.log"
            stdout_path.touch()
            stderr_path.touch()
            prepared.append(
                PreparedJob(
                    item.job,
                    artifact_dir,
                    model_path,
                    options_path,
                    stdout_path,
                    stderr_path,
                )
            )
        return tuple(prepared)
    except FileExistsError as error:
        raise InputError(f"выходной каталог уже существует: {config.output_dir}") from error
    except OSError as error:
        raise InputError(f"не удалось подготовить output: {error}") from error


def find_docker() -> str:
    docker = shutil.which("docker")
    if docker is None:
        raise BackendError("Docker CLI не найден в PATH")
    try:
        check = subprocess.run(
            [docker, "version", "--format", "{{.Server.Version}}"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BackendError(f"не удалось проверить Docker: {error}") from error
    if check.returncode != 0:
        details = check.stderr.strip() or check.stdout.strip()
        raise BackendError(f"Docker недоступен: {details or check.returncode}")
    return docker


def solver_argv(solver: str, model_name: str, options_name: str) -> list[str]:
    if solver != "highs":
        raise BackendError(f"неизвестный solver {solver!r}; поддерживается только highs")
    return [
        "highs",
        "--model_file",
        model_name,
        "--options_file",
        options_name,
    ]


@dataclass
class DockerHandle:
    process: subprocess.Popen[bytes] | None
    stdout_file: object | None
    stderr_file: object | None
    container: ContainerSpec
    remote_dir: str
    finished: bool = False


class DockerBackend:
    """Backend that only uses docker exec/cp against user-owned containers."""

    def __init__(self, docker_path: str, grace_seconds: float = 2.0) -> None:
        self.docker_path = docker_path
        self.grace_seconds = grace_seconds
        self.run_token = f"{os.getpid()}-{uuid.uuid4().hex}"
        self.remote_root = f"/tmp/lp-parallel-runner-{self.run_token}"

    def _run(self, command: list[str], timeout: float = 20.0) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise BackendError(f"не удалось выполнить {' '.join(command[:3])}: {error}") from error

    def probe(self, container: ContainerSpec) -> ContainerProbe:
        inspect = self._run([self.docker_path, "inspect", container.container_name])
        if inspect.returncode != 0:
            details = inspect.stderr.strip() or inspect.stdout.strip()
            raise BackendError(
                f"контейнер {container.container_name!r} не найден: {details}"
            )
        try:
            payload = json.loads(inspect.stdout)
            record = payload[0]
            runtime_id = str(record["Id"])
            running = bool(record["State"]["Running"])
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise BackendError(
                f"docker inspect вернул неожиданный ответ для {container.container_name!r}"
            ) from error
        if not running:
            raise BackendError(f"контейнер {container.container_name!r} не запущен")

        version = self._run(
            [
                self.docker_path,
                "exec",
                container.container_name,
                "sh",
                "-c",
                'for tool in highs mkdir rm cat; do '
                'command -v "$tool" >/dev/null 2>&1 || '
                '{ printf "missing tool: %s\\n" "$tool" >&2; exit 127; }; '
                'done; exec highs --version',
            ]
        )
        combined = "\n".join(part for part in (version.stdout.strip(), version.stderr.strip()) if part)
        if version.returncode != 0 or "highs" not in combined.lower():
            raise BackendError(
                f"контейнер {container.container_name!r} не подходит: "
                f"HiGHS или POSIX sh недоступен ({combined or 'нет вывода'})"
            )
        return ContainerProbe(runtime_id, inspect.stdout, combined + "\n")

    def _remote_command(self, container: ContainerSpec, script: str, *args: str) -> subprocess.CompletedProcess[str]:
        return self._run(
            [self.docker_path, "exec", container.container_name, "sh", "-c", script, "runner", *args]
        )

    def _cleanup(self, handle: DockerHandle) -> None:
        expected = self.remote_root + "/"
        if not handle.remote_dir.startswith(expected):
            raise BackendError("отказ от очистки каталога вне namespace runner")
        result = self._remote_command(
            handle.container,
            'rm -rf -- "$1"; status=$?; '
            'if [ "$status" -eq 0 ]; then rmdir -- "$2" 2>/dev/null || true; fi; '
            'exit "$status"',
            handle.remote_dir,
            self.remote_root,
        )
        if result.returncode != 0:
            raise BackendError(
                f"не удалось очистить каталог job в {handle.container.container_name}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

    def start(self, container: ContainerSpec, job: PreparedJob) -> DockerHandle:
        remote_dir = f"{self.remote_root}/{job.job.input_index}-{uuid.uuid4().hex}"
        create = self._remote_command(
            container,
            'umask 077; mkdir -p -- "$1"',
            remote_dir,
        )
        if create.returncode != 0:
            raise BackendError(
                f"не удалось создать каталог job в {container.container_name}: "
                f"{create.stderr.strip() or create.stdout.strip()}"
            )
        stdout_file = None
        stderr_file = None
        process = None
        try:
            for local in (job.model_path, job.options_path):
                copy = self._run(
                    [
                        self.docker_path,
                        "cp",
                        str(local),
                        f"{container.container_name}:{remote_dir}/{local.name}",
                    ],
                    timeout=120,
                )
                if copy.returncode != 0:
                    raise BackendError(
                        f"не удалось передать {local.name} в {container.container_name}: "
                        f"{copy.stderr.strip() or copy.stdout.strip()}"
                    )
            argv = solver_argv(job.job.solver, job.model_path.name, job.options_path.name)
            stdout_file = job.stdout_path.open("wb")
            stderr_file = job.stderr_path.open("wb")
            command = [
                self.docker_path,
                "exec",
                container.container_name,
                "sh",
                "-c",
                'cd "$1" || exit 125; printf "%s\\n" "$$" > runner.pid || exit 125; '
                'shift; exec "$@"',
                "runner",
                remote_dir,
                *argv,
            ]
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_file,
                    stderr=stderr_file,
                )
            except OSError as error:
                stdout_file.close()
                stderr_file.close()
                raise BackendError(f"не удалось запустить docker exec: {error}") from error
            return DockerHandle(process, stdout_file, stderr_file, container, remote_dir)
        except (Exception, KeyboardInterrupt) as error:
            if process is not None and stdout_file is not None and stderr_file is not None:
                try:
                    self.cancel(
                        DockerHandle(
                            process, stdout_file, stderr_file, container, remote_dir
                        ),
                        "interrupt" if isinstance(error, KeyboardInterrupt) else "start_error",
                    )
                except BackendError:
                    pass
            else:
                if stdout_file is not None:
                    stdout_file.close()
                if stderr_file is not None:
                    stderr_file.close()
                try:
                    self._cleanup(DockerHandle(None, None, None, container, remote_dir))
                except BackendError:
                    pass
            raise

    def poll(self, handle: object) -> int | None:
        assert isinstance(handle, DockerHandle) and handle.process is not None
        return handle.process.poll()

    def _finish(
        self,
        handle: DockerHandle,
        cancel_reason: str = "",
        *,
        cleanup: bool = True,
        backend_error: str = "",
    ) -> RawExecution:
        assert handle.process is not None
        exit_code = handle.process.wait()
        if not handle.finished:
            assert handle.stdout_file is not None and handle.stderr_file is not None
            handle.stdout_file.close()  # type: ignore[union-attr]
            handle.stderr_file.close()  # type: ignore[union-attr]
            if cleanup:
                try:
                    self._cleanup(handle)
                except BackendError as error:
                    backend_error = str(error)
            handle.finished = True
        return RawExecution(exit_code, cancel_reason, backend_error)

    def complete(self, handle: object) -> RawExecution:
        assert isinstance(handle, DockerHandle)
        return self._finish(handle)

    def _signal(self, handle: DockerHandle, signal: str) -> None:
        result = self._remote_command(
            handle.container,
            'pid=$(cat -- "$1/runner.pid" 2>/dev/null) || exit 0; '
            'case "$pid" in ""|*[!0-9]*) exit 65;; esac; '
            f'kill -{signal} "$pid" 2>/dev/null || true',
            handle.remote_dir,
        )
        if result.returncode != 0:
            raise BackendError(
                f"не удалось послать {signal} процессу job в "
                f"{handle.container.container_name}: {result.stderr.strip() or result.stdout.strip()}"
            )

    def cancel(self, handle: object, reason: str) -> RawExecution:
        assert isinstance(handle, DockerHandle) and handle.process is not None
        if handle.process.poll() is not None:
            return self._finish(handle)
        signal_error = ""
        try:
            self._signal(handle, "TERM")
            try:
                handle.process.wait(timeout=self.grace_seconds)
            except subprocess.TimeoutExpired:
                self._signal(handle, "KILL")
                handle.process.wait(timeout=max(5.0, self.grace_seconds))
        except (BackendError, subprocess.TimeoutExpired) as error:
            signal_error = str(error)
            if handle.process.poll() is None:
                # This only stops the local docker client. The error stays visible.
                handle.process.terminate()
                try:
                    handle.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    handle.process.kill()
                    handle.process.wait()
            if handle.stderr_file is not None:
                try:
                    handle.stderr_file.write(("\nОшибка отмены: " + signal_error + "\n").encode())  # type: ignore[union-attr]
                except (OSError, ValueError):
                    pass
        if signal_error:
            # The remote process may still be alive, so its directory must not be
            # removed. Stopping the local docker client is not treated as a kill.
            raw = self._finish(
                handle,
                reason,
                cleanup=False,
                backend_error=f"kill_error={signal_error}",
            )
            return RawExecution(
                raw.exit_code,
                raw.cancel_reason,
                raw.backend_error,
                reusable_container=False,
            )
        return self._finish(handle, reason)


def write_probe(output_dir: Path, container: ContainerSpec, probe: ContainerProbe) -> None:
    target = output_dir / "containers" / container.container_id
    target.mkdir()
    (target / "inspect.json").write_text(probe.inspect_text, encoding="utf-8")
    (target / "highs-version.txt").write_text(probe.solver_version, encoding="utf-8")


def append_stderr(job: PreparedJob, message: str) -> None:
    try:
        with job.stderr_path.open("a", encoding="utf-8") as stream:
            stream.write(message.rstrip() + "\n")
    except OSError:
        pass


def read_log(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def normalize_highs_status(value: str) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "_", value.upper()).strip("_")
    if normalized in {"UNBOUNDED_OR_INFEASIBLE", "INFEASIBLE_OR_UNBOUNDED"}:
        return "UNBOUNDED_OR_INFEASIBLE"
    if normalized.startswith("TIME_LIMIT"):
        return "TIMEOUT"
    if normalized.startswith("OPTIMAL"):
        return "OPTIMAL"
    if normalized.startswith("INFEASIBLE"):
        return "INFEASIBLE"
    if normalized.startswith("UNBOUNDED"):
        return "UNBOUNDED"
    if normalized.startswith("FEASIBLE"):
        return "FEASIBLE"
    if "ERROR" in normalized:
        return "ERROR"
    return "UNKNOWN"


def parse_highs_output(
    stdout: str, stderr: str, exit_code: int
) -> tuple[str, str | None, float | None, float | None]:
    output = stdout + "\n" + stderr
    timings = list(HIGHS_TIMING_RE.finditer(output))
    solver_seconds = None
    if timings:
        try:
            solver_seconds = float(timings[-1].group(1))
        except ValueError:
            pass
    statuses = [
        (match.start(), match.group(1))
        for pattern in (HIGHS_MODEL_STATUS_RE, HIGHS_REPORT_STATUS_RE)
        for match in pattern.finditer(output)
    ]
    if not statuses:
        return ("ERROR" if exit_code != 0 else "UNKNOWN"), None, None, solver_seconds
    status = normalize_highs_status(max(statuses, key=lambda item: item[0])[1])
    if exit_code != 0:
        return "ERROR", None, None, solver_seconds
    if status != "OPTIMAL":
        return status, None, None, solver_seconds
    objectives = [
        (match.start(), match.group(1))
        for pattern in (HIGHS_OBJECTIVE_RE, HIGHS_PRIMAL_BOUND_RE)
        for match in pattern.finditer(output)
    ]
    if not objectives:
        return status, None, None, solver_seconds
    objective = max(objectives, key=lambda item: item[0])[1]
    try:
        objective_value = float(objective)
    except ValueError:
        objective_value = None
    return status, objective, objective_value, solver_seconds


def result_from_raw(
    active: ActiveJob,
    raw: RawExecution,
    finished_at: float,
) -> JobResult:
    job = active.prepared.job
    duration = max(0.0, finished_at - active.started_at)
    if raw.backend_error:
        status, objective, objective_value, solver_seconds = "ERROR", None, None, None
        append_stderr(active.prepared, f"Ошибка Docker backend: {raw.backend_error}")
    elif raw.cancel_reason.startswith("timeout"):
        status, objective, objective_value, solver_seconds = "TIMEOUT", None, None, None
    elif raw.cancel_reason:
        status, objective, objective_value, solver_seconds = "CANCELLED", None, None, None
    elif raw.exit_code is None:
        status, objective, objective_value, solver_seconds = "ERROR", None, None, None
    else:
        status, objective, objective_value, solver_seconds = parse_highs_output(
            read_log(active.prepared.stdout_path),
            read_log(active.prepared.stderr_path),
            raw.exit_code,
        )
    return JobResult(
        input_index=job.input_index,
        problem_id=job.problem_id,
        job_id=job.job_id,
        container_id=active.container.container_id,
        container_name=active.container.container_name,
        solver=job.solver,
        order=job.order.text(),
        timeout_seconds=job.timeout_seconds,
        status=status,
        objective=objective,
        objective_value=objective_value,
        solver_seconds=solver_seconds,
        container_seconds=duration,
        exit_code=raw.exit_code,
        cancel_reason=(
            raw.cancel_reason
            if not raw.backend_error
            else "; ".join(part for part in (raw.cancel_reason, raw.backend_error) if part)
        ),
    )


def synthetic_result(job: PreparedJob, status: str, reason: str) -> JobResult:
    return JobResult(
        input_index=job.job.input_index,
        problem_id=job.job.problem_id,
        job_id=job.job.job_id,
        container_id="",
        container_name="",
        solver=job.job.solver,
        order=job.job.order.text(),
        timeout_seconds=job.job.timeout_seconds,
        status=status,
        objective=None,
        objective_value=None,
        solver_seconds=None,
        container_seconds=0.0,
        exit_code=None,
        cancel_reason=reason,
    )


def error_result(container: ContainerSpec, job: PreparedJob, message: str) -> JobResult:
    append_stderr(job, message)
    base = synthetic_result(job, "ERROR", "start_error")
    return JobResult(
        **{
            **asdict(base),
            "container_id": container.container_id,
            "container_name": container.container_name,
        }
    )


def write_job_result(output_dir: Path, result: JobResult) -> None:
    path = output_dir / "jobs" / result.job_id / "result.json"
    payload = asdict(result)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def schedule(
    config: RunConfig,
    prepared: tuple[PreparedJob, ...],
    backend: RunnerBackend,
    *,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    poll_interval: float = 0.05,
) -> ScheduleOutcome:
    pending = list(prepared)
    active: dict[str, ActiveJob] = {}
    disabled_containers: set[str] = set()
    results: dict[int, JobResult] = {}
    winners: dict[str, JobResult] = {}
    interrupted = False

    def record(result: JobResult) -> None:
        results[result.input_index] = result
        write_job_result(config.output_dir, result)

    def cancel_queued(problem_id: str) -> None:
        kept: list[PreparedJob] = []
        for queued in pending:
            if queued.job.problem_id == problem_id:
                record(synthetic_result(queued, "CANCELLED", "portfolio_winner"))
            else:
                kept.append(queued)
        pending[:] = kept

    def cancel_running(problem_id: str, winner_job_id: str) -> None:
        for container_id, item in list(active.items()):
            if item.prepared.job.problem_id != problem_id:
                continue
            if item.prepared.job.job_id == winner_job_id:
                continue
            try:
                raw = backend.cancel(item.handle, "portfolio_winner")
            except BackendError as error:
                append_stderr(item.prepared, str(error))
                raw = RawExecution(None, "portfolio_winner", str(error), False)
            del active[container_id]
            record(result_from_raw(item, raw, clock()))
            if not raw.reusable_container:
                disabled_containers.add(container_id)

    def accept_completed_result(result: JobResult) -> None:
        record(result)
        if (
            config.mode == "portfolio"
            and result.status in WINNING_STATUSES
            and result.problem_id not in winners
        ):
            winners[result.problem_id] = result
            cancel_queued(result.problem_id)
            cancel_running(result.problem_id, result.job_id)

    try:
        while pending or active:
            for container in config.containers:
                if (
                    container.container_id in active
                    or container.container_id in disabled_containers
                    or not pending
                ):
                    continue
                job = pending.pop(0)
                if config.mode == "portfolio" and job.job.problem_id in winners:
                    record(synthetic_result(job, "CANCELLED", "portfolio_winner"))
                    continue
                started_at = clock()
                try:
                    handle = backend.start(container, job)
                except KeyboardInterrupt:
                    pending.insert(0, job)
                    raise
                except BackendError as error:
                    record(error_result(container, job, str(error)))
                    disabled_containers.add(container.container_id)
                    continue
                active[container.container_id] = ActiveJob(
                    container, job, handle, started_at
                )

            made_progress = False
            for container_id, item in list(active.items()):
                # A winner handled earlier in this snapshot may already have
                # cancelled and removed this sibling.
                if active.get(container_id) is not item:
                    continue
                exit_code = backend.poll(item.handle)
                if exit_code is None:
                    continue
                raw = backend.complete(item.handle)
                del active[container_id]
                result = result_from_raw(item, raw, clock())
                accept_completed_result(result)
                made_progress = True

            now = clock()
            for container_id, item in list(active.items()):
                if active.get(container_id) is not item:
                    continue
                if now - item.started_at < item.prepared.job.timeout_seconds:
                    continue
                try:
                    raw = backend.cancel(item.handle, "timeout")
                except BackendError as error:
                    append_stderr(item.prepared, str(error))
                    raw = RawExecution(None, "timeout", str(error), False)
                del active[container_id]
                accept_completed_result(result_from_raw(item, raw, clock()))
                if not raw.reusable_container:
                    disabled_containers.add(container_id)
                made_progress = True

            if pending and not active and len(disabled_containers) == len(config.containers):
                for job in pending:
                    append_stderr(job, "Нет доступных контейнеров после ошибки отмены")
                    record(synthetic_result(job, "ERROR", "no_available_container"))
                pending.clear()

            if active and not made_progress:
                sleeper(poll_interval)
    except KeyboardInterrupt:
        interrupted = True
        for container_id, item in list(active.items()):
            try:
                raw = backend.cancel(item.handle, "interrupt")
            except BackendError as error:
                append_stderr(item.prepared, str(error))
                raw = RawExecution(None, "interrupt", str(error), False)
            del active[container_id]
            record(result_from_raw(item, raw, clock()))
        for job in pending:
            record(synthetic_result(job, "CANCELLED", "interrupt"))
        pending.clear()

    ordered = tuple(results[index] for index in sorted(results))
    return ScheduleOutcome(ordered, winners, interrupted)


def objective_conflicts(left: JobResult, right: JobResult) -> bool:
    if left.objective_value is not None and right.objective_value is not None:
        return not math.isclose(
            left.objective_value,
            right.objective_value,
            rel_tol=1e-7,
            abs_tol=1e-9,
        )
    return left.objective != right.objective


def find_conflicts(results: Sequence[JobResult]) -> tuple[Conflict, ...]:
    grouped: dict[str, list[JobResult]] = {}
    for result in results:
        if result.status in WINNING_STATUSES:
            grouped.setdefault(result.problem_id, []).append(result)
    conflicts: list[Conflict] = []
    for problem_id, proven in grouped.items():
        statuses = sorted({result.status for result in proven})
        if len(statuses) > 1:
            conflicts.append(Conflict(problem_id, "status", ", ".join(statuses)))
            continue
        if statuses == ["OPTIMAL"]:
            reference = proven[0]
            disagreeing = [
                result
                for result in proven[1:]
                if objective_conflicts(reference, result)
            ]
            if disagreeing:
                conflicts.append(
                    Conflict(
                        problem_id,
                        "objective",
                        f"{reference.job_id}={reference.objective!r}; "
                        + ", ".join(
                            f"{result.job_id}={result.objective!r}"
                            for result in disagreeing
                        ),
                    )
                )
    return tuple(conflicts)


def format_optional(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_reports(
    config: RunConfig,
    outcome: ScheduleOutcome,
    conflicts: tuple[Conflict, ...],
    setup_error: str = "",
) -> None:
    jobs_path = config.output_dir / "jobs.tsv"
    with jobs_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "problem_id",
                "job_id",
                "container_id",
                "container_name",
                "solver",
                "order",
                "timeout_seconds",
                "status",
                "objective",
                "solver_seconds",
                "container_seconds",
                "exit_code",
                "cancel_reason",
            ]
        )
        for result in outcome.results:
            writer.writerow(
                [
                    result.problem_id,
                    result.job_id,
                    result.container_id,
                    result.container_name,
                    result.solver,
                    result.order,
                    format_optional(result.timeout_seconds),
                    result.status,
                    format_optional(result.objective),
                    format_optional(result.solver_seconds),
                    format_optional(result.container_seconds),
                    format_optional(result.exit_code),
                    result.cancel_reason,
                ]
            )

    with (config.output_dir / "winners.tsv").open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(["problem_id", "job_id", "container_id", "status", "objective"])
        for problem_id in sorted(outcome.winners):
            winner = outcome.winners[problem_id]
            writer.writerow(
                [
                    problem_id,
                    winner.job_id,
                    winner.container_id,
                    winner.status,
                    format_optional(winner.objective),
                ]
            )

    with (config.output_dir / "conflicts.tsv").open(
        "w", encoding="utf-8", newline=""
    ) as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(["problem_id", "kind", "details"])
        for conflict in conflicts:
            writer.writerow([conflict.problem_id, conflict.kind, conflict.details])

    counts = Counter(result.status for result in outcome.results)
    lines = [
        "# Результаты",
        "",
        f"- Режим: `{config.mode}`",
        f"- Заданий в манифесте: {len(config.jobs)}",
        f"- Контейнеров в пуле: {len(config.containers)}",
        f"- Результатов: {len(outcome.results)}",
    ]
    if setup_error:
        lines.append(f"- Ошибка подготовки: {setup_error}")
    if outcome.interrupted:
        lines.append("- Запуск прерван пользователем; активные и ожидавшие job отменены.")
    lines.extend(["", "## Статусы", ""])
    if counts:
        lines.extend(f"- `{status}`: {counts[status]}" for status in sorted(counts))
    else:
        lines.append("- Результатов нет.")
    lines.extend(["", "## Победители portfolio", ""])
    if outcome.winners:
        lines.extend(
            f"- `{problem_id}`: `{winner.job_id}` — {winner.status}"
            for problem_id, winner in sorted(outcome.winners.items())
        )
    else:
        lines.append("- Нет.")
    lines.extend(["", "## Конфликты benchmark", ""])
    if conflicts:
        lines.extend(
            f"- `{item.problem_id}`: {item.kind} — {item.details}" for item in conflicts
        )
    else:
        lines.append("- Не обнаружены.")
    (config.output_dir / "RESULTS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_setup_failure(
    config: RunConfig, prepared: tuple[PreparedJob, ...], message: str
) -> None:
    results: list[JobResult] = []
    for job in prepared:
        append_stderr(job, message)
        result = synthetic_result(job, "ERROR", "setup_error")
        write_job_result(config.output_dir, result)
        results.append(result)
    write_reports(
        config,
        ScheduleOutcome(tuple(results), {}, False),
        (),
        setup_error=message,
    )
    (config.output_dir / "setup-error.txt").write_text(message.rstrip() + "\n", encoding="utf-8")


def probe_all_containers(config: RunConfig, backend: RunnerBackend) -> None:
    runtime_ids: set[str] = set()
    for container in config.containers:
        try:
            probe = backend.probe(container)
        except BackendError as error:
            target = config.output_dir / "containers" / container.container_id
            target.mkdir(exist_ok=True)
            (target / "error.txt").write_text(str(error) + "\n", encoding="utf-8")
            raise
        if probe.runtime_id in runtime_ids:
            raise BackendError(
                f"несколько строк containers.tsv указывают на один контейнер {probe.runtime_id}"
            )
        runtime_ids.add(probe.runtime_id)
        write_probe(config.output_dir, container, probe)


def finish_execution(
    config: RunConfig,
    prepared: tuple[PreparedJob, ...],
    backend: RunnerBackend,
) -> int:
    try:
        probe_all_containers(config, backend)
    except BackendError as error:
        write_setup_failure(config, prepared, str(error))
        print(f"Ошибка Docker backend: {error}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR
    except KeyboardInterrupt:
        message = "Проверка контейнеров прервана пользователем"
        write_setup_failure(config, prepared, message)
        print(message, file=sys.stderr)
        return EXIT_INTERRUPTED

    outcome = schedule(config, prepared, backend)
    conflicts = find_conflicts(outcome.results) if config.mode == "benchmark" else ()
    write_reports(config, outcome, conflicts)
    if outcome.interrupted:
        print("Запуск прерван; отчёты сохранены", file=sys.stderr)
        return EXIT_INTERRUPTED
    if conflicts:
        print("Обнаружен конфликт доказанных результатов", file=sys.stderr)
        return EXIT_CONFLICT
    if any(result.status == "ERROR" for result in outcome.results):
        print("Один или несколько job завершились с ERROR", file=sys.stderr)
        return EXIT_RUNTIME_ERROR
    print(
        f"Запуск завершён: режим={config.mode}, job={len(outcome.results)}, "
        f"output={config.output_dir}"
    )
    return EXIT_OK


def execute(config: RunConfig, backend: RunnerBackend) -> int:
    content = prepare_all_content(config.jobs)
    prepared = create_output(config, content)
    return finish_execution(config, prepared, backend)


def run(arguments: Sequence[str] | None = None) -> int:
    try:
        parsed = build_parser().parse_args(arguments)
        config = load_config(parsed)
        content = prepare_all_content(config.jobs)
    except InputError as error:
        print(f"Ошибка входных данных: {error}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    try:
        prepared = create_output(config, content)
    except InputError as error:
        print(f"Ошибка входных данных: {error}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    try:
        docker = find_docker()
    except BackendError as error:
        write_setup_failure(config, prepared, str(error))
        print(f"Ошибка Docker backend: {error}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR
    except KeyboardInterrupt:
        message = "Проверка Docker прервана пользователем"
        write_setup_failure(config, prepared, message)
        print(message, file=sys.stderr)
        return EXIT_INTERRUPTED

    return finish_execution(config, prepared, DockerBackend(docker))


if __name__ == "__main__":
    raise SystemExit(run())
