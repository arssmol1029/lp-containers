#!/usr/bin/env python3

"""Run solver jobs in a pool of already-running Docker containers."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Sequence


EXIT_OK = 0
EXIT_RUNTIME_ERROR = 2
EXIT_INPUT_ERROR = 64
EXIT_INTERRUPTED = 130

CONTAINER_COLUMNS = ["container_name", "solver", "threads"]
JOB_COLUMNS = ["job_id", "solver", "model", "options", "timeout_seconds"]
ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
CONTAINER_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
UINT_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
PROVEN_STATUSES = {"OPTIMAL", "INFEASIBLE", "UNBOUNDED"}

HIGHS_FORCED_OPTIONS = (
    ("output_flag", "true"),
    ("log_to_console", "true"),
    ("threads", "1"),
    ("parallel", "off"),
)
SCIP_FORCED_OPTIONS = (("lp/threads", "1"),)
FSCIP_SCIP_OPTIONS = "# Задано parallel_runner.py\nlp/threads = 1\n"


@dataclass(frozen=True)
class SolverDefinition:
    executable: str
    options_suffix: str
    model_formats: frozenset[str]


SOLVERS = {
    "highs": SolverDefinition("highs", ".options", frozenset({"lp", "mps"})),
    "scip": SolverDefinition("scip", ".set", frozenset({"lp", "mps"})),
    "fscip": SolverDefinition("fscip", ".prm", frozenset({"lp", "mps"})),
    "lp_solve": SolverDefinition("lp_solve", ".ini", frozenset({"mps"})),
}

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
SCIP_STATUS_RE = re.compile(
    r"^\s*SCIP Status\s*:\s*(.*?)\s*$", re.MULTILINE | re.IGNORECASE
)
SCIP_PRIMAL_BOUND_RE = re.compile(
    r"^\s*Primal Bound\s*:\s*(\S+)", re.MULTILINE | re.IGNORECASE
)
SCIP_DUAL_BOUND_RE = re.compile(
    r"^\s*Dual Bound\s*:\s*(\S+)", re.MULTILINE | re.IGNORECASE
)
SCIP_OBJECTIVE_RE = re.compile(
    r"^\s*objective value\s*:\s*(\S+)", re.MULTILINE | re.IGNORECASE
)
SCIP_GAP_RE = re.compile(
    r"^\s*Gap\s*:\s*(\S+)", re.MULTILINE | re.IGNORECASE
)
SCIP_TIMING_RE = re.compile(
    r"^\s*(?:Solving Time \(sec\)|Total Time)\s*:\s*(\S+)",
    re.MULTILINE | re.IGNORECASE,
)
LPSOLVE_OBJECTIVE_RE = re.compile(
    r"^\s*Value of objective function\s*:\s*(\S+)",
    re.MULTILINE | re.IGNORECASE,
)
LPSOLVE_TIMING_RE = re.compile(
    r"^\s*CPU Time for solving\s*:\s*(\S+?)s(?:\s|$)",
    re.MULTILINE | re.IGNORECASE,
)


class InputError(Exception):
    """Invalid command line or manifest data."""


class BackendError(Exception):
    """Docker could not perform an operation."""


class RunnerArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise InputError(message)


@dataclass(frozen=True)
class ContainerSpec:
    name: str
    solver: str
    threads: int


@dataclass(frozen=True)
class JobSpec:
    input_index: int
    job_id: str
    solver: str
    model_path: Path
    options_path: Path
    timeout_seconds: float
    model_format: str


@dataclass(frozen=True)
class RunConfig:
    containers_path: Path
    jobs_path: Path
    wait_all: bool
    output_dir: Path
    containers: tuple[ContainerSpec, ...]
    jobs: tuple[JobSpec, ...]


@dataclass(frozen=True)
class PreparedContent:
    job: JobSpec
    model_bytes: bytes
    options_text: str
    auxiliary_options: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PreparedJob:
    job: JobSpec
    model_path: Path
    options_path: Path
    auxiliary_paths: tuple[Path, ...]
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
    job_id: str
    container_name: str
    solver: str
    timeout_seconds: float
    status: str
    objective: str | None
    solver_seconds: float | None
    container_seconds: float
    exit_code: int | None
    cancel_reason: str
    selected: bool = False


@dataclass
class ActiveJob:
    container: ContainerSpec
    prepared: PreparedJob
    handle: DockerHandle
    started_at: float


@dataclass(frozen=True)
class RunOutcome:
    results: tuple[JobResult, ...]
    selected_job_id: str | None
    interrupted: bool


def build_parser() -> RunnerArgumentParser:
    parser = RunnerArgumentParser(
        description="Параллельный запуск solver job в готовых Docker-контейнерах"
    )
    parser.add_argument("--containers", required=True, type=Path)
    parser.add_argument("--jobs", required=True, type=Path)
    parser.add_argument(
        "--wait-all",
        action="store_true",
        help="дождаться всех job, а не первого доказанного результата",
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
            f"строка {line_number}: {label} должен соответствовать "
            f"{ID_RE.pattern!r}, получено {value!r}"
        )
    return value


def parse_solver(value: str, line_number: int) -> str:
    if value not in SOLVERS:
        raise InputError(
            f"строка {line_number}: solver должен быть одним из: "
            f"{', '.join(SOLVERS)}; получено {value!r}"
        )
    return value


def parse_threads(value: str, solver: str, line_number: int) -> int:
    if not UINT_RE.fullmatch(value) or value == "0":
        raise InputError(f"строка {line_number}: threads должен быть целым числом > 0")
    threads = int(value)
    if solver == "fscip" and threads < 2:
        raise InputError(f"строка {line_number}: для fscip threads должен быть не меньше 2")
    if solver != "fscip" and threads != 1:
        raise InputError(
            f"строка {line_number}: для solver {solver!r} threads должен быть равен 1"
        )
    return threads


def parse_timeout(value: str, line_number: int) -> float:
    try:
        timeout = float(value)
    except ValueError as error:
        raise InputError(f"строка {line_number}: timeout_seconds должен быть числом") from error
    if not math.isfinite(timeout) or timeout <= 0:
        raise InputError(
            f"строка {line_number}: timeout_seconds должен быть конечным числом > 0"
        )
    return timeout


def resolve_manifest_path(manifest: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = manifest.parent / path
    return path.resolve()


def detect_model_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".lp", ".mps"}:
        return suffix[1:]
    raise InputError(f"модель должна иметь расширение .lp или .mps: {path}")


def load_containers(path: Path) -> tuple[ContainerSpec, ...]:
    rows = read_tsv(path, CONTAINER_COLUMNS, "containers.tsv")
    if not rows:
        raise InputError("containers.tsv не содержит контейнеров")
    result: list[ContainerSpec] = []
    names: set[str] = set()
    for row in rows:
        line = int(row["__line__"])
        name = row["container_name"]
        if not CONTAINER_NAME_RE.fullmatch(name):
            raise InputError(f"строка {line}: недопустимое container_name {name!r}")
        if name in names:
            raise InputError(f"строка {line}: контейнер {name!r} указан повторно")
        solver = parse_solver(row["solver"], line)
        result.append(ContainerSpec(name, solver, parse_threads(row["threads"], solver, line)))
        names.add(name)
    return tuple(result)


def load_jobs(path: Path) -> tuple[JobSpec, ...]:
    rows = read_tsv(path, JOB_COLUMNS, "jobs.tsv")
    if not rows:
        raise InputError("jobs.tsv не содержит job")
    result: list[JobSpec] = []
    job_ids: set[str] = set()
    for input_index, row in enumerate(rows):
        line = int(row["__line__"])
        job_id = parse_identifier(row["job_id"], "job_id", line)
        if job_id in job_ids:
            raise InputError(f"строка {line}: повторяющийся job_id {job_id!r}")
        solver = parse_solver(row["solver"], line)
        model_path = resolve_manifest_path(path, row["model"])
        options_path = resolve_manifest_path(path, row["options"])
        require_readable_file(model_path, f"модель в строке {line}")
        require_readable_file(options_path, f"options в строке {line}")
        model_format = detect_model_format(model_path)
        definition = SOLVERS[solver]
        if model_format not in definition.model_formats:
            allowed = ", ".join(f".{item}" for item in sorted(definition.model_formats))
            raise InputError(
                f"строка {line}: solver {solver!r} поддерживает модели только {allowed}"
            )
        if options_path.suffix.lower() != definition.options_suffix:
            raise InputError(
                f"строка {line}: options для solver {solver!r} должен иметь "
                f"расширение {definition.options_suffix}"
            )
        result.append(
            JobSpec(
                input_index,
                job_id,
                solver,
                model_path,
                options_path,
                parse_timeout(row["timeout_seconds"], line),
                model_format,
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
        raise InputError(f"родитель выходного каталога не является каталогом")
    containers = load_containers(containers_path)
    jobs = load_jobs(jobs_path)
    missing = sorted({job.solver for job in jobs} - {item.solver for item in containers})
    if missing:
        raise InputError("в containers.tsv нет контейнеров для solver: " + ", ".join(missing))
    return RunConfig(containers_path, jobs_path, arguments.wait_all, output_dir, containers, jobs)


def option_name(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    match = re.match(r"([^=\s]+)\s*=", stripped)
    return match.group(1) if match else None


def force_options(source: str, forced: tuple[tuple[str, str], ...]) -> str:
    forced_names = {name for name, _ in forced}
    lines = [line for line in source.splitlines() if option_name(line) not in forced_names]
    if lines and lines[-1] != "":
        lines.append("")
    lines.append("# Задано parallel_runner.py")
    lines.extend(f"{name} = {value}" for name, value in forced)
    return "\n".join(lines) + "\n"


def make_effective_options(source: str, solver: str) -> str:
    if solver == "highs":
        return force_options(source, HIGHS_FORCED_OPTIONS)
    if solver == "scip":
        return force_options(source, SCIP_FORCED_OPTIONS)
    return source if not source or source.endswith("\n") else source + "\n"


def read_utf8(path: Path, label: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InputError(f"не удалось прочитать {label} {path}: {error}") from error


def prepare_content(jobs: tuple[JobSpec, ...]) -> tuple[PreparedContent, ...]:
    result: list[PreparedContent] = []
    for job in jobs:
        try:
            model_bytes = job.model_path.read_bytes()
        except OSError as error:
            raise InputError(f"не удалось прочитать модель {job.model_path}: {error}") from error
        options = make_effective_options(read_utf8(job.options_path, "options"), job.solver)
        auxiliary = (("runner.set", FSCIP_SCIP_OPTIONS),) if job.solver == "fscip" else ()
        result.append(PreparedContent(job, model_bytes, options, auxiliary))
    return tuple(result)


def create_output(config: RunConfig, content: tuple[PreparedContent, ...]) -> tuple[PreparedJob, ...]:
    try:
        config.output_dir.mkdir(parents=True, exist_ok=False)
        inputs_dir = config.output_dir / "inputs"
        jobs_dir = config.output_dir / "jobs"
        (config.output_dir / "containers").mkdir()
        inputs_dir.mkdir()
        jobs_dir.mkdir()
        shutil.copyfile(config.containers_path, inputs_dir / "containers.tsv")
        shutil.copyfile(config.jobs_path, inputs_dir / "jobs.tsv")
        prepared: list[PreparedJob] = []
        for item in content:
            artifact_dir = jobs_dir / item.job.job_id
            artifact_dir.mkdir()
            model_path = artifact_dir / f"model.{item.job.model_format}"
            options_path = artifact_dir / f"effective{SOLVERS[item.job.solver].options_suffix}"
            model_path.write_bytes(item.model_bytes)
            options_path.write_text(item.options_text, encoding="utf-8")
            auxiliary_paths: list[Path] = []
            for name, source in item.auxiliary_options:
                path = artifact_dir / name
                path.write_text(source, encoding="utf-8")
                auxiliary_paths.append(path)
            stdout_path = artifact_dir / "stdout.log"
            stderr_path = artifact_dir / "stderr.log"
            stdout_path.touch()
            stderr_path.touch()
            prepared.append(
                PreparedJob(
                    item.job,
                    model_path,
                    options_path,
                    tuple(auxiliary_paths),
                    stdout_path,
                    stderr_path,
                )
            )
        write_results(config.output_dir, ())
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


def solver_argv(solver: str, model_name: str, options_name: str, threads: int) -> list[str]:
    if solver == "highs":
        return ["highs", "--model_file", model_name, "--options_file", options_name]
    if solver == "scip":
        return ["scip", "-s", options_name, "-f", model_name]
    if solver == "fscip":
        return [
            "fscip",
            options_name,
            model_name,
            "-sth",
            str(threads),
            "-sl",
            "runner.set",
            "-s",
            "runner.set",
            "-sr",
            "runner.set",
        ]
    if solver == "lp_solve":
        return ["lp_solve", "-rpar", options_name, "-time", "-S1", "-mps", model_name]
    raise BackendError(f"неизвестный solver {solver!r}")


def cpuset_size(value: str) -> int:
    cpus: set[int] = set()
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        if "-" in item:
            start_text, end_text = item.split("-", maxsplit=1)
            start, end = int(start_text), int(end_text)
            if start > end:
                raise ValueError(value)
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(item))
    return len(cpus)


def solver_probe_script(container: ContainerSpec) -> tuple[str, str]:
    executable = SOLVERS[container.solver].executable
    required = [executable, "mkdir", "rm", "cat"]
    if container.solver == "fscip":
        required.append("scip")
    checks = " ".join(required)
    if container.solver in {"highs", "scip"}:
        command = f"exec {executable} --version"
        marker = "highs" if container.solver == "highs" else "scip version"
    elif container.solver == "fscip":
        command = "fscip /dev/null /dev/null -sth 2 || true"
        marker = "parallelized by ug"
    else:
        command = "exec lp_solve -h"
        marker = "usage of lp_solve version"
    script = (
        f"for tool in {checks}; do "
        'command -v "$tool" >/dev/null 2>&1 || '
        '{ printf "missing tool: %s\\n" "$tool" >&2; exit 127; }; '
        f"done; {command}"
    )
    return script, marker


@dataclass
class DockerHandle:
    process: subprocess.Popen[bytes]
    stdout_file: object
    stderr_file: object
    container: ContainerSpec
    remote_dir: str
    finished: bool = False


class DockerBackend:
    def __init__(self, docker_path: str, grace_seconds: float = 2.0) -> None:
        self.docker_path = docker_path
        self.grace_seconds = grace_seconds
        self.remote_root = f"/tmp/lp-parallel-runner-{os.getpid()}-{uuid.uuid4().hex}"

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
        inspect = self._run([self.docker_path, "inspect", container.name])
        if inspect.returncode != 0:
            details = inspect.stderr.strip() or inspect.stdout.strip()
            raise BackendError(f"контейнер {container.name!r} не найден: {details}")
        try:
            record = json.loads(inspect.stdout)[0]
            runtime_id = str(record["Id"])
            running = bool(record["State"]["Running"])
            host_config = record.get("HostConfig", {})
            nano_cpus = int(host_config.get("NanoCpus") or 0)
            cpuset_cpus = str(host_config.get("CpusetCpus") or "")
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise BackendError(f"docker inspect вернул неожиданный ответ для {container.name!r}") from error
        if not running:
            raise BackendError(f"контейнер {container.name!r} не запущен")
        if container.solver == "fscip":
            if nano_cpus and nano_cpus < container.threads * 1_000_000_000:
                raise BackendError(
                    f"контейнер {container.name!r} ограничен "
                    f"{nano_cpus / 1_000_000_000:g} CPU, но threads={container.threads}"
                )
            if cpuset_cpus:
                try:
                    available = cpuset_size(cpuset_cpus)
                except ValueError as error:
                    raise BackendError(f"не удалось разобрать CpusetCpus {cpuset_cpus!r}") from error
                if available < container.threads:
                    raise BackendError(
                        f"контейнер {container.name!r} имеет {available} CPU, "
                        f"но threads={container.threads}"
                    )
        script, marker = solver_probe_script(container)
        version = self._run([self.docker_path, "exec", container.name, "sh", "-c", script])
        combined = "\n".join(
            part for part in (version.stdout.strip(), version.stderr.strip()) if part
        )
        if version.returncode != 0 or marker not in combined.lower():
            raise BackendError(
                f"контейнер {container.name!r} не подходит: "
                f"{container.solver} или POSIX sh недоступен ({combined or 'нет вывода'})"
            )
        return ContainerProbe(runtime_id, inspect.stdout, combined + "\n")

    def _remote_command(
        self, container: ContainerSpec, script: str, *args: str
    ) -> subprocess.CompletedProcess[str]:
        return self._run(
            [self.docker_path, "exec", container.name, "sh", "-c", script, "runner", *args]
        )

    def _cleanup(self, container: ContainerSpec, remote_dir: str) -> None:
        if not remote_dir.startswith(self.remote_root + "/"):
            raise BackendError("отказ от очистки каталога вне namespace runner")
        result = self._remote_command(
            container,
            'rm -rf -- "$1"; status=$?; '
            'if [ "$status" -eq 0 ]; then rmdir -- "$2" 2>/dev/null || true; fi; '
            'exit "$status"',
            remote_dir,
            self.remote_root,
        )
        if result.returncode != 0:
            raise BackendError(
                f"не удалось очистить каталог job в {container.name}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

    def start(self, container: ContainerSpec, job: PreparedJob) -> DockerHandle:
        remote_dir = f"{self.remote_root}/{job.job.input_index}-{uuid.uuid4().hex}"
        create = self._remote_command(container, 'umask 077; mkdir -p -- "$1"', remote_dir)
        if create.returncode != 0:
            raise BackendError(
                f"не удалось создать каталог job в {container.name}: "
                f"{create.stderr.strip() or create.stdout.strip()}"
            )
        stdout_file = None
        stderr_file = None
        process = None
        try:
            for local in (job.model_path, job.options_path, *job.auxiliary_paths):
                copy = self._run(
                    [self.docker_path, "cp", str(local), f"{container.name}:{remote_dir}/{local.name}"],
                    timeout=120,
                )
                if copy.returncode != 0:
                    raise BackendError(
                        f"не удалось передать {local.name} в {container.name}: "
                        f"{copy.stderr.strip() or copy.stdout.strip()}"
                    )
            argv = solver_argv(
                job.job.solver,
                job.model_path.name,
                job.options_path.name,
                container.threads,
            )
            stdout_file = job.stdout_path.open("wb")
            stderr_file = job.stderr_path.open("wb")
            process = subprocess.Popen(
                [
                    self.docker_path,
                    "exec",
                    container.name,
                    "sh",
                    "-c",
                    'cd "$1" || exit 125; printf "%s\\n" "$$" > runner.pid || exit 125; '
                    'shift; exec "$@"',
                    "runner",
                    remote_dir,
                    *argv,
                ],
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
            )
            return DockerHandle(process, stdout_file, stderr_file, container, remote_dir)
        except (Exception, KeyboardInterrupt) as error:
            if process is not None and stdout_file is not None and stderr_file is not None:
                try:
                    self.cancel(
                        DockerHandle(process, stdout_file, stderr_file, container, remote_dir),
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
                    self._cleanup(container, remote_dir)
                except BackendError:
                    pass
            if isinstance(error, OSError):
                raise BackendError(f"не удалось запустить docker exec: {error}") from error
            raise

    @staticmethod
    def poll(handle: DockerHandle) -> int | None:
        return handle.process.poll()

    def _finish(
        self,
        handle: DockerHandle,
        cancel_reason: str = "",
        *,
        cleanup: bool = True,
        backend_error: str = "",
    ) -> RawExecution:
        exit_code = handle.process.wait()
        if not handle.finished:
            handle.stdout_file.close()  # type: ignore[union-attr]
            handle.stderr_file.close()  # type: ignore[union-attr]
            if cleanup:
                try:
                    self._cleanup(handle.container, handle.remote_dir)
                except BackendError as error:
                    backend_error = str(error)
            handle.finished = True
        return RawExecution(exit_code, cancel_reason, backend_error)

    def complete(self, handle: DockerHandle) -> RawExecution:
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
                f"не удалось послать {signal} процессу job в {handle.container.name}: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )

    def cancel(self, handle: DockerHandle, reason: str) -> RawExecution:
        if handle.process.poll() is not None:
            return self._finish(handle, reason)
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
                handle.process.terminate()
                try:
                    handle.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    handle.process.kill()
                    handle.process.wait()
            try:
                handle.stderr_file.write(("\nОшибка отмены: " + signal_error + "\n").encode())  # type: ignore[union-attr]
            except (OSError, ValueError):
                pass
        if signal_error:
            raw = self._finish(
                handle,
                reason,
                cleanup=False,
                backend_error=f"kill_error={signal_error}",
            )
            return RawExecution(raw.exit_code, raw.cancel_reason, raw.backend_error, False)
        return self._finish(handle, reason)


def write_probe(output_dir: Path, container: ContainerSpec, probe: ContainerProbe) -> None:
    target = output_dir / "containers" / container.name
    target.mkdir()
    (target / "inspect.json").write_text(probe.inspect_text, encoding="utf-8")
    (target / "solver-version.txt").write_text(probe.solver_version, encoding="utf-8")


def probe_all_containers(config: RunConfig, backend: DockerBackend) -> None:
    runtime_ids: set[str] = set()
    for container in config.containers:
        probe = backend.probe(container)
        if probe.runtime_id in runtime_ids:
            raise BackendError("несколько строк containers.tsv указывают на один контейнер")
        runtime_ids.add(probe.runtime_id)
        write_probe(config.output_dir, container, probe)


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
    solver_seconds = parse_float(last_match(HIGHS_TIMING_RE, output))
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
    return status, objective, parse_float(objective), solver_seconds


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def last_match(pattern: re.Pattern[str], output: str) -> str | None:
    matches = list(pattern.finditer(output))
    return matches[-1].group(1) if matches else None


def parse_scip_output(
    stdout: str, stderr: str, exit_code: int
) -> tuple[str, str | None, float | None, float | None]:
    output = stdout + "\n" + stderr
    solver_seconds = parse_float(last_match(SCIP_TIMING_RE, output))
    if exit_code != 0:
        return "ERROR", None, None, solver_seconds
    status_text = last_match(SCIP_STATUS_RE, output)
    if status_text is None:
        return "UNKNOWN", None, None, solver_seconds
    primal_text = last_match(SCIP_PRIMAL_BOUND_RE, output)
    objective_text = primal_text or last_match(SCIP_OBJECTIVE_RE, output)
    objective_value = parse_float(objective_text)
    dual_text = last_match(SCIP_DUAL_BOUND_RE, output)
    gap_value = parse_float(last_match(SCIP_GAP_RE, output))
    normalized = status_text.lower()
    if "infeasible or unbounded" in normalized or "unbounded or infeasible" in normalized:
        status = "UNBOUNDED_OR_INFEASIBLE"
    elif "optimal solution found" in normalized:
        status = "OPTIMAL"
    elif "infeasible" in normalized:
        status = "INFEASIBLE"
    elif "unbounded" in normalized:
        status = "UNBOUNDED"
    elif "time limit" in normalized:
        status = "TIMEOUT"
    elif "interrupted" in normalized or "limit reached" in normalized:
        status = "FEASIBLE" if objective_value is not None else "UNKNOWN"
    elif "problem is solved" in normalized:
        primal_lower = (primal_text or "").lower()
        dual_lower = (dual_text or "").lower()
        if objective_value is not None and gap_value == 0.0:
            status = "OPTIMAL"
        elif "infinity" in primal_lower and "infinity" in dual_lower:
            status = "UNBOUNDED" if "-" in primal_lower else "INFEASIBLE"
        else:
            status = "UNKNOWN"
    else:
        status = "UNKNOWN"
    if status not in {"OPTIMAL", "FEASIBLE"} or objective_value is None:
        return status, None, None, solver_seconds
    return status, objective_text, objective_value, solver_seconds


def parse_lpsolve_output(
    stdout: str, stderr: str, exit_code: int
) -> tuple[str, str | None, float | None, float | None]:
    output = stdout + "\n" + stderr
    solver_seconds = parse_float(last_match(LPSOLVE_TIMING_RE, output))
    if exit_code != 0:
        return "ERROR", None, None, solver_seconds
    objective = last_match(LPSOLVE_OBJECTIVE_RE, output)
    objective_value = parse_float(objective)
    normalized = output.lower()
    if "infeasible" in normalized:
        status = "INFEASIBLE"
    elif "unbounded" in normalized:
        status = "UNBOUNDED"
    elif "suboptimal" in normalized:
        status = "FEASIBLE" if objective_value is not None else "UNKNOWN"
    elif "timeout" in normalized or "time limit" in normalized:
        status = "TIMEOUT"
    elif objective_value is not None:
        status = "OPTIMAL"
    else:
        status = "UNKNOWN"
    if status not in {"OPTIMAL", "FEASIBLE"} or objective_value is None:
        return status, None, None, solver_seconds
    return status, objective, objective_value, solver_seconds


def parse_solver_output(
    solver: str, stdout: str, stderr: str, exit_code: int
) -> tuple[str, str | None, float | None, float | None]:
    if solver == "highs":
        return parse_highs_output(stdout, stderr, exit_code)
    if solver in {"scip", "fscip"}:
        return parse_scip_output(stdout, stderr, exit_code)
    if solver == "lp_solve":
        return parse_lpsolve_output(stdout, stderr, exit_code)
    return "ERROR", None, None, None


def result_from_raw(active: ActiveJob, raw: RawExecution, finished_at: float) -> JobResult:
    job = active.prepared.job
    if raw.backend_error:
        status, objective, solver_seconds = "ERROR", None, None
        append_stderr(active.prepared, f"Ошибка Docker backend: {raw.backend_error}")
    elif raw.cancel_reason == "timeout":
        status, objective, solver_seconds = "TIMEOUT", None, None
    elif raw.cancel_reason:
        status, objective, solver_seconds = "CANCELLED", None, None
    elif raw.exit_code is None:
        status, objective, solver_seconds = "ERROR", None, None
    else:
        status, objective, _objective_value, solver_seconds = parse_solver_output(
            job.solver,
            read_log(active.prepared.stdout_path),
            read_log(active.prepared.stderr_path),
            raw.exit_code,
        )
    return JobResult(
        job.input_index,
        job.job_id,
        active.container.name,
        job.solver,
        job.timeout_seconds,
        status,
        objective,
        solver_seconds,
        max(0.0, finished_at - active.started_at),
        raw.exit_code,
        raw.cancel_reason if not raw.backend_error else "; ".join(
            part for part in (raw.cancel_reason, raw.backend_error) if part
        ),
    )


def synthetic_result(job: PreparedJob, status: str, reason: str) -> JobResult:
    return JobResult(
        job.job.input_index,
        job.job.job_id,
        "",
        job.job.solver,
        job.job.timeout_seconds,
        status,
        None,
        None,
        0.0,
        None,
        reason,
    )


def error_result(container: ContainerSpec, job: PreparedJob, message: str) -> JobResult:
    append_stderr(job, message)
    return replace(synthetic_result(job, "ERROR", "start_error"), container_name=container.name)


def format_optional(value: object | None) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_results(output_dir: Path, results: Sequence[JobResult]) -> None:
    with (output_dir / "jobs.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "job_id",
                "container_name",
                "solver",
                "timeout_seconds",
                "status",
                "objective",
                "solver_seconds",
                "container_seconds",
                "exit_code",
                "cancel_reason",
                "selected",
            ]
        )
        for result in sorted(results, key=lambda item: item.input_index):
            writer.writerow(
                [
                    result.job_id,
                    result.container_name,
                    result.solver,
                    format_optional(result.timeout_seconds),
                    result.status,
                    format_optional(result.objective),
                    format_optional(result.solver_seconds),
                    format_optional(result.container_seconds),
                    format_optional(result.exit_code),
                    result.cancel_reason,
                    "true" if result.selected else "false",
                ]
            )


def schedule(
    config: RunConfig,
    prepared: tuple[PreparedJob, ...],
    backend: DockerBackend,
    poll_interval: float = 0.05,
) -> RunOutcome:
    pending = list(prepared)
    active: dict[str, ActiveJob] = {}
    disabled: set[str] = set()
    results: dict[int, JobResult] = {}
    selected_job_id: str | None = None
    interrupted = False

    def record(result: JobResult) -> None:
        results[result.input_index] = result
        write_results(config.output_dir, tuple(results.values()))

    def cancel_remaining(reason: str) -> None:
        for job in pending:
            record(synthetic_result(job, "CANCELLED", reason))
        pending.clear()
        for container_name, item in list(active.items()):
            try:
                raw = backend.cancel(item.handle, reason)
            except BackendError as error:
                raw = RawExecution(None, reason, str(error), False)
            del active[container_name]
            record(result_from_raw(item, raw, time.monotonic()))
            if not raw.reusable_container:
                disabled.add(container_name)

    try:
        while pending or active:
            for container in config.containers:
                if container.name in active or container.name in disabled:
                    continue
                job_index = next(
                    (
                        index
                        for index, candidate in enumerate(pending)
                        if candidate.job.solver == container.solver
                    ),
                    None,
                )
                if job_index is None:
                    continue
                job = pending.pop(job_index)
                started_at = time.monotonic()
                try:
                    handle = backend.start(container, job)
                except KeyboardInterrupt:
                    pending.insert(0, job)
                    raise
                except BackendError as error:
                    record(error_result(container, job, str(error)))
                    disabled.add(container.name)
                    continue
                active[container.name] = ActiveJob(container, job, handle, started_at)

            made_progress = False
            selected_now = False
            for container_name, item in list(active.items()):
                if backend.poll(item.handle) is None:
                    continue
                raw = backend.complete(item.handle)
                del active[container_name]
                result = result_from_raw(item, raw, time.monotonic())
                if not config.wait_all and result.status in PROVEN_STATUSES:
                    result = replace(result, selected=True)
                    selected_job_id = result.job_id
                    record(result)
                    cancel_remaining("first_result")
                    selected_now = True
                    made_progress = True
                    break
                record(result)
                made_progress = True
            if selected_now:
                continue

            now = time.monotonic()
            for container_name, item in list(active.items()):
                if now - item.started_at < item.prepared.job.timeout_seconds:
                    continue
                try:
                    raw = backend.cancel(item.handle, "timeout")
                except BackendError as error:
                    raw = RawExecution(None, "timeout", str(error), False)
                del active[container_name]
                record(result_from_raw(item, raw, time.monotonic()))
                if not raw.reusable_container:
                    disabled.add(container_name)
                made_progress = True

            available_solvers = {
                item.solver for item in config.containers if item.name not in disabled
            }
            unavailable = [job for job in pending if job.job.solver not in available_solvers]
            if unavailable:
                unavailable_indexes = {job.job.input_index for job in unavailable}
                pending[:] = [
                    job for job in pending if job.job.input_index not in unavailable_indexes
                ]
                for job in unavailable:
                    append_stderr(job, f"Нет доступных контейнеров для solver {job.job.solver!r}")
                    record(synthetic_result(job, "ERROR", "no_available_container"))
                made_progress = True

            if active and not made_progress:
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        interrupted = True
        cancel_remaining("interrupt")

    return RunOutcome(
        tuple(results[index] for index in sorted(results)),
        selected_job_id,
        interrupted,
    )


def setup_failure(prepared: tuple[PreparedJob, ...], output_dir: Path, message: str) -> None:
    results: list[JobResult] = []
    for job in prepared:
        append_stderr(job, message)
        results.append(synthetic_result(job, "ERROR", "setup_error"))
    write_results(output_dir, results)


def run(arguments: Sequence[str] | None = None) -> int:
    try:
        parsed = build_parser().parse_args(arguments)
        config = load_config(parsed)
        content = prepare_content(config.jobs)
        prepared = create_output(config, content)
    except InputError as error:
        print(f"Ошибка входных данных: {error}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    try:
        backend = DockerBackend(find_docker())
        probe_all_containers(config, backend)
    except BackendError as error:
        setup_failure(prepared, config.output_dir, str(error))
        print(f"Ошибка Docker: {error}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR
    except KeyboardInterrupt:
        message = "Проверка Docker прервана"
        setup_failure(prepared, config.output_dir, message)
        print(message, file=sys.stderr)
        return EXIT_INTERRUPTED

    outcome = schedule(config, prepared, backend)
    write_results(config.output_dir, outcome.results)
    if outcome.interrupted:
        print(f"Запуск прерван; output={config.output_dir}", file=sys.stderr)
        return EXIT_INTERRUPTED
    if any(result.status == "ERROR" for result in outcome.results):
        print(f"Ошибка выполнения; output={config.output_dir}", file=sys.stderr)
        return EXIT_RUNTIME_ERROR
    if config.wait_all:
        print(f"Запуск завершён: job={len(outcome.results)}, output={config.output_dir}")
    else:
        print(
            f"Запуск завершён: selected={outcome.selected_job_id or 'none'}, "
            f"job={len(outcome.results)}, output={config.output_dir}"
        )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(run())
