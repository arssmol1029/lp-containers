#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import math
import os
import queue
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


EXIT_OK = 0
EXIT_NO_RESULT = 2
EXIT_CONFLICT = 3
EXIT_INPUT_ERROR = 64
EXIT_INTERRUPTED = 130
WORKER_COLUMNS = ["id", "solver", "options", "order_seed"]
WORKER_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
SEED_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
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
LP_COMPARISON_RE = re.compile(
    r"<=|>=|(?<![<>])=(?!=)|(?<!<)<(?!=)|(?<!>)>(?!=)"
)
HIGHS_MODEL_STATUS_RE = re.compile(
    r"^\s*Model status\s*:\s*(.*?)\s*$", re.MULTILINE
)
HIGHS_REPORT_STATUS_RE = re.compile(
    r"^\s*Status\s+(.*?)\s*$", re.MULTILINE
)
HIGHS_OBJECTIVE_RE = re.compile(
    r"^\s*Objective value\s*:\s*(\S+)", re.MULTILINE
)
HIGHS_PRIMAL_BOUND_RE = re.compile(
    r"^\s*Primal bound\s+(\S+)", re.MULTILINE
)
HIGHS_TIMING_RE = re.compile(r"^\s*Timing\s+(\S+)\s*$", re.MULTILINE)
WINNING_STATUSES = {"OPTIMAL", "INFEASIBLE", "UNBOUNDED"}


class InputError(Exception):
    pass


class RunnerArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise InputError(message)


@dataclass(frozen=True)
class WorkerSpec:
    worker_id: str
    solver: str
    options_path: Path
    order_seed: int | None


@dataclass(frozen=True)
class RunConfig:
    model_path: Path
    model_format: str
    manifest_path: Path
    image: str
    output_dir: Path
    docker_path: str
    workers: tuple[WorkerSpec, ...]


@dataclass(frozen=True)
class PreparedWorker:
    spec: WorkerSpec
    work_dir: Path
    model_path: Path
    options_path: Path


@dataclass(frozen=True)
class MpsLayout:
    lines: tuple[str, ...]
    constraint_positions: tuple[int, ...]


@dataclass(frozen=True)
class LpLayout:
    prefix: tuple[str, ...]
    constraint_blocks: tuple[tuple[str, ...], ...]
    suffix: tuple[str, ...]


@dataclass
class WorkerState:
    prepared: PreparedWorker
    container_name: str
    stdout_path: Path
    stderr_path: Path
    started_at: float | None = None
    process: subprocess.Popen[bytes] | None = None
    cancelled: bool = False
    result: WorkerResult | None = None


@dataclass(frozen=True)
class WorkerResult:
    worker_id: str
    solver: str
    order_seed: int | None
    status: str
    objective: str | None
    objective_value: float | None
    solver_seconds: float | None
    container_seconds: float
    exit_code: int | None


def build_parser() -> RunnerArgumentParser:
    parser = RunnerArgumentParser(
        description="Параллельный запуск нескольких контейнеров с решателями"
    )
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--workers", required=True, type=Path)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def require_readable_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise InputError(f"{label} не найден: {path}")
    if not os.access(path, os.R_OK):
        raise InputError(f"{label} недоступен для чтения: {path}")


def detect_model_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".lp":
        return "lp"
    if suffix == ".mps":
        return "mps"
    raise InputError("модель должна иметь расширение .lp или .mps")


def resolve_options_path(manifest_path: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = manifest_path.parent / path
    return path.resolve()


def parse_order_seed(value: str, line_number: int) -> int | None:
    if value == "original":
        return None
    if not SEED_RE.fullmatch(value):
        raise InputError(
            f"строка {line_number}: order_seed должен быть original "
            "или неотрицательным целым числом"
        )
    return int(value)


def load_workers(manifest_path: Path) -> tuple[WorkerSpec, ...]:
    try:
        manifest_file = manifest_path.open(
            "r", encoding="utf-8-sig", newline=""
        )
    except (OSError, UnicodeError) as error:
        raise InputError(f"не удалось прочитать манифест: {error}") from error

    workers: list[WorkerSpec] = []
    seen_ids: set[str] = set()
    with manifest_file:
        reader = csv.DictReader(manifest_file, delimiter="\t")
        if reader.fieldnames != WORKER_COLUMNS:
            actual = "\t".join(reader.fieldnames or [])
            expected = "\t".join(WORKER_COLUMNS)
            raise InputError(
                f"ожидался заголовок манифеста {expected!r}, получен {actual!r}"
            )

        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise InputError(
                    f"строка {line_number}: слишком много колонок"
                )
            worker_id = (row["id"] or "").strip()
            solver = (row["solver"] or "").strip()
            options_value = (row["options"] or "").strip()
            seed_value = (row["order_seed"] or "").strip()

            if not WORKER_ID_RE.fullmatch(worker_id):
                raise InputError(
                    f"строка {line_number}: недопустимый id worker'а {worker_id!r}"
                )
            if worker_id in seen_ids:
                raise InputError(
                    f"строка {line_number}: повторяющийся id worker'а {worker_id!r}"
                )
            if solver != "highs":
                raise InputError(
                    f"строка {line_number}: пока поддерживается только solver=highs"
                )
            if not options_value:
                raise InputError(
                    f"строка {line_number}: не указан options-файл"
                )

            options_path = resolve_options_path(manifest_path, options_value)
            require_readable_file(
                options_path, f"options-файл в строке {line_number}"
            )
            order_seed = parse_order_seed(seed_value, line_number)
            workers.append(
                WorkerSpec(worker_id, solver, options_path, order_seed)
            )
            seen_ids.add(worker_id)

    if not workers:
        raise InputError("манифест не содержит worker'ов")
    return tuple(workers)


def find_docker() -> str:
    docker_path = shutil.which("docker")
    if docker_path is None:
        raise InputError("Docker CLI не найден в PATH")

    try:
        result = subprocess.run(
            [docker_path, "version", "--format", "{{.Server.Version}}"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise InputError(f"не удалось проверить Docker: {error}") from error

    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip()
        if details:
            raise InputError(f"Docker недоступен: {details}")
        raise InputError(f"Docker завершил проверку с кодом {result.returncode}")
    return docker_path


def load_config(arguments: argparse.Namespace) -> RunConfig:
    model_path = arguments.model.expanduser().resolve()
    manifest_path = arguments.workers.expanduser().resolve()
    output_dir = arguments.output.expanduser().resolve()
    image = arguments.image.strip()

    require_readable_file(model_path, "модель")
    model_format = detect_model_format(model_path)
    require_readable_file(manifest_path, "манифест")
    workers = load_workers(manifest_path)

    if not image:
        raise InputError("имя Docker-образа не должно быть пустым")
    if output_dir.exists() or output_dir.is_symlink():
        raise InputError(f"выходной каталог уже существует: {output_dir}")
    if output_dir.parent.exists() and not output_dir.parent.is_dir():
        raise InputError(
            f"родитель выходного каталога не является каталогом: {output_dir.parent}"
        )

    docker_path = find_docker()
    return RunConfig(
        model_path=model_path,
        model_format=model_format,
        manifest_path=manifest_path,
        image=image,
        output_dir=output_dir,
        docker_path=docker_path,
        workers=workers,
    )


def read_options(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise InputError(f"не удалось прочитать options-файл {path}: {error}") from error


def option_name(line: str) -> str | None:
    if not line or line.startswith("#") or "=" not in line:
        return None
    name, _ = line.split("=", maxsplit=1)
    return name.strip(" \t\r\n\"'")


def make_effective_options(source: str) -> str:
    forced_names = {name for name, _ in FORCED_OPTIONS}
    lines = [
        line
        for line in source.splitlines()
        if option_name(line) not in forced_names
    ]
    if lines and lines[-1] != "":
        lines.append("")
    lines.append("# Задано parallel_runner.py")
    lines.extend(f"{name} = {value}" for name, value in FORCED_OPTIONS)
    return "\n".join(lines) + "\n"


def read_model(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8", newline="") as model_file:
            return model_file.read()
    except (OSError, UnicodeError) as error:
        raise InputError(f"не удалось прочитать модель {path}: {error}") from error


def normalize_lp_section(line: str) -> str:
    return " ".join(line.strip().lower().split())


def parse_mps_layout(source: str) -> MpsLayout:
    lines = tuple(source.splitlines(keepends=True))
    rows_headers = [
        index
        for index, line in enumerate(lines)
        if line.strip().upper() == "ROWS"
    ]
    if len(rows_headers) != 1:
        raise InputError("MPS должен содержать ровно одну секцию ROWS")

    rows_start = rows_headers[0]
    columns_headers = [
        index
        for index in range(rows_start + 1, len(lines))
        if lines[index].strip().upper() == "COLUMNS"
    ]
    if not columns_headers:
        raise InputError("после секции ROWS в MPS не найдена секция COLUMNS")
    rows_end = columns_headers[0]

    constraint_positions: list[int] = []
    row_names: set[str] = set()
    for index in range(rows_start + 1, rows_end):
        stripped = lines[index].strip()
        if not stripped or stripped.startswith("*"):
            continue
        fields = stripped.split()
        if len(fields) != 2 or fields[0].upper() not in {"N", "E", "L", "G"}:
            raise InputError(
                f"неоднозначная строка {index + 1} секции ROWS в MPS"
            )
        row_type = fields[0].upper()
        row_name = fields[1]
        if row_name in row_names:
            raise InputError(
                f"повторяющееся имя строки {row_name!r} в секции ROWS"
            )
        row_names.add(row_name)
        if row_type in {"E", "L", "G"}:
            constraint_positions.append(index)

    return MpsLayout(lines, tuple(constraint_positions))


def render_mps(layout: MpsLayout, seed: int) -> str:
    result = list(layout.lines)
    shuffled = [layout.lines[index] for index in layout.constraint_positions]
    random.Random(seed).shuffle(shuffled)
    for index, line in zip(layout.constraint_positions, shuffled):
        result[index] = line
    return "".join(result)


def parse_lp_layout(source: str) -> LpLayout:
    lines = tuple(source.splitlines(keepends=True))
    masked_lines = tuple(line.split("\\", maxsplit=1)[0] for line in lines)

    section_headers = [
        index
        for index, line in enumerate(masked_lines)
        if normalize_lp_section(line) in LP_CONSTRAINT_SECTIONS
    ]
    if len(section_headers) != 1:
        raise InputError("LP должен содержать ровно одну секцию Subject To")

    section_start = section_headers[0]
    section_end = len(lines)
    for index in range(section_start + 1, len(masked_lines)):
        if normalize_lp_section(masked_lines[index]) in LP_END_SECTIONS:
            section_end = index
            break

    block_starts: list[int] = []
    block_names: list[str] = []
    for index in range(section_start + 1, section_end):
        match = LP_CONSTRAINT_NAME_RE.match(masked_lines[index])
        if match is not None:
            block_starts.append(index)
            block_names.append(match.group(1))

    if not block_starts:
        raise InputError("секция Subject To не содержит именованных ограничений")
    if len(block_names) != len(set(block_names)):
        raise InputError("имена ограничений LP должны быть уникальными")

    preamble = "".join(masked_lines[section_start + 1 : block_starts[0]])
    if preamble.strip():
        raise InputError("обнаружено безымянное ограничение в начале Subject To")

    blocks: list[tuple[str, ...]] = []
    for block_number, start in enumerate(block_starts):
        end = (
            block_starts[block_number + 1]
            if block_number + 1 < len(block_starts)
            else section_end
        )
        masked_block = "".join(masked_lines[start:end])
        comparisons = LP_COMPARISON_RE.findall(masked_block)
        if len(comparisons) != 1:
            raise InputError(
                f"ограничение {block_names[block_number]!r} неоднозначно: "
                f"ожидался один оператор сравнения, найдено {len(comparisons)}"
            )
        blocks.append(lines[start:end])

    return LpLayout(
        prefix=lines[: block_starts[0]],
        constraint_blocks=tuple(blocks),
        suffix=lines[section_end:],
    )


def render_lp(layout: LpLayout, seed: int) -> str:
    blocks = list(layout.constraint_blocks)
    random.Random(seed).shuffle(blocks)
    shuffled_lines = [line for block in blocks for line in block]
    return "".join((*layout.prefix, *shuffled_lines, *layout.suffix))


def prepare_workers(config: RunConfig) -> tuple[PreparedWorker, ...]:
    model_source = read_model(config.model_path)
    if config.model_format == "mps":
        model_layout: MpsLayout | LpLayout = parse_mps_layout(model_source)
    else:
        model_layout = parse_lp_layout(model_source)

    effective_options = {
        worker.worker_id: make_effective_options(read_options(worker.options_path))
        for worker in config.workers
    }

    prepared: list[PreparedWorker] = []
    created_output = False
    try:
        config.output_dir.mkdir(parents=True)
        created_output = True
        workers_dir = config.output_dir / "workers"
        workers_dir.mkdir()

        for worker in config.workers:
            work_dir = workers_dir / worker.worker_id
            work_dir.mkdir()
            model_path = work_dir / f"model.{config.model_format}"
            options_path = work_dir / "effective.options"
            if worker.order_seed is None:
                shutil.copyfile(config.model_path, model_path)
            elif config.model_format == "mps":
                model_path.write_text(
                    render_mps(model_layout, worker.order_seed), encoding="utf-8"
                )
            else:
                model_path.write_text(
                    render_lp(model_layout, worker.order_seed), encoding="utf-8"
                )
            options_path.write_text(
                effective_options[worker.worker_id], encoding="utf-8"
            )
            prepared.append(
                PreparedWorker(worker, work_dir, model_path, options_path)
            )
    except OSError as error:
        if created_output:
            shutil.rmtree(config.output_dir, ignore_errors=True)
        raise InputError(f"не удалось подготовить выходной каталог: {error}") from error

    return tuple(prepared)


def solver_arguments(worker: PreparedWorker) -> list[str]:
    if worker.spec.solver == "highs":
        return [
            "--model_file",
            f"/work/{worker.model_path.name}",
            "--options_file",
            f"/work/{worker.options_path.name}",
        ]
    raise InputError(f"неизвестный solver: {worker.spec.solver}")


def build_container_command(
    config: RunConfig, state: WorkerState
) -> list[str]:
    return [
        config.docker_path,
        "run",
        "--name",
        state.container_name,
        "--network",
        "none",
        "--volume",
        f"{state.prepared.work_dir}:/work:ro",
        "--workdir",
        "/tmp",
        config.image,
        *solver_arguments(state.prepared),
    ]


def make_worker_states(
    prepared: tuple[PreparedWorker, ...],
) -> list[WorkerState]:
    run_token = uuid.uuid4().hex[:12]
    return [
        WorkerState(
            prepared=worker,
            container_name=(
                f"lp-containers-{os.getpid()}-{run_token}-{worker.spec.worker_id}"
            ),
            stdout_path=worker.work_dir / "stdout.log",
            stderr_path=worker.work_dir / "stderr.log",
        )
        for worker in prepared
    ]


def error_result(state: WorkerState, message: str) -> WorkerResult:
    try:
        state.stderr_path.write_text(message + "\n", encoding="utf-8")
        state.stdout_path.touch()
    except OSError:
        pass
    return WorkerResult(
        worker_id=state.prepared.spec.worker_id,
        solver=state.prepared.spec.solver,
        order_seed=state.prepared.spec.order_seed,
        status="ERROR",
        objective=None,
        objective_value=None,
        solver_seconds=None,
        container_seconds=0.0,
        exit_code=None,
    )


def launch_workers(config: RunConfig, states: list[WorkerState]) -> None:
    for state in states:
        try:
            stdout_file = state.stdout_path.open("wb")
            stderr_file = state.stderr_path.open("wb")
        except OSError as error:
            state.result = error_result(
                state, f"Не удалось открыть файлы вывода: {error}"
            )
            continue

        state.started_at = time.monotonic()
        try:
            state.process = subprocess.Popen(
                build_container_command(config, state),
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
            )
        except OSError as error:
            state.result = error_result(
                state, f"Не удалось запустить docker run: {error}"
            )
        finally:
            stdout_file.close()
            stderr_file.close()


def wait_for_worker(
    state: WorkerState,
    completions: queue.Queue[tuple[WorkerState, int, float]],
) -> None:
    if state.process is None:
        return
    exit_code = state.process.wait()
    completions.put((state, exit_code, time.monotonic()))


def start_waiters(
    states: list[WorkerState],
    completions: queue.Queue[tuple[WorkerState, int, float]],
) -> list[threading.Thread]:
    threads: list[threading.Thread] = []
    for state in states:
        if state.process is None:
            continue
        thread = threading.Thread(
            target=wait_for_worker,
            args=(state, completions),
            daemon=True,
        )
        thread.start()
        threads.append(thread)
    return threads


def read_log(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def normalize_highs_status(value: str) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "_", value.upper()).strip("_")
    if normalized in {
        "UNBOUNDED_OR_INFEASIBLE",
        "INFEASIBLE_OR_UNBOUNDED",
    }:
        return "UNBOUNDED_OR_INFEASIBLE"
    if normalized.startswith("TIME_LIMIT"):
        return "TIMEOUT"
    if "ERROR" in normalized:
        return "ERROR"
    return normalized or "UNKNOWN"


def parse_highs_output(
    stdout: str, stderr: str, exit_code: int
) -> tuple[str, str | None, float | None, float | None]:
    output = stdout + "\n" + stderr
    timing_matches = list(HIGHS_TIMING_RE.finditer(output))
    solver_seconds = None
    if timing_matches:
        try:
            solver_seconds = float(timing_matches[-1].group(1))
        except ValueError:
            pass

    statuses = [
        (match.start(), match.group(1))
        for pattern in (HIGHS_MODEL_STATUS_RE, HIGHS_REPORT_STATUS_RE)
        for match in pattern.finditer(output)
    ]
    if not statuses:
        status = "ERROR" if exit_code != 0 else "UNKNOWN"
        return status, None, None, solver_seconds
    _, raw_status = max(statuses, key=lambda item: item[0])
    status = normalize_highs_status(raw_status)
    if exit_code != 0 and status in WINNING_STATUSES:
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

    _, objective = max(objectives, key=lambda item: item[0])
    try:
        objective_value = float(objective)
    except ValueError:
        objective_value = None
    return status, objective, objective_value, solver_seconds


def parse_solver_output(
    solver: str, stdout: str, stderr: str, exit_code: int
) -> tuple[str, str | None, float | None, float | None]:
    if solver == "highs":
        return parse_highs_output(stdout, stderr, exit_code)
    return "ERROR", None, None, None


def completed_result(
    state: WorkerState, exit_code: int, finished_at: float
) -> WorkerResult:
    started_at = state.started_at if state.started_at is not None else finished_at
    container_seconds = max(0.0, finished_at - started_at)
    if state.cancelled:
        status = "CANCELLED"
        objective = None
        objective_value = None
        solver_seconds = None
    else:
        status, objective, objective_value, solver_seconds = parse_solver_output(
            state.prepared.spec.solver,
            read_log(state.stdout_path),
            read_log(state.stderr_path),
            exit_code,
        )
    return WorkerResult(
        worker_id=state.prepared.spec.worker_id,
        solver=state.prepared.spec.solver,
        order_seed=state.prepared.spec.order_seed,
        status=status,
        objective=objective,
        objective_value=objective_value,
        solver_seconds=solver_seconds,
        container_seconds=container_seconds,
        exit_code=exit_code,
    )


def remove_containers(
    config: RunConfig, container_names: list[str]
) -> None:
    if not container_names:
        return
    try:
        subprocess.run(
            [config.docker_path, "rm", "-f", *container_names],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        pass


def stop_running_workers(config: RunConfig, states: list[WorkerState]) -> None:
    targets = [
        state
        for state in states
        if state.process is not None and state.process.poll() is None
    ]
    for state in targets:
        state.cancelled = True

    stop_processes: list[subprocess.Popen[bytes]] = []
    for state in targets:
        try:
            stop_processes.append(
                subprocess.Popen(
                    [
                        config.docker_path,
                        "stop",
                        "--time",
                        "2",
                        state.container_name,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
        except OSError:
            pass

    for process in stop_processes:
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()

    remove_containers(
        config, [state.container_name for state in targets]
    )


def monitor_workers(
    config: RunConfig,
    states: list[WorkerState],
    completions: queue.Queue[tuple[WorkerState, int, float]],
) -> WorkerResult | None:
    remaining = sum(state.process is not None for state in states)
    winner: WorkerResult | None = None
    while remaining:
        state, exit_code, finished_at = completions.get()
        remaining -= 1
        state.result = completed_result(state, exit_code, finished_at)
        if winner is None and state.result.status in WINNING_STATUSES:
            winner = state.result
            stop_running_workers(config, states)
    return winner


def finish_after_interrupt(config: RunConfig, states: list[WorkerState]) -> None:
    stop_running_workers(config, states)
    remove_containers(
        config,
        [state.container_name for state in states if state.process is not None],
    )

    for state in states:
        if state.result is not None:
            continue
        if state.process is None:
            state.result = WorkerResult(
                worker_id=state.prepared.spec.worker_id,
                solver=state.prepared.spec.solver,
                order_seed=state.prepared.spec.order_seed,
                status="CANCELLED",
                objective=None,
                objective_value=None,
                solver_seconds=None,
                container_seconds=0.0,
                exit_code=None,
            )
            continue

        try:
            exit_code = state.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            state.process.kill()
            exit_code = state.process.wait()
        state.result = completed_result(state, exit_code, time.monotonic())


def results_conflict(results: list[WorkerResult]) -> bool:
    proven = [result for result in results if result.status in WINNING_STATUSES]
    if len(proven) < 2:
        return False

    reference = proven[0]
    for result in proven[1:]:
        if result.status != reference.status:
            return True
        if (
            result.status == "OPTIMAL"
            and reference.objective_value is not None
            and result.objective_value is not None
            and not math.isclose(
                reference.objective_value,
                result.objective_value,
                rel_tol=1e-7,
                abs_tol=1e-9,
            )
        ):
            return True
    return False


def seed_text(seed: int | None) -> str:
    return "original" if seed is None else str(seed)


def write_reports(
    config: RunConfig,
    results: list[WorkerResult],
    winner: WorkerResult | None,
) -> None:
    workers_path = config.output_dir / "workers.tsv"
    with workers_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "id",
                "solver",
                "order_seed",
                "status",
                "objective",
                "solver_seconds",
                "container_seconds",
                "exit_code",
            ]
        )
        for result in results:
            writer.writerow(
                [
                    result.worker_id,
                    result.solver,
                    seed_text(result.order_seed),
                    result.status,
                    result.objective or "",
                    (
                        ""
                        if result.solver_seconds is None
                        else f"{result.solver_seconds:.6f}"
                    ),
                    f"{result.container_seconds:.6f}",
                    "" if result.exit_code is None else result.exit_code,
                ]
            )

    winner_path = config.output_dir / "winner.tsv"
    with winner_path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, delimiter="\t", lineterminator="\n")
        writer.writerow(
            [
                "id",
                "solver",
                "status",
                "objective",
                "solver_seconds",
                "container_seconds",
            ]
        )
        if winner is not None:
            writer.writerow(
                [
                    winner.worker_id,
                    winner.solver,
                    winner.status,
                    winner.objective or "",
                    (
                        ""
                        if winner.solver_seconds is None
                        else f"{winner.solver_seconds:.6f}"
                    ),
                    f"{winner.container_seconds:.6f}",
                ]
            )


def execute_workers(
    config: RunConfig, prepared: tuple[PreparedWorker, ...]
) -> int:
    states = make_worker_states(prepared)
    completions: queue.Queue[tuple[WorkerState, int, float]] = queue.Queue()
    waiters: list[threading.Thread] = []
    winner: WorkerResult | None = None
    interrupted = False

    try:
        launch_workers(config, states)
        waiters = start_waiters(states, completions)
        winner = monitor_workers(config, states, completions)
    except KeyboardInterrupt:
        interrupted = True
        finish_after_interrupt(config, states)
    finally:
        remove_containers(
            config,
            [state.container_name for state in states if state.process is not None],
        )
        for thread in waiters:
            thread.join(timeout=1)

    for state in states:
        if state.result is None:
            state.result = error_result(state, "Worker не вернул результат")
    results = [state.result for state in states if state.result is not None]

    conflict = False if interrupted else results_conflict(results)
    report_winner = None if interrupted or conflict else winner
    try:
        write_reports(config, results, report_winner)
    except OSError as error:
        print(f"Не удалось записать итоговые TSV: {error}", file=sys.stderr)
        return EXIT_INTERRUPTED if interrupted else EXIT_NO_RESULT

    if interrupted:
        print("Запуск прерван", file=sys.stderr)
        return EXIT_INTERRUPTED
    if conflict:
        print("Обнаружен конфликт результатов решателей", file=sys.stderr)
        return EXIT_CONFLICT
    if winner is None:
        print("Доказанный конечный результат не получен", file=sys.stderr)
        return EXIT_NO_RESULT

    solver_time = (
        "недоступно"
        if winner.solver_seconds is None
        else f"{winner.solver_seconds:.6f} с"
    )
    print(
        f"Победитель: {winner.worker_id}, статус: {winner.status}, "
        f"время решателя: {solver_time}, "
        f"время контейнера: {winner.container_seconds:.6f} с"
    )
    return EXIT_OK


def run(arguments: Sequence[str] | None = None) -> int:
    try:
        parsed = build_parser().parse_args(arguments)
        config = load_config(parsed)
        prepared = prepare_workers(config)
    except InputError as error:
        print(f"Ошибка входных данных: {error}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    return execute_workers(config, prepared)


if __name__ == "__main__":
    raise SystemExit(run())
