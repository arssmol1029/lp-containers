#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import os
import random
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


EXIT_INPUT_ERROR = 64
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


def run(arguments: Sequence[str] | None = None) -> int:
    try:
        parsed = build_parser().parse_args(arguments)
        config = load_config(parsed)
        prepared = prepare_workers(config)
    except InputError as error:
        print(f"Ошибка входных данных: {error}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    print(f"Подготовлено worker'ов: {len(prepared)}")
    print(f"Выходной каталог: {config.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
