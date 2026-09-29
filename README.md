# Parallel solver runner

`parallel_runner.py` запускает задачи HiGHS, SCIP, FSCIP и lp_solve в пуле
уже работающих Docker-контейнеров. Каждая job попадает только в контейнер
с тем же `solver`; в одном контейнере одновременно выполняется не более
одной job.

Runner не создаёт, не останавливает и не удаляет пользовательские контейнеры.
Для каждого запуска он создаёт свой каталог `/tmp/lp-parallel-runner-...` внутри
выбранного контейнера и удаляет только этот каталог.

## Требования

- Python 3.10 или новее; дополнительные Python-пакеты не нужны;
- Docker CLI с доступом к daemon;
- заранее запущенные контейнеры с POSIX `sh` и нужным solver в `PATH`.

До первой job runner проверяет Docker, каждый контейнер, доступность solver и
его версию. Для FSCIP дополнительно проверяется доступное контейнеру число CPU.

## Образы и контейнеры

В репозитории есть три Dockerfile:

- `Dockerfile.highs` — HiGHS 1.15.1;
- `Dockerfile.scip` — SCIP Optimization Suite 10.1.0 с бинарниками `scip` и `fscip`;
- `Dockerfile.lp_solve` — lp_solve 5.5.2.14.

Сборка:

```bash
docker build -f Dockerfile.highs -t lp-highs:1.15.1 .
docker build -f Dockerfile.scip -t lp-scip:10.1.0 .
docker build -f Dockerfile.lp_solve -t lp-lp-solve:5.5.2.14 .
```

SCIP и FSCIP используют один образ, но разные контейнеры. Контейнер FSCIP должен
получить не меньше CPU, чем указано в `threads`:

```bash
docker run -d --name highs-a --network none --entrypoint sleep lp-highs:1.15.1 infinity
docker run -d --name scip-a --network none --entrypoint sleep lp-scip:10.1.0 infinity
docker run -d --name fscip-4t --cpus 4 --network none --entrypoint sleep lp-scip:10.1.0 infinity
docker run -d --name lp-solve-a --network none --entrypoint sleep lp-lp-solve:5.5.2.14 infinity
```

## Манифесты

Оба файла — TSV с обязательным заголовком и ровно указанными колонками.

`containers.tsv`:

```text
id	container_name	solver	threads
highs-a	highs-a	highs	1
scip-a	scip-a	scip	1
fscip-4t	fscip-4t	fscip	4
lp-solve-a	lp-solve-a	lp_solve	1
```

`threads` должен быть равен `1` для HiGHS, SCIP и lp_solve. Для FSCIP требуется
не меньше `2`; runner передаёт это значение через `fscip -sth`.

`jobs.tsv`:

```text
problem_id	job_id	solver	model	options	order	timeout_seconds
afiro	afiro-highs	highs	models/afiro.mps	options/highs.options	original	300
afiro	afiro-scip	scip	models/afiro.mps	options/scip.set	original	300
afiro	afiro-fscip	fscip	models/afiro.mps	options/fscip.prm	original	300
afiro	afiro-lp-solve	lp_solve	models/afiro.mps	options/lp_solve.ini	original	300
```

Пути `model` и `options` разрешаются относительно каталога `jobs.tsv`. `problem_id`
группирует альтернативные job одной задачи; `job_id` уникален во всём файле.

Поддерживаемые форматы:

| `solver` | Модель | Настройки |
| --- | --- | --- |
| `highs` | `.lp`, `.mps` | `.options` |
| `scip` | `.lp`, `.mps` | `.set` |
| `fscip` | `.lp`, `.mps` | `.prm` |
| `lp_solve` | только `.mps` | `.ini` |

Для HiGHS runner принудительно задаёт `output_flag=true`, `log_to_console=true`,
`threads=1` и `parallel=off`. Для SCIP и внутренних SCIP-процессов FSCIP он задаёт
`lp/threads = 1`. Пользовательский `.prm` FSCIP содержит только параметры UG.

Допустимые значения `order`:

- `original` — исходный файл копируется без изменений;
- `shuffle:<uint64>` — воспроизводимая перестановка ограничений;
- `low-arity-first` — стабильная сортировка по числу разных переменных.

Для LP переставляются именованные блоки из `Subject To`; для MPS — записи из `ROWS`.
Неоднозначные или безымянные ограничения отклоняются.

## Запуск

Флаг `--wait-all` выполняет все job и эквивалентен `--mode benchmark`:

```bash
python3 parallel_runner.py \
  --containers containers.tsv \
  --jobs jobs.tsv \
  --wait-all \
  --output results-benchmark
```

Без `--wait-all` runner работает как portfolio и выбирает первую доказанную job для каждого
`problem_id`:

```bash
python3 parallel_runner.py \
  --containers containers.tsv \
  --jobs jobs.tsv \
  --output results-portfolio
```

То же самое можно задать явно через `--mode benchmark|portfolio`. `--mode` и `--wait-all`
взаимоисключающие.

Победные статусы: `OPTIMAL`, `INFEASIBLE`, `UNBOUNDED`. После победы ожидающие job того же
`problem_id` помечаются `CANCELLED`, а активные останавливаются. Job других `problem_id`
продолжают работать.

Каталог `--output` должен быть новым: существующий файл, каталог или symlink не
перезаписывается.

## Результаты

Runner сохраняет копии манифестов, `docker inspect`, версию solver, подготовленные
модели и настройки, stdout/stderr и `result.json` каждой job. Общие файлы:

```text
output/
  inputs/containers.tsv
  inputs/jobs.tsv
  containers/<id>/inspect.json
  containers/<id>/solver-version.txt
  jobs/<job_id>/...
  jobs.tsv
  winners.tsv
  conflicts.tsv
  RESULTS.md
```

`jobs.tsv` содержит solver, статус, objective, время solver и контейнера, exit code и
причину отмены. В benchmark `conflicts.tsv` и `RESULTS.md` показывают несовпадения статусов
или objective внутри одного `problem_id`.

Коды завершения:

- `0` — нет runtime-ошибок и конфликтов;
- `2` — ошибка Docker/backend или хотя бы одна job завершилась с `ERROR`;
- `3` — конфликт результатов benchmark;
- `64` — ошибка CLI, манифеста или входного файла;
- `130` — запуск прерван пользователем.

`TIMEOUT`, отсутствие победителя portfolio и другие корректно зафиксированные
недоказанные статусы сами по себе не считаются ошибкой runner.
