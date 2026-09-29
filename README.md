# Parallel solver runner

`parallel_runner.py` запускает job HiGHS, SCIP, FSCIP и lp_solve в уже
работающих Docker-контейнерах. Job назначается свободному контейнеру с тем
же `solver`; в одном контейнере одновременно выполняется не более одной job.

В обычном режиме runner выбирает первую job с доказанным статусом и
завершает остальные. Флаг `--wait-all` заставляет дождаться всех job.

Runner не создаёт, не останавливает и не удаляет пользовательские контейнеры.
Он удаляет только свои временные каталоги `/tmp/lp-parallel-runner-...`.

## Требования

- Python 3.10 или новее; дополнительные Python-пакеты не нужны;
- Docker CLI с доступом к daemon;
- заранее запущенные контейнеры с POSIX `sh` и нужным solver в `PATH`.

До первой job runner проверяет Docker, состояние контейнеров, бинарники и версии
решателей. Для FSCIP дополнительно проверяется доступное контейнеру число CPU.

## Образы и контейнеры

- `Dockerfile.highs` — HiGHS 1.15.1;
- `Dockerfile.scip` — SCIP Optimization Suite 10.1.0 с `scip` и `fscip`;
- `Dockerfile.lp_solve` — lp_solve 5.5.2.14.

```bash
docker build -f Dockerfile.highs -t lp-highs:1.15.1 .
docker build -f Dockerfile.scip -t lp-scip:10.1.0 .
docker build -f Dockerfile.lp_solve -t lp-lp-solve:5.5.2.14 .
```

SCIP и FSCIP используют один образ, но разные контейнеры. FSCIP должен получить не меньше
CPU, чем указано в `threads`:

```bash
docker run -d --name lp-highs-a --network none --entrypoint sleep lp-highs:1.15.1 infinity
docker run -d --name lp-scip-a --network none --entrypoint sleep lp-scip:10.1.0 infinity
docker run -d --name lp-fscip-4t --cpus 4 --network none --entrypoint sleep lp-scip:10.1.0 infinity
docker run -d --name lp-lp-solve-a --network none --entrypoint sleep lp-lp-solve:5.5.2.14 infinity
```

## Манифесты

Оба файла — TSV с обязательным заголовком и ровно указанными колонками.

`containers.tsv`:

```text
container_name	solver	threads
lp-highs-a	highs	1
lp-scip-a	scip	1
lp-fscip-4t	fscip	4
lp-lp-solve-a	lp_solve	1
```

`threads` должен быть равен `1` для HiGHS, SCIP и lp_solve. Для FSCIP требуется
значение не меньше `2`; runner передаёт его через `fscip -sth`.

`jobs.tsv`:

```text
job_id	solver	model	options	timeout_seconds
afiro-highs	highs	models/afiro.mps	options/highs.options	300
afiro-scip	scip	models/afiro.mps	options/scip.set	300
afiro-fscip	fscip	models/afiro.mps	options/fscip.prm	300
afiro-lp-solve	lp_solve	models/afiro.mps	options/lp_solve.ini	300
```

Пути `model` и `options` разрешаются относительно каталога `jobs.tsv`.

| `solver` | Модель | Настройки |
| --- | --- | --- |
| `highs` | `.lp`, `.mps` | `.options` |
| `scip` | `.lp`, `.mps` | `.set` |
| `fscip` | `.lp`, `.mps` | `.prm` |
| `lp_solve` | только `.mps` | `.ini` |

Для HiGHS runner принудительно задаёт `output_flag=true`, `log_to_console=true`,
`threads=1` и `parallel=off`. Для SCIP и внутренних SCIP-процессов FSCIP он задаёт
`lp/threads = 1`. Пользовательский `.prm` FSCIP содержит только параметры UG.

## Запуск

Без `--wait-all` runner останавливается после первой job со статусом `OPTIMAL`,
`INFEASIBLE` или `UNBOUNDED`. Активные job завершаются, ожидающие помечаются
`CANCELLED`. `ERROR`, `TIMEOUT`, `FEASIBLE` и `UNKNOWN` не останавливают остальные job.

```bash
python3 parallel_runner.py \
  --containers containers.tsv \
  --jobs jobs.tsv \
  --output results-first
```

Чтобы дождаться всех job:

```bash
python3 parallel_runner.py \
  --containers containers.tsv \
  --jobs jobs.tsv \
  --wait-all \
  --output results-all
```

Каталог `--output` должен быть новым: существующий файл, каталог или symlink не
перезаписывается.

## Результаты

```text
output/
  inputs/containers.tsv
  inputs/jobs.tsv
  containers/<container_name>/inspect.json
  containers/<container_name>/solver-version.txt
  jobs/<job_id>/model.lp|mps
  jobs/<job_id>/effective.options|set|prm|ini
  jobs/<job_id>/runner.set              # только FSCIP
  jobs/<job_id>/stdout.log
  jobs/<job_id>/stderr.log
  jobs.tsv
```

`jobs.tsv` содержит контейнер, solver, timeout, статус, objective, время solver и
контейнера, exit code, причину отмены и признак `selected`. Результаты перезаписываются
после каждого завершения job.

Коды завершения:

- `0` — нет runtime-ошибок;
- `2` — ошибка Docker или хотя бы одна job завершилась с `ERROR`;
- `64` — ошибка CLI, манифеста или входного файла;
- `130` — запуск прерван пользователем.

`TIMEOUT` и отсутствие доказанного результата сами по себе не считаются ошибкой runner.
