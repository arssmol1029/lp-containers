# Parallel HiGHS runner

`parallel_runner.py` выполняет независимые задачи HiGHS в пуле **уже
запущенных** Docker-контейнеров. Контейнеры взаимозаменяемы: runner назначает
каждый job ровно одному свободному контейнеру и никогда не запускает в одном
контейнере более одного своего job одновременно.

Runner не создаёт, не останавливает и не удаляет пользовательские контейнеры.
Для каждого запуска он копирует модель и options в собственный случайно
именованный каталог `/tmp/lp-parallel-runner-...` внутри выбранного контейнера,
а после завершения удаляет только этот каталог.

## Требования

- Python 3.10 или новее, дополнительных Python-пакетов нет;
- Docker CLI с доступом к daemon;
- один или несколько уже запущенных контейнеров с POSIX `sh` и `highs` в
  `PATH`.

Сейчас поддерживается только HiGHS. Проверка всех контейнеров выполняется до
старта первого job. Остановленный контейнер, отсутствие `sh`/`highs`, повторная
ссылка на один runtime-контейнер или неизвестный solver приводят к понятной
ошибке.

## Подготовка контейнеров

Образ из репозитория можно собрать так:

```bash
docker build -t lp-highs .
```

Контейнер должен оставаться запущенным, поэтому для данного образа нужно
переопределить entrypoint:

```bash
docker run -d --name highs-a --network none --entrypoint sleep lp-highs infinity
docker run -d --name highs-b --network none --entrypoint sleep lp-highs infinity
```

Эти команды — только пример ручной подготовки. Сам runner `docker run`,
`docker stop` и `docker rm` не вызывает.

## Манифесты

Оба файла — TSV с обязательным заголовком и ровно указанными колонками.

`containers.tsv`:

```text
id	container_name
worker-a	highs-a
worker-b	highs-b
```

- `id` — уникальный идентификатор worker в отчётах;
- `container_name` — уникальное имя уже запущенного контейнера.

Идентификаторы `id`, `problem_id` и `job_id` должны начинаться с буквы или
цифры и содержать только ASCII-буквы, цифры, `.`, `_`, `-` (до 128 символов).

`jobs.tsv`:

```text
problem_id	job_id	model	options	order	timeout_seconds
p01	p01-default	models/p01.lp	options/default.options	original	300
p01	p01-shuffled	models/p01.lp	options/alt.options	shuffle:42	300
p02	p02-low-arity	models/p02.mps	options/default.options	low-arity-first	120.5
```

Пути `model` и `options` разрешаются относительно каталога `jobs.tsv`.
`problem_id` группирует альтернативные job одной математической задачи;
`job_id` уникален во всём манифесте. `timeout_seconds` — конечное положительное
число.

Допустимые значения `order`:

- `original` — исходный файл копируется побайтно;
- `shuffle:<uint64>` — воспроизводимая перестановка ограничений;
- `low-arity-first` — стабильная сортировка по числу разных переменных в
  ограничении.

Для LP переставляются целиком именованные, в том числе многострочные, блоки из
`Subject To`. Для MPS переставляются полные именованные записи ограничений в
`ROWS`, а арность вычисляется по `COLUMNS`. Коэффициенты, правая часть, bounds и
остальные секции не меняются. Неоднозначные или безымянные ограничения
отклоняются вместо потенциального изменения математики.

В effective options runner принудительно задаёт `output_flag=true`,
`log_to_console=true`, `threads=1` и `parallel=off`, заменяя одноимённые строки
из исходного options-файла.

## Запуск

По умолчанию runner работает как portfolio. Флаг `--wait-all` возвращает
поведение полного ожидания и выполняет все job независимо от результатов
соседей:

```bash
python3 parallel_runner.py \
  --containers containers.tsv \
  --jobs jobs.tsv \
  --wait-all \
  --output results-benchmark
```

Без `--wait-all` portfolio выбирает первого доказанного победителя отдельно для
каждого `problem_id`:

```bash
python3 parallel_runner.py \
  --containers containers.tsv \
  --jobs jobs.tsv \
  --output results-portfolio
```

Для явного выбора также сохранён `--mode benchmark|portfolio`. `--mode` и
`--wait-all` взаимоисключающие; `--wait-all` эквивалентен
`--mode benchmark`.

Победные статусы: `OPTIMAL`, `INFEASIBLE`, `UNBOUNDED`. Статусы
`UNBOUNDED_OR_INFEASIBLE`, `FEASIBLE`, `TIMEOUT`, `UNKNOWN` и `ERROR` не
останавливают sibling-job. После победы ожидающие sibling-job помечаются
`CANCELLED`, а активным процессам отправляются `TERM`, затем после grace period
`KILL`; job других `problem_id` продолжаются. Та же адресная отмена используется
при timeout и `Ctrl-C`.

Output должен быть новым каталогом: существующий файл, каталог или symlink
никогда не перезаписывается.

## Результаты

Runner сохраняет:

```text
output/
  inputs/containers.tsv
  inputs/jobs.tsv
  containers/<id>/inspect.json
  containers/<id>/highs-version.txt
  jobs/<job_id>/model.lp|mps
  jobs/<job_id>/effective.options
  jobs/<job_id>/stdout.log
  jobs/<job_id>/stderr.log
  jobs/<job_id>/result.json
  jobs.tsv
  winners.tsv
  conflicts.tsv
  RESULTS.md
```

`jobs.tsv` содержит статус, objective, solver/container time, exit code,
контейнер и причину отмены каждого job. `winners.tsv` заполняется в portfolio.
В benchmark `conflicts.tsv` и `RESULTS.md` показывают несовместимые доказанные
статусы или objective внутри одного `problem_id`.

Коды завершения:

- `0` — запуск завершён без runtime-ошибок и конфликтов;
- `2` — Docker/backend или хотя бы один job завершился с `ERROR`;
- `3` — конфликт результатов benchmark;
- `64` — ошибка CLI, манифеста или входного файла;
- `130` — прерывание пользователем, после адресной отмены и записи отчётов.

`TIMEOUT`, отсутствие доказанного победителя portfolio и прочие корректно
зафиксированные недоказанные статусы сами по себе не являются ошибкой runner.

## Тесты

Все автоматические тесты работают через fake backend и не требуют Docker:

```bash
python3 -m unittest discover -s tests -v
python3 -m py_compile parallel_runner.py tests/*.py
```

Они покрывают manifests, scheduler, ограничение параллелизма, benchmark,
portfolio по `problem_id`, timeout, interrupt, отмену queued/running job,
конфликты, LP/MPS permutation и отказ от перезаписи output.
