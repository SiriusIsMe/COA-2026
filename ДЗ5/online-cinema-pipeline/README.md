# Online Cinema: Event Streaming + Analytics Pipeline

## Запуск

```bash
docker compose up -d --build
```

Больше ничего делать не нужно: контейнер `kafka-init` сам создаёт topic и регистрирует схему.

| Сервис | Адрес |
|---|---|
| Kafka (с хоста) | `localhost:9092`, `localhost:9093`, `localhost:9094` |
| Kafka (из контейнеров) | `kafka-1:29092,kafka-2:29092,kafka-3:29092` |
| Schema Registry | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |

## Часть 1. Kafka topic и схема событий

### Схема
`schemas/movie_event.avsc` — Avro-схема `cinema.events.MovieEvent`:

| Поле | Avro-тип |
|---|---|
| event_id | `string` + logicalType `uuid` |
| user_id, movie_id, session_id | `string` |
| event_type | `enum EventType` (VIEW_STARTED, VIEW_FINISHED, VIEW_PAUSED, VIEW_RESUMED, LIKED, SEARCHED) |
| timestamp | `long` + logicalType `timestamp-millis` (UTC) |
| device_type | `enum DeviceType` (MOBILE, DESKTOP, TV, TABLET) |
| progress_seconds | `["null", "int"]`, default `null` — у LIKED/SEARCHED прогресса нет |

### Schema Registry
- Subject: `movie-events-value` (TopicNameStrategy).
- Режим совместимости: `BACKWARD` — новая версия схемы должна читать данные старой.
- Схему регистрирует `kafka-init`; producer работает с `auto.register.schemas=false`,
  поэтому расхождение схемы в коде с зарегистрированной даёт ошибку, а не незаметную новую версию.
- Каждое сообщение — Confluent wire format: байт `0`, 4 байта ID схемы, данные Avro.

Проверка:
```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/movie-events-value/versions
curl http://localhost:8081/subjects/movie-events-value/versions/latest
```

### Topic
`movie-events`: 3 партиции, `replication.factor=3`, `min.insync.replicas=2`, retention 7 дней (подробнее — часть 8).
Автосоздание топиков выключено.

### Ключ партиционирования: `user_id`
Kafka выбирает партицию как `hash(key) % partitions`, а порядок гарантирует только внутри партиции.

1. **Порядок событий пользователя.** Цепочка `VIEW_STARTED → VIEW_PAUSED → VIEW_RESUMED →
   VIEW_FINISHED` в сессии не перемешивается. Это важно для времени просмотра, сессий и retention.
2. **Равномерная нагрузка.** Пользователей много, вклад каждого мал, поэтому данные
   распределяются ровно.
3. **Почему не `movie_id`:** популярные фильмы (премьеры) дают «горячую» партицию,
   а события одного пользователя разлетаются по разным партициям, и порядок теряется.
4. **Минус:** аномально активный пользователь создаёт небольшой перекос, но на фоне
   общего числа пользователей это несущественно.

### Проверка
```bash
docker compose logs kafka-init          # topic создан, схема: id / version
docker compose logs -f movie-service    # сервис публикует события
docker compose run --rm movie-service python check_consume.py 500
```
`check_consume.py` декодирует сообщения через Schema Registry, выводит ID схемы,
распределение по партициям и проверяет, что каждый `user_id` лежит ровно в одной партиции.

## Часть 2. Продюсер (Movie Service)

FastAPI-сервис `movie-service` (http://localhost:8000, Swagger — http://localhost:8000/docs).

### HTTP API
| Метод | Путь | Описание |
|---|---|---|
| POST | `/events` | принять событие (JSON), провалидировать, опубликовать |
| GET | `/health` | проверка живости |
| GET | `/generator` | статус генератора (running, published, failed) |
| POST | `/generator/start`, `/generator/stop` | включить / выключить генератор |

`POST /events`:
- `201` — `{"event_id", "partition", "offset"}`;
- `422` — ошибка валидации (тип, enum, пустые id, `progress_seconds` обязателен для `VIEW_*`
  и запрещён для `LIKED`/`SEARCHED`, время без часового пояса или из будущего, лишние поля);
- `400` — событие не прошло проверку по Avro-схеме;
- `503` — Kafka недоступна после всех повторных попыток.

`event_id` и `timestamp` можно не передавать: сервис проставит их сам.

```bash
curl -X POST http://localhost:8000/events -H 'Content-Type: application/json' -d '{
  "user_id": "user_00001", "movie_id": "movie_0001", "event_type": "VIEW_STARTED",
  "device_type": "TV", "session_id": "s-1", "progress_seconds": 0}'
```

### Генератор
Включается при старте (`GENERATOR_ENABLED=true`, `EVENTS_PER_SECOND=5`). Одновременно идут
`GENERATOR_CONCURRENCY=20` сессий разных пользователей, их события перемежаются. Сессия:
`[SEARCHED] → VIEW_STARTED → (VIEW_PAUSED → VIEW_RESUMED)* → VIEW_FINISHED | VIEW_PAUSED → [LIKED]`.
Гарантии (покрыты тестами): `VIEW_STARTED` раньше `VIEW_FINISHED`, `progress_seconds` не убывает
и растёт между паузами, время в сессии строго возрастает, у пользователя не бывает двух сессий сразу.

### Kafka-продюсер (`publisher.py`)
- **Валидация по схеме:** Pydantic-модель (бизнес-правила) + `fastavro.validate` по той же
  `.avsc`; сериализация `AvroSerializer` по схеме из Schema Registry.
- **Ключ:** `user_id`.
- **Подтверждение:** `acks=all`, `enable.idempotence=true`; HTTP-ответ отправляется только после
  подтверждения брокера (возвращаются partition и offset).
- **Retry:** внутренние ретраи librdkafka (`retry.backoff.ms`/`retry.backoff.max.ms`) плюс
  ретраи приложения: до `PUBLISH_MAX_ATTEMPTS=5` попыток, задержка — exponential backoff с jitter
  `uniform(0, min(8, 0.5·2^(n-1)))`. Ретраятся только временные ошибки (retriable-ошибки Kafka,
  переполненная очередь, таймаут, недоступный Schema Registry); остальные сразу дают ошибку.
  Повтор после таймаута может дать дубль — его уберёт дедупликация по `event_id` в ClickHouse.
- **Логирование:** `published event_id=… event_type=… timestamp=… partition=… offset=… attempt=…`,
  а также предупреждения о повторах и ошибка при исчерпании попыток.

### Тесты
```bash
docker compose run --rm --no-deps movie-service pytest -v
```
Kafka для тестов не нужна: producer и publisher подменяются фейками.

## Часть 3. ClickHouse: приём и хранение событий

Миграции лежат в `clickhouse/migrations/*.sql` и применяются автоматически: при `docker compose up`
одноразовый контейнер `clickhouse-migrations` выполняет новые файлы по порядку и записывает их
в `cinema.schema_migrations`.

```
movie-events (Kafka) ──► movie_events_queue (Kafka Engine, AvroConfluent)
                              │ movie_events_mv          (без ошибок)
                              ├──────────────► movie_events        (ReplacingMergeTree)
                              │ movie_events_errors_mv   (_error ≠ '')
                              └──────────────► movie_events_errors (dead letter, TTL 30 дней)
```

- **Kafka Engine** (`movie_events_queue`): формат `AvroConfluent`. ClickHouse читает ID схемы
  из заголовка сообщения и скачивает схему из Schema Registry. Consumer-группа
  `clickhouse-movie-events`, новая группа читает топик с начала (`clickhouse/config.d/kafka.xml`).
  `kafka_handle_error_mode='stream'`: битое сообщение не останавливает поток, а попадает в `movie_events_errors`.
- **Хранилище** (`movie_events`) — `ReplacingMergeTree(ingested_at)`:
  - продюсер может прислать дубль после повторной попытки; ключ сортировки содержит `event_id`,
    поэтому дубли схлопываются при слияниях (для точных подсчётов — `FINAL` / `uniqExact(event_id)`);
  - `PARTITION BY toYYYYMM(event_date)` — запросы метрик идут по диапазону дат, ненужные месяцы
    отсекаются, старые легко выгружать в S3 и удалять;
  - `ORDER BY (event_date, event_type, user_id, timestamp, event_id)` — DAU и retention фильтруют
    по дате и считают `uniq(user_id)`, конверсии фильтруют по `event_type` внутри дня, а события
    пользователя за день лежат подряд и по времени;
  - служебные колонки: `event_date`, `kafka_partition`, `kafka_offset`, `ingested_at`.
- **Перекладка** — Materialized View `movie_events_mv`, срабатывает на каждую прочитанную пачку.
- **Соответствие схеме п.1**: Avro → ClickHouse: `uuid` → `UUID`, `string` → `String`,
  `enum` → `Enum8` с теми же символами, `timestamp-millis` → `DateTime64(3, 'UTC')`,
  `["null","int"]` → `Nullable(Int32)`. Это проверяет тест `tests/test_clickhouse_schema.py`.

Проверка:
```bash
docker compose logs clickhouse-migrations
docker compose exec movie-service python e2e_clickhouse.py     # API -> Kafka -> ClickHouse
docker compose exec clickhouse clickhouse-client --user cinema --password cinema \
  -q "SELECT event_type, count() FROM cinema.movie_events GROUP BY event_type"
```
Веб-консоль для SQL: http://localhost:8123/play (логин и пароль `cinema`).

## Часть 4. Интеграционный тест pipeline

```bash
docker compose run --rm integration-tests
```
Одна команда: compose сам поднимает Kafka, Schema Registry, ClickHouse, применяет миграции,
дожидается готовности `movie-service` и запускает `pytest -m integration`. Сервис в профиле `test`,
поэтому обычный `docker compose up` его не запускает.

`movie-service/tests/integration/test_pipeline.py`:

| Тест | Что проверяет |
|---|---|
| `test_event_from_producer_lands_in_clickhouse` | событие из `EventPublisher` появилось в `movie_events`; все 8 полей совпадают (время с точностью до мс), `event_date`, а `kafka_partition`/`kafka_offset` равны подтверждённым брокером |
| `test_nullable_progress_for_liked` | `progress_seconds = NULL` проходит весь путь |
| `test_event_via_http_api` | путь `POST /events` → Kafka → ClickHouse |
| `test_user_events_keep_order_in_one_partition` | `STARTED → PAUSED → RESUMED → FINISHED` одного пользователя: одна партиция, порядок и прогресс сохранены |
| `test_broken_message_goes_to_errors_table` | не-Avro сообщение попадает в `movie_events_errors`, а следующее корректное событие доходит |

**Повторный запуск и изоляция.** Каждый прогон получает уникальный `run_id`, а все тестовые события
имеют `user_id`/`session_id` с префиксом `it_<run_id>`. Тесты ищут только свои события по `event_id`
или `session_id`, поэтому генератор, работающий параллельно, им не мешает. Фикстура удаляет
тестовые строки после прогона и остатки прерванных прогонов перед стартом
(`ALTER TABLE … DELETE`, `mutations_sync=2`), так что метрики не засоряются.

Kafka Engine сбрасывает данные пачками раз в несколько секунд, поэтому тесты опрашивают
ClickHouse до 60 с (`IT_WAIT_TIMEOUT`).

Без поднятого стека интеграционные тесты пропускаются, и обычный
`docker compose run --rm --no-deps movie-service pytest` запускает только юнит-тесты.

## Часть 5. Сервис агрегации (ClickHouse → метрики → PostgreSQL)

```
       по расписанию (AGG_INTERVAL_SECONDS / AGG_CRON) или POST /recompute?date=
                                   │
ClickHouse                         ▼                                  PostgreSQL
cinema.movie_events ──INSERT…SELECT──► cinema.daily_metrics ──SELECT──► aggregation ──UPSERT──► daily_metrics
     (raw)          (aggregation-service)   (материализованные агрегаты)   service           aggregation_runs
```

Отдельный контейнер `aggregation-service` (http://localhost:8001/docs). Он не связан
с продюсером и Kafka: читает только ClickHouse и пишет только в PostgreSQL.

### Цикл вычислений за дату D (`app/pipeline.py`)
1. SQL из `aggregation-service/sql/*.sql` выполняются как `INSERT … SELECT`: ClickHouse считает
   агрегаты по `cinema.movie_events FINAL` (без дублей) и материализует их в `cinema.daily_metrics`
   с меткой `computed_at` этого цикла.
2. Результаты цикла читаются по точному `computed_at`.
3. Одной транзакцией выполняется UPSERT в PostgreSQL и удаляются строки прошлых циклов за D,
   которых нет в новом результате (например, фильм выпал из топа).
4. В журнал `aggregation_runs` записываются статус, число событий, число строк и длительность.
5. В лог пишется `Цикл начат: дата=…` и `Цикл завершён: дата=…, обработано N событий, записано M метрик
   в PostgreSQL за X сек`. Плановый проход по всем датам завершается итоговой строкой.

### Метрики (за день)
| metric_name | Что считаем | Функции ClickHouse |
|---|---|---|
| `dau` | уникальные пользователи за день | `uniqExact(user_id)` |
| `avg_watch_time_seconds` | средний `progress_seconds` у `VIEW_FINISHED` | `avgIf` |
| `views_started`, `views_finished` | сессии с началом / завершением просмотра | `uniqExactIf(session_id, …)` |
| `view_conversion` | `views_finished / views_started` | |
| `top_movie_views` (`dimension = movie_id`) | топ-10 фильмов по числу `VIEW_STARTED` | `count()` + оконная `row_number() OVER` |
| `retention_cohort_size` | пользователи с первым `VIEW_STARTED` за всю историю в день D | `minIf`, `groupUniqArray` |
| `retention_d1`, `retention_d7` | доля когорты с любой активностью ровно в D+1 / D+7 | `countIf(has(…))` |
| `events_total` | событий за день (число «обработанных записей») | `count()` |

- Retention D1/D7 пишется, только когда день D+1/D+7 полностью завершён, иначе значение было бы
  заниженным. Каждый плановый цикл пересчитывает сегодня и `AGG_LOOKBACK_DAYS=8` прошедших дней,
  поэтому D7 дозаполняется автоматически.
- Пустой знаменатель даёт 0, а не NaN. Данные интеграционных тестов (`it_*`) исключаются.
- Минимальная гранулярность — день. Неделю и месяц можно получить агрегацией `v_daily_kpi`.

### Хранение
**ClickHouse** `cinema.daily_metrics` — `ReplacingMergeTree(computed_at)`,
`ORDER BY (metric_date, metric_name, dimension)`, `PARTITION BY toYYYYMM(metric_date)`.
Длинный формат: новая метрика не требует миграции.

**PostgreSQL** (`postgres/migrations/`, применяются сервисом при старте):
- `daily_metrics(metric_date, metric_name, dimension, value, computed_at, updated_at)`,
  `PRIMARY KEY (metric_date, metric_name, dimension)`;
- запись идёт через `INSERT … ON CONFLICT DO UPDATE … WHERE daily_metrics.computed_at <= EXCLUDED.computed_at`:
  повторный пересчёт обновляет строки, а не дублирует их, и старый результат не затирает новый;
- `aggregation_runs` — журнал циклов (trigger `schedule` / `manual` / `cli`, status, events, rows, duration, error);
- представления `v_daily_kpi` (день → KPI в колонках) и `v_top_movies` (топ с рангом).

**Ошибки записи:** до `PG_RETRY_ATTEMPTS=5` попыток с exponential backoff и jitter на временных
ошибках (`OperationalError`, `InterfaceError`, `SerializationFailure`, `DeadlockDetected`). Каждая
попытка логируется. Если попытки исчерпаны, цикл помечается `failed` в логе и в `aggregation_runs`,
а сервис продолжает работать и повторит пересчёт на следующем запуске.

### Настройки (`docker-compose.yml`)
| Переменная | По умолчанию | |
|---|---|---|
| `AGG_INTERVAL_SECONDS` | 60 | период планового пересчёта |
| `AGG_CRON` | — | cron-расписание вместо интервала, например `*/5 * * * *` |
| `AGG_LOOKBACK_DAYS` | 8 | сколько прошедших дней пересчитывать кроме сегодня |
| `AGG_RUN_ON_START` | true | первый цикл сразу после старта |
| `TOP_MOVIES_LIMIT` | 10 | размер топа |

### HTTP API
| Метод | Путь | |
|---|---|---|
| POST | `/recompute?date=YYYY-MM-DD` | пересчитать дату сейчас; в ответе статус, events_processed, rows_written, длительность и метрики |
| GET | `/metrics/{date}` | готовые метрики из PostgreSQL |
| GET | `/runs?limit=20` | журнал циклов |
| GET | `/schedule` | расписание, время следующего запуска |
| GET | `/health` | |

### Данные для демонстрации
Живой генератор пишет события только за сегодня. Чтобы были метрики по дням и retention,
сгенерируйте историю за 14 дней (с тем же `seed` повтор безопасен):
```bash
curl -X POST "http://localhost:8000/generator/backfill?days=14"
curl -X POST "http://localhost:8001/recompute?date=$(date -v-7d +%F)"   # macOS; Linux: date -d '-7 day' +%F
docker compose exec postgres psql -U metrics -c "SELECT * FROM v_daily_kpi ORDER BY metric_date"
```
Ручной пересчёт диапазона: `docker compose exec aggregation-service python -m app.cli compute --days 15 --quiet`.

### Тесты
```bash
docker compose run --rm --no-deps aggregation-service pytest     # юнит-тесты
docker compose run --rm aggregation-integration-tests             # сквозные
```
Сквозной тест записывает в ClickHouse контрольный набор событий на случайную дату в 2000-х, вызывает
`POST /recompute` и сверяет результат с посчитанными вручную значениями: DAU = 3, среднее время = 4500 с,
конверсия = 2/3, топ (m1 = 2, m2 = 1), retention D1 = 1/3, D7 = 2/3. Значения проверяются и в
`cinema.daily_metrics`, и в PostgreSQL. Ещё один тест проверяет идемпотентность: при повторном
пересчёте число строк не меняется, `updated_at` растёт, а новые данные обновляют значение.
После тестов данные удаляются.

## Часть 6. Grafana

http://localhost:3000. Дашборд **Online Cinema — аналитика** открывается как домашняя страница
без логина; для редактирования — `admin` / `admin`.

Datasource и дашборд создаются автоматически при старте (provisioning), руками ничего настраивать не нужно:
- `grafana/provisioning/datasources/clickhouse.yaml` — плагин `grafana-clickhouse-datasource`,
  подключение к `clickhouse:8123`, база `cinema`;
- `grafana/provisioning/dashboards/dashboards.yaml` + `grafana/dashboards/cinema.json` — дашборд.

**Дашборд строится только по агрегатам из п.5.** Все панели читают представление
`cinema.v_daily_metrics_latest` (миграция 003) — это последний расчёт сервиса агрегации за каждый день
поверх `cinema.daily_metrics`. Raw-таблица `movie_events` в запросах дашборда не используется.
Для панелей сервис агрегации считает два дополнительных агрегата:

| metric_name | dimension | SQL |
|---|---|---|
| `cohort_retention` | `day_0` … `day_7` | `aggregation-service/sql/04_cohort_retention.sql` |
| `device_users` | MOBILE / DESKTOP / TV / TABLET | `aggregation-service/sql/05_devices.sql` |

### Панели
1. **Retention Cohort Heatmap** (таблица с цветовой заливкой, от красного к зелёному).
   - Строки — когорты по дате первого просмотра, столбцы — Day 0 … Day 7, в ячейке — % когорты,
     вернувшийся в этот день жизни. Пустая ячейка значит, что день ещё не завершился.
   - Как считается (`04_cohort_retention.sql`): для каждого пользователя
     `minIf(event_date, event_type = 'VIEW_STARTED')` даёт дату первого просмотра, а `groupUniqArray(event_date)` —
     дни активности. Пользователи когорты D размножаются `ARRAY JOIN range(max_day + 1) AS n`,
     и `countIf(has(active_days, D + n)) / count()` даёт долю вернувшихся в день n.
   - В Grafana строки «дата × день» разворачиваются в столбцы через `maxIfOrNull(value, dimension = 'day_N')`.
2. **Топ-10 фильмов за период** — сумма дневных топов (`top_movie_views`) за выбранный интервал.
3. **Пользователи по устройствам** — доли устройств (`device_users`).

Период выбирается в Grafana (по умолчанию 14 дней). Дашборд обновляется раз в минуту, агрегаты —
по расписанию сервиса агрегации.

## Часть 7. Экспорт агрегатов в S3 (SeaweedFS)

```
PostgreSQL daily_metrics ──► aggregation-service (Exporter) ──► s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json
                                     │
                                     └──► PostgreSQL s3_exports (журнал: success / failed, хеш, попытки)
```

- **S3-совместимое хранилище — SeaweedFS.** MinIO с осени 2025 года не публикует Docker-образы
  (`minio/minio` удалён с Docker Hub), поэтому используется аналог.
  - S3 API: `localhost:8333`, ключи `cinema` / `cinema-secret` (`seaweedfs/s3.json`).
  - Просмотр файлов в браузере: http://localhost:8888/buckets/movie-analytics/daily/.
  - Бакет `movie-analytics` сервис создаёт сам при первой выгрузке. В коде используется обычный
    S3-клиент (библиотека `minio`), поэтому хранилище можно заменить на AWS S3 или MinIO через `S3_ENDPOINT`.
- **Экспортёр** (`aggregation-service/app/exporter.py`) — отдельное задание планировщика внутри сервиса
  агрегации. Он читает агрегаты даты из PostgreSQL и пишет один JSON-файл:
  `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`.
- **Формат JSON** (в файле все метрики дня):
  ```json
  {
    "format_version": 1, "date": "2026-11-24",
    "computed_at": "…", "exported_at": "…", "source": "postgresql:daily_metrics",
    "summary":    {"dau": 234, "view_conversion": 0.70, "avg_watch_time_seconds": 6943.5,
                   "retention_d1": 0.40, "retention_d7": …, "retention_cohort_size": 40, …},
    "top_movies": [{"rank": 1, "movie_id": "movie_0057", "views": 6}, …],
    "breakdowns": {"cohort_retention": {"day_0": 1.0, "day_1": 0.4, …}, "device_users": {"TV": 61, …}},
    "metrics":    [{"metric_name": "dau", "dimension": "", "value": 234, "computed_at": "…"}, …]
  }
  ```
  `metrics` — все строки дня с временем вычисления; `summary` / `top_movies` / `breakdowns` — те же данные
  в удобном для чтения виде.
- **Автоматический запуск.** Каждые `EXPORT_INTERVAL_SECONDS=300` (или по `EXPORT_CRON`, например
  `15 0 * * *`) задание проверяет завершённые дни за последние `EXPORT_LOOKBACK_DAYS=10`, не включая сегодня.
  Дата выгружается, если:
  - её ещё нет в S3 — так вчерашний день появляется в S3 вскоре после полуночи;
  - прошлая попытка выгрузки упала;
  - метрики изменились с прошлой выгрузки — сравнивается `content_hash` значений, поэтому досчитанный
    через неделю retention D7 попадёт в файл.

  Неизменённые даты пропускаются.
- **Ручной запуск:** `POST http://localhost:8001/export?date=YYYY-MM-DD` (всегда выгружает) или
  `docker compose exec aggregation-service python -m app.cli export YYYY-MM-DD`.
  Журнал выгрузок: `GET /exports`.
- **Без дубликатов.** Ключ объекта однозначно определяется датой, поэтому повторный экспорт перезаписывает
  тот же объект.
- **Ошибки.** Если PostgreSQL или S3 недоступен:
  - делается до 3 быстрых повторов с exponential backoff;
  - затем ошибка логируется (`Экспорт не удался: … — повтор при следующем запуске`), а в `s3_exports`
    пишется `status=failed`, `attempts+1` (если PostgreSQL доступен);
  - сервис продолжает работать, и следующий плановый запуск выгрузит эту дату.

Проверка:
```bash
curl -s -X POST "http://localhost:8001/export?date=$(date -v-1d +%F)"    # macOS; Linux: date -d yesterday +%F
curl -s "http://localhost:8001/exports?limit=5"
docker compose stop seaweedfs && curl -s -X POST "http://localhost:8001/export?date=$(date -v-1d +%F)"  # 503, ошибка в логе
docker compose start seaweedfs   # следующий плановый запуск (≤ 5 мин) повторит выгрузку
```
Тесты: юнит-тесты (`tests/test_exporter.py`: перезапись, пропуск неизменённых дат, падение S3 или PostgreSQL
и повтор при следующем запуске) и сквозной тест `tests/integration/test_export.py`
(recompute → export → чтение файла из S3 → изменение данных → повторный экспорт: объект по-прежнему один,
значения обновились).

## Часть 8. Отказоустойчивая инфраструктура Kafka

**Кластер из 3 узлов** `kafka-1`, `kafka-2`, `kafka-3` (KRaft, без ZooKeeper). Каждый узел — и broker,
и controller. Почему три, а не два: контроллеры выбирают лидера большинством голосов. Из двух узлов
при падении одного большинства нет, и кластер перестаёт работать. Из трёх он переживает потерю любого узла.

| Настройка | Значение | Зачем |
|---|---|---|
| `replication.factor` топика `movie-events` | 3 | копия каждой партиции на каждом брокере |
| `min.insync.replicas` топика | 2 | вместе с `acks=all` у продюсера запись подтверждается, только когда её приняли минимум 2 реплики |
| `default.replication.factor`, `offsets.topic.replication.factor`, `transaction.state.log.replication.factor` | 3 | служебные топики (offsets consumer-групп, в т.ч. ClickHouse) тоже реплицированы |
| `_schemas` (Schema Registry) | RF 3 | схемы не теряются при падении брокера |
| `unclean.leader.election.enable` | false | лидером не станет отставшая реплика — без потери подтверждённых данных |

Итог: при падении одного брокера запись и чтение продолжаются без потери подтверждённых данных.
При падении двух продюсер получает ошибку (ISR = 1 < 2), повторяет попытки с backoff и отвечает 503:
данные не теряются молча.

- Клиенты знают все три брокера: `KAFKA_BOOTSTRAP_SERVERS=kafka-1:29092,kafka-2:29092,kafka-3:29092`
  у продюсера, Schema Registry, Kafka UI. В ClickHouse Kafka-таблица пересоздаётся
  миграцией `004_kafka_cluster.sql` со всеми брокерами.
- `kafka-init` ждёт, пока в кластере зарегистрируются 3 брокера, и создаёт топик с RF=3 / min ISR=2.
  В лог выводятся лидер, реплики и ISR каждой партиции.
- **Schema Registry** поднят (`http://localhost:8081`), хранит схемы в реплицированном `_schemas`.

### Health checks
| Сервис | Проверка |
|---|---|
| kafka-1/2/3 | `kafka-broker-api-versions` к своему брокеру |
| schema-registry | HTTP `GET /subjects` → 200 (через bash `/dev/tcp`: в образе нет curl) |
| clickhouse | `clickhouse-client -q 'SELECT 1'` |
| postgres | `pg_isready` |
| movie-service, aggregation-service | HTTP `/health` |
| grafana | HTTP `/api/health` |
| kafka-ui | HTTP `/actuator/health` |
| seaweedfs | HTTP master `/cluster/status` + открыт S3-порт 8333 |

Одноразовые задания (`kafka-init`, `clickhouse-migrations`, `*-integration-tests`) работают как
`service_completed_successfully`-зависимости: они не висят в фоне, и health check им не нужен.
Порядок старта задан через `depends_on: condition: service_healthy`. Статус: `docker compose ps`.

### Проверка отказоустойчивости
```bash
docker compose exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic movie-events
# ReplicationFactor: 3, Configs: min.insync.replicas=2, у каждой партиции Isr из 3 брокеров

docker compose stop kafka-2                     # «роняем» брокер
docker compose exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --describe --topic movie-events
# лидеры переехали на 1 и 3, Isr из 2 брокеров
curl -s -X POST http://localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"user_id":"u1","movie_id":"m1","event_type":"LIKED","device_type":"TV","session_id":"s1"}'
# 201 — запись продолжается
docker compose run --rm --no-deps integration-tests 2>&1 | tail -1   # --no-deps: не поднимать kafka-2 обратно
docker compose start kafka-2                    # брокер догоняет реплики и возвращается в ISR
```

## Остановка
```bash
docker compose down        # остановить
docker compose down -v     # остановить и удалить данные
```
