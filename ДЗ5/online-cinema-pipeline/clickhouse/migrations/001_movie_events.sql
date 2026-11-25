-- =====================================================================
-- 001: приём событий из Kafka и их постоянное хранение
--   movie_events_queue  (Kafka Engine)  --MV-->  movie_events (ReplacingMergeTree)
--                                       --MV-->  movie_events_errors (битые сообщения)
-- Колонки и типы совпадают со схемой schemas/movie_event.avsc
-- (проверяется тестом movie-service/tests/test_clickhouse_schema.py).
-- =====================================================================

CREATE DATABASE IF NOT EXISTS cinema;

-- ---------------------------------------------------------------------
-- 1. Kafka Engine: «окно» в топик. Сама данные не хранит — при SELECT
--    читает следующую порцию сообщений и сдвигает offset consumer-группы.
--    Формат AvroConfluent: ClickHouse берёт ID схемы из заголовка
--    сообщения и скачивает схему из Schema Registry.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cinema.movie_events_queue
(
    event_id         UUID,                                  -- string + logicalType uuid
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),                  -- long + timestamp-millis
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32)                        -- ["null", "int"]
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,            -- до 3 (= числу партиций) при росте нагрузки
    kafka_max_block_size = 10000,
    kafka_handle_error_mode = 'stream'; -- битые сообщения не останавливают поток: _error / _raw_message

-- ---------------------------------------------------------------------
-- 2. Постоянное хранилище.
--   ReplacingMergeTree: продюсер ретраит отправку, возможны дубли —
--     строки с одинаковым ключом сортировки (он включает event_id) схлопываются
--     при слияниях; для точных подсчётов — FINAL или uniqExact(event_id).
--   PARTITION BY месяц: запросы метрик идут по диапазону дат, старые месяцы
--     отсекаются целиком и легко удаляются/выгружаются в S3.
--   ORDER BY (event_date, event_type, user_id, timestamp, event_id):
--     DAU / retention  — фильтр по дате, uniq(user_id);
--     конверсии        — фильтр по event_type внутри дня;
--     события пользователя за день идут подряд и по времени (сессии, воронки).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cinema.movie_events
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32),

    -- служебные колонки
    event_date       Date,                                   -- toDate(timestamp)
    kafka_partition  UInt16,
    kafka_offset     UInt64,
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id, timestamp, event_id);

-- ---------------------------------------------------------------------
-- 3. Автоматическая перекладка Kafka -> MergeTree.
--    MV срабатывает на каждую прочитанную пачку и вставляет её в movie_events.
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_mv
TO cinema.movie_events
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds,
    toDate(timestamp) AS event_date,
    _partition        AS kafka_partition,
    _offset           AS kafka_offset,
    now64(3)          AS ingested_at
FROM cinema.movie_events_queue
WHERE length(_error) = 0;

-- ---------------------------------------------------------------------
-- 4. Dead letter: сообщения, которые не удалось разобрать.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cinema.movie_events_errors
(
    received_at     DateTime DEFAULT now(),
    kafka_topic     String,
    kafka_partition UInt16,
    kafka_offset    UInt64,
    raw_message     String,
    error           String
)
ENGINE = MergeTree
ORDER BY received_at
TTL received_at + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.movie_events_errors_mv
TO cinema.movie_events_errors
AS
SELECT
    now()          AS received_at,
    _topic         AS kafka_topic,
    _partition     AS kafka_partition,
    _offset        AS kafka_offset,
    _raw_message   AS raw_message,
    _error         AS error
FROM cinema.movie_events_queue
WHERE length(_error) > 0;
