-- =====================================================================
-- 004: Kafka стала кластером из 3 брокеров — пересоздаём Kafka Engine таблицу
-- со списком всех брокеров (если один упадёт, ClickHouse подключится к другим).
-- Kafka-таблица данных не хранит, MergeTree-таблицы не затрагиваются.
-- Порядок: сначала MV (зависят от queue), затем queue, затем всё обратно.
-- =====================================================================
DROP VIEW IF EXISTS cinema.movie_events_mv;
DROP VIEW IF EXISTS cinema.movie_events_errors_mv;
DROP TABLE IF EXISTS cinema.movie_events_queue;

CREATE TABLE cinema.movie_events_queue
(
    event_id         UUID,
    user_id          String,
    movie_id         String,
    event_type       Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3,
                           'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp        DateTime64(3, 'UTC'),
    device_type      Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id       String,
    progress_seconds Nullable(Int32)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:29092,kafka-2:29092,kafka-3:29092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_max_block_size = 10000,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW cinema.movie_events_mv
TO cinema.movie_events
AS
SELECT
    event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds,
    toDate(timestamp) AS event_date,
    _partition        AS kafka_partition,
    _offset           AS kafka_offset,
    now64(3)          AS ingested_at
FROM cinema.movie_events_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW cinema.movie_events_errors_mv
TO cinema.movie_events_errors
AS
SELECT
    now()        AS received_at,
    _topic       AS kafka_topic,
    _partition   AS kafka_partition,
    _offset      AS kafka_offset,
    _raw_message AS raw_message,
    _error       AS error
FROM cinema.movie_events_queue
WHERE length(_error) > 0;
