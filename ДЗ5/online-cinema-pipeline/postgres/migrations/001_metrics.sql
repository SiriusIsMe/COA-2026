-- =====================================================================
-- PostgreSQL: хранилище готовых метрик для дашбордов, других сервисов и экспорта в S3.
-- =====================================================================

-- Дневные метрики. Тот же длинный формат, что и cinema.daily_metrics в ClickHouse.
-- PRIMARY KEY (metric_date, metric_name, dimension) делает запись идемпотентной:
-- повторный пересчёт за дату делает UPSERT (ON CONFLICT DO UPDATE), а не дублирует строки.
CREATE TABLE IF NOT EXISTS daily_metrics
(
    metric_date  date             NOT NULL,
    metric_name  text             NOT NULL,
    dimension    text             NOT NULL DEFAULT '',   -- '' = весь сервис; movie_id для топа фильмов
    value        double precision NOT NULL,
    computed_at  timestamptz      NOT NULL,              -- когда значение вычислено в ClickHouse
    updated_at   timestamptz      NOT NULL DEFAULT now(),-- когда строка последний раз записана сюда
    PRIMARY KEY (metric_date, metric_name, dimension)
);

-- для графиков «метрика по дням»
CREATE INDEX IF NOT EXISTS daily_metrics_name_date_idx ON daily_metrics (metric_name, metric_date);

-- Журнал циклов вычисления: когда, за какую дату, сколько обработано, сколько заняло, ошибки.
CREATE TABLE IF NOT EXISTS aggregation_runs
(
    id               bigserial   PRIMARY KEY,
    metric_date      date        NOT NULL,
    trigger          text        NOT NULL,              -- schedule | manual | cli
    status           text        NOT NULL,              -- success | failed
    events_processed bigint      NOT NULL DEFAULT 0,
    rows_written     integer     NOT NULL DEFAULT 0,
    duration_ms      integer     NOT NULL DEFAULT 0,
    error            text,
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS aggregation_runs_date_idx ON aggregation_runs (metric_date, started_at DESC);

-- Удобные представления для потребителей ---------------------------------

-- Одна строка на день: основные KPI в колонках
CREATE OR REPLACE VIEW v_daily_kpi AS
SELECT
    metric_date,
    max(value) FILTER (WHERE metric_name = 'dau')                    AS dau,
    max(value) FILTER (WHERE metric_name = 'avg_watch_time_seconds') AS avg_watch_time_seconds,
    max(value) FILTER (WHERE metric_name = 'view_conversion')        AS view_conversion,
    max(value) FILTER (WHERE metric_name = 'retention_cohort_size')  AS retention_cohort_size,
    max(value) FILTER (WHERE metric_name = 'retention_d1')           AS retention_d1,
    max(value) FILTER (WHERE metric_name = 'retention_d7')           AS retention_d7,
    max(value) FILTER (WHERE metric_name = 'events_total')           AS events_total,
    max(computed_at)                                                 AS computed_at
FROM daily_metrics
WHERE dimension = ''
GROUP BY metric_date;

-- Топ фильмов по дням с рангом
CREATE OR REPLACE VIEW v_top_movies AS
SELECT
    metric_date,
    row_number() OVER (PARTITION BY metric_date ORDER BY value DESC, dimension) AS rank,
    dimension      AS movie_id,
    value::bigint  AS views,
    computed_at
FROM daily_metrics
WHERE metric_name = 'top_movie_views';
