-- =====================================================================
-- 002: материализованные дневные агрегаты (их считает aggregation-service)
--
-- Длинный формат «дата — метрика — измерение — значение»: новые метрики
-- добавляются без изменения схемы, а такая таблица один в один ложится
-- на таблицу в PostgreSQL.
--   dimension = ''        — метрика по всему сервису (dau, retention_d1, ...)
--   dimension = movie_id  — метрика в разрезе фильма (top_movie_views)
--
-- ReplacingMergeTree(computed_at): пересчёт за ту же дату вставляет строки
-- с новым computed_at, а при слияниях остаётся самая свежая версия ключа.
-- Сервис читает результаты своего цикла по точному computed_at, поэтому
-- фильмы, выпавшие из топа после пересчёта, в выдачу не попадают.
-- =====================================================================
CREATE TABLE IF NOT EXISTS cinema.daily_metrics
(
    metric_date  Date,
    metric_name  LowCardinality(String),
    dimension    String DEFAULT '',
    value        Float64,
    computed_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name, dimension);
