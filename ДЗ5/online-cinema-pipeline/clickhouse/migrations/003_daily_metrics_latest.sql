-- =====================================================================
-- 003: представление «последний расчёт за каждый день» поверх cinema.daily_metrics.
-- Каждый цикл записывает все метрики дня с одним computed_at; FINAL оставляет
-- последнюю версию ключа, а фильтр по max(computed_at) отбрасывает строки,
-- которых в последнем расчёте уже нет (например, фильм выпал из топа).
-- Из него читает Grafana: дашборд строится только по агрегатам, не по raw-событиям.
-- =====================================================================
CREATE VIEW IF NOT EXISTS cinema.v_daily_metrics_latest AS
SELECT metric_date, metric_name, dimension, value, computed_at
FROM cinema.daily_metrics FINAL
WHERE (metric_date, computed_at) IN
(
    SELECT metric_date, max(computed_at)
    FROM cinema.daily_metrics
    GROUP BY metric_date
);
