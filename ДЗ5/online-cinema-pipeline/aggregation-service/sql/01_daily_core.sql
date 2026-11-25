-- DAU, среднее время просмотра, конверсия просмотра за день {d}.
-- uniqExact / avgIf / uniqExactIf — агрегатные функции ClickHouse.
-- Конверсия = сессии с VIEW_FINISHED / сессии с VIEW_STARTED за день.
-- FINAL — без дублей от повторных отправок продюсера; it_* — данные интеграционных тестов.
INSERT INTO cinema.daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT
    {d:Date},
    tupleElement(m, 1),
    '',
    tupleElement(m, 2),
    parseDateTime64BestEffort({ts:String}, 3, 'UTC')
FROM
(
    SELECT
        uniqExact(user_id)                                                          AS dau,
        ifNotFinite(avgIf(assumeNotNull(progress_seconds), event_type = 'VIEW_FINISHED'), 0) AS avg_watch,
        uniqExactIf(session_id, event_type = 'VIEW_STARTED')                        AS started,
        uniqExactIf(session_id, event_type = 'VIEW_FINISHED')                       AS finished,
        count()                                                                     AS events
    FROM cinema.movie_events FINAL
    WHERE event_date = {d:Date}
      AND NOT startsWith(user_id, 'it_')
)
ARRAY JOIN [
    ('dau',                    toFloat64(dau)),
    ('avg_watch_time_seconds', toFloat64(avg_watch)),
    ('views_started',          toFloat64(started)),
    ('views_finished',         toFloat64(finished)),
    ('view_conversion',        if(started = 0, 0., finished / started)),
    ('events_total',           toFloat64(events))
] AS m;
