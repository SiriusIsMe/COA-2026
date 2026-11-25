-- Уникальные пользователи по типу устройства за день {d}.
-- Пользователь, смотревший с двух устройств, учитывается в обоих.
INSERT INTO cinema.daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT
    {d:Date},
    'device_users',
    toString(device_type),
    toFloat64(uniqExact(user_id)),
    parseDateTime64BestEffort({ts:String}, 3, 'UTC')
FROM cinema.movie_events FINAL
WHERE event_date = {d:Date}
  AND NOT startsWith(user_id, 'it_')
GROUP BY device_type;
