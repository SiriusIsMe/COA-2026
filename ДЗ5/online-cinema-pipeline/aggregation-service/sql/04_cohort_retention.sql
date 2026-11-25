-- Когортный retention по дням жизни для когорты {d} (для тепловой карты в Grafana).
-- Когорта: пользователи, у которых первый VIEW_STARTED за всю историю был в {d}.
-- day_N = доля когорты с любой активностью в день {d} + N, N = 0 … {max_day}.
-- {max_day} = последний полностью завершённый день жизни (не больше 7); day_0 = 100% по определению.
-- ARRAY JOIN range(...) размножает каждого пользователя когорты на дни 0..max_day,
-- после чего countIf(has(active_days, d + n)) считает вернувшихся в каждый день.
INSERT INTO cinema.daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT
    {d:Date},
    'cohort_retention',
    concat('day_', toString(n)),
    if(cohort = 0, 0., returned / cohort),
    parseDateTime64BestEffort({ts:String}, 3, 'UTC')
FROM
(
    SELECT
        n,
        count()                                  AS cohort,
        countIf(has(active_days, {d:Date} + n))  AS returned
    FROM
    (
        SELECT
            user_id,
            minIf(event_date, event_type = 'VIEW_STARTED') AS first_view_date,
            groupUniqArray(event_date)                     AS active_days
        FROM cinema.movie_events FINAL
        WHERE event_date <= {d:Date} + 7
          AND NOT startsWith(user_id, 'it_')
        GROUP BY user_id
        HAVING countIf(event_type = 'VIEW_STARTED') > 0
           AND first_view_date = {d:Date}
    )
    ARRAY JOIN range(toUInt8({max_day:UInt8} + 1)) AS n
    GROUP BY n
);
