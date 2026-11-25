-- Retention для когорты дня {d}: пользователи, у которых первый VIEW_STARTED за всю историю был в {d}.
-- D1 / D7 — доля когорты, проявившей любую активность ровно через 1 / 7 дней.
-- D1 пишется, только если день d+1 уже завершился ({with_d1} = 1), D7 — если завершился d+7;
-- иначе значение было бы заниженным. Пересчёт последних дней по расписанию дозаполнит их.
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
        count()                                 AS cohort,
        countIf(has(active_days, {d:Date} + 1)) AS returned_d1,
        countIf(has(active_days, {d:Date} + 7)) AS returned_d7
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
)
ARRAY JOIN [
    ('retention_cohort_size', toFloat64(cohort),                          1),
    ('retention_d1',          if(cohort = 0, 0., returned_d1 / cohort), {with_d1:UInt8}),
    ('retention_d7',          if(cohort = 0, 0., returned_d7 / cohort), {with_d7:UInt8})
] AS m
WHERE tupleElement(m, 3) = 1;
