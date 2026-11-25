-- Топ-{top_n} фильмов за день {d} по числу просмотров (VIEW_STARTED).
-- Ранг — оконная функция row_number(); при равенстве — по movie_id.
INSERT INTO cinema.daily_metrics (metric_date, metric_name, dimension, value, computed_at)
SELECT
    {d:Date},
    'top_movie_views',
    movie_id,
    toFloat64(views),
    parseDateTime64BestEffort({ts:String}, 3, 'UTC')
FROM
(
    SELECT
        movie_id,
        views,
        row_number() OVER (ORDER BY views DESC, movie_id ASC) AS rnk
    FROM
    (
        SELECT movie_id, count() AS views
        FROM cinema.movie_events FINAL
        WHERE event_date = {d:Date}
          AND event_type = 'VIEW_STARTED'
          AND NOT startsWith(user_id, 'it_')
        GROUP BY movie_id
    )
)
WHERE rnk <= {top_n:UInt16};
