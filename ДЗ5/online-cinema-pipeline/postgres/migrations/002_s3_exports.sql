-- Журнал выгрузок в S3: по одной строке на дату (последняя попытка).
-- status = success | failed. content_hash — хеш выгруженных значений: если метрики за дату
-- изменились (например, досчитался retention D7), плановый экспорт перезапишет файл.
CREATE TABLE IF NOT EXISTS s3_exports
(
    metric_date   date        PRIMARY KEY,
    object_key    text        NOT NULL,
    status        text        NOT NULL,
    rows_exported integer     NOT NULL DEFAULT 0,
    size_bytes    integer     NOT NULL DEFAULT 0,
    content_hash  text,
    attempts      integer     NOT NULL DEFAULT 1,  -- попыток подряд с последнего успеха (для failed)
    error         text,
    exported_at   timestamptz,                      -- время последней успешной выгрузки
    updated_at    timestamptz NOT NULL DEFAULT now()
);
