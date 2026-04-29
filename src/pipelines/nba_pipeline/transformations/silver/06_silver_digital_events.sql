-- Silver layer: Cleaned digital events
CREATE OR REFRESH MATERIALIZED VIEW silver_digital_events
COMMENT 'Cleaned digital engagement events'
TBLPROPERTIES (
  'quality' = 'silver'
)
AS SELECT
  event_id,
  member_id,
  session_id,
  event_date,
  event_timestamp,
  event_type,
  page_name,
  device_type,
  platform,
  browser,
  duration_seconds,
  is_error,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_digital_events
WHERE event_id IS NOT NULL
  AND member_id IS NOT NULL;
