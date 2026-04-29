-- Silver layer: Cleaned CRM events
CREATE OR REFRESH MATERIALIZED VIEW silver_crm_events
COMMENT 'Cleaned CRM/marketing events data'
TBLPROPERTIES (
  'quality' = 'silver'
)
AS SELECT
  event_id,
  member_id,
  event_date,
  event_timestamp,
  event_type,
  campaign_id,
  campaign_name,
  campaign_type,
  channel,
  responded,
  response_date,
  CASE WHEN responded THEN DATEDIFF(response_date, event_date) ELSE NULL END AS days_to_response,
  utm_source,
  utm_campaign,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_crm_events
WHERE event_id IS NOT NULL
  AND member_id IS NOT NULL;
