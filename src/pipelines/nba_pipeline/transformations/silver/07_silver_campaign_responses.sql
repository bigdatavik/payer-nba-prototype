-- Silver layer: Cleaned campaign responses
CREATE OR REFRESH MATERIALIZED VIEW silver_campaign_responses
COMMENT 'Cleaned campaign response data with outcome classification'
TBLPROPERTIES (
  'quality' = 'silver'
)
AS SELECT
  response_id,
  member_id,
  campaign_id,
  campaign_name,
  campaign_type,
  send_date,
  response_date,
  channel,
  outcome,
  CASE
    WHEN outcome IN ('Converted', 'Engaged') THEN true
    ELSE false
  END AS is_positive_response,
  CASE
    WHEN outcome = 'Converted' THEN 'conversion'
    WHEN outcome = 'Engaged' THEN 'engagement'
    WHEN outcome = 'No Response' THEN 'no_response'
    WHEN outcome = 'Opted Out' THEN 'opt_out'
    ELSE 'unknown'
  END AS response_category,
  revenue_impact,
  cost,
  ROUND(revenue_impact - cost, 2) AS net_impact,
  attribution_model,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_campaign_responses
WHERE response_id IS NOT NULL
  AND member_id IS NOT NULL;
