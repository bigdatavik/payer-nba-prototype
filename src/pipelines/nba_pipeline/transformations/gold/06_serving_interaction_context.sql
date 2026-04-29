-- Serving layer: Interaction context for Lakebase sync
-- Recent interactions for member 360 display

CREATE OR REFRESH MATERIALIZED VIEW serving_interaction_context
COMMENT 'Serving table: Recent interactions for Lakebase sync'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'lakebase_sync',
  'sync_mode' = 'SNAPSHOT',
  'primary_key' = 'interaction_id'
)
AS SELECT
  interaction_id,
  member_id,
  interaction_date,
  interaction_type,
  channel,
  reason,
  resolution,
  CAST(satisfaction_score AS INT) AS satisfaction_score,
  sentiment,
  inferred_intent,
  CAST(friction_score AS DECIMAL(3,2)) AS friction_score,
  interaction_summary,
  is_complaint,
  is_escalated,
  CAST(recency_rank AS INT) AS recency_rank,
  _processed_at AS context_timestamp

FROM LIVE.gold_interaction_context;
