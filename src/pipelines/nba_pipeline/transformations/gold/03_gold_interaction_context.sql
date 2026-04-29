-- Gold layer: Recent interaction context for member lookup
-- Optimized for fast retrieval during member 360 display

CREATE OR REFRESH MATERIALIZED VIEW gold_interaction_context
COMMENT 'Recent member interactions for context in NBA console'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'serving'
)
AS
WITH recent_interactions AS (
  SELECT
    member_id,
    interaction_id,
    interaction_date,
    interaction_timestamp,
    interaction_type,
    channel,
    reason,
    resolution,
    satisfaction_score,
    sentiment,
    inferred_intent,
    friction_score,
    interaction_summary,
    is_complaint,
    is_escalated,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY interaction_timestamp DESC) AS recency_rank
  FROM LIVE.silver_interactions_enriched
  WHERE interaction_date >= DATE_SUB(current_date(), 90)
)

SELECT
  member_id,
  interaction_id,
  interaction_date,
  interaction_timestamp,
  interaction_type,
  channel,
  reason,
  resolution,
  satisfaction_score,
  sentiment,
  inferred_intent,
  friction_score,
  interaction_summary,
  is_complaint,
  is_escalated,
  recency_rank,
  current_timestamp() AS _processed_at
FROM recent_interactions
WHERE recency_rank <= 10;  -- Keep last 10 interactions per member
