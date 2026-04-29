-- Serving layer: Member recommendations for Lakebase sync
-- Optimized for fast lookup by member_id

CREATE OR REFRESH MATERIALIZED VIEW serving_member_recommendations
COMMENT 'Serving table: Current recommendations for Lakebase sync'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'lakebase_sync',
  'sync_mode' = 'SNAPSHOT',
  'primary_key' = 'recommendation_id'
)
AS SELECT
  recommendation_id,
  member_id,
  full_name,
  member_segment,
  plan_type,
  action,
  CAST(priority_score AS DECIMAL(5,3)) AS priority_score,
  CAST(confidence_pct AS INT) AS confidence_pct,
  preferred_channel,
  explanation,
  generated_at,
  valid_until,
  model_version,

  -- Alternative scores for transparency
  CAST(score_benefit_reminder AS DECIMAL(5,3)) AS score_benefit_reminder,
  CAST(score_service_agent AS DECIMAL(5,3)) AS score_service_agent,
  CAST(score_care_outreach AS DECIMAL(5,3)) AS score_care_outreach,
  CAST(score_digital_nudge AS DECIMAL(5,3)) AS score_digital_nudge,
  CAST(score_plan_education AS DECIMAL(5,3)) AS score_plan_education,
  CAST(score_retention_followup AS DECIMAL(5,3)) AS score_retention_followup,

  CAST(churn_risk_score AS DECIMAL(5,3)) AS churn_risk_score,
  CAST(engagement_score AS DECIMAL(5,2)) AS engagement_score,
  channel_affinity

FROM LIVE.gold_member_recommendations;
