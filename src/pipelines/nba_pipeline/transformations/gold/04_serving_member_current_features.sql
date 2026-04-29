-- Serving layer: Member current features for Lakebase sync
-- Flattened, lookup-friendly schema optimized for online access

CREATE OR REFRESH MATERIALIZED VIEW serving_member_current_features
COMMENT 'Serving table: Current member features for Lakebase sync'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'lakebase_sync',
  'sync_mode' = 'SNAPSHOT',
  'primary_key' = 'member_id'
)
AS SELECT
  member_id,
  full_name,
  age,
  age_band,
  gender,
  region,
  state,
  plan_type,
  member_segment,
  tenure_months,
  communication_preference,
  language_preference,
  has_caregiver,
  is_dual_eligible,

  -- Key metrics
  total_claims_12m,
  total_paid_12m,
  total_interactions_12m,
  avg_satisfaction,
  digital_sessions_12m,

  -- Scores
  CAST(churn_risk_score AS DECIMAL(5,3)) AS churn_risk_score,
  CAST(engagement_score AS DECIMAL(5,2)) AS engagement_score,
  channel_affinity,
  CAST(escalation_likelihood AS DECIMAL(5,3)) AS escalation_likelihood,
  CAST(care_outreach_likelihood AS DECIMAL(5,3)) AS care_outreach_likelihood,
  CAST(plan_switch_propensity AS DECIMAL(5,3)) AS plan_switch_propensity,

  -- Care program
  active_care_program,

  -- Timestamps
  last_claim_date,
  last_interaction_date,
  last_digital_activity,
  _feature_computed_at AS feature_timestamp

FROM LIVE.gold_member_features;
