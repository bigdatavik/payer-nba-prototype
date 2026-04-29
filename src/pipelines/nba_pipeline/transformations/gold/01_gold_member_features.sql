-- Gold layer: Member-level feature engineering
-- This table aggregates all member signals into a feature vector for recommendations

CREATE OR REFRESH MATERIALIZED VIEW gold_member_features
COMMENT 'Member-level features for NBA recommendations'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'feature_store',
  'refresh_frequency' = 'daily'
)
AS
WITH claims_features AS (
  SELECT
    member_id,
    COUNT(*) AS total_claims_12m,
    SUM(paid_amount) AS total_paid_12m,
    AVG(paid_amount) AS avg_claim_amount,
    COUNT(DISTINCT claim_category) AS distinct_claim_categories,
    SUM(CASE WHEN cost_tier = 'High Cost' THEN 1 ELSE 0 END) AS high_cost_claims,
    SUM(CASE WHEN is_preventive THEN 1 ELSE 0 END) AS preventive_claims,
    SUM(CASE WHEN is_chronic_related THEN 1 ELSE 0 END) AS chronic_claims,
    MAX(claim_date) AS last_claim_date,
    SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) AS denied_claims
  FROM LIVE.silver_claims
  WHERE claim_date >= DATE_SUB(current_date(), 365)
  GROUP BY member_id
),

interaction_features AS (
  SELECT
    member_id,
    COUNT(*) AS total_interactions_12m,
    AVG(satisfaction_score) AS avg_satisfaction,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_interactions,
    SUM(CASE WHEN is_complaint THEN 1 ELSE 0 END) AS complaints,
    SUM(CASE WHEN is_escalated THEN 1 ELSE 0 END) AS escalations,
    AVG(friction_score) AS avg_friction_score,
    MAX(interaction_date) AS last_interaction_date,
    -- Channel preferences
    SUM(CASE WHEN channel = 'Phone Call' THEN 1 ELSE 0 END) AS phone_interactions,
    SUM(CASE WHEN channel IN ('Secure Message', 'Email', 'Portal Visit') THEN 1 ELSE 0 END) AS digital_interactions
  FROM LIVE.silver_interactions_enriched
  WHERE interaction_date >= DATE_SUB(current_date(), 365)
  GROUP BY member_id
),

campaign_features AS (
  SELECT
    member_id,
    COUNT(*) AS campaigns_received_12m,
    SUM(CASE WHEN is_positive_response THEN 1 ELSE 0 END) AS positive_responses,
    AVG(CASE WHEN is_positive_response THEN 1.0 ELSE 0.0 END) AS response_rate,
    SUM(revenue_impact) AS campaign_revenue_12m,
    -- Best performing channels
    FIRST_VALUE(channel) OVER (
      PARTITION BY member_id
      ORDER BY SUM(CASE WHEN is_positive_response THEN 1 ELSE 0 END) DESC
    ) AS best_response_channel
  FROM LIVE.silver_campaign_responses
  WHERE response_date >= DATE_SUB(current_date(), 365)
  GROUP BY member_id, channel
),

campaign_agg AS (
  SELECT
    member_id,
    SUM(campaigns_received_12m) AS campaigns_received_12m,
    SUM(positive_responses) AS positive_responses,
    AVG(response_rate) AS response_rate,
    SUM(campaign_revenue_12m) AS campaign_revenue_12m,
    MAX(best_response_channel) AS best_response_channel
  FROM campaign_features
  GROUP BY member_id
),

digital_features AS (
  SELECT
    member_id,
    COUNT(DISTINCT session_id) AS digital_sessions_12m,
    COUNT(*) AS digital_events_12m,
    AVG(duration_seconds) AS avg_session_duration,
    SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS digital_errors,
    SUM(CASE WHEN platform LIKE '%App%' THEN 1 ELSE 0 END) AS mobile_events,
    MAX(event_date) AS last_digital_activity
  FROM LIVE.silver_digital_events
  WHERE event_date >= DATE_SUB(current_date(), 365)
  GROUP BY member_id
),

care_features AS (
  SELECT
    member_id,
    COUNT(*) AS care_touchpoints_12m,
    AVG(member_engagement_score) AS care_engagement_score,
    SUM(barriers_identified) AS total_barriers,
    MAX(program_name) AS active_care_program,
    MAX(next_touchpoint_date) AS next_care_touchpoint
  FROM LIVE.silver_care_management
  WHERE interaction_date >= DATE_SUB(current_date(), 365)
  GROUP BY member_id
)

SELECT
  m.member_id,
  m.full_name,
  m.age,
  m.age_band,
  m.gender,
  m.region,
  m.state,
  m.plan_type,
  m.member_segment,
  m.tenure_months,
  m.communication_preference,
  m.language_preference,
  m.has_caregiver,
  m.is_dual_eligible,

  -- Claims features
  COALESCE(cf.total_claims_12m, 0) AS total_claims_12m,
  COALESCE(cf.total_paid_12m, 0) AS total_paid_12m,
  COALESCE(cf.avg_claim_amount, 0) AS avg_claim_amount,
  COALESCE(cf.distinct_claim_categories, 0) AS distinct_claim_categories,
  COALESCE(cf.high_cost_claims, 0) AS high_cost_claims,
  COALESCE(cf.preventive_claims, 0) AS preventive_claims,
  COALESCE(cf.chronic_claims, 0) AS chronic_claims,
  cf.last_claim_date,
  COALESCE(cf.denied_claims, 0) AS denied_claims,

  -- Interaction features
  COALESCE(inf.total_interactions_12m, 0) AS total_interactions_12m,
  COALESCE(inf.avg_satisfaction, 3.0) AS avg_satisfaction,
  COALESCE(inf.negative_interactions, 0) AS negative_interactions,
  COALESCE(inf.complaints, 0) AS complaints,
  COALESCE(inf.escalations, 0) AS escalations,
  COALESCE(inf.avg_friction_score, 0) AS avg_friction_score,
  inf.last_interaction_date,
  COALESCE(inf.phone_interactions, 0) AS phone_interactions,
  COALESCE(inf.digital_interactions, 0) AS digital_interactions,

  -- Campaign features
  COALESCE(cmp.campaigns_received_12m, 0) AS campaigns_received_12m,
  COALESCE(cmp.positive_responses, 0) AS positive_responses,
  COALESCE(cmp.response_rate, 0) AS response_rate,
  COALESCE(cmp.campaign_revenue_12m, 0) AS campaign_revenue_12m,
  cmp.best_response_channel,

  -- Digital features
  COALESCE(df.digital_sessions_12m, 0) AS digital_sessions_12m,
  COALESCE(df.digital_events_12m, 0) AS digital_events_12m,
  COALESCE(df.avg_session_duration, 0) AS avg_session_duration,
  COALESCE(df.digital_errors, 0) AS digital_errors,
  COALESCE(df.mobile_events, 0) AS mobile_events,
  df.last_digital_activity,

  -- Care management features
  COALESCE(crf.care_touchpoints_12m, 0) AS care_touchpoints_12m,
  COALESCE(crf.care_engagement_score, 5) AS care_engagement_score,
  COALESCE(crf.total_barriers, 0) AS total_barriers,
  crf.active_care_program,
  crf.next_care_touchpoint,

  -- Derived scores
  -- Churn risk score (0-1, higher = more likely to churn)
  ROUND(
    LEAST(1.0, GREATEST(0.0,
      (COALESCE(inf.complaints, 0) * 0.3) +
      (COALESCE(inf.escalations, 0) * 0.2) +
      (COALESCE(cf.denied_claims, 0) * 0.15) +
      ((5 - COALESCE(inf.avg_satisfaction, 3)) / 5 * 0.2) +
      (COALESCE(inf.avg_friction_score, 0) * 0.15)
    )), 3) AS churn_risk_score,

  -- Engagement score (0-10, higher = more engaged)
  ROUND(
    LEAST(10.0, GREATEST(0.0,
      (COALESCE(df.digital_sessions_12m, 0) * 0.1) +
      (COALESCE(cmp.response_rate, 0) * 3) +
      (COALESCE(inf.avg_satisfaction, 3) * 0.5) +
      (COALESCE(crf.care_engagement_score, 5) * 0.3) +
      (CASE WHEN m.tenure_months > 24 THEN 2 ELSE m.tenure_months / 12 END)
    )), 2) AS engagement_score,

  -- Channel affinity
  CASE
    WHEN COALESCE(inf.digital_interactions, 0) > COALESCE(inf.phone_interactions, 0) * 2 THEN 'Digital First'
    WHEN COALESCE(inf.phone_interactions, 0) > COALESCE(inf.digital_interactions, 0) * 2 THEN 'Phone Preferred'
    ELSE 'Omnichannel'
  END AS channel_affinity,

  -- Escalation likelihood (0-1)
  ROUND(
    LEAST(1.0, GREATEST(0.0,
      (COALESCE(inf.escalations, 0) / NULLIF(COALESCE(inf.total_interactions_12m, 1), 0)) +
      (COALESCE(inf.complaints, 0) * 0.2)
    )), 3) AS escalation_likelihood,

  -- Care outreach likelihood (0-1)
  CASE
    WHEN m.member_segment IN ('Chronic Care', 'Rising Risk') THEN 0.8
    WHEN COALESCE(cf.chronic_claims, 0) > 3 THEN 0.7
    WHEN COALESCE(cf.high_cost_claims, 0) > 2 THEN 0.6
    WHEN crf.active_care_program IS NOT NULL THEN 0.5
    ELSE 0.2
  END AS care_outreach_likelihood,

  -- Plan switch propensity (0-1)
  ROUND(
    LEAST(1.0, GREATEST(0.0,
      CASE
        WHEN MONTH(current_date()) IN (10, 11, 12) THEN 0.3  -- AEP season boost
        ELSE 0.1
      END +
      (COALESCE(cf.denied_claims, 0) * 0.1) +
      ((5 - COALESCE(inf.avg_satisfaction, 3)) / 10)
    )), 3) AS plan_switch_propensity,

  current_timestamp() AS _feature_computed_at

FROM LIVE.silver_members m
LEFT JOIN claims_features cf ON m.member_id = cf.member_id
LEFT JOIN interaction_features inf ON m.member_id = inf.member_id
LEFT JOIN campaign_agg cmp ON m.member_id = cmp.member_id
LEFT JOIN digital_features df ON m.member_id = df.member_id
LEFT JOIN care_features crf ON m.member_id = crf.member_id;
