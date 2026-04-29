-- Gold layer: Next-Best-Action Recommendations
-- Recommendation engine using weighted scoring based on member features

CREATE OR REFRESH MATERIALIZED VIEW gold_member_recommendations
COMMENT 'Next-best-action recommendations for each member'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'serving',
  'refresh_frequency' = 'daily'
)
AS
WITH recommendation_scores AS (
  SELECT
    member_id,
    full_name,
    member_segment,
    plan_type,
    channel_affinity,
    communication_preference,
    churn_risk_score,
    engagement_score,
    escalation_likelihood,
    care_outreach_likelihood,
    plan_switch_propensity,
    complaints,
    negative_interactions,
    denied_claims,
    preventive_claims,
    total_paid_12m,
    tenure_months,
    active_care_program,

    -- Action: Send Benefit Reminder
    CASE
      WHEN preventive_claims < 2 AND tenure_months >= 6 THEN 0.7
      WHEN engagement_score < 3 THEN 0.5
      ELSE 0.2
    END AS score_benefit_reminder,

    -- Action: Route to Service Agent
    CASE
      WHEN escalation_likelihood > 0.5 THEN 0.9
      WHEN complaints > 0 THEN 0.8
      WHEN negative_interactions > 2 THEN 0.7
      ELSE 0.1
    END AS score_service_agent,

    -- Action: Trigger Care Outreach
    CASE
      WHEN care_outreach_likelihood > 0.6 AND active_care_program IS NULL THEN 0.9
      WHEN member_segment = 'Rising Risk' THEN 0.8
      WHEN member_segment = 'Chronic Care' AND active_care_program IS NULL THEN 0.85
      ELSE care_outreach_likelihood * 0.5
    END AS score_care_outreach,

    -- Action: Push Digital Nudge
    CASE
      WHEN channel_affinity = 'Digital First' THEN 0.8
      WHEN engagement_score > 5 AND channel_affinity = 'Omnichannel' THEN 0.6
      ELSE 0.3
    END AS score_digital_nudge,

    -- Action: Recommend Plan Education
    CASE
      WHEN tenure_months < 6 THEN 0.8
      WHEN denied_claims > 1 THEN 0.7
      WHEN plan_switch_propensity > 0.3 THEN 0.65
      ELSE 0.2
    END AS score_plan_education,

    -- Action: Offer Retention Follow-up
    CASE
      WHEN churn_risk_score > 0.6 THEN 0.95
      WHEN churn_risk_score > 0.4 AND complaints > 0 THEN 0.85
      WHEN negative_interactions > 3 THEN 0.7
      ELSE churn_risk_score * 0.5
    END AS score_retention_followup

  FROM LIVE.gold_member_features
),

ranked_recommendations AS (
  SELECT
    member_id,
    full_name,
    member_segment,
    plan_type,
    channel_affinity,
    communication_preference,
    churn_risk_score,
    engagement_score,

    -- Determine best action and channel
    CASE
      WHEN GREATEST(score_benefit_reminder, score_service_agent, score_care_outreach,
                    score_digital_nudge, score_plan_education, score_retention_followup)
           = score_retention_followup THEN 'Offer Retention Follow-up'
      WHEN GREATEST(score_benefit_reminder, score_service_agent, score_care_outreach,
                    score_digital_nudge, score_plan_education, score_retention_followup)
           = score_service_agent THEN 'Route to Service Agent'
      WHEN GREATEST(score_benefit_reminder, score_service_agent, score_care_outreach,
                    score_digital_nudge, score_plan_education, score_retention_followup)
           = score_care_outreach THEN 'Trigger Care Outreach'
      WHEN GREATEST(score_benefit_reminder, score_service_agent, score_care_outreach,
                    score_digital_nudge, score_plan_education, score_retention_followup)
           = score_plan_education THEN 'Recommend Plan Education'
      WHEN GREATEST(score_benefit_reminder, score_service_agent, score_care_outreach,
                    score_digital_nudge, score_plan_education, score_retention_followup)
           = score_digital_nudge THEN 'Push Digital Nudge'
      ELSE 'Send Benefit Reminder'
    END AS recommended_action,

    GREATEST(score_benefit_reminder, score_service_agent, score_care_outreach,
             score_digital_nudge, score_plan_education, score_retention_followup) AS priority_score,

    score_benefit_reminder,
    score_service_agent,
    score_care_outreach,
    score_digital_nudge,
    score_plan_education,
    score_retention_followup

  FROM recommendation_scores
)

SELECT
  CONCAT('REC-', member_id, '-', DATE_FORMAT(current_date(), 'yyyyMMdd')) AS recommendation_id,
  member_id,
  full_name,
  member_segment,
  plan_type,
  recommended_action AS action,
  ROUND(priority_score, 3) AS priority_score,
  ROUND(priority_score * 100, 0) AS confidence_pct,

  -- Preferred channel based on affinity and action
  CASE
    WHEN recommended_action = 'Route to Service Agent' THEN 'Phone'
    WHEN recommended_action = 'Push Digital Nudge' THEN
      CASE WHEN channel_affinity = 'Digital First' THEN 'App Push' ELSE 'Email' END
    WHEN recommended_action = 'Trigger Care Outreach' THEN 'Phone'
    WHEN channel_affinity = 'Digital First' THEN 'Email'
    WHEN channel_affinity = 'Phone Preferred' THEN 'Phone'
    ELSE communication_preference
  END AS preferred_channel,

  -- Human-readable explanation
  CASE
    WHEN recommended_action = 'Offer Retention Follow-up' THEN
      CONCAT('Member shows elevated churn risk (', ROUND(churn_risk_score * 100, 0), '%). ',
             'Proactive outreach recommended to address concerns and reinforce value.')
    WHEN recommended_action = 'Route to Service Agent' THEN
      'Recent negative interactions indicate member may need specialized support. '
      || 'Routing to experienced service agent for personalized resolution.'
    WHEN recommended_action = 'Trigger Care Outreach' THEN
      CONCAT('Member profile suggests benefit from care management enrollment. ',
             'Segment: ', member_segment, '. Outreach to discuss available programs.')
    WHEN recommended_action = 'Recommend Plan Education' THEN
      'Member may benefit from better understanding of plan benefits and coverage. '
      || 'Educational outreach can improve utilization and satisfaction.'
    WHEN recommended_action = 'Push Digital Nudge' THEN
      'Member shows preference for digital engagement. '
      || 'Targeted digital nudge can drive engagement with self-service tools.'
    ELSE
      'Benefit reminder recommended to ensure member is aware of available preventive care '
      || 'and wellness programs under their plan.'
  END AS explanation,

  -- Metadata
  current_timestamp() AS generated_at,
  DATE_ADD(current_date(), 7) AS valid_until,
  'nba_engine_v1' AS model_version,

  -- Store all scores for transparency
  ROUND(score_benefit_reminder, 3) AS score_benefit_reminder,
  ROUND(score_service_agent, 3) AS score_service_agent,
  ROUND(score_care_outreach, 3) AS score_care_outreach,
  ROUND(score_digital_nudge, 3) AS score_digital_nudge,
  ROUND(score_plan_education, 3) AS score_plan_education,
  ROUND(score_retention_followup, 3) AS score_retention_followup,

  churn_risk_score,
  engagement_score,
  channel_affinity

FROM ranked_recommendations
WHERE priority_score >= 0.3;  -- Only recommend if confidence is meaningful
