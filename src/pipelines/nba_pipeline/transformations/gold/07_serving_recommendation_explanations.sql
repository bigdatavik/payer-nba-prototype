-- Serving layer: Recommendation explanations for UI display
-- Uses AI Functions to generate personalized, natural language explanations
-- Requires: Serverless SQL Warehouse, Foundation Model API access

CREATE OR REFRESH MATERIALIZED VIEW serving_recommendation_explanations
COMMENT 'Serving table: AI-powered recommendation explanations for Lakebase sync'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'lakebase_sync',
  'sync_mode' = 'SNAPSHOT',
  'ai_functions' = 'ai_gen'
)
AS
WITH member_context AS (
  SELECT
    r.member_id,
    r.full_name,
    r.recommendation_id,
    r.action,
    r.priority_score,
    r.explanation AS primary_explanation,
    r.member_segment,
    f.complaints,
    f.negative_interactions,
    f.denied_claims,
    f.total_paid_12m,
    f.tenure_months,
    f.preventive_claims,
    f.active_care_program,
    f.churn_risk_score,
    f.engagement_score,
    f.plan_type
  FROM LIVE.gold_member_recommendations r
  LEFT JOIN LIVE.gold_member_features f ON r.member_id = f.member_id
)

SELECT
  recommendation_id,
  member_id,
  action,
  CAST(priority_score AS DECIMAL(5,3)) AS priority_score,

  -- AI-generated personalized explanation using member context
  ai_gen(
    CONCAT(
      'Write ONE short sentence (max 20 words) explaining why a healthcare payer recommends "', action, '" for this member. ',
      'Context: ', member_segment, ' segment, ',
      CASE WHEN churn_risk_score > 0.6 THEN 'high churn risk' WHEN churn_risk_score > 0.3 THEN 'moderate risk' ELSE 'stable' END, ', ',
      CASE WHEN complaints > 0 THEN CONCAT(CAST(complaints AS STRING), ' recent complaints') ELSE 'no complaints' END, '. ',
      'Rules: NO filler phrases like "valued member" or "long-standing relationship". ',
      'Start with a verb or the specific reason. Be direct and specific to the action.'
    )
  ) AS explanation,

  -- Supporting data points for the explanation
  CASE
    WHEN action = 'Offer Retention Follow-up' THEN
      CONCAT('Risk indicators: ',
             CAST(complaints AS STRING), ' complaints, ',
             CAST(negative_interactions AS STRING), ' negative interactions')
    WHEN action = 'Route to Service Agent' THEN
      CONCAT('Service history: ',
             CAST(complaints AS STRING), ' complaints requiring attention')
    WHEN action = 'Trigger Care Outreach' THEN
      CONCAT('Care eligibility: ',
             COALESCE(active_care_program, 'No active program'), ', ',
             'Total spend: $', FORMAT_NUMBER(total_paid_12m, 0))
    WHEN action = 'Recommend Plan Education' THEN
      CONCAT('Education opportunity: ',
             CAST(denied_claims AS STRING), ' denied claims, ',
             CAST(tenure_months AS STRING), ' months tenure')
    WHEN action = 'Push Digital Nudge' THEN
      CONCAT('Digital engagement: ',
             'Engagement score ', CAST(ROUND(engagement_score, 1) AS STRING))
    ELSE
      CONCAT('Preventive care: ',
             CAST(preventive_claims AS STRING), ' preventive visits in past year')
  END AS supporting_data,

  -- AI-generated personalized talking points for the agent
  ai_gen(
    CONCAT(
      'You are a call center coach. Write 3 specific talking points for an agent calling about "', action, '". ',
      'Member profile: ', member_segment, ' segment, ', plan_type, ' plan, ',
      CAST(tenure_months AS STRING), ' months tenure, ',
      CASE WHEN complaints > 0 THEN CONCAT(CAST(complaints AS STRING), ' recent complaints') ELSE 'no complaints' END, ', ',
      CASE WHEN churn_risk_score > 0.6 THEN 'HIGH churn risk' WHEN churn_risk_score > 0.3 THEN 'moderate churn risk' ELSE 'low churn risk' END, '. ',
      'Format exactly as: "1. [action verb phrase] 2. [action verb phrase] 3. [action verb phrase]". ',
      'Each point: 5-8 words, start with verb, be specific to their situation. ',
      'GOOD examples: "Acknowledge frustration about denied claim", "Offer to review coverage options together", "Schedule nurse callback for tomorrow morning". ',
      'BAD examples: "Address their concerns", "Provide support", "Help the member". ',
      'Make points empathetic and actionable for THIS specific member.'
    )
  ) AS talking_points,

  current_timestamp() AS explanation_timestamp

FROM member_context;
