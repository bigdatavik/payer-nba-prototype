-- Silver layer: Cleaned care management data
CREATE OR REFRESH MATERIALIZED VIEW silver_care_management
COMMENT 'Cleaned care management interactions'
TBLPROPERTIES (
  'quality' = 'silver',
  'contains_phi' = 'true'
)
AS SELECT
  care_interaction_id,
  member_id,
  care_manager_id,
  program_name,
  program_enrollment_date,
  interaction_date,
  interaction_type,
  duration_minutes,
  goals_discussed,
  barriers_identified,
  referrals_made,
  member_engagement_score,
  notes,
  next_touchpoint_date,
  CASE
    WHEN member_engagement_score >= 8 THEN 'Highly Engaged'
    WHEN member_engagement_score >= 5 THEN 'Moderately Engaged'
    ELSE 'Low Engagement'
  END AS engagement_level,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_care_management
WHERE care_interaction_id IS NOT NULL
  AND member_id IS NOT NULL;
