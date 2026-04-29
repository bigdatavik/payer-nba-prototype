-- Silver layer: Cleaned policy data
CREATE OR REFRESH MATERIALIZED VIEW silver_policies
COMMENT 'Cleaned policy data with derived status fields'
TBLPROPERTIES (
  'quality' = 'silver'
)
AS SELECT
  policy_id,
  member_id,
  plan_type,
  plan_id,
  effective_date,
  termination_date,
  CASE
    WHEN termination_date IS NULL OR termination_date > current_date() THEN 'Active'
    ELSE 'Terminated'
  END AS policy_status,
  monthly_premium,
  annual_deductible,
  deductible_met_amount,
  ROUND(deductible_met_amount / NULLIF(annual_deductible, 0) * 100, 1) AS deductible_pct_met,
  out_of_pocket_max,
  copay_primary_care,
  copay_specialist,
  copay_emergency,
  coinsurance_percent,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_policies
WHERE policy_id IS NOT NULL
  AND member_id IS NOT NULL;
