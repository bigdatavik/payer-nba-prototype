-- Silver layer: Cleaned claims data with categorization
CREATE OR REFRESH MATERIALIZED VIEW silver_claims
COMMENT 'Cleaned claims data with derived cost metrics'
TBLPROPERTIES (
  'quality' = 'silver',
  'contains_phi' = 'true'
)
AS SELECT
  claim_id,
  member_id,
  claim_date,
  service_date,
  claim_type,
  claim_category,
  diagnosis_code,
  COALESCE(diagnosis_description, 'Unknown diagnosis') AS diagnosis_description,
  procedure_code,
  COALESCE(procedure_description, 'Unknown procedure') AS procedure_description,
  provider_id,
  provider_name,
  facility_type,
  ROUND(billed_amount, 2) AS billed_amount,
  ROUND(allowed_amount, 2) AS allowed_amount,
  ROUND(paid_amount, 2) AS paid_amount,
  ROUND(member_responsibility, 2) AS member_responsibility,
  ROUND(billed_amount - allowed_amount, 2) AS discount_amount,
  ROUND((billed_amount - allowed_amount) / NULLIF(billed_amount, 0) * 100, 1) AS discount_pct,
  status AS claim_status,
  is_preventive,
  is_chronic_related,
  CASE
    WHEN paid_amount > 10000 THEN 'High Cost'
    WHEN paid_amount > 1000 THEN 'Medium Cost'
    ELSE 'Low Cost'
  END AS cost_tier,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_claims
WHERE claim_id IS NOT NULL
  AND member_id IS NOT NULL
  AND claim_date IS NOT NULL;
