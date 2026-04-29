-- Silver layer: Cleaned and validated member data
CREATE OR REFRESH MATERIALIZED VIEW silver_members
COMMENT 'Cleaned member dimension with validated data quality'
TBLPROPERTIES (
  'quality' = 'silver',
  'contains_pii' = 'true'
)
AS SELECT
  member_id,
  INITCAP(first_name) AS first_name,
  INITCAP(last_name) AS last_name,
  CONCAT(INITCAP(first_name), ' ', INITCAP(last_name)) AS full_name,
  date_of_birth,
  FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) AS age,
  CASE
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) < 18 THEN 'Pediatric'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) < 26 THEN 'Young Adult'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) < 40 THEN 'Adult'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) < 55 THEN 'Middle Age'
    WHEN FLOOR(DATEDIFF(current_date(), date_of_birth) / 365.25) < 65 THEN 'Pre-Senior'
    ELSE 'Senior'
  END AS age_band,
  gender,
  LOWER(email) AS email,
  REGEXP_REPLACE(phone, '[^0-9]', '') AS phone_cleaned,
  address_line1,
  INITCAP(city) AS city,
  UPPER(state) AS state,
  SUBSTRING(zip_code, 1, 5) AS zip_code,
  region,
  plan_type,
  plan_id,
  member_segment,
  enrollment_date,
  FLOOR(DATEDIFF(current_date(), enrollment_date) / 30) AS tenure_months,
  pcp_provider_id,
  communication_preference,
  language_preference,
  has_caregiver,
  is_dual_eligible,
  _ingested_at,
  current_timestamp() AS _processed_at
FROM LIVE.bronze_members
WHERE member_id IS NOT NULL
  AND first_name IS NOT NULL
  AND last_name IS NOT NULL;
