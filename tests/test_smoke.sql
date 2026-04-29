-- Smoke tests for the NBA data pipeline
-- Run these queries after pipeline execution to verify data quality

-- ============================================================
-- 1. BRONZE LAYER TESTS
-- ============================================================

-- Test: Bronze members table has data
SELECT 'bronze_members' AS table_name,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.bronze_members;

-- Test: Bronze claims table has data
SELECT 'bronze_claims' AS table_name,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.bronze_claims;

-- Test: Bronze interactions table has data
SELECT 'bronze_interactions' AS table_name,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.bronze_interactions;

-- ============================================================
-- 2. SILVER LAYER TESTS
-- ============================================================

-- Test: Silver members has cleaned data
SELECT 'silver_members' AS table_name,
       COUNT(*) AS row_count,
       COUNT(DISTINCT member_id) AS unique_members,
       CASE WHEN COUNT(*) > 0 AND COUNT(*) = COUNT(DISTINCT member_id) THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.silver_members;

-- Test: Silver interactions have enrichment columns
SELECT 'silver_interactions_enriched' AS table_name,
       COUNT(*) AS row_count,
       COUNT(sentiment) AS sentiment_count,
       COUNT(inferred_intent) AS intent_count,
       CASE WHEN COUNT(sentiment) > 0 AND COUNT(inferred_intent) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.silver_interactions_enriched;

-- ============================================================
-- 3. GOLD LAYER TESTS
-- ============================================================

-- Test: Member features computed correctly
SELECT 'gold_member_features' AS table_name,
       COUNT(*) AS row_count,
       AVG(churn_risk_score) AS avg_churn_risk,
       AVG(engagement_score) AS avg_engagement,
       CASE
         WHEN COUNT(*) > 0
          AND AVG(churn_risk_score) BETWEEN 0 AND 1
          AND AVG(engagement_score) BETWEEN 0 AND 10
         THEN 'PASS' ELSE 'FAIL'
       END AS status
FROM ${catalog}.${schema}.gold_member_features;

-- Test: Recommendations generated
SELECT 'gold_member_recommendations' AS table_name,
       COUNT(*) AS row_count,
       COUNT(DISTINCT action) AS unique_actions,
       AVG(priority_score) AS avg_priority,
       CASE
         WHEN COUNT(*) > 0 AND COUNT(DISTINCT action) >= 3 THEN 'PASS' ELSE 'FAIL'
       END AS status
FROM ${catalog}.${schema}.gold_member_recommendations;

-- ============================================================
-- 4. SERVING LAYER TESTS
-- ============================================================

-- Test: Serving tables are populated
SELECT 'serving_member_current_features' AS table_name,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.serving_member_current_features;

SELECT 'serving_member_recommendations' AS table_name,
       COUNT(*) AS row_count,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.serving_member_recommendations;

-- ============================================================
-- 5. DATA QUALITY TESTS
-- ============================================================

-- Test: No null member_ids in key tables
SELECT 'null_member_id_check' AS test_name,
       (SELECT COUNT(*) FROM ${catalog}.${schema}.gold_member_features WHERE member_id IS NULL) AS null_count,
       CASE
         WHEN (SELECT COUNT(*) FROM ${catalog}.${schema}.gold_member_features WHERE member_id IS NULL) = 0
         THEN 'PASS' ELSE 'FAIL'
       END AS status;

-- Test: Churn risk scores in valid range
SELECT 'churn_risk_range_check' AS test_name,
       COUNT(*) AS out_of_range_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.gold_member_features
WHERE churn_risk_score < 0 OR churn_risk_score > 1;

-- Test: Engagement scores in valid range
SELECT 'engagement_score_range_check' AS test_name,
       COUNT(*) AS out_of_range_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.gold_member_features
WHERE engagement_score < 0 OR engagement_score > 10;

-- Test: All recommended actions are valid
SELECT 'valid_actions_check' AS test_name,
       COUNT(*) AS invalid_action_count,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM ${catalog}.${schema}.gold_member_recommendations
WHERE action NOT IN (
  'Send Benefit Reminder',
  'Route to Service Agent',
  'Trigger Care Outreach',
  'Push Digital Nudge',
  'Recommend Plan Education',
  'Offer Retention Follow-up'
);

-- ============================================================
-- 6. SUMMARY
-- ============================================================

SELECT 'SMOKE TESTS COMPLETE' AS message,
       current_timestamp() AS completed_at;
