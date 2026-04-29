-- Silver layer: Enriched interactions with AI-derived sentiment and intent
-- Uses Databricks AI Functions for real-time AI enrichment
-- Requires: Serverless SQL Warehouse, Foundation Model API access
-- NOTE: Data volume is kept small (~150 interactions) to ensure fast AI processing

CREATE OR REFRESH MATERIALIZED VIEW silver_interactions_enriched
COMMENT 'Enriched interactions with AI-powered sentiment, intent classification, and summaries'
TBLPROPERTIES (
  'quality' = 'silver',
  'contains_pii' = 'true',
  'ai_enriched' = 'true',
  'ai_functions' = 'ai_analyze_sentiment, ai_classify, ai_summarize'
)
AS SELECT
  interaction_id,
  member_id,
  interaction_date,
  interaction_timestamp,
  interaction_type,
  channel,
  reason,
  sub_reason,
  agent_id,
  handle_time_minutes,
  queue_time_seconds,
  resolution,
  satisfaction_score,
  transcript_notes,
  is_complaint,
  is_escalated,
  follow_up_required,

  -- AI-powered sentiment analysis using Foundation Model APIs
  -- Returns: 'positive', 'negative', 'neutral', or 'mixed'
  COALESCE(
    ai_analyze_sentiment(transcript_notes),
    CASE
      WHEN satisfaction_score >= 4 THEN 'positive'
      WHEN satisfaction_score = 3 THEN 'neutral'
      WHEN satisfaction_score <= 2 THEN 'negative'
      ELSE 'neutral'
    END
  ) AS sentiment,

  -- AI-powered intent classification
  -- Classifies member intent into one of the defined categories
  COALESCE(
    ai_classify(
      CONCAT(reason, ': ', COALESCE(transcript_notes, '')),
      ARRAY(
        'claims_inquiry',
        'benefits_question',
        'provider_search',
        'billing_dispute',
        'enrollment_inquiry',
        'rx_inquiry',
        'complaint',
        'care_coordination',
        'general_inquiry'
      )
    ),
    'general_inquiry'
  ) AS inferred_intent,

  -- Friction/dissatisfaction signal (computed metric)
  CASE
    WHEN is_complaint = true THEN 1.0
    WHEN is_escalated = true THEN 0.8
    WHEN satisfaction_score <= 2 THEN 0.7
    WHEN handle_time_minutes > 30 THEN 0.5
    WHEN queue_time_seconds > 300 THEN 0.4
    WHEN follow_up_required = true THEN 0.3
    ELSE 0.0
  END AS friction_score,

  -- AI-powered interaction summary (concise 30-word summary)
  COALESCE(
    ai_summarize(
      CONCAT(
        'Channel: ', channel, '. ',
        'Reason: ', reason, '. ',
        'Notes: ', COALESCE(transcript_notes, 'No notes recorded.'), ' ',
        'Resolution: ', COALESCE(resolution, 'Pending'), '.'
      ),
      30
    ),
    CONCAT('Member contacted via ', LOWER(channel), ' regarding ', LOWER(reason), '.')
  ) AS interaction_summary,

  _ingested_at,
  current_timestamp() AS _processed_at

FROM LIVE.bronze_interactions
WHERE interaction_id IS NOT NULL
  AND member_id IS NOT NULL;
