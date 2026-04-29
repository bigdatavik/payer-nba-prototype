# Databricks notebook source
# MAGIC %md
# MAGIC # Feedback Backfill: Lakebase to Unity Catalog
# MAGIC This notebook reads recommendation feedback from Lakebase (written by the app)
# MAGIC and backfills it into Unity Catalog for analytics and model retraining.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "humana_payer")
dbutils.widgets.text("schema", "next_best_action")
dbutils.widgets.text("lakebase_instance_uid", "2ba1a370-4b04-4ead-b734-3127ea5d66ec")
dbutils.widgets.text("lakebase_schema", "next_best_action_ps")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LAKEBASE_INSTANCE_UID = dbutils.widgets.get("lakebase_instance_uid")
LAKEBASE_SCHEMA = dbutils.widgets.get("lakebase_schema")

FEEDBACK_TABLE = f"{CATALOG}.{SCHEMA}.recommendation_feedback"

print(f"Target: {FEEDBACK_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feedback Table (if not exists)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FEEDBACK_TABLE} (
  feedback_id STRING,
  recommendation_id STRING,
  member_id STRING,
  action STRING,
  feedback_type STRING,
  feedback_value STRING,
  agent_id STRING,
  agent_notes STRING,
  feedback_timestamp TIMESTAMP,
  outcome_date DATE,
  outcome_value STRING,
  channel STRING,
  _backfilled_at TIMESTAMP
)
USING DELTA
COMMENT 'Recommendation feedback from NBA app, backfilled from Lakebase'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'feedback_loop',
  'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print(f"Table ready: {FEEDBACK_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import psycopg2
import pandas as pd

w = WorkspaceClient()

# Get Lakebase connection details
# For Databricks Apps, credentials are auto-injected
# For notebooks, we need to get them from the SDK

try:
    # Get database instance info
    db_info = w.database.get(instance_name=LAKEBASE_INSTANCE_UID)

    # Get connection credentials
    creds = w.database.generate_credentials(instance_name=LAKEBASE_INSTANCE_UID)

    PGHOST = db_info.read_write_dns
    PGDATABASE = "databricks_postgres"
    PGUSER = creds.username
    PGPASSWORD = creds.password
    PGPORT = 5432

    print(f"Connected to Lakebase: {PGHOST}")

except Exception as e:
    print(f"Note: Running without live Lakebase connection: {e}")
    print("In production, ensure proper credentials are available.")
    PGHOST = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Feedback from Lakebase

# COMMAND ----------

def read_feedback_from_lakebase(since_timestamp=None):
    """Read new feedback records from Lakebase."""

    if PGHOST is None:
        print("Skipping Lakebase read - no connection available")
        return pd.DataFrame()

    conn = psycopg2.connect(
        host=PGHOST,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        port=PGPORT,
        sslmode="require"
    )

    query = f"""
    SELECT
        feedback_id,
        recommendation_id,
        member_id,
        action,
        feedback_type,
        feedback_value,
        agent_id,
        agent_notes,
        feedback_timestamp,
        outcome_date,
        outcome_value,
        channel
    FROM {LAKEBASE_SCHEMA}.recommendation_feedback
    """

    if since_timestamp:
        query += f" WHERE feedback_timestamp > '{since_timestamp}'"

    query += " ORDER BY feedback_timestamp"

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Read {len(df)} feedback records from Lakebase")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Last Backfill Timestamp

# COMMAND ----------

try:
    last_backfill = spark.sql(f"""
        SELECT MAX(_backfilled_at) as last_ts
        FROM {FEEDBACK_TABLE}
    """).collect()[0]["last_ts"]

    print(f"Last backfill: {last_backfill}")
except Exception as e:
    last_backfill = None
    print(f"No previous backfill found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Backfill

# COMMAND ----------

from datetime import datetime

# Read new feedback
feedback_df = read_feedback_from_lakebase(since_timestamp=last_backfill)

if len(feedback_df) > 0:
    # Add backfill timestamp
    feedback_df["_backfilled_at"] = datetime.now()

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(feedback_df)

    # Append to UC table
    spark_df.write.mode("append").saveAsTable(FEEDBACK_TABLE)

    print(f"Backfilled {len(feedback_df)} records to {FEEDBACK_TABLE}")
else:
    print("No new feedback to backfill")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

stats = spark.sql(f"""
SELECT
    feedback_type,
    feedback_value,
    COUNT(*) as count,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT recommendation_id) as unique_recommendations
FROM {FEEDBACK_TABLE}
GROUP BY feedback_type, feedback_value
ORDER BY count DESC
""")

display(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feedback Analysis View
# MAGIC Create a view for downstream analytics

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_feedback_analysis AS
SELECT
    f.feedback_id,
    f.recommendation_id,
    f.member_id,
    f.action,
    f.feedback_type,
    f.feedback_value,
    f.feedback_timestamp,
    f.outcome_value,

    -- Join with member features for analysis
    m.member_segment,
    m.plan_type,
    m.churn_risk_score,
    m.engagement_score,

    -- Feedback success metrics
    CASE
        WHEN f.feedback_value IN ('accepted', 'completed') THEN 1
        ELSE 0
    END AS is_positive_outcome,

    DATE(f.feedback_timestamp) AS feedback_date,
    HOUR(f.feedback_timestamp) AS feedback_hour,
    DAYOFWEEK(f.feedback_timestamp) AS feedback_dow

FROM {FEEDBACK_TABLE} f
LEFT JOIN {CATALOG}.{SCHEMA}.serving_member_current_features m
    ON f.member_id = m.member_id
""")

print(f"Created view: {CATALOG}.{SCHEMA}.v_feedback_analysis")

# COMMAND ----------

print("\nBackfill complete!")
print(f"""
Feedback data is now available in:
- Table: {FEEDBACK_TABLE}
- View: {CATALOG}.{SCHEMA}.v_feedback_analysis

Use the view for:
- Model retraining with outcome labels
- A/B test analysis
- Recommendation effectiveness reporting
""")
