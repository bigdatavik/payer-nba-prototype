# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Serving Tables to Lakebase
# MAGIC This notebook syncs the curated serving tables from Unity Catalog to Lakebase
# MAGIC for low-latency online access by the NBA application.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters
dbutils.widgets.text("catalog", "humana_payer")
dbutils.widgets.text("schema", "next_best_action")
dbutils.widgets.text("lakebase_host", "ep-orange-scene-d22vm6d5.database.us-east-1.cloud.databricks.com")
dbutils.widgets.text("lakebase_schema", "next_best_action_ps")
dbutils.widgets.text("lakebase_project", "lakebase-demo-autoscale")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LAKEBASE_HOST = dbutils.widgets.get("lakebase_host")
LAKEBASE_SCHEMA = dbutils.widgets.get("lakebase_schema")
LAKEBASE_PROJECT = dbutils.widgets.get("lakebase_project")

print(f"Source: {CATALOG}.{SCHEMA}")
print(f"Target Lakebase Host: {LAKEBASE_HOST}")
print(f"Target Lakebase Schema: {LAKEBASE_SCHEMA}")
print(f"Lakebase Project: {LAKEBASE_PROJECT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables to Sync
# MAGIC
# MAGIC | Source Table (Unity Catalog) | Target Table (Lakebase) | Purpose |
# MAGIC |------------------------------|-------------------------|---------|
# MAGIC | serving_member_current_features | member_current_features | Member feature lookup |
# MAGIC | serving_member_recommendations | member_recommendations | NBA recommendations |
# MAGIC | serving_interaction_context | interaction_context | Recent interactions |
# MAGIC | serving_recommendation_explanations | recommendation_explanations | Explanation text |

# COMMAND ----------

import requests
import psycopg2
from psycopg2.extras import execute_values

# Get workspace context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = ctx.apiUrl().get()
workspace_token = ctx.apiToken().get()
current_user = ctx.userName().get()

print(f"Workspace: {workspace_url}")
print(f"User: {current_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Lakebase Connection

# COMMAND ----------

def get_lakebase_connection():
    """Get connection to Lakebase using REST API to generate OAuth token.

    API Reference: https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/api-usage
    Endpoint: POST /api/2.0/postgres/credentials
    """

    print(f"Lakebase host: {LAKEBASE_HOST}")
    print(f"Connecting as: {current_user}")

    headers = {
        "Authorization": f"Bearer {workspace_token}",
        "Content-Type": "application/json"
    }

    # Generate database credential using the correct REST API endpoint
    # Per docs: POST /api/2.0/postgres/credentials
    endpoint_name = f"projects/{LAKEBASE_PROJECT}/branches/production/endpoints/primary"
    print(f"Generating credential for: {endpoint_name}")

    cred_url = f"{workspace_url}/api/2.0/postgres/credentials"
    cred_resp = requests.post(
        cred_url,
        headers=headers,
        json={"endpoint": endpoint_name}
    )

    if cred_resp.status_code != 200:
        print(f"API Response: {cred_resp.text}")
        raise Exception(f"Failed to generate credential: {cred_resp.status_code} - {cred_resp.text}")

    token = cred_resp.json().get('token') or cred_resp.json().get('password')
    print("Credential generated, connecting to Lakebase...")

    # Create connection using the OAuth token
    conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        database='databricks_postgres',
        user=current_user,
        password=token,
        sslmode='require'
    )
    return conn

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Sync Configuration

# COMMAND ----------

TABLES_TO_SYNC = [
    {
        "source_table": f"{CATALOG}.{SCHEMA}.serving_member_current_features",
        "target_table": "member_current_features",
        "primary_key": "member_id",
        "description": "Member features for NBA lookup"
    },
    {
        "source_table": f"{CATALOG}.{SCHEMA}.serving_member_recommendations",
        "target_table": "member_recommendations",
        "primary_key": "member_id",
        "description": "Current NBA recommendations"
    },
    {
        "source_table": f"{CATALOG}.{SCHEMA}.serving_interaction_context",
        "target_table": "interaction_context",
        "primary_key": "interaction_id",
        "description": "Recent member interactions (up to 5 per member)"
    },
    {
        "source_table": f"{CATALOG}.{SCHEMA}.serving_recommendation_explanations",
        "target_table": "recommendation_explanations",
        "primary_key": "recommendation_id",
        "description": "Recommendation explanations"
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Tables via Direct Insert

# COMMAND ----------

def get_pg_type(spark_type: str) -> str:
    """Map Spark types to PostgreSQL types."""
    type_map = {
        'string': 'TEXT',
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'long': 'BIGINT',
        'double': 'DOUBLE PRECISION',
        'float': 'REAL',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        'decimal': 'NUMERIC',
    }

    # Handle decimal with precision
    if spark_type.startswith('decimal'):
        return spark_type.upper().replace('DECIMAL', 'NUMERIC')

    return type_map.get(spark_type.lower(), 'TEXT')


def sync_table_direct(conn, source_table: str, target_table: str, primary_key: str, description: str):
    """Sync a table from Unity Catalog to Lakebase via direct insert."""
    print(f"\n{'='*60}")
    print(f"Syncing: {source_table} -> {LAKEBASE_SCHEMA}.{target_table}")
    print(f"{'='*60}")

    try:
        # Read source data from Unity Catalog
        print(f"  Reading from Unity Catalog...")
        df = spark.table(source_table)

        row_count = df.count()
        print(f"  Source row count: {row_count}")

        if row_count == 0:
            print(f"  WARNING: Source table is empty, skipping")
            return True

        # Get schema
        schema = df.schema

        # Build CREATE TABLE statement
        columns = []
        for field in schema.fields:
            pg_type = get_pg_type(field.dataType.simpleString())
            nullable = "" if field.nullable else " NOT NULL"
            pk = " PRIMARY KEY" if field.name == primary_key else ""
            columns.append(f'"{field.name}" {pg_type}{nullable}{pk}')

        create_sql = f"""
        DROP TABLE IF EXISTS {LAKEBASE_SCHEMA}.{target_table};
        CREATE TABLE {LAKEBASE_SCHEMA}.{target_table} (
            {', '.join(columns)}
        );
        """

        # Create table in Lakebase
        print(f"  Creating table in Lakebase...")
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

        # Convert to pandas and insert
        print(f"  Loading data into Lakebase...")
        pdf = df.toPandas()

        # Handle NaN/None values for proper insertion
        pdf = pdf.where(pdf.notnull(), None)

        # Prepare insert
        col_names = [f'"{c}"' for c in pdf.columns]
        insert_sql = f"INSERT INTO {LAKEBASE_SCHEMA}.{target_table} ({', '.join(col_names)}) VALUES %s"

        # Convert dataframe to list of tuples, handling numpy types
        values = []
        for _, row in pdf.iterrows():
            row_values = []
            for v in row:
                if v is None:
                    row_values.append(None)
                elif hasattr(v, 'item'):  # numpy types
                    row_values.append(v.item())
                elif str(type(v)).lower().__contains__('nat'):
                    row_values.append(None)
                else:
                    row_values.append(v)
            values.append(tuple(row_values))

        # Batch insert
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=1000)
        conn.commit()

        # Verify
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {LAKEBASE_SCHEMA}.{target_table}")
            count = cur.fetchone()[0]

        print(f"  SUCCESS: {count} rows inserted into {LAKEBASE_SCHEMA}.{target_table}")
        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Sync

# COMMAND ----------

# Connect to Lakebase
print("Connecting to Lakebase...")
conn = get_lakebase_connection()
print("Connected!")

# Ensure schema exists
with conn.cursor() as cur:
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKEBASE_SCHEMA}")
conn.commit()
print(f"Schema {LAKEBASE_SCHEMA} ready")

# COMMAND ----------

results = []

for table_config in TABLES_TO_SYNC:
    success = sync_table_direct(
        conn=conn,
        source_table=table_config["source_table"],
        target_table=table_config["target_table"],
        primary_key=table_config["primary_key"],
        description=table_config["description"]
    )
    results.append({
        "table": table_config["target_table"],
        "source": table_config["source_table"],
        "success": success
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("SYNC SUMMARY")
print("=" * 60)

successful = sum(1 for r in results if r["success"])
failed = sum(1 for r in results if not r["success"])

for r in results:
    status = "OK" if r["success"] else "FAILED"
    print(f"  [{status}] {r['source']} -> {r['table']}")

print(f"\nTotal: {successful} synced, {failed} failed")

if failed > 0:
    print("\nFailed tables may have schema issues. Check the error messages above.")
else:
    print("\nAll tables synced successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Sync

# COMMAND ----------

# Verify the synced tables
print("\nVerifying synced tables:\n")

with conn.cursor() as cur:
    for table_config in TABLES_TO_SYNC:
        target = table_config["target_table"]
        try:
            cur.execute(f"SELECT COUNT(*) FROM {LAKEBASE_SCHEMA}.{target}")
            count = cur.fetchone()[0]
            print(f"  {LAKEBASE_SCHEMA}.{target}: {count} rows")
        except Exception as e:
            print(f"  {LAKEBASE_SCHEMA}.{target}: ERROR - {e}")

# COMMAND ----------

# Close connection
conn.close()
print("\nLakebase connection closed.")
