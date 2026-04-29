# Databricks notebook source
# MAGIC %md
# MAGIC # Reset Schemas
# MAGIC Drops and recreates Unity Catalog and Lakebase schemas for a clean slate.

# COMMAND ----------

dbutils.widgets.text("catalog", "humana_payer", "Unity Catalog Name")
dbutils.widgets.text("schema", "next_best_action", "Unity Catalog Schema")
dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_schema", "next_best_action_ps", "Lakebase Schema")
dbutils.widgets.text("lakebase_project", "lakebase-demo-autoscale", "Lakebase Project")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_schema = dbutils.widgets.get("lakebase_schema")
lakebase_project = dbutils.widgets.get("lakebase_project")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Unity Catalog Schema

# COMMAND ----------

print(f"Resetting Unity Catalog schema: {catalog}.{schema}")

spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

print(f"Schema {catalog}.{schema} reset successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset Lakebase Schema

# COMMAND ----------

if lakebase_host:
    import subprocess
    import json

    # Generate Lakebase credential
    endpoint_path = f"projects/{lakebase_project}/branches/production/endpoints/primary"

    result = subprocess.run(
        ["databricks", "postgres", "generate-database-credential", endpoint_path, "-o", "json"],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        token = json.loads(result.stdout).get("token")

        # Get current user
        user_result = subprocess.run(
            ["databricks", "current-user", "me", "-o", "json"],
            capture_output=True, text=True
        )
        user_email = json.loads(user_result.stdout).get("userName")

        # Reset Lakebase schema using psql
        import os
        env = os.environ.copy()
        env["PGPASSWORD"] = token

        psql_cmd = [
            "psql",
            f"host={lakebase_host} port=5432 dbname=databricks_postgres user={user_email} sslmode=require",
            "-c", f"DROP SCHEMA IF EXISTS {lakebase_schema} CASCADE; CREATE SCHEMA {lakebase_schema};"
        ]

        psql_result = subprocess.run(psql_cmd, capture_output=True, text=True, env=env)

        if psql_result.returncode == 0:
            print(f"Lakebase schema {lakebase_schema} reset successfully")
        else:
            print(f"Lakebase reset warning: {psql_result.stderr}")
    else:
        print(f"Could not generate Lakebase credential: {result.stderr}")
else:
    print("Lakebase host not provided, skipping Lakebase reset")

# COMMAND ----------

print("Schema reset complete!")
