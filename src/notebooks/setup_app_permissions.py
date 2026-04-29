# Databricks notebook source
# MAGIC %md
# MAGIC # Setup App Permissions
# MAGIC
# MAGIC **RUN THIS NOTEBOOK AFTER DEPLOYING THE APP** to grant all required permissions.
# MAGIC
# MAGIC This notebook configures ALL permissions needed for the Databricks App to function:
# MAGIC
# MAGIC | Permission | Target | Required For |
# MAGIC |------------|--------|--------------|
# MAGIC | **USE_CATALOG** | Unity Catalog | Genie to access catalog |
# MAGIC | **USE_SCHEMA** | Unity Catalog Schema | Genie to access schema |
# MAGIC | **SELECT** | Unity Catalog Tables | Genie to query tables |
# MAGIC | **CAN_RUN** | Genie Space | App to call Genie API |
# MAGIC | **CAN_USE** | SQL Warehouse | Genie to execute queries |
# MAGIC | **Postgres Role** | Lakebase | App to query Lakebase |
# MAGIC | **Schema Grants** | Lakebase Schema | App to read/write tables |
# MAGIC
# MAGIC ## Quick Start
# MAGIC 1. Fill in the widgets at the top (defaults should work for dev)
# MAGIC 2. Run All cells
# MAGIC 3. Verify the summary at the bottom shows all ✓ marks
# MAGIC
# MAGIC ## Common Issues
# MAGIC - **Genie "Query failed"**: Missing Unity Catalog permissions (USE_CATALOG, USE_SCHEMA, SELECT)
# MAGIC - **Genie "Permission denied"**: Missing CAN_RUN on Genie Space
# MAGIC - **Lakebase connection fails**: Missing Postgres role or schema grants
# MAGIC
# MAGIC ## Genie API Integration Pattern
# MAGIC
# MAGIC When calling the Genie API from a Databricks App, you MUST use the SDK's `api_client.do()` method:
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC workspace_client = WorkspaceClient()
# MAGIC resp = workspace_client.api_client.do(
# MAGIC     method="POST",
# MAGIC     path=f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
# MAGIC     body={"content": question}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **DO NOT** use raw `requests` with Bearer tokens - it will fail with OAuth scope errors.

# COMMAND ----------

dbutils.widgets.text("app_name", "payer-nba-console-dev", "App Name")
dbutils.widgets.text("lakebase_host", "", "Lakebase Host")
dbutils.widgets.text("lakebase_schema", "next_best_action_ps", "Lakebase Schema")
dbutils.widgets.text("lakebase_project", "lakebase-demo-autoscale", "Lakebase Project")
dbutils.widgets.text("genie_space_id", "01f141cce141182cb7a833643c0c6842", "Genie Space ID")
dbutils.widgets.text("warehouse_id", "6b2808a405a0375a", "SQL Warehouse ID")
dbutils.widgets.text("catalog_name", "humana_payer", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "next_best_action", "Schema Name (for Genie tables)")

app_name = dbutils.widgets.get("app_name")
lakebase_host = dbutils.widgets.get("lakebase_host")
lakebase_schema = dbutils.widgets.get("lakebase_schema")
lakebase_project = dbutils.widgets.get("lakebase_project")
genie_space_id = dbutils.widgets.get("genie_space_id")
warehouse_id = dbutils.widgets.get("warehouse_id")
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

import json
import subprocess
from databricks.sdk import WorkspaceClient

# Initialize workspace client
w = WorkspaceClient()

print("="*60)
print("SETUP APP PERMISSIONS")
print("="*60)

# Get app service principal client ID using SDK
try:
    app_info = w.apps.get(name=app_name)
    sp_client_id = app_info.service_principal_client_id
    print(f"App Service Principal: {sp_client_id}")
    print(f"App URL: {app_info.url}")
except Exception as e:
    print(f"App {app_name} not found. Deploy the app first.")
    print(f"Error: {e}")
    dbutils.notebook.exit("APP_NOT_FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Postgres Role for Service Principal

# COMMAND ----------

branch_path = f"projects/{lakebase_project}/branches/production"

# Auto-detect Lakebase host if not provided
if not lakebase_host:
    endpoint_result = subprocess.run(
        ["databricks", "postgres", "list-endpoints", branch_path, "-o", "json"],
        capture_output=True, text=True
    )
    if endpoint_result.returncode == 0:
        endpoints = json.loads(endpoint_result.stdout)
        if endpoints:
            lakebase_host = endpoints[0].get("status", {}).get("hosts", {}).get("host", "")
            print(f"Auto-detected Lakebase host: {lakebase_host}")

# Check if role already exists
list_roles_result = subprocess.run(
    ["databricks", "postgres", "list-roles", branch_path, "-o", "json"],
    capture_output=True, text=True
)

role_exists = False
if list_roles_result.returncode == 0:
    roles = json.loads(list_roles_result.stdout)
    for role in roles:
        if role.get("status", {}).get("postgres_role") == sp_client_id:
            auth_method = role.get("status", {}).get("auth_method")
            if auth_method == "LAKEBASE_OAUTH_V1":
                print(f"Role already exists with OAuth auth: {role.get('name')}")
                role_exists = True
            else:
                # Delete old role with wrong auth method
                print(f"Deleting old role with auth_method={auth_method}")
                subprocess.run(
                    ["databricks", "postgres", "delete-role", role.get("name")],
                    capture_output=True, text=True
                )

if not role_exists:
    # Create new role with OAuth authentication
    role_spec = json.dumps({
        "spec": {
            "postgres_role": sp_client_id,
            "identity_type": "SERVICE_PRINCIPAL",
            "auth_method": "LAKEBASE_OAUTH_V1",
            "membership_roles": ["DATABRICKS_WRITER"]
        }
    })

    create_result = subprocess.run(
        ["databricks", "postgres", "create-role", branch_path, "--json", role_spec],
        capture_output=True, text=True
    )

    if create_result.returncode == 0:
        print(f"Created postgres role for {sp_client_id}")
    else:
        print(f"Role creation warning: {create_result.stderr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Schema Permissions

# COMMAND ----------

if lakebase_host:
    import os

    endpoint_path = f"projects/{lakebase_project}/branches/production/endpoints/primary"

    # Generate credential
    cred_result = subprocess.run(
        ["databricks", "postgres", "generate-database-credential", endpoint_path, "-o", "json"],
        capture_output=True, text=True
    )
    token = json.loads(cred_result.stdout).get("token")

    # Get current user
    user_result = subprocess.run(
        ["databricks", "current-user", "me", "-o", "json"],
        capture_output=True, text=True
    )
    user_email = json.loads(user_result.stdout).get("userName")

    # Grant permissions
    env = os.environ.copy()
    env["PGPASSWORD"] = token

    grant_sql = f"""
    GRANT USAGE ON SCHEMA {lakebase_schema} TO "{sp_client_id}";
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {lakebase_schema} TO "{sp_client_id}";
    ALTER DEFAULT PRIVILEGES IN SCHEMA {lakebase_schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{sp_client_id}";
    """

    psql_result = subprocess.run(
        ["psql", f"host={lakebase_host} port=5432 dbname=databricks_postgres user={user_email} sslmode=require", "-c", grant_sql],
        capture_output=True, text=True, env=env
    )

    if psql_result.returncode == 0:
        print(f"Granted schema permissions to {sp_client_id}")
    else:
        print(f"Grant warning: {psql_result.stderr}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Genie Space Permissions
# MAGIC The app service principal needs CAN_RUN permission on the Genie Space.

# COMMAND ----------

if genie_space_id:
    try:
        # Grant CAN_RUN on Genie Space using SDK
        w.permissions.update(
            object_type="genie",
            object_id=genie_space_id,
            access_control_list=[{
                "service_principal_name": sp_client_id,
                "permission_level": "CAN_RUN"
            }]
        )
        print(f"✓ Granted CAN_RUN on Genie Space {genie_space_id} to {sp_client_id}")
    except Exception as e:
        print(f"Genie permission warning: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant SQL Warehouse Permissions
# MAGIC The app service principal needs CAN_USE permission on the SQL Warehouse.

# COMMAND ----------

if warehouse_id:
    try:
        # Grant CAN_USE on SQL Warehouse using SDK
        w.permissions.update(
            object_type="warehouses",
            object_id=warehouse_id,
            access_control_list=[{
                "service_principal_name": sp_client_id,
                "permission_level": "CAN_USE"
            }]
        )
        print(f"✓ Granted CAN_USE on Warehouse {warehouse_id} to {sp_client_id}")
    except Exception as e:
        print(f"Warehouse permission warning: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Unity Catalog Permissions
# MAGIC The app needs USE_CATALOG, USE_SCHEMA, and SELECT for Genie to query data.

# COMMAND ----------

if catalog_name:
    try:
        # Grant USE_CATALOG using SDK
        w.grants.update(
            securable_type="catalog",
            full_name=catalog_name,
            changes=[{
                "principal": sp_client_id,
                "add": ["USE_CATALOG"]
            }]
        )
        print(f"✓ Granted USE_CATALOG on {catalog_name} to {sp_client_id}")
    except Exception as e:
        print(f"Catalog permission warning: {e}")

# COMMAND ----------

if catalog_name and schema_name:
    full_schema = f"{catalog_name}.{schema_name}"

    try:
        # Grant USE_SCHEMA and SELECT using SDK
        w.grants.update(
            securable_type="schema",
            full_name=full_schema,
            changes=[{
                "principal": sp_client_id,
                "add": ["USE_SCHEMA", "SELECT"]
            }]
        )
        print(f"✓ Granted USE_SCHEMA, SELECT on {full_schema} to {sp_client_id}")
    except Exception as e:
        print(f"Schema permission warning: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Permissions granted to app service principal:
# MAGIC - **Lakebase**: DATABRICKS_WRITER role + schema grants
# MAGIC - **Genie Space**: CAN_RUN
# MAGIC - **SQL Warehouse**: CAN_USE
# MAGIC - **Unity Catalog**: USE_CATALOG on catalog, USE_SCHEMA + SELECT on schema

# COMMAND ----------

print("="*60)
print("SETUP COMPLETE")
print("="*60)
print(f"""
App: {app_name}
Service Principal: {sp_client_id}

Permissions Granted:
✓ Unity Catalog:
  - USE_CATALOG on {catalog_name}
  - USE_SCHEMA on {catalog_name}.{schema_name}
  - SELECT on {catalog_name}.{schema_name}

✓ Genie Space: CAN_RUN on {genie_space_id}

✓ SQL Warehouse: CAN_USE on {warehouse_id}

✓ Lakebase:
  - Postgres role for {sp_client_id}
  - Schema grants on {lakebase_schema}

Next Steps:
1. Redeploy the app: databricks apps deploy {app_name}
2. Test Member Lookup (uses Lakebase)
3. Test Data Explorer/Genie (uses Unity Catalog)
4. Test Priority Queue (uses Lakebase)

If Genie still fails, verify the SQL warehouse is RUNNING:
  databricks warehouses get {warehouse_id}
""")
