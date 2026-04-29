# Healthcare Payer Next-Best-Action Platform

A complete Databricks prototype for healthcare payer member engagement, featuring a Next-Best-Action recommendation engine, Member 360 console, and natural language analytics via Genie.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         UNITY CATALOG (Governed Source)                  │
│  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐       │
│  │  Bronze  │────▶│  Silver  │────▶│   Gold   │────▶│ Serving  │       │
│  │   Raw    │     │ Cleaned  │     │ Features │     │  Tables  │       │
│  └──────────┘     └──────────┘     └──────────┘     └────┬─────┘       │
└───────────────────────────────────────────────────────────┼─────────────┘
                                                            │ Sync
┌───────────────────────────────────────────────────────────▼─────────────┐
│                         LAKEBASE (Online Serving)                        │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────┐    │
│  │ member_features  │  │  recommendations │  │ interaction_context│    │
│  └──────────────────┘  └──────────────────┘  └────────────────────┘    │
└───────────────────────────────────────────────────────────┬─────────────┘
                                                            │
┌───────────────────────────────────────────────────────────▼─────────────┐
│                              APPLICATIONS                                │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────┐    │
│  │  NBA Console     │  │   Genie Space    │  │  Feedback Loop     │    │
│  │  (Streamlit)     │  │   (Analytics)    │  │  (Back to UC)      │    │
│  └──────────────────┘  └──────────────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Environment Details

| Component | Value |
|-----------|-------|
| Workspace URL | https://fevm-serverless-mwelio-humana.cloud.databricks.com |
| Unity Catalog | `humana_payer` |
| Schema | `next_best_action` |
| Lakebase Instance | `2ba1a370-4b04-4ead-b734-3127ea5d66ec` |
| Lakebase Database | `projects/lakebase-demo-autoscale` |
| Lakebase Schema | `next_best_action_ps` |

## Project Structure

```
payer-nba-prototype/
├── databricks.yml              # Main bundle configuration
├── resources/
│   ├── pipeline.yml            # Declarative pipeline resource
│   ├── app.yml                 # Streamlit app resource
│   └── jobs.yml                # Jobs for data gen, sync, feedback
├── src/
│   ├── pipelines/
│   │   └── nba_pipeline/
│   │       └── transformations/
│   │           ├── bronze/     # Raw data ingestion (8 tables)
│   │           ├── silver/     # Cleaned + enriched (8 tables)
│   │           └── gold/       # Features + recommendations (7 tables)
│   ├── app/
│   │   ├── app.py              # Streamlit NBA Console
│   │   ├── app.yaml            # App configuration
│   │   └── requirements.txt    # Python dependencies
│   ├── scripts/
│   │   └── generate_synthetic_data.py  # Data generation
│   └── notebooks/
│       ├── reset_schemas.py            # Drop and recreate UC schemas
│       ├── sync_to_lakebase.py         # UC to Lakebase sync
│       ├── setup_genie_space.py        # Create/recreate Genie Space
│       ├── setup_app_permissions.py    # Grant app permissions
│       └── feedback_backfill.py        # Lakebase to UC feedback
├── genie/
│   ├── genie_space_config.json # Genie Space configuration
│   ├── create_genie_space.py   # Creation script
│   ├── sample_questions.md     # Example questions
│   └── semantic_instructions.md # Domain guidance
├── tests/
│   ├── test_bundle_validate.sh # Bundle validation tests
│   ├── test_app_local.py       # App unit tests
│   └── test_smoke.sql          # Data quality tests
├── .env.example                # Environment template
└── README.md
```

## Quick Start (One Command!)

### Step 1: Edit Configuration

Edit `config.yml` with your values:

```yaml
# Databricks Workspace
workspace:
  host: "https://YOUR-WORKSPACE.cloud.databricks.com"
  profile: "your-cli-profile"

# Unity Catalog
unity_catalog:
  catalog: "your_catalog"
  schema: "your_schema"

# Lakebase
lakebase:
  project: "your-lakebase-project"
  branch: "production"
  database: "projects/your-project/branches/production/databases/your-db-id"
  host: "your-endpoint.database.us-east-1.cloud.databricks.com"
  schema: "your_lakebase_schema"
```

### Step 2: Run Deploy Script

```bash
# Install yq if needed
brew install yq

# Run full deployment
./deploy.sh
```

That's it! The script will automatically:

| Step | What It Does |
|------|--------------|
| 0. Lakebase | Creates Lakebase project + database (or skips if exists) |
| 1. Validate | Validates bundle configuration |
| 2. Deploy | Deploys bundle resources (pipeline, jobs) |
| 3. Pipeline | Resets schemas + generates data + runs pipeline + **creates Genie Space** |
| 4. Configure | Gets Genie Space ID and updates app configuration |
| 5. App | Creates/deploys app with Lakebase + Genie resources |
| 6. Permissions | Grants Lakebase, Genie, and Unity Catalog permissions |
| 7. URL | Prints app URL and sample Genie questions |

**Everything is automated - including Genie Space creation!**

### Options

```bash
./deploy.sh                # Full deployment with schema reset
./deploy.sh --skip-reset   # Incremental update (no schema reset)
./deploy.sh --app-only     # Only deploy the app
```

---

## Manual Step-by-Step Setup

### 1. Prerequisites

- Databricks CLI v0.285.0+ (for Lakebase Autoscaling support)
- A Databricks workspace with serverless enabled
- Lakebase Autoscaling project (or create one)

### 2. Configure Databricks CLI

```bash
# Set up authentication
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile myprofile

# Verify
databricks auth profiles
```

### 3. Set Up Lakebase (if needed)

```bash
# Create a Lakebase project (auto-creates production branch + primary endpoint)
databricks postgres create-project my-nba-project \
  --json '{"spec": {"display_name": "NBA Prototype"}}' \
  -p myprofile

# Get the endpoint host
databricks postgres list-endpoints projects/my-nba-project/branches/production \
  -p myprofile -o json | jq -r '.[0].status.hosts.host'

# Create a database for the schema
HOST=$(databricks postgres list-endpoints projects/my-nba-project/branches/production -p myprofile -o json | jq -r '.[0].status.hosts.host')
TOKEN=$(databricks postgres generate-database-credential projects/my-nba-project/branches/production/endpoints/primary -p myprofile -o json | jq -r '.token')
EMAIL=$(databricks current-user me -p myprofile -o json | jq -r '.userName')

PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=postgres user=$EMAIL sslmode=require" \
  -c "CREATE DATABASE databricks_postgres;"
```

### 4. Update Variables

Edit `databricks.yml` to set your values:

```yaml
variables:
  catalog: your_catalog
  schema: your_schema
  lakebase_host: YOUR-ENDPOINT.database.us-east-1.cloud.databricks.com
  lakebase_project: your-project-name
  lakebase_database: projects/your-project/branches/production/databases/your-db-id
```

### 5. Validate Bundle

```bash
cd payer-nba-prototype
databricks bundle validate -p myprofile
```

### 6. Deploy Bundle

```bash
# Deploy to dev (default)
databricks bundle deploy -p myprofile

# Or deploy to prod
databricks bundle deploy -t prod -p myprofile
```

### 7. Generate Synthetic Data

```bash
databricks bundle run generate_synthetic_data -p myprofile
```

### 8. Run Data Pipeline

```bash
databricks bundle run run_nba_pipeline -p myprofile
```

### 9. Sync to Lakebase

```bash
databricks bundle run sync_to_lakebase -p myprofile
```

### 10. Create Genie Space

```bash
databricks bundle run setup_genie_space -p myprofile
```

This creates (or recreates) the Genie Space with:
- 6 silver/gold tables for analytics
- 12 sample questions
- Domain-specific semantic instructions

The Genie Space ID will be printed in the output.

### 11. Set Up App Service Principal Permissions

After the app is created, its service principal needs a postgres role:

```bash
# Get the app's service principal client ID
APP_SP=$(databricks apps get payer-nba-console-dev -p myprofile -o json | jq -r '.service_principal_client_id')

# Create postgres role for the service principal
databricks postgres create-role projects/YOUR-PROJECT/branches/production \
  --json "{\"spec\": {\"postgres_role\": \"$APP_SP\", \"identity_type\": \"SERVICE_PRINCIPAL\", \"auth_method\": \"LAKEBASE_OAUTH_V1\", \"membership_roles\": [\"DATABRICKS_WRITER\"]}}" \
  -p myprofile

# Grant schema permissions
PGPASSWORD=$TOKEN psql "host=$HOST port=5432 dbname=databricks_postgres user=$EMAIL sslmode=require" -c "
GRANT USAGE ON SCHEMA next_best_action_ps TO \"$APP_SP\";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA next_best_action_ps TO \"$APP_SP\";
"
```

### 12. Deploy App

```bash
databricks apps deploy payer-nba-console-dev \
  --source-code-path "/Workspace/Users/YOU@company.com/.bundle/payer-nba-prototype/dev/files/src/app" \
  -p myprofile
```

## Data Model

### Bronze Layer (Raw Data)
- `bronze_members` - Raw member demographics
- `bronze_policies` - Raw policy data
- `bronze_claims` - Raw claims transactions
- `bronze_interactions` - Raw member interactions
- `bronze_crm_events` - Raw CRM/marketing events
- `bronze_digital_events` - Raw digital engagement
- `bronze_campaign_responses` - Raw campaign responses
- `bronze_care_management` - Raw care management data

### Silver Layer (Cleaned + Enriched)
- `silver_members` - Cleaned member data with derived fields
- `silver_policies` - Cleaned policy data
- `silver_claims` - Cleaned claims with cost tiers
- `silver_interactions_enriched` - **AI-enriched** using `ai_analyze_sentiment()`, `ai_classify()`, `ai_summarize()`
- `silver_crm_events` - Cleaned CRM events
- `silver_digital_events` - Cleaned digital events
- `silver_campaign_responses` - Cleaned campaign responses
- `silver_care_management` - Cleaned care data

### Gold Layer (Features + Recommendations)
- `gold_member_features` - Feature engineering (scores, affinities)
- `gold_member_recommendations` - NBA recommendations with weighted scoring
- `gold_interaction_context` - Recent interaction context

### Serving Layer (Lakebase Sync)
- `serving_member_current_features` - Synced to Lakebase
- `serving_member_recommendations` - Synced to Lakebase
- `serving_interaction_context` - Synced to Lakebase
- `serving_recommendation_explanations` - **AI-generated** using `ai_gen()` for personalized explanations

## Recommendation Engine

The NBA engine uses weighted scoring across these actions:

| Action | Use Case |
|--------|----------|
| Send Benefit Reminder | Low preventive care utilization |
| Route to Service Agent | High escalation likelihood, complaints |
| Trigger Care Outreach | Rising risk segment, care eligible |
| Push Digital Nudge | Digital-first members |
| Recommend Plan Education | New members, high denied claims |
| Offer Retention Follow-up | High churn risk |

### Feature Inputs
- Churn risk score (0-1)
- Engagement score (0-10)
- Escalation likelihood (0-1)
- Care outreach likelihood (0-1)
- Plan switch propensity (0-1)
- Claims history, interactions, digital engagement

## Application

### Member 360 Console (Streamlit)

Features:
- Member search by name or ID
- Member profile with demographics
- Feature snapshot (risk scores, engagement)
- Top recommendation with explanation
- Recent interactions with sentiment
- Feedback capture (accepted/rejected/completed)
- Priority queue for high-risk members

### Genie Space (Analytics)

The Data Explorer tab provides natural language analytics powered by Databricks Genie. The Genie Space is **automatically created** during deployment with:

- **6 tables**: silver_members, silver_claims, silver_interactions_enriched, silver_campaign_responses, gold_member_features, gold_member_recommendations
- **12 sample questions** for common analytics queries
- **Semantic instructions** for healthcare payer domain terminology

Sample questions:
- "How many members do we have by plan type?"
- "What is the average churn risk score by member segment?"
- "Which members have the highest priority recommendations?"
- "Show me engagement trends by month for Medicare Advantage members"

## Feedback Loop

1. **App writes feedback** to Lakebase `recommendation_feedback` table
2. **Backfill job** syncs feedback from Lakebase to Unity Catalog
3. **Analytics view** (`v_feedback_analysis`) joins feedback with features
4. **Model retraining** can use feedback as outcome labels

## Governance

| Table | PII | PHI | Classification |
|-------|-----|-----|----------------|
| members | Yes | No | Confidential |
| claims | Yes | Yes | Highly Confidential |
| interactions | Yes | Yes | Highly Confidential |
| care_management | Yes | Yes | Highly Confidential |

All data flows through Unity Catalog for governance and lineage.

## Deployment Workflow

### One-Command Deployment (Recommended)

```bash
./deploy.sh
```

### Or Run Full Deployment Job

```bash
# 1. Validate and deploy
databricks bundle validate
databricks bundle deploy

# 2. Run full deployment (includes data gen, pipeline, Lakebase sync, Genie setup)
databricks bundle run full_deployment

# 3. Deploy app
databricks apps deploy payer-nba-console-dev --source-code-path <path>
```

### Individual Jobs

```bash
# Generate synthetic data only
databricks bundle run generate_synthetic_data

# Run pipeline only
databricks bundle run run_nba_pipeline

# Sync to Lakebase only
databricks bundle run sync_to_lakebase

# Create/recreate Genie Space only
databricks bundle run setup_genie_space

# Backfill feedback from Lakebase to UC
databricks bundle run feedback_backfill
```

## Testing

### Bundle Validation
```bash
./tests/test_bundle_validate.sh
```

### Local App Tests
```bash
pip install pytest
python -m pytest tests/test_app_local.py -v
```

### Smoke Tests (after deployment)
Run `tests/test_smoke.sql` in a SQL warehouse to verify data quality.

## AI Functions Used

This project uses **Databricks AI Functions** for real-time AI enrichment directly in SQL:

| Function | Location | Purpose |
|----------|----------|---------|
| `ai_analyze_sentiment()` | Silver interactions | Sentiment analysis from interaction transcripts |
| `ai_classify()` | Silver interactions | Intent classification (claims, benefits, complaints, etc.) |
| `ai_summarize()` | Silver interactions | 30-word interaction summaries |
| `ai_gen()` | Serving explanations | Personalized recommendation explanations and talking points |

**Requirements:**
- Serverless SQL Warehouse (Pro or Serverless)
- Foundation Model API access enabled in workspace
- Workspace in a supported region

**Cost Considerations:**
- AI Functions are billed per token via Foundation Model APIs
- Use `LIMIT` during development to control costs
- Consider caching for production (explanations refresh daily)

## Post-Deployment: App Permissions Setup

> **Note**: If you used `./deploy.sh`, permissions are configured automatically. This section is only needed for manual deployments.

### Option 1: Use deploy.sh (Recommended)

The `deploy.sh` script automatically grants all permissions:
- Unity Catalog: USE_CATALOG, USE_SCHEMA, SELECT
- Genie Space: CAN_RUN
- Lakebase: Postgres role + schema grants

### Option 2: Run the Notebook

Run the `setup_app_permissions` notebook in the Databricks workspace:

```
/Workspace/Users/YOUR-EMAIL/.bundle/payer-nba-prototype/dev/files/src/notebooks/setup_app_permissions
```

### Option 3: Manual Permission Grants

```bash
# Get app service principal ID
APP_SP=$(databricks apps get payer-nba-console-dev -p fevm -o json | jq -r '.service_principal_client_id')

# 1. Unity Catalog permissions (REQUIRED for Genie)
databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID -p fevm \
  --sql "GRANT USE CATALOG ON CATALOG humana_payer TO \`$APP_SP\`"

databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID -p fevm \
  --sql "GRANT USE SCHEMA ON SCHEMA humana_payer.next_best_action TO \`$APP_SP\`"

databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID -p fevm \
  --sql "GRANT SELECT ON SCHEMA humana_payer.next_best_action TO \`$APP_SP\`"

# 2. Genie Space permission (REQUIRED for Data Explorer)
databricks permissions update genie GENIE_SPACE_ID -p fevm \
  --json '{"access_control_list": [{"service_principal_name": "'$APP_SP'", "permission_level": "CAN_RUN"}]}'

# 3. SQL Warehouse permission
databricks permissions update warehouses WAREHOUSE_ID -p fevm \
  --json '{"access_control_list": [{"service_principal_name": "'$APP_SP'", "permission_level": "CAN_USE"}]}'

# 4. Lakebase postgres role
databricks postgres create-role projects/lakebase-demo-autoscale/branches/production \
  --json '{"spec": {"postgres_role": "'$APP_SP'", "identity_type": "SERVICE_PRINCIPAL", "auth_method": "LAKEBASE_OAUTH_V1", "membership_roles": ["DATABRICKS_WRITER"]}}' \
  -p fevm
```

### Option 3: Add Genie Resource to App (Automatic Permissions)

Update the app with a Genie Space resource:

```bash
databricks apps update payer-nba-console-dev -p fevm --json '{
  "resources": [
    {
      "name": "lakebase",
      "postgres": {
        "branch": "projects/lakebase-demo-autoscale/branches/production",
        "database": "projects/lakebase-demo-autoscale/branches/production/databases/YOUR-DB-ID",
        "permission": "CAN_CONNECT_AND_CREATE"
      }
    },
    {
      "name": "genie_space",
      "genie_space": {
        "space_id": "YOUR_GENIE_SPACE_ID",
        "permission": "CAN_RUN"
      }
    }
  ]
}'
```

**Note**: Even with the Genie resource, you still need Unity Catalog permissions (USE_CATALOG, USE_SCHEMA, SELECT) for Genie to query the tables.

---

## Troubleshooting

### Genie "Query failed" or "PERMISSION_DENIED"

**Symptom**: Data Explorer shows "Error: Query failed" or "PERMISSION_DENIED: No access to tables"

**Root Cause**: App service principal lacks Unity Catalog permissions

**Fix**: Grant UC permissions to the app's service principal:
```sql
GRANT USE CATALOG ON CATALOG humana_payer TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT USE SCHEMA ON SCHEMA humana_payer.next_best_action TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT SELECT ON SCHEMA humana_payer.next_best_action TO `APP_SERVICE_PRINCIPAL_ID`;
```

### Genie "PENDING_WAREHOUSE" Timeout

**Symptom**: Genie queries hang and eventually time out

**Root Cause**: SQL warehouse is stopped

**Fix**: Start the warehouse:
```bash
databricks warehouses start WAREHOUSE_ID -p fevm
```

### Member Lookup Returns No Results

**Symptom**: Searching for members returns empty results

**Root Causes**:
1. Sample member IDs don't exist in current dataset
2. Lakebase tables are empty

**Fix**:
1. Use valid member IDs from the current dataset (e.g., MBR00000001)
2. Re-run the sync_to_lakebase job to populate Lakebase

### Lakebase Connection Fails

**Symptom**: App shows "Demo Mode (no database)" or connection errors

**Root Causes**:
1. App service principal lacks postgres role
2. Missing schema grants

**Fix**: Run the setup_app_permissions notebook or manually create the postgres role.

### Pipeline Hangs on AI Functions

**Symptom**: Pipeline runs for hours on `silver_interactions_enriched`

**Root Cause**: AI functions process each row individually (~100-200ms per row)

**Fix**: Reduce data volume in `generate_synthetic_data.py`:
```python
NUM_MEMBERS = 50  # Reduced from 5000
NUM_INTERACTIONS_PER_MEMBER_AVG = 3  # ~150 total interactions
```

---

## Known Limitations

1. **AI Function Costs**: AI enrichment adds Foundation Model API token costs per row
2. **Lakebase Sync**: Requires Lakebase Autoscaling feature enablement
3. **Authentication**: Requires workspace credentials for deployment
4. **Warehouse Auto-Stop**: SQL warehouse may stop after idle timeout, causing Genie to fail

## Next Steps

1. Add A/B testing for recommendation actions
2. Implement real-time sync (CONTINUOUS mode)
3. Add dashboard for feedback analytics
4. Integrate with external care management systems
5. Fine-tune AI prompts for domain-specific responses
