# Healthcare Payer Next-Best-Action Platform

A complete Databricks prototype for healthcare payer member engagement, featuring a Next-Best-Action recommendation engine, Member 360 console, and natural language analytics via Genie.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Manual Setup](#manual-setup)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Application Features](#application-features)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Features

- **Member 360 Console** - Streamlit app with member search, profiles, and AI recommendations
- **Next-Best-Action Engine** - Weighted scoring across 6 engagement actions
- **Genie Space Integration** - Natural language data exploration
- **Lakebase** - Sub-10ms member lookups via managed PostgreSQL
- **AI Functions** - Sentiment analysis, intent classification, personalized explanations
- **Medallion Architecture** - Bronze/Silver/Gold data pipeline with 23 tables
- **Feedback Loop** - Capture outcomes for model retraining

## Architecture

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

## Prerequisites

Before you begin, ensure you have the following:

### Required Tools

| Tool | Version | Installation |
|------|---------|--------------|
| Databricks CLI | >= 0.285.0 | `brew install databricks` or [download](https://docs.databricks.com/dev-tools/cli/install.html) |
| yq | >= 4.0 | `brew install yq` |
| jq | >= 1.6 | `brew install jq` |
| Python | >= 3.9 | Required for local testing |

### Databricks Workspace Requirements

- Serverless compute enabled
- Unity Catalog enabled
- Foundation Model APIs enabled (for AI Functions)
- Lakebase Autoscaling feature enabled

### Verify CLI Installation

```bash
# Check Databricks CLI version (must be >= 0.285.0)
databricks --version

# Authenticate to your workspace
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com

# Verify authentication
databricks current-user me
```

## Quick Start

### Step 1: Clone the Repository

```bash
git clone https://github.com/bigdatavik/payer-nba-prototype.git
cd payer-nba-prototype
```

### Step 2: Configure Your Environment

Edit `config.yml` with your workspace details:

```yaml
# Databricks Workspace
workspace:
  host: "https://YOUR-WORKSPACE.cloud.databricks.com"
  profile: "DEFAULT"  # Or your CLI profile name

# Unity Catalog
unity_catalog:
  catalog: "your_catalog"
  schema: "next_best_action"

# Lakebase (leave empty to auto-create)
lakebase:
  project: ""  # Will be created as "nba-prototype-lakebase"
  branch: "production"
  host: ""     # Will be populated after creation
  schema: "next_best_action_ps"

# SQL Warehouse (for Genie Space)
sql_warehouse:
  id: "YOUR_WAREHOUSE_ID"  # Required for Genie
```

### Step 3: Run the Deploy Script

```bash
# Make the script executable
chmod +x deploy.sh

# Run full deployment
./deploy.sh
```

The script will automatically:

| Step | Action |
|------|--------|
| 1 | Create Lakebase project (if needed) |
| 2 | Validate bundle configuration |
| 3 | Deploy bundle resources |
| 4 | Generate synthetic data (5,000 members) |
| 5 | Run medallion pipeline (bronze → silver → gold) |
| 6 | Sync serving tables to Lakebase |
| 7 | Create Genie Space with sample questions |
| 8 | Deploy Streamlit app |
| 9 | Configure permissions |

### Step 4: Access the App

After deployment completes, you'll see:

```
========================================
DEPLOYMENT COMPLETE!
========================================
App URL: https://your-app-name.aws.databricksapps.com
Genie Space ID: 01f14366xxxxx
```

Open the App URL in your browser and authenticate with your Databricks credentials.

### Deploy Options

```bash
./deploy.sh                # Full deployment (recommended for first run)
./deploy.sh --skip-reset   # Incremental update (keeps existing data)
./deploy.sh --app-only     # Only redeploy the app
```

## Manual Setup

If you prefer to set up manually or need more control:

### 1. Set Up Lakebase

```bash
# Create a Lakebase project
databricks postgres create-project nba-prototype \
  --json '{"spec": {"display_name": "NBA Prototype"}}'

# Wait for project to be ready, then get the endpoint host
databricks postgres list-endpoints projects/nba-prototype/branches/production \
  -o json | jq -r '.[0].status.hosts.host'
```

### 2. Update Variables

Edit `databricks.yml`:

```yaml
variables:
  catalog:
    default: "your_catalog"
  schema:
    default: "next_best_action"
  lakebase_host:
    default: "YOUR-ENDPOINT.database.us-east-1.cloud.databricks.com"
  lakebase_project:
    default: "nba-prototype"
  lakebase_schema:
    default: "next_best_action_ps"
  warehouse_id:
    default: "YOUR_WAREHOUSE_ID"
```

### 3. Deploy Bundle

```bash
# Validate configuration
databricks bundle validate

# Deploy resources
databricks bundle deploy
```

### 4. Run Jobs

```bash
# Option A: Run full deployment job (all steps)
databricks bundle run full_deployment

# Option B: Run individual jobs
databricks bundle run generate_synthetic_data
databricks bundle run run_nba_pipeline
databricks bundle run sync_to_lakebase
databricks bundle run setup_genie_space
```

### 5. Deploy App

```bash
# Get the workspace files path
WORKSPACE_PATH="/Workspace/Users/$(databricks current-user me -o json | jq -r '.userName')/.bundle/payer-nba-prototype/dev/files/src/app"

# Create and deploy the app
databricks apps create payer-nba-console-dev \
  --json '{"name": "payer-nba-console-dev", "description": "Healthcare Payer NBA Console"}'

databricks apps deploy payer-nba-console-dev \
  --source-code-path "$WORKSPACE_PATH"
```

### 6. Configure Permissions

```bash
# Get app service principal ID
APP_SP=$(databricks apps get payer-nba-console-dev -o json | jq -r '.service_principal_client_id')

# Grant Unity Catalog permissions
databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID \
  --sql "GRANT USE CATALOG ON CATALOG your_catalog TO \`$APP_SP\`"

databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID \
  --sql "GRANT USE SCHEMA ON SCHEMA your_catalog.next_best_action TO \`$APP_SP\`"

databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID \
  --sql "GRANT SELECT ON SCHEMA your_catalog.next_best_action TO \`$APP_SP\`"

# Grant Genie Space permission
databricks permissions update genie YOUR_GENIE_SPACE_ID \
  --json '{"access_control_list": [{"service_principal_name": "'$APP_SP'", "permission_level": "CAN_RUN"}]}'
```

## Project Structure

```
payer-nba-prototype/
├── databricks.yml              # Main bundle configuration
├── config.yml                  # User configuration (edit this)
├── deploy.sh                   # One-command deployment script
├── resources/
│   ├── pipeline.yml            # Spark Declarative Pipeline
│   ├── app.yml                 # Streamlit app resource
│   └── jobs.yml                # Orchestration jobs
├── src/
│   ├── app/
│   │   ├── app.py              # Streamlit NBA Console
│   │   ├── app.yaml            # App configuration
│   │   └── requirements.txt    # Python dependencies
│   ├── pipelines/
│   │   └── nba_pipeline/
│   │       └── transformations/
│   │           ├── bronze/     # Raw data ingestion (8 tables)
│   │           ├── silver/     # Cleaned + AI-enriched (8 tables)
│   │           └── gold/       # Features + recommendations (7 tables)
│   ├── scripts/
│   │   └── generate_synthetic_data.py
│   └── notebooks/
│       ├── reset_schemas.py
│       ├── sync_to_lakebase.py
│       ├── setup_genie_space.py
│       ├── setup_app_permissions.py
│       └── feedback_backfill.py
└── tests/
    ├── test_bundle_validate.sh
    ├── test_app_local.py
    └── test_smoke.sql
```

## Data Model

### Bronze Layer (Raw Data)
| Table | Description |
|-------|-------------|
| bronze_members | Raw member demographics |
| bronze_policies | Raw policy data |
| bronze_claims | Raw claims transactions |
| bronze_interactions | Raw member interactions |
| bronze_crm_events | Raw CRM/marketing events |
| bronze_digital_events | Raw digital engagement |
| bronze_campaign_responses | Raw campaign responses |
| bronze_care_management | Raw care management data |

### Silver Layer (Cleaned + Enriched)
| Table | Description |
|-------|-------------|
| silver_members | Cleaned member data with derived fields |
| silver_policies | Cleaned policy data |
| silver_claims | Cleaned claims with cost tiers |
| silver_interactions_enriched | **AI-enriched** with sentiment, intent, summaries |
| silver_crm_events | Cleaned CRM events |
| silver_digital_events | Cleaned digital events |
| silver_campaign_responses | Cleaned campaign responses |
| silver_care_management | Cleaned care data |

### Gold Layer (Features + Recommendations)
| Table | Description |
|-------|-------------|
| gold_member_features | Feature engineering (scores, affinities) |
| gold_member_recommendations | NBA recommendations with weighted scoring |
| gold_interaction_context | Recent interaction context |

### Serving Layer (Synced to Lakebase)
| Table | Description |
|-------|-------------|
| serving_member_current_features | Member profiles for lookup |
| serving_member_recommendations | Current recommendations |
| serving_interaction_context | Recent interactions |
| serving_recommendation_explanations | **AI-generated** personalized explanations |

## Application Features

### Member 360 Console

The Streamlit app provides:

1. **Member Search** - Search by name or member ID
2. **Member Profile** - Demographics, tenure, flags
3. **Feature Snapshot** - Churn risk, engagement score, claims history
4. **AI Insights** - Sentiment analysis and intent from recent interactions
5. **Next Best Action** - Top recommendation with confidence score and explanation
6. **Feedback Capture** - Accept/Reject/Complete buttons for model improvement
7. **Priority Queue** - High-risk members sorted by priority score

### Data Explorer (Genie)

Natural language analytics powered by Databricks Genie:

- "How many members do we have by segment?"
- "Which members have the highest churn risk?"
- "Show me claims by diagnosis category"
- "What's the average engagement score by plan type?"

### Recommendation Engine

The NBA engine uses weighted scoring across 6 actions:

| Action | Use Case |
|--------|----------|
| Send Benefit Reminder | Low preventive care utilization |
| Route to Service Agent | High escalation likelihood |
| Trigger Care Outreach | Rising risk segment |
| Push Digital Nudge | Digital-first members |
| Recommend Plan Education | New members, high denied claims |
| Offer Retention Follow-up | High churn risk |

## AI Functions Used

| Function | Location | Purpose |
|----------|----------|---------|
| `ai_analyze_sentiment()` | Silver interactions | Sentiment from transcripts |
| `ai_classify()` | Silver interactions | Intent classification |
| `ai_summarize()` | Silver interactions | 30-word summaries |
| `ai_gen()` | Serving explanations | Personalized explanations |

## Testing

### Validate Bundle

```bash
databricks bundle validate
```

### Run Smoke Tests

```bash
# After deployment, run SQL smoke tests
databricks sql query execute --warehouse-id YOUR_WAREHOUSE_ID \
  --file tests/test_smoke.sql
```

### Local App Tests

```bash
pip install pytest streamlit psycopg2-binary
python -m pytest tests/test_app_local.py -v
```

## Troubleshooting

### "Demo Mode" - Lakebase Not Connected

**Cause**: App service principal lacks Lakebase permissions

**Fix**:
```bash
APP_SP=$(databricks apps get payer-nba-console-dev -o json | jq -r '.service_principal_client_id')

databricks postgres create-role projects/YOUR-PROJECT/branches/production \
  --json '{"spec": {"postgres_role": "'$APP_SP'", "identity_type": "SERVICE_PRINCIPAL", "auth_method": "LAKEBASE_OAUTH_V1", "membership_roles": ["DATABRICKS_WRITER"]}}'
```

### Genie "Query Failed" or "Permission Denied"

**Cause**: App service principal lacks Unity Catalog permissions

**Fix**:
```sql
GRANT USE CATALOG ON CATALOG your_catalog TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT USE SCHEMA ON SCHEMA your_catalog.next_best_action TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT SELECT ON SCHEMA your_catalog.next_best_action TO `APP_SERVICE_PRINCIPAL_ID`;
```

### Genie Timeout

**Cause**: SQL warehouse is stopped

**Fix**:
```bash
databricks warehouses start YOUR_WAREHOUSE_ID
```

### Pipeline Hangs on AI Functions

**Cause**: AI functions process each row individually (~100-200ms per row)

**Fix**: Reduce data volume in `generate_synthetic_data.py`:
```python
NUM_MEMBERS = 50  # Reduced from 5000
```

### Member Search Returns No Results

**Cause**: Lakebase tables are empty

**Fix**: Re-run the sync job:
```bash
databricks bundle run sync_to_lakebase
```

## Cost Considerations

- **AI Functions**: Billed per token via Foundation Model APIs
- **Lakebase**: Billed for compute and storage
- **SQL Warehouse**: Billed for Genie queries
- **Serverless Compute**: Billed for pipeline and job runs

For development, use `--skip-reset` to avoid regenerating data.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `databricks bundle validate`
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
