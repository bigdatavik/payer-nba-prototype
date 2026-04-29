#!/bin/bash
# =============================================================================
# PAYER NBA PROTOTYPE - ONE-COMMAND DEPLOYMENT
# =============================================================================
# Usage: ./deploy.sh [--skip-reset] [--app-only]
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/config.yml"

# Use the newer Databricks CLI (v0.200+)
if [ -f "/opt/homebrew/bin/databricks" ]; then
  DATABRICKS_CLI="/opt/homebrew/bin/databricks"
elif [ -f "/usr/local/bin/databricks" ]; then
  DATABRICKS_CLI="/usr/local/bin/databricks"
else
  DATABRICKS_CLI="databricks"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Parse arguments
SKIP_RESET=false
APP_ONLY=false
for arg in "$@"; do
  case $arg in
    --skip-reset) SKIP_RESET=true ;;
    --app-only) APP_ONLY=true ;;
    --help)
      echo "Usage: ./deploy.sh [--skip-reset] [--app-only]"
      echo "  --skip-reset  Skip schema reset (incremental update)"
      echo "  --app-only    Only deploy the app (skip data pipeline)"
      exit 0
      ;;
  esac
done

# Check dependencies
command -v databricks >/dev/null 2>&1 || { log_error "databricks CLI not found. Install it first."; exit 1; }
command -v yq >/dev/null 2>&1 || { log_error "yq not found. Install with: brew install yq"; exit 1; }

# Read config
log_info "Reading configuration from config.yml..."
WORKSPACE_HOST=$(yq '.workspace.host' "$CONFIG_FILE")
PROFILE=$(yq '.workspace.profile' "$CONFIG_FILE")
CATALOG=$(yq '.unity_catalog.catalog' "$CONFIG_FILE")
SCHEMA=$(yq '.unity_catalog.schema' "$CONFIG_FILE")
LAKEBASE_PROJECT=$(yq '.lakebase.project' "$CONFIG_FILE")
LAKEBASE_BRANCH=$(yq '.lakebase.branch' "$CONFIG_FILE")
LAKEBASE_DATABASE=$(yq '.lakebase.database' "$CONFIG_FILE")
LAKEBASE_HOST=$(yq '.lakebase.host' "$CONFIG_FILE")
LAKEBASE_SCHEMA=$(yq '.lakebase.schema' "$CONFIG_FILE")
APP_PREFIX=$(yq '.app.name_prefix' "$CONFIG_FILE")

log_info "Configuration:"
echo "  Workspace: $WORKSPACE_HOST"
echo "  Profile: $PROFILE"
echo "  Catalog: $CATALOG.$SCHEMA"
echo "  Lakebase: $LAKEBASE_PROJECT ($LAKEBASE_HOST)"
echo ""

# Update databricks.yml with config values
log_info "Updating databricks.yml with config values..."
cat > "$SCRIPT_DIR/databricks.yml" << EOF
bundle:
  name: payer-nba-prototype

include:
  - resources/*.yml

variables:
  catalog:
    description: Unity Catalog catalog name
    default: $CATALOG
  schema:
    description: Unity Catalog schema name
    default: $SCHEMA
  lakebase_host:
    description: Lakebase PostgreSQL endpoint host
    default: $LAKEBASE_HOST
  lakebase_project:
    description: Lakebase autoscaling project name
    default: $LAKEBASE_PROJECT
  lakebase_branch:
    description: Lakebase branch
    default: $LAKEBASE_BRANCH
  lakebase_database:
    description: Lakebase database resource path
    default: $LAKEBASE_DATABASE
  lakebase_schema:
    description: Lakebase schema for serving tables
    default: $LAKEBASE_SCHEMA
  warehouse_id:
    description: SQL Warehouse ID for queries
    lookup:
      warehouse: "Serverless Starter Warehouse"

workspace:
  host: $WORKSPACE_HOST

targets:
  dev:
    default: true
    mode: development

  prod:
    mode: production
EOF

# Update app.py with config values
log_info "Updating app configuration..."
APP_PY="$SCRIPT_DIR/src/app/app.py"
sed -i '' "s|PGHOST = os.getenv(\"PGHOST\", \".*\")|PGHOST = os.getenv(\"PGHOST\", \"$LAKEBASE_HOST\")|" "$APP_PY"
sed -i '' "s|LAKEBASE_ENDPOINT = \".*\"|LAKEBASE_ENDPOINT = \"projects/$LAKEBASE_PROJECT/branches/$LAKEBASE_BRANCH/endpoints/primary\"|" "$APP_PY"
sed -i '' "s|DATABRICKS_HOST = os.getenv(\"DATABRICKS_HOST\", \".*\")|DATABRICKS_HOST = os.getenv(\"DATABRICKS_HOST\", \"${WORKSPACE_HOST#https://}\")|" "$APP_PY"

# Step 0: Check/Create Lakebase project (if needed)
log_info "Step 0/7: Checking Lakebase setup..."
LAKEBASE_EXISTS=$($DATABRICKS_CLI postgres get-project "projects/$LAKEBASE_PROJECT" -p "$PROFILE" 2>/dev/null && echo "yes" || echo "no")

if [ "$LAKEBASE_EXISTS" = "no" ]; then
  log_warn "Lakebase project '$LAKEBASE_PROJECT' does not exist."
  log_info "Creating Lakebase project..."

  $DATABRICKS_CLI postgres create-project "$LAKEBASE_PROJECT" \
    --json '{"spec": {"display_name": "NBA Prototype Lakebase"}}' \
    -p "$PROFILE"

  log_info "Waiting for Lakebase to be ready (this may take 1-2 minutes)..."
  sleep 60

  # Get the new endpoint host
  NEW_HOST=$($DATABRICKS_CLI postgres list-endpoints "projects/$LAKEBASE_PROJECT/branches/production" -p "$PROFILE" -o json | jq -r '.[0].status.hosts.host')

  if [ -n "$NEW_HOST" ] && [ "$NEW_HOST" != "null" ]; then
    log_info "Lakebase endpoint: $NEW_HOST"
    LAKEBASE_HOST="$NEW_HOST"

    # Update config.yml with new host
    yq -i ".lakebase.host = \"$NEW_HOST\"" "$CONFIG_FILE"

    # Get database path
    TOKEN=$($DATABRICKS_CLI postgres generate-database-credential "projects/$LAKEBASE_PROJECT/branches/production/endpoints/primary" -p "$PROFILE" -o json | jq -r '.token')
    USER_EMAIL=$($DATABRICKS_CLI current-user me -p "$PROFILE" -o json | jq -r '.userName')

    # Create database
    log_info "Creating database..."
    PGPASSWORD=$TOKEN psql "host=$NEW_HOST port=5432 dbname=postgres user=$USER_EMAIL sslmode=require" \
      -c "CREATE DATABASE databricks_postgres;" 2>/dev/null || true

    log_success "Lakebase project created"
  else
    log_error "Failed to get Lakebase endpoint. Please create manually."
    exit 1
  fi
else
  log_success "Lakebase project exists"
fi

# Step 1: Validate bundle
log_info "Step 1/7: Validating bundle..."
$DATABRICKS_CLI bundle validate -p "$PROFILE"
log_success "Bundle validated"

# Step 2: Deploy bundle
log_info "Step 2/7: Deploying bundle..."
$DATABRICKS_CLI bundle deploy -p "$PROFILE"
log_success "Bundle deployed"

if [ "$APP_ONLY" = true ]; then
  log_info "Skipping data pipeline (--app-only mode)"
else
  # Step 3: Run full deployment job (includes Genie space setup)
  if [ "$SKIP_RESET" = true ]; then
    log_info "Step 3/7: Running pipeline (skip reset)..."
    $DATABRICKS_CLI bundle run run_nba_pipeline -p "$PROFILE"
    $DATABRICKS_CLI bundle run sync_to_lakebase -p "$PROFILE"
    $DATABRICKS_CLI bundle run setup_genie_space -p "$PROFILE"
  else
    log_info "Step 3/7: Running full deployment (with schema reset + Genie setup)..."
    $DATABRICKS_CLI bundle run full_deployment -p "$PROFILE"
  fi
  log_success "Data pipeline and Genie space complete"
fi

# Step 4: Get Genie space ID and update app configuration
log_info "Step 4/7: Getting Genie space ID..."
GENIE_SPACE_NAME="Payer Engagement Analytics Copilot"
GENIE_SPACE_ID=$($DATABRICKS_CLI api get /api/2.0/genie/spaces -p "$PROFILE" 2>/dev/null | jq -r ".spaces[] | select(.title == \"$GENIE_SPACE_NAME\") | .space_id" 2>/dev/null || echo "")

if [ -n "$GENIE_SPACE_ID" ] && [ "$GENIE_SPACE_ID" != "null" ]; then
  log_success "Found Genie space: $GENIE_SPACE_ID"

  # Update app.yaml with the new Genie space ID
  APP_YAML="$SCRIPT_DIR/src/app/app.yaml"
  log_info "Updating app.yaml with Genie space ID..."

  # Update the genie_space resource
  sed -i '' "s|space_id: \".*\"|space_id: \"$GENIE_SPACE_ID\"|g" "$APP_YAML"

  # Update the GENIE_SPACE_ID env var
  sed -i '' "s|value: \"[a-f0-9]*\"|value: \"$GENIE_SPACE_ID\"|g" "$APP_YAML"

  # Also update app.py default value
  sed -i '' "s|GENIE_SPACE_ID = os.getenv(\"GENIE_SPACE_ID\", \".*\")|GENIE_SPACE_ID = os.getenv(\"GENIE_SPACE_ID\", \"$GENIE_SPACE_ID\")|" "$APP_PY"

  # Update genie_space_config.json
  GENIE_CONFIG="$SCRIPT_DIR/genie/genie_space_config.json"
  if [ -f "$GENIE_CONFIG" ]; then
    # Add or update space_id in config
    jq --arg id "$GENIE_SPACE_ID" '. + {space_id: $id}' "$GENIE_CONFIG" > "${GENIE_CONFIG}.tmp" && mv "${GENIE_CONFIG}.tmp" "$GENIE_CONFIG"
  fi

  log_success "App configuration updated with Genie space ID"
else
  log_warn "Could not find Genie space. Data Explorer may not work."
  log_warn "You can manually create a Genie space and update app.yaml"
fi

# Redeploy bundle to sync updated app.yaml
log_info "Redeploying bundle with updated configuration..."
$DATABRICKS_CLI bundle deploy -p "$PROFILE"

# Step 5: Deploy app
log_info "Step 5/7: Deploying app..."
APP_NAME="${APP_PREFIX}-dev"
CURRENT_USER=$($DATABRICKS_CLI current-user me -p "$PROFILE" -o json | jq -r '.userName')
SOURCE_PATH="/Workspace/Users/$CURRENT_USER/.bundle/payer-nba-prototype/dev/files/src/app"

# Check if app exists, create if not
APP_EXISTS=$($DATABRICKS_CLI apps get "$APP_NAME" -p "$PROFILE" 2>/dev/null && echo "yes" || echo "no")

if [ "$APP_EXISTS" = "no" ]; then
  log_info "Creating app $APP_NAME..."
  $DATABRICKS_CLI apps create "$APP_NAME" \
    --json "{\"name\": \"$APP_NAME\", \"description\": \"Healthcare Payer Next-Best-Action Member 360 Console\"}" \
    -p "$PROFILE"

  # Wait for app to be ready
  sleep 5

  # Add Lakebase and Genie resources to app
  log_info "Adding Lakebase resource to app..."
  if [ -n "$GENIE_SPACE_ID" ] && [ "$GENIE_SPACE_ID" != "null" ]; then
    $DATABRICKS_CLI apps update "$APP_NAME" \
      --json "{\"resources\": [{\"name\": \"lakebase\", \"postgres\": {\"branch\": \"projects/$LAKEBASE_PROJECT/branches/$LAKEBASE_BRANCH\", \"database\": \"$LAKEBASE_DATABASE\", \"permission\": \"CAN_CONNECT_AND_CREATE\"}}, {\"name\": \"genie_space\", \"genie_space\": {\"space_id\": \"$GENIE_SPACE_ID\", \"permission\": \"CAN_RUN\"}}]}" \
      -p "$PROFILE" 2>/dev/null || true
  else
    $DATABRICKS_CLI apps update "$APP_NAME" \
      --json "{\"resources\": [{\"name\": \"lakebase\", \"postgres\": {\"branch\": \"projects/$LAKEBASE_PROJECT/branches/$LAKEBASE_BRANCH\", \"database\": \"$LAKEBASE_DATABASE\", \"permission\": \"CAN_CONNECT_AND_CREATE\"}}]}" \
      -p "$PROFILE" 2>/dev/null || true
  fi
fi

$DATABRICKS_CLI apps deploy "$APP_NAME" --source-code-path "$SOURCE_PATH" -p "$PROFILE"
log_success "App deployed"

# Step 6: Setup app permissions for Lakebase and Genie
log_info "Step 6/7: Setting up app permissions..."
APP_SP=$($DATABRICKS_CLI apps get "$APP_NAME" -p "$PROFILE" -o json | jq -r '.service_principal_client_id')

# Check if postgres role exists, create if not
ROLE_EXISTS=$($DATABRICKS_CLI postgres list-roles "projects/$LAKEBASE_PROJECT/branches/$LAKEBASE_BRANCH" -p "$PROFILE" -o json | jq -r ".[] | select(.status.postgres_role == \"$APP_SP\") | .status.auth_method")

if [ "$ROLE_EXISTS" != "LAKEBASE_OAUTH_V1" ]; then
  log_info "Creating postgres role for service principal..."
  $DATABRICKS_CLI postgres create-role "projects/$LAKEBASE_PROJECT/branches/$LAKEBASE_BRANCH" \
    --json "{\"spec\": {\"postgres_role\": \"$APP_SP\", \"identity_type\": \"SERVICE_PRINCIPAL\", \"auth_method\": \"LAKEBASE_OAUTH_V1\", \"membership_roles\": [\"DATABRICKS_WRITER\"]}}" \
    -p "$PROFILE" 2>/dev/null || true
fi

# Grant schema permissions
log_info "Granting schema permissions..."
TOKEN=$($DATABRICKS_CLI postgres generate-database-credential "projects/$LAKEBASE_PROJECT/branches/$LAKEBASE_BRANCH/endpoints/primary" -p "$PROFILE" -o json | jq -r '.token')
PGPASSWORD=$TOKEN psql "host=$LAKEBASE_HOST port=5432 dbname=databricks_postgres user=$CURRENT_USER sslmode=require" -c "
GRANT USAGE ON SCHEMA $LAKEBASE_SCHEMA TO \"$APP_SP\";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA $LAKEBASE_SCHEMA TO \"$APP_SP\";
ALTER DEFAULT PRIVILEGES IN SCHEMA $LAKEBASE_SCHEMA GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"$APP_SP\";
" 2>/dev/null || true

# Grant Genie space permissions if we have a space ID
if [ -n "$GENIE_SPACE_ID" ] && [ "$GENIE_SPACE_ID" != "null" ]; then
  log_info "Granting Genie space permissions..."
  $DATABRICKS_CLI permissions update genie "$GENIE_SPACE_ID" \
    --json "{\"access_control_list\": [{\"service_principal_name\": \"$APP_SP\", \"permission_level\": \"CAN_RUN\"}]}" \
    -p "$PROFILE" 2>/dev/null || true

  # Grant Unity Catalog permissions for Genie to query tables
  log_info "Granting Unity Catalog permissions for Genie..."

  # Get a working warehouse ID - prefer serverless
  SQL_WAREHOUSE_ID=$($DATABRICKS_CLI warehouses list -p "$PROFILE" -o json | jq -r '[.[] | select(.name | contains("Serverless") or contains("serverless"))] | .[0].id // empty')
  if [ -z "$SQL_WAREHOUSE_ID" ]; then
    SQL_WAREHOUSE_ID=$($DATABRICKS_CLI warehouses list -p "$PROFILE" -o json | jq -r '.[0].id // empty')
  fi

  if [ -n "$SQL_WAREHOUSE_ID" ]; then
    log_info "Using SQL warehouse: $SQL_WAREHOUSE_ID"

    # Grant USE CATALOG
    log_info "Granting USE CATALOG to app service principal..."
    $DATABRICKS_CLI api post /api/2.0/sql/statements -p "$PROFILE" \
      --json "{\"warehouse_id\": \"$SQL_WAREHOUSE_ID\", \"statement\": \"GRANT USE CATALOG ON CATALOG $CATALOG TO \\\`$APP_SP\\\`\", \"wait_timeout\": \"30s\"}" 2>/dev/null || log_warn "USE CATALOG grant may have failed"

    # Grant USE SCHEMA
    log_info "Granting USE SCHEMA to app service principal..."
    $DATABRICKS_CLI api post /api/2.0/sql/statements -p "$PROFILE" \
      --json "{\"warehouse_id\": \"$SQL_WAREHOUSE_ID\", \"statement\": \"GRANT USE SCHEMA ON SCHEMA $CATALOG.$SCHEMA TO \\\`$APP_SP\\\`\", \"wait_timeout\": \"30s\"}" 2>/dev/null || log_warn "USE SCHEMA grant may have failed"

    # Grant SELECT on all tables in schema
    log_info "Granting SELECT on schema to app service principal..."
    $DATABRICKS_CLI api post /api/2.0/sql/statements -p "$PROFILE" \
      --json "{\"warehouse_id\": \"$SQL_WAREHOUSE_ID\", \"statement\": \"GRANT SELECT ON SCHEMA $CATALOG.$SCHEMA TO \\\`$APP_SP\\\`\", \"wait_timeout\": \"30s\"}" 2>/dev/null || log_warn "SELECT grant may have failed"

    log_success "Unity Catalog grants submitted"
  else
    log_warn "No SQL warehouse found - UC grants will need to be applied manually"
    log_warn "Run: GRANT USE CATALOG ON CATALOG $CATALOG TO \`$APP_SP\`"
    log_warn "Run: GRANT USE SCHEMA ON SCHEMA $CATALOG.$SCHEMA TO \`$APP_SP\`"
    log_warn "Run: GRANT SELECT ON SCHEMA $CATALOG.$SCHEMA TO \`$APP_SP\`"
  fi
fi

log_success "App permissions configured"

# Step 7: Get app URL
log_info "Step 7/7: Getting app URL..."
APP_URL=$($DATABRICKS_CLI apps get "$APP_NAME" -p "$PROFILE" -o json | jq -r '.url')

echo ""
echo "=============================================="
log_success "DEPLOYMENT COMPLETE!"
echo "=============================================="
echo ""
echo "App URL: $APP_URL"
echo ""
if [ -n "$GENIE_SPACE_ID" ] && [ "$GENIE_SPACE_ID" != "null" ]; then
  echo "Genie Space ID: $GENIE_SPACE_ID"
  echo ""
fi
echo "Sample searches to try:"
echo "  - Morgan"
echo "  - MBR00000222"
echo "  - Karen"
echo ""
echo "Data Explorer (Genie) sample questions:"
echo "  - How many members do we have by plan type?"
echo "  - What is the average churn risk score by member segment?"
echo "  - Which members have the highest priority recommendations?"
echo ""
