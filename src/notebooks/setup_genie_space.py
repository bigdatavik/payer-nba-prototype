# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Genie Space
# MAGIC
# MAGIC This notebook creates (or recreates) the Genie Space for the Payer NBA Analytics platform.
# MAGIC It handles:
# MAGIC 1. Deleting existing Genie space (if exists) by display name
# MAGIC 2. Creating new Genie space with configured tables
# MAGIC 3. Outputting the new space ID for app configuration
# MAGIC
# MAGIC **Run this after the pipeline has populated the silver/gold tables.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "humana_payer", "Unity Catalog name")
dbutils.widgets.text("schema", "next_best_action", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (optional)")
dbutils.widgets.text("genie_space_name", "Payer Engagement Analytics Copilot", "Genie Space display name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
genie_space_name = dbutils.widgets.get("genie_space_name")

print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Genie Space Name: {genie_space_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Tables to include in the Genie Space
GENIE_TABLES = [
    f"{catalog}.{schema}.silver_members",
    f"{catalog}.{schema}.silver_claims",
    f"{catalog}.{schema}.silver_interactions_enriched",
    f"{catalog}.{schema}.silver_campaign_responses",
    f"{catalog}.{schema}.gold_member_features",
    f"{catalog}.{schema}.gold_member_recommendations",
]

# Description for the Genie Space
GENIE_DESCRIPTION = """Natural language analytics for healthcare payer member engagement, retention, and next-best-action insights. Query member data, recommendations, interactions, and engagement metrics using conversational questions.

## Available Data:
- **Members**: Demographics, plans, segments, tenure
- **Recommendations**: Current NBA recommendations with scores and explanations
- **Interactions**: Call center, digital, and care management touchpoints
- **Features**: Churn risk, engagement scores, channel affinity
- **Campaigns**: Response rates and effectiveness

## Key Metrics:
- Churn risk score (0-1)
- Engagement score (0-10)
- Satisfaction score (1-5)
- Priority score for recommendations (0-1)

## Table Relationships:
- All tables join on member_id
- Recommendations reference member features
- Interactions provide context for recommendations"""

# Sample questions
SAMPLE_QUESTIONS = [
    "How many members do we have by plan type?",
    "What is the average churn risk score by member segment?",
    "Which members have the highest priority recommendations?",
    "Show me members with churn risk above 0.5 in the Northeast region",
    "What are the top reasons for member interactions this month?",
    "How many complaints did we receive by channel?",
    "What is the average satisfaction score by plan type?",
    "Which recommended actions have the highest priority scores?",
    "Show me engagement trends by month for Medicare Advantage members",
    "What percentage of campaigns resulted in conversions?",
    "List members who need care outreach with engagement score below 4",
    "Compare escalation rates between digital and phone channels",
]

# Instructions for the Genie
GENIE_INSTRUCTIONS = """This Genie room queries Payer Member Engagement and Next-Best-Action data. Use it for member retention, engagement, recommendations, and campaign analysis.

## Tables (all join on member_id)

1. **silver_members**: Core member data — one row per member. Columns: member_id, first_name, last_name, email, phone, date_of_birth, gender, address_line1, city, state, zip_code, plan_type, plan_start_date, member_segment, region, primary_care_provider.
2. **silver_claims**: Claims history. Columns: claim_id, member_id, claim_date, service_date, claim_type, diagnosis_code, procedure_code, provider_npi, billed_amount, allowed_amount, paid_amount, member_responsibility, claim_status.
3. **silver_interactions_enriched**: Member interactions with sentiment. Columns: interaction_id, member_id, interaction_date, channel, interaction_type, reason, resolution, duration_minutes, agent_id, sentiment_score, escalated.
4. **silver_campaign_responses**: Campaign outreach tracking. Columns: response_id, member_id, campaign_id, campaign_name, channel, sent_date, response_date, response_type, converted.
5. **gold_member_features**: Computed risk and engagement scores. Columns: member_id, churn_risk_score, engagement_score, satisfaction_score, escalation_likelihood, care_outreach_likelihood, channel_affinity, last_interaction_date, total_interactions, total_claims, avg_claim_amount, _feature_computed_at.
6. **gold_member_recommendations**: NBA recommendations. Columns: member_id, action, reason, priority_score, channel, status, created_at, expires_at.

## Key calculations

- **Churn Risk**: churn_risk_score (0-1), higher = more likely to churn. Display as percentage.
- **Engagement**: engagement_score (0-10), below 4 is low engagement.
- **Satisfaction**: satisfaction_score (1-5), 1-2 indicates dissatisfaction.
- **Priority**: priority_score (0-1), above 0.7 is high priority.

## Member segments
High Value, Rising Risk, Chronic Care, Healthy, New Member

## Plan types
Medicare Advantage, Commercial PPO, Commercial HMO, Medicaid, Individual Exchange

## Channel affinity
Digital First, Phone Preferred, Omnichannel

## Response style
Clear tables/charts; rates as percentages; protect PII."""

# Join specifications for Genie
def get_join_specs(catalog: str, schema: str) -> list:
    """Define table relationships for Genie to use in queries."""
    return [
        {
            "left": {"identifier": f"{catalog}.{schema}.gold_member_features", "alias": "gold_member_features"},
            "right": {"identifier": f"{catalog}.{schema}.silver_members", "alias": "silver_members"},
            "sql": ["`gold_member_features`.`member_id` = `silver_members`.`member_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
        },
        {
            "left": {"identifier": f"{catalog}.{schema}.gold_member_recommendations", "alias": "gold_member_recommendations"},
            "right": {"identifier": f"{catalog}.{schema}.silver_members", "alias": "silver_members"},
            "sql": ["`gold_member_recommendations`.`member_id` = `silver_members`.`member_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
        },
        {
            "left": {"identifier": f"{catalog}.{schema}.silver_claims", "alias": "silver_claims"},
            "right": {"identifier": f"{catalog}.{schema}.silver_members", "alias": "silver_members"},
            "sql": ["`silver_claims`.`member_id` = `silver_members`.`member_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
        },
        {
            "left": {"identifier": f"{catalog}.{schema}.silver_interactions_enriched", "alias": "silver_interactions_enriched"},
            "right": {"identifier": f"{catalog}.{schema}.silver_members", "alias": "silver_members"},
            "sql": ["`silver_interactions_enriched`.`member_id` = `silver_members`.`member_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
        },
        {
            "left": {"identifier": f"{catalog}.{schema}.silver_campaign_responses", "alias": "silver_campaign_responses"},
            "right": {"identifier": f"{catalog}.{schema}.silver_members", "alias": "silver_members"},
            "sql": ["`silver_campaign_responses`.`member_id` = `silver_members`.`member_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
        },
        {
            "left": {"identifier": f"{catalog}.{schema}.gold_member_recommendations", "alias": "gold_member_recommendations"},
            "right": {"identifier": f"{catalog}.{schema}.gold_member_features", "alias": "gold_member_features"},
            "sql": ["`gold_member_recommendations`.`member_id` = `gold_member_features`.`member_id`", "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]
        },
    ]

# Example SQL queries for Genie to learn from
def get_example_sqls(catalog: str, schema: str) -> list:
    """SQL examples that teach Genie how to write queries for this domain."""
    return [
        {
            "question": ["Members by plan type"],
            "sql": [f"SELECT plan_type, COUNT(*) AS member_count FROM {catalog}.{schema}.silver_members GROUP BY plan_type ORDER BY member_count DESC"]
        },
        {
            "question": ["Average churn risk by segment"],
            "sql": [f"SELECT m.member_segment, ROUND(AVG(f.churn_risk_score) * 100, 2) AS avg_churn_risk_pct FROM {catalog}.{schema}.gold_member_features f JOIN {catalog}.{schema}.silver_members m ON f.member_id = m.member_id GROUP BY m.member_segment ORDER BY avg_churn_risk_pct DESC"]
        },
        {
            "question": ["High priority recommendations"],
            "sql": [f"SELECT m.first_name, m.last_name, r.action, r.reason, ROUND(r.priority_score * 100, 2) AS priority_pct FROM {catalog}.{schema}.gold_member_recommendations r JOIN {catalog}.{schema}.silver_members m ON r.member_id = m.member_id WHERE r.priority_score > 0.7 ORDER BY r.priority_score DESC LIMIT 20"]
        },
        {
            "question": ["Members with high churn risk"],
            "sql": [f"SELECT m.member_id, m.first_name, m.last_name, m.plan_type, ROUND(f.churn_risk_score * 100, 2) AS churn_risk_pct FROM {catalog}.{schema}.gold_member_features f JOIN {catalog}.{schema}.silver_members m ON f.member_id = m.member_id WHERE f.churn_risk_score > 0.5 ORDER BY f.churn_risk_score DESC"]
        },
        {
            "question": ["Interaction counts by channel"],
            "sql": [f"SELECT channel, COUNT(*) AS interaction_count, SUM(CASE WHEN escalated = true THEN 1 ELSE 0 END) AS escalated_count FROM {catalog}.{schema}.silver_interactions_enriched GROUP BY channel ORDER BY interaction_count DESC"]
        },
        {
            "question": ["Campaign conversion rate"],
            "sql": [f"SELECT campaign_name, COUNT(*) AS total_sent, SUM(CASE WHEN converted = true THEN 1 ELSE 0 END) AS conversions, ROUND(SUM(CASE WHEN converted = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS conversion_rate_pct FROM {catalog}.{schema}.silver_campaign_responses GROUP BY campaign_name ORDER BY conversion_rate_pct DESC"]
        },
        {
            "question": ["Low engagement members needing outreach"],
            "sql": [f"SELECT m.member_id, m.first_name, m.last_name, f.engagement_score, ROUND(f.care_outreach_likelihood * 100, 2) AS outreach_likelihood_pct FROM {catalog}.{schema}.gold_member_features f JOIN {catalog}.{schema}.silver_members m ON f.member_id = m.member_id WHERE f.engagement_score < 4 ORDER BY f.care_outreach_likelihood DESC LIMIT 20"]
        },
        {
            "question": ["Average satisfaction by plan type"],
            "sql": [f"SELECT m.plan_type, ROUND(AVG(f.satisfaction_score), 2) AS avg_satisfaction FROM {catalog}.{schema}.gold_member_features f JOIN {catalog}.{schema}.silver_members m ON f.member_id = m.member_id GROUP BY m.plan_type ORDER BY avg_satisfaction DESC"]
        },
    ]

# SQL snippets (filters, expressions, measures)
def get_sql_snippets(catalog: str, schema: str) -> dict:
    """SQL snippets for common filters, expressions, and measures."""
    return {
        "filters": [
            {"sql": ["gold_member_features.churn_risk_score > 0.5"], "display_name": "High churn risk", "instruction": ["Filter for members with churn risk above 50%."], "synonyms": ["high risk", "at risk", "likely to churn"]},
            {"sql": ["gold_member_features.engagement_score < 4"], "display_name": "Low engagement", "instruction": ["Filter for members with engagement score below 4 (low engagement)."], "synonyms": ["disengaged", "inactive", "low activity"]},
            {"sql": ["gold_member_recommendations.priority_score > 0.7"], "display_name": "High priority recommendations", "instruction": ["Filter for high priority (>0.7) recommendations."], "synonyms": ["urgent", "priority", "important"]},
            {"sql": ["silver_interactions_enriched.escalated = true"], "display_name": "Escalated interactions", "instruction": ["Filter for interactions that were escalated."], "synonyms": ["escalations", "escalated calls", "supervisor"]},
            {"sql": ["silver_members.plan_type = 'Medicare Advantage'"], "display_name": "Medicare Advantage only", "instruction": ["Filter for Medicare Advantage members."], "synonyms": ["MA", "Medicare", "MA members"]},
            {"sql": ["silver_campaign_responses.converted = true"], "display_name": "Converted campaigns", "instruction": ["Filter for campaigns that resulted in conversions."], "synonyms": ["successful", "converted", "responded positively"]},
        ],
        "expressions": [
            {"sql": ["silver_members.plan_type"], "display_name": "Plan type", "instruction": ["Group by plan type: Medicare Advantage, Commercial PPO, Commercial HMO, Medicaid, Individual Exchange."], "synonyms": ["plan", "product", "coverage type"]},
            {"sql": ["silver_members.member_segment"], "display_name": "Member segment", "instruction": ["Group by segment: High Value, Rising Risk, Chronic Care, Healthy, New Member."], "synonyms": ["segment", "member type", "cohort"]},
            {"sql": ["silver_members.region"], "display_name": "Region", "instruction": ["Group by geographic region."], "synonyms": ["geography", "area", "location"]},
            {"sql": ["gold_member_features.channel_affinity"], "display_name": "Channel affinity", "instruction": ["Group by preferred channel: Digital First, Phone Preferred, Omnichannel."], "synonyms": ["channel preference", "contact preference"]},
            {"sql": ["silver_interactions_enriched.channel"], "display_name": "Interaction channel", "instruction": ["Group by interaction channel."], "synonyms": ["channel", "contact method"]},
            {"sql": ["gold_member_recommendations.action"], "display_name": "Recommended action", "instruction": ["Group by the recommended next-best-action."], "synonyms": ["NBA", "recommendation", "action type"]},
            {"sql": ["DATE_TRUNC('month', silver_interactions_enriched.interaction_date)"], "display_name": "Interaction month", "instruction": ["Group interactions by month."], "synonyms": ["month", "time period"]},
        ],
        "measures": [
            {"sql": ["ROUND(AVG(gold_member_features.churn_risk_score) * 100, 2)"], "display_name": "Avg churn risk %", "instruction": ["Average churn risk as percentage."], "synonyms": ["churn rate", "risk score", "attrition risk"]},
            {"sql": ["ROUND(AVG(gold_member_features.engagement_score), 2)"], "display_name": "Avg engagement score", "instruction": ["Average engagement score (0-10 scale)."], "synonyms": ["engagement", "activity score"]},
            {"sql": ["ROUND(AVG(gold_member_features.satisfaction_score), 2)"], "display_name": "Avg satisfaction", "instruction": ["Average satisfaction score (1-5 scale)."], "synonyms": ["CSAT", "satisfaction", "NPS proxy"]},
            {"sql": ["COUNT(DISTINCT silver_members.member_id)"], "display_name": "Member count", "instruction": ["Count of unique members."], "synonyms": ["members", "count", "total members"]},
            {"sql": ["SUM(CASE WHEN silver_interactions_enriched.escalated = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)"], "display_name": "Escalation rate %", "instruction": ["Percentage of interactions that escalated."], "synonyms": ["escalation %", "escalation rate"]},
            {"sql": ["SUM(CASE WHEN silver_campaign_responses.converted = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)"], "display_name": "Conversion rate %", "instruction": ["Campaign conversion rate as percentage."], "synonyms": ["conversion %", "response rate"]},
        ]
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Databricks SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()
print(f"Connected to workspace: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Find and Delete Existing Genie Space

# COMMAND ----------

def find_genie_space_by_name(client: WorkspaceClient, display_name: str) -> str | None:
    """Find a Genie space by its display name and return its ID."""
    try:
        # List all Genie spaces
        response = client.api_client.do(
            method="GET",
            path="/api/2.0/genie/spaces"
        )

        spaces = response.get("spaces", [])
        for space in spaces:
            if space.get("title") == display_name or space.get("display_name") == display_name:
                return space.get("space_id") or space.get("id")

        return None
    except Exception as e:
        print(f"Error listing Genie spaces: {e}")
        return None


def delete_genie_space(client: WorkspaceClient, space_id: str) -> bool:
    """Delete a Genie space by ID."""
    try:
        client.api_client.do(
            method="DELETE",
            path=f"/api/2.0/genie/spaces/{space_id}"
        )
        print(f"Deleted Genie space: {space_id}")
        return True
    except Exception as e:
        print(f"Error deleting Genie space {space_id}: {e}")
        return False


# Find existing space
existing_space_id = find_genie_space_by_name(w, genie_space_name)

if existing_space_id:
    print(f"Found existing Genie space '{genie_space_name}' with ID: {existing_space_id}")
    print("Deleting existing space...")
    delete_genie_space(w, existing_space_id)
    print("Waiting for deletion to complete...")
    import time
    time.sleep(5)
else:
    print(f"No existing Genie space found with name '{genie_space_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Get SQL Warehouse ID

# COMMAND ----------

def get_default_warehouse_id(client: WorkspaceClient) -> str | None:
    """Get the first available serverless SQL warehouse ID."""
    try:
        warehouses = list(client.warehouses.list())
        # Prefer serverless, then any running warehouse
        for wh in warehouses:
            if "serverless" in wh.name.lower():
                return wh.id
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                return wh.id
        # Just return the first one
        if warehouses:
            return warehouses[0].id
        return None
    except Exception as e:
        print(f"Error getting warehouses: {e}")
        return None


# Get warehouse ID
if not warehouse_id:
    warehouse_id = get_default_warehouse_id(w)
    print(f"Using warehouse: {warehouse_id}")
else:
    print(f"Using provided warehouse: {warehouse_id}")

if not warehouse_id:
    raise ValueError("No SQL warehouse available. Please provide a warehouse_id parameter.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create New Genie Space

# COMMAND ----------

def create_genie_space(
    client: WorkspaceClient,
    display_name: str,
    description: str,
    table_identifiers: list[str],
    warehouse_id: str,
    sample_questions: list[str] = None,
    instructions: str = None,
    join_specs: list = None,
    example_sqls: list = None,
    sql_snippets: dict = None
) -> dict:
    """Create a new Genie space using the serialized_space format with full instructions."""
    import json
    import uuid

    def generate_id():
        """Generate a unique ID for Genie space elements."""
        return uuid.uuid4().hex[:32]

    # Add IDs to join_specs and sort by ID (API requires sorted by ID)
    join_specs_with_ids = sorted(
        [{"id": generate_id(), **spec} for spec in (join_specs or [])],
        key=lambda x: x["id"]
    )

    # Add IDs to example_sqls and sort by ID
    example_sqls_with_ids = sorted(
        [{"id": generate_id(), **ex} for ex in (example_sqls or [])],
        key=lambda x: x["id"]
    )

    # Add IDs to sql_snippets and sort by ID in each category
    snippets = sql_snippets or {"filters": [], "expressions": [], "measures": []}
    snippets_with_ids = {
        "filters": sorted([{"id": generate_id(), **f} for f in snippets.get("filters", [])], key=lambda x: x["id"]),
        "expressions": sorted([{"id": generate_id(), **e} for e in snippets.get("expressions", [])], key=lambda x: x["id"]),
        "measures": sorted([{"id": generate_id(), **m} for m in snippets.get("measures", [])], key=lambda x: x["id"])
    }

    # Build sample questions with IDs and sort by ID
    sample_q = sorted(
        [{"id": generate_id(), "question": [q]} for q in (sample_questions or [])],
        key=lambda x: x["id"]
    )

    # Build the serialized_space structure
    serialized_config = {
        "version": 2,
        "config": {
            "sample_questions": sample_q
        },
        "data_sources": {
            "tables": [
                {"identifier": table}
                for table in sorted(table_identifiers)  # Must be sorted alphabetically
            ]
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": generate_id(),
                    # Each line must end with \n for proper formatting in the UI
                    "content": [line + "\n" for line in instructions.split("\n")] if instructions else []
                }
            ] if instructions else [],
            "example_question_sqls": example_sqls_with_ids,
            "join_specs": join_specs_with_ids,
            "sql_snippets": snippets_with_ids
        }
    }

    # Convert to JSON string
    serialized_space = json.dumps(serialized_config, indent=2)

    payload = {
        "display_name": display_name,
        "title": display_name,  # API uses title field
        "description": description,
        "warehouse_id": warehouse_id,
        "serialized_space": serialized_space,
    }

    try:
        response = client.api_client.do(
            method="POST",
            path="/api/2.0/genie/spaces",
            body=payload
        )
        return response
    except Exception as e:
        print(f"Error creating Genie space: {e}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        raise


# Create the Genie space
print(f"Creating Genie space '{genie_space_name}'...")
print(f"Tables: {GENIE_TABLES}")

# Get the join specs, example SQLs, and snippets for this catalog/schema
join_specs = get_join_specs(catalog, schema)
example_sqls = get_example_sqls(catalog, schema)
sql_snippets = get_sql_snippets(catalog, schema)

print(f"Join specs: {len(join_specs)}")
print(f"Example SQLs: {len(example_sqls)}")
print(f"SQL snippets: {len(sql_snippets['filters'])} filters, {len(sql_snippets['expressions'])} expressions, {len(sql_snippets['measures'])} measures")

result = create_genie_space(
    client=w,
    display_name=genie_space_name,
    description=GENIE_DESCRIPTION,
    table_identifiers=GENIE_TABLES,
    warehouse_id=warehouse_id,
    sample_questions=SAMPLE_QUESTIONS,
    instructions=GENIE_INSTRUCTIONS,
    join_specs=join_specs,
    example_sqls=example_sqls,
    sql_snippets=sql_snippets
)

new_space_id = result.get("space_id") or result.get("id")
print(f"\nGenie space created successfully!")
print(f"Space ID: {new_space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Output Configuration Updates

# COMMAND ----------

# Store the space ID for use in downstream tasks
dbutils.jobs.taskValues.set(key="genie_space_id", value=new_space_id)

print("=" * 60)
print("GENIE SPACE CREATED SUCCESSFULLY")
print("=" * 60)
print(f"\nSpace ID: {new_space_id}")
print(f"Space Name: {genie_space_name}")
print(f"\n## Next Steps:")
print(f"\n1. Update app.yaml with the new space ID:")
print(f"""
resources:
  - name: genie_space
    genie_space:
      space_id: "{new_space_id}"
      permission: "CAN_RUN"

env:
  - name: GENIE_SPACE_ID
    value: "{new_space_id}"
""")

print(f"\n2. Update genie/genie_space_config.json:")
print(f'   "space_id": "{new_space_id}"')

print(f"\n3. Redeploy the app to pick up the new space ID:")
print(f"   databricks apps deploy <app-name> --source-code-path <path>")

print(f"\n4. Grant permissions to the app service principal:")
print(f"   Run the setup_app_permissions.py notebook with genie_space_id={new_space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Update Configuration Files (Optional - if running interactively)

# COMMAND ----------

# This cell updates the local config files if running in a context where file access is available
# Skip if running as a job task

try:
    import os

    # Try to find the project root
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    print(f"Running from: {notebook_path}")

    # If we're in a workspace context, we can't update local files directly
    # The space ID will be passed via task values to downstream tasks
    print("\nRunning in workspace context - configuration files should be updated locally.")
    print(f"Use the space ID: {new_space_id}")

except Exception as e:
    print(f"Could not determine notebook context: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify the space was created correctly
try:
    verify_response = w.api_client.do(
        method="GET",
        path=f"/api/2.0/genie/spaces/{new_space_id}"
    )

    print("Genie Space Details:")
    print(f"  ID: {verify_response.get('space_id') or verify_response.get('id')}")
    print(f"  Name: {verify_response.get('display_name') or verify_response.get('title')}")
    print(f"  Tables: {len(verify_response.get('table_identifiers', []))}")
    print(f"  Sample Questions: {len(verify_response.get('sample_questions', []))}")

except Exception as e:
    print(f"Could not verify space: {e}")

# COMMAND ----------

# Final output
print(f"\n\nGENIE_SPACE_ID={new_space_id}")
