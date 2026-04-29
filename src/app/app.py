"""
Healthcare Payer Next-Best-Action Console
Member 360 + Recommendation Engine

This Streamlit app connects to Lakebase for low-latency member lookups
and recommendation delivery.
"""

import os
import time
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import requests

# =============================================================================
# Configuration
# =============================================================================

# Lakebase connection settings - try DB_CONNECTION_STRING first, then individual vars
DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING", "")
PGHOST = os.getenv("PGHOST", "ep-orange-scene-d22vm6d5.database.us-east-1.cloud.databricks.com")
PGDATABASE = os.getenv("PGDATABASE", "databricks_postgres")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGPORT = os.getenv("PGPORT", "5432")

# Lakebase endpoint for credential generation (autoscaling tier)
LAKEBASE_ENDPOINT = "projects/lakebase-demo-autoscale/branches/production/endpoints/primary"
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "fevm-serverless-mwelio-humana.cloud.databricks.com")

# Schema configuration
LAKEBASE_SCHEMA = os.getenv("LAKEBASE_SCHEMA", "next_best_action_ps")

# App configuration
APP_TITLE = "Healthcare Payer NBA Console"
PAGE_ICON = "healthcare"

# Genie Space configuration - MUST be set via env var in app.yaml
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")
if not GENIE_SPACE_ID:
    raise ValueError("GENIE_SPACE_ID environment variable is required. Set it in app.yaml.")

# Credential cache
_credential_cache = {"token": None, "user": None, "expires": 0, "error": None}


def parse_connection_string(conn_str: str) -> dict:
    """Parse a PostgreSQL connection string into components."""
    # Format: postgresql://user:pass@host:port/database
    result = {"host": None, "port": "5432", "database": None, "user": None, "password": None}
    if not conn_str or "://" not in conn_str:
        return result
    try:
        from urllib.parse import urlparse
        parsed = urlparse(conn_str)
        result["host"] = parsed.hostname
        result["port"] = str(parsed.port) if parsed.port else "5432"
        result["database"] = parsed.path.lstrip("/") if parsed.path else None
        result["user"] = parsed.username
        result["password"] = parsed.password
    except Exception:
        pass
    return result


def get_databricks_host() -> str:
    """Get the Databricks workspace host, cleaned of protocol prefixes."""
    try:
        from databricks.sdk.core import Config
        cfg = Config()
        host = cfg.host or DATABRICKS_HOST
    except Exception:
        host = DATABRICKS_HOST

    # Clean the host - remove any protocol prefixes and trailing slashes
    if host:
        host = host.replace("https://", "").replace("http://", "").rstrip("/")
    return host or "fevm-serverless-mwelio-humana.cloud.databricks.com"

# =============================================================================
# Database Connection
# =============================================================================

def get_user_token():
    """Get user's access token from Streamlit headers (user auth)."""
    try:
        return st.context.headers.get("x-forwarded-access-token")
    except Exception:
        return None


def get_current_user_email() -> str:
    """Get current user's email from Databricks API."""
    try:
        import requests
        host = get_databricks_host()
        user_token = get_user_token()
        if user_token:
            resp = requests.get(
                f"https://{host}/api/2.0/preview/scim/v2/Me",
                headers={"Authorization": f"Bearer {user_token}"},
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json().get("userName", "user")
    except Exception:
        pass
    return "user"


def generate_lakebase_credential():
    """Generate Lakebase credential using app's service principal."""
    global _credential_cache

    # Check cache (tokens valid for ~1 hour, refresh at 50 min)
    if _credential_cache.get("expires", 0) > time.time():
        return _credential_cache.get("token"), _credential_cache.get("user")

    try:
        import requests
        from databricks.sdk.core import Config

        host = get_databricks_host()
        if not host or host == "https" or len(host) < 5:
            _credential_cache["error"] = f"Invalid host: {host}"
            return None, None

        # Use app's service principal credentials (not user token)
        # Service principal has Lakebase resource bound with CAN_CONNECT_AND_CREATE
        cfg = Config()
        auth_headers = cfg.authenticate()
        auth_headers["Content-Type"] = "application/json"

        # Get service principal identity for postgres user
        sp_client_id = cfg.client_id or os.getenv("DATABRICKS_CLIENT_ID", "service-principal")

        # Generate postgres credential via REST API
        resp = requests.post(
            f"https://{host}/api/2.0/postgres/credentials",
            headers=auth_headers,
            json={"endpoint": LAKEBASE_ENDPOINT},
            timeout=15
        )

        if resp.status_code != 200:
            _credential_cache["error"] = f"API {resp.status_code}: {resp.text[:80]}"
            return None, None

        data = resp.json()
        db_token = data.get("token") or data.get("password")

        if not db_token:
            _credential_cache["error"] = "No token in response"
            return None, None

        # Cache for 50 minutes
        _credential_cache.update({
            "token": db_token,
            "user": sp_client_id,
            "expires": time.time() + 3000,
            "error": None
        })

        return db_token, sp_client_id

    except Exception as e:
        _credential_cache["error"] = f"Error: {str(e)[:70]}"
        return None, None


def get_db_connection():
    """Create a database connection to Lakebase."""
    global _credential_cache

    # Option 1: Use DB_CONNECTION_STRING if provided (from valueFrom)
    if DB_CONNECTION_STRING:
        parsed = parse_connection_string(DB_CONNECTION_STRING)
        if all([parsed.get("host"), parsed.get("user"), parsed.get("password")]):
            try:
                return psycopg2.connect(
                    host=parsed["host"],
                    database=parsed.get("database") or "postgres",
                    user=parsed["user"],
                    password=parsed["password"],
                    port=parsed.get("port", "5432"),
                    sslmode="require"
                )
            except Exception as e:
                _credential_cache["error"] = f"ConnStr: {str(e)[:60]}"

    # Option 2: Use injected PG* credentials if all available
    if all([PGHOST, PGUSER, PGPASSWORD]):
        try:
            return psycopg2.connect(
                host=PGHOST,
                database=PGDATABASE,
                user=PGUSER,
                password=PGPASSWORD,
                port=PGPORT,
                sslmode="require"
            )
        except Exception as e:
            _credential_cache["error"] = f"PG env: {str(e)[:60]}"

    # Option 3: Generate credentials dynamically via REST API using service principal
    db_token, sp_id = generate_lakebase_credential()
    if db_token and sp_id:
        try:
            return psycopg2.connect(
                host=PGHOST,
                database=PGDATABASE,
                user=sp_id,
                password=db_token,
                port=PGPORT,
                sslmode="require"
            )
        except Exception as e:
            _credential_cache["error"] = f"Gen creds: {str(e)[:60]}"

    return None


# Cache the connection at module level
_db_connection = None


def get_cached_connection():
    """Get or create cached database connection."""
    global _db_connection
    if _db_connection is None:
        _db_connection = get_db_connection()
    # Check if connection is still valid
    if _db_connection is not None:
        try:
            with _db_connection.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            _db_connection = get_db_connection()
    return _db_connection


def execute_query(query: str, params: tuple = None) -> List[Dict]:
    """Execute a query and return results as list of dicts."""
    conn = get_cached_connection()
    if conn is None:
        # Return mock data for demo purposes
        return []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            return [dict(row) for row in results]
    except Exception as e:
        st.error(f"Database error: {e}")
        return []


def execute_insert(query: str, params: tuple) -> bool:
    """Execute an insert/update query."""
    conn = get_cached_connection()
    if conn is None:
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Database error: {e}")
        conn.rollback()
        return False


# =============================================================================
# Mock Data for Demo Mode
# =============================================================================

def get_mock_members() -> List[Dict]:
    """Return mock member data for demo mode when Lakebase is unavailable."""
    return [
        {
            "member_id": "MBR00000001",
            "full_name": "John Smith",
            "plan_type": "Medicare Advantage",
            "member_segment": "High Value",
            "region": "Northeast",
            "state": "NY",
            "age": 67,
            "age_band": "65-74",
            "gender": "Male",
            "tenure_months": 36,
            "language_preference": "English",
            "communication_preference": "Phone",
            "is_dual_eligible": False,
            "has_caregiver": True,
            "active_care_program": "Chronic Care",
            "churn_risk_score": 0.72,
            "engagement_score": 4.2,
            "total_claims_12m": 23,
            "total_paid_12m": 15420.50,
            "channel_affinity": "Phone",
            "escalation_likelihood": 0.45,
            "care_outreach_likelihood": 0.68,
            "plan_switch_propensity": 0.35,
        },
        {
            "member_id": "MBR00000002",
            "full_name": "Sarah Johnson",
            "plan_type": "Medicaid",
            "member_segment": "At Risk",
            "region": "Southeast",
            "state": "FL",
            "age": 45,
            "age_band": "45-54",
            "gender": "Female",
            "tenure_months": 18,
            "language_preference": "Spanish",
            "communication_preference": "SMS",
            "is_dual_eligible": True,
            "has_caregiver": False,
            "active_care_program": None,
            "churn_risk_score": 0.85,
            "engagement_score": 2.8,
            "total_claims_12m": 45,
            "total_paid_12m": 28750.00,
            "channel_affinity": "Digital",
            "escalation_likelihood": 0.62,
            "care_outreach_likelihood": 0.78,
            "plan_switch_propensity": 0.55,
        },
        {
            "member_id": "MBR00000003",
            "full_name": "Michael Williams",
            "plan_type": "Commercial PPO",
            "member_segment": "Standard",
            "region": "Midwest",
            "state": "IL",
            "age": 38,
            "age_band": "35-44",
            "gender": "Male",
            "tenure_months": 24,
            "language_preference": "English",
            "communication_preference": "Email",
            "is_dual_eligible": False,
            "has_caregiver": False,
            "active_care_program": None,
            "churn_risk_score": 0.25,
            "engagement_score": 7.5,
            "total_claims_12m": 8,
            "total_paid_12m": 3200.00,
            "channel_affinity": "Email",
            "escalation_likelihood": 0.15,
            "care_outreach_likelihood": 0.22,
            "plan_switch_propensity": 0.18,
        },
    ]


def get_mock_recommendation(member_id: str) -> Optional[Dict]:
    """Return mock recommendation for demo mode."""
    mock_recs = {
        "MBR00000001": {
            "recommendation_id": "REC-DEMO-001",
            "member_id": "MBR00000001",
            "action": "Trigger Care Outreach",
            "priority_score": 0.85,
            "preferred_channel": "Phone",
            "confidence_pct": 87,
            "explanation": "John has shown declining engagement over the past 3 months with a high churn risk score of 72%. His recent claim for chronic condition management suggests he would benefit from proactive care coordination. Recommended outreach via phone based on his channel preference.",
            "score_benefit_reminder": 0.45,
            "score_service_agent": 0.62,
            "score_care_outreach": 0.85,
            "score_digital_nudge": 0.38,
            "score_plan_education": 0.52,
            "score_retention_followup": 0.71,
            "model_version": "v2.3.1-demo",
            "generated_at": "2024-01-15 10:30:00",
            "valid_until": "2024-01-22 10:30:00",
        },
        "MBR00000002": {
            "recommendation_id": "REC-DEMO-002",
            "member_id": "MBR00000002",
            "action": "Offer Retention Follow-up",
            "priority_score": 0.92,
            "preferred_channel": "SMS",
            "confidence_pct": 91,
            "explanation": "Sarah is flagged as high churn risk (85%) with dual eligibility complexity. Recent negative sentiment in interactions indicates dissatisfaction. Immediate retention outreach via SMS (her preferred channel) is critical to prevent disenrollment.",
            "score_benefit_reminder": 0.55,
            "score_service_agent": 0.78,
            "score_care_outreach": 0.72,
            "score_digital_nudge": 0.48,
            "score_plan_education": 0.65,
            "score_retention_followup": 0.92,
            "model_version": "v2.3.1-demo",
            "generated_at": "2024-01-15 10:30:00",
            "valid_until": "2024-01-22 10:30:00",
        },
        "MBR00000003": {
            "recommendation_id": "REC-DEMO-003",
            "member_id": "MBR00000003",
            "action": "Push Digital Nudge",
            "priority_score": 0.45,
            "preferred_channel": "Email",
            "confidence_pct": 78,
            "explanation": "Michael is a low-risk, digitally engaged member. A gentle reminder about preventive care benefits via email can increase utilization and reinforce positive engagement patterns.",
            "score_benefit_reminder": 0.42,
            "score_service_agent": 0.25,
            "score_care_outreach": 0.28,
            "score_digital_nudge": 0.45,
            "score_plan_education": 0.38,
            "score_retention_followup": 0.22,
            "model_version": "v2.3.1-demo",
            "generated_at": "2024-01-15 10:30:00",
            "valid_until": "2024-01-22 10:30:00",
        },
    }
    return mock_recs.get(member_id)


def get_mock_interactions(member_id: str) -> List[Dict]:
    """Return mock interactions for demo mode."""
    mock_interactions = {
        "MBR00000001": [
            {
                "interaction_date": "2024-01-10",
                "interaction_type": "Call",
                "channel": "Phone",
                "reason": "Benefits inquiry",
                "is_complaint": False,
                "sentiment": "neutral",
                "inferred_intent": "information_seeking",
                "interaction_summary": "Member called to understand coverage for upcoming specialist visit. Resolved successfully.",
            },
            {
                "interaction_date": "2024-01-05",
                "interaction_type": "Portal",
                "channel": "Digital",
                "reason": "Claim status check",
                "is_complaint": False,
                "sentiment": "positive",
                "inferred_intent": "self_service",
                "interaction_summary": "Member checked claim status online. No issues reported.",
            },
        ],
        "MBR00000002": [
            {
                "interaction_date": "2024-01-12",
                "interaction_type": "Call",
                "channel": "Phone",
                "reason": "Billing dispute",
                "is_complaint": True,
                "sentiment": "negative",
                "inferred_intent": "complaint_resolution",
                "interaction_summary": "Member frustrated about unexpected bill. Escalated to supervisor. Pending resolution.",
            },
            {
                "interaction_date": "2024-01-08",
                "interaction_type": "Chat",
                "channel": "Digital",
                "reason": "Provider search",
                "is_complaint": False,
                "sentiment": "neutral",
                "inferred_intent": "provider_lookup",
                "interaction_summary": "Member searched for in-network specialists via chat. Provided list of 5 nearby providers.",
            },
        ],
        "MBR00000003": [
            {
                "interaction_date": "2024-01-11",
                "interaction_type": "Email",
                "channel": "Email",
                "reason": "Preventive care reminder",
                "is_complaint": False,
                "sentiment": "positive",
                "inferred_intent": "wellness_engagement",
                "interaction_summary": "Member responded positively to annual wellness visit reminder. Scheduled appointment.",
            },
        ],
    }
    return mock_interactions.get(member_id, [])


def is_demo_mode() -> bool:
    """Check if app is running in demo mode (no database connection)."""
    return get_cached_connection() is None


# =============================================================================
# Genie API Functions
# =============================================================================
#
# IMPORTANT: Genie API Integration Pattern
# ----------------------------------------
# When calling Genie from a Databricks App, you MUST use:
#   workspace_client.api_client.do(method, path, body)
#
# DO NOT use raw requests with Bearer tokens - this fails with:
#   "OAuth token does not have required scopes: genie"
#
# Required permissions (run setup_app_permissions.py notebook):
#   - CAN_RUN on Genie Space
#   - CAN_USE on SQL Warehouse
#   - USE_CATALOG on Unity Catalog
#   - USE_SCHEMA + SELECT on schema
#
# =============================================================================

def get_databricks_token() -> Optional[str]:
    """Get Databricks access token for API calls using WorkspaceClient (like healthcare-payor-ai-pa)."""
    try:
        # Use WorkspaceClient approach - this works in the other project
        from databricks.sdk import WorkspaceClient
        workspace_client = WorkspaceClient()

        # First try to get token directly from config
        token = workspace_client.config.token
        if token:
            return token

        # If no token, try authenticate() method
        token = workspace_client.config.authenticate()
        if isinstance(token, dict):
            # authenticate() returns headers dict
            auth_header = token.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                return auth_header.replace("Bearer ", "")
        elif isinstance(token, str):
            return token

    except Exception as e:
        # Log for debugging
        import sys
        print(f"WorkspaceClient auth failed: {e}", file=sys.stderr)

    # Fallback: try user token from headers
    try:
        user_token = get_user_token()
        if user_token:
            return user_token
    except Exception:
        pass

    return None


def ask_genie(question: str) -> Dict[str, Any]:
    """Ask a question to the Genie Space using SDK's api_client (like healthcare-payor-ai-pa)."""
    try:
        from databricks.sdk import WorkspaceClient

        # Use SDK's api_client which handles auth properly
        workspace_client = WorkspaceClient()

        # Start conversation using SDK's do() method
        start_resp = workspace_client.api_client.do(
            method="POST",
            path=f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
            body={"content": question}
        )

        if not start_resp:
            return {"error": "Empty response from Genie API"}

        conversation_id = start_resp.get("conversation_id")
        message_id = start_resp.get("message_id")

        if not conversation_id or not message_id:
            return {"error": f"Missing conversation or message ID. Response: {start_resp}"}

        # Poll for result (max 60 seconds)
        for _ in range(30):
            time.sleep(2)

            msg_resp = workspace_client.api_client.do(
                method="GET",
                path=f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
            )

            if not msg_resp:
                continue

            status = msg_resp.get("status")

            if status == "COMPLETED":
                return parse_genie_response(msg_resp, workspace_client)
            elif status in ["FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"]:
                # Extract detailed error from response - check multiple locations
                error_msg = ""

                # Check top-level error
                if msg_resp.get("error"):
                    error_msg = msg_resp.get("error", {}).get("message", str(msg_resp.get("error")))

                # Check attachments for errors
                attachments = msg_resp.get("attachments", [])
                for att in attachments:
                    if "error" in att:
                        err_obj = att.get("error", {})
                        error_msg = err_obj.get("message", str(err_obj)) if isinstance(err_obj, dict) else str(err_obj)
                    # Also check for query errors
                    if "query" in att:
                        query_obj = att.get("query", {})
                        if query_obj.get("error"):
                            error_msg = query_obj.get("error", {}).get("message", str(query_obj.get("error")))

                # If still no error, include the full response for debugging
                if not error_msg:
                    import json
                    error_msg = f"Full response: {json.dumps(msg_resp, default=str)[:500]}"

                return {"error": f"Query {status.lower()}: {error_msg}"}

        return {"error": "Query timed out"}

    except Exception as e:
        import traceback
        return {"error": f"{str(e)}\n{traceback.format_exc()}"}


def parse_genie_response(msg_data: Dict, workspace_client=None) -> Dict[str, Any]:
    """Parse the Genie response into a structured format."""
    result = {
        "question": msg_data.get("content", ""),
        "sql": None,
        "description": None,
        "data": None,
        "columns": None,
        "row_count": 0,
        "suggested_questions": [],
        "text_response": None
    }

    attachments = msg_data.get("attachments", [])
    statement_id = None

    for attachment in attachments:
        # Query attachment
        if "query" in attachment:
            query_data = attachment["query"]
            result["sql"] = query_data.get("query")
            result["description"] = query_data.get("description")
            # Get statement_id from query attachment
            statement_id = query_data.get("statement_id")

            # Get row count from metadata
            metadata = query_data.get("query_result_metadata", {})
            result["row_count"] = metadata.get("row_count", 0)

        # Suggested questions
        if "suggested_questions" in attachment:
            result["suggested_questions"] = attachment["suggested_questions"].get("questions", [])

        # Text response (summary)
        if "text" in attachment:
            text_content = attachment["text"].get("content", "")
            if text_content:
                result["text_response"] = text_content

    # Fetch actual query results if we have a statement_id
    if statement_id and workspace_client:
        result["data"], result["columns"] = fetch_genie_results(statement_id, workspace_client)

    return result


def fetch_genie_results(statement_id: str, workspace_client=None) -> tuple:
    """Fetch the actual query results from a Genie statement using SDK's api_client."""
    try:
        if workspace_client is None:
            from databricks.sdk import WorkspaceClient
            workspace_client = WorkspaceClient()

        # Use SDK's api_client.do() for consistent auth
        data = workspace_client.api_client.do(
            method="GET",
            path=f"/api/2.0/sql/statements/{statement_id}"
        )

        if not data:
            return None, None

        # Extract columns
        manifest = data.get("manifest", {})
        schema = manifest.get("schema", {})
        columns = [col.get("name") for col in schema.get("columns", [])]

        # Extract data
        result_data = data.get("result", {})
        rows = result_data.get("data_array", [])

        return rows, columns

    except Exception as e:
        import sys
        print(f"Error fetching Genie results: {e}", file=sys.stderr)
        return None, None


# =============================================================================
# Data Access Functions
# =============================================================================

def search_members(search_term: str) -> List[Dict]:
    """Search for members by name or ID."""
    # Demo mode: return filtered mock data
    if is_demo_mode():
        search_lower = search_term.lower()
        return [
            m for m in get_mock_members()
            if search_lower in m['full_name'].lower() or search_lower in m['member_id'].lower()
        ]

    query = f"""
    SELECT member_id, full_name, plan_type, member_segment, region, state
    FROM {LAKEBASE_SCHEMA}.member_current_features
    WHERE LOWER(full_name) LIKE LOWER(%s)
       OR LOWER(member_id) LIKE LOWER(%s)
    ORDER BY full_name
    LIMIT 20
    """
    return execute_query(query, (f"%{search_term}%", f"%{search_term}%"))


def get_member_features(member_id: str) -> Optional[Dict]:
    """Get member features by ID."""
    # Demo mode: return mock data
    if is_demo_mode():
        for m in get_mock_members():
            if m['member_id'] == member_id:
                return m
        return None

    query = f"""
    SELECT *
    FROM {LAKEBASE_SCHEMA}.member_current_features
    WHERE member_id = %s
    """
    results = execute_query(query, (member_id,))
    return results[0] if results else None


def get_member_recommendation(member_id: str) -> Optional[Dict]:
    """Get current recommendation for a member."""
    # Demo mode: return mock recommendation
    if is_demo_mode():
        return get_mock_recommendation(member_id)

    query = f"""
    SELECT *
    FROM {LAKEBASE_SCHEMA}.member_recommendations
    WHERE member_id = %s
    ORDER BY priority_score DESC
    LIMIT 1
    """
    results = execute_query(query, (member_id,))
    return results[0] if results else None


def get_recommendation_explanation(recommendation_id: str) -> Optional[Dict]:
    """Get explanation for a recommendation."""
    query = f"""
    SELECT *
    FROM {LAKEBASE_SCHEMA}.recommendation_explanations
    WHERE recommendation_id = %s
    """
    results = execute_query(query, (recommendation_id,))
    return results[0] if results else None


def get_recent_interactions(member_id: str, limit: int = 5) -> List[Dict]:
    """Get recent interactions for a member."""
    # Demo mode: return mock interactions
    if is_demo_mode():
        return get_mock_interactions(member_id)[:limit]

    query = f"""
    SELECT *
    FROM {LAKEBASE_SCHEMA}.interaction_context
    WHERE member_id = %s
    ORDER BY recency_rank
    LIMIT %s
    """
    return execute_query(query, (member_id, limit))


def record_feedback(
    recommendation_id: str,
    member_id: str,
    action: str,
    feedback_type: str,
    feedback_value: str,
    agent_id: str = None,
    agent_notes: str = None,
    channel: str = "Console"
) -> bool:
    """Record feedback on a recommendation."""
    # Create feedback table if not exists
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {LAKEBASE_SCHEMA}.recommendation_feedback (
        feedback_id VARCHAR(50) PRIMARY KEY,
        recommendation_id VARCHAR(100),
        member_id VARCHAR(20),
        action VARCHAR(100),
        feedback_type VARCHAR(50),
        feedback_value VARCHAR(50),
        agent_id VARCHAR(20),
        agent_notes TEXT,
        feedback_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        outcome_date DATE,
        outcome_value VARCHAR(50),
        channel VARCHAR(50)
    )
    """
    execute_insert(create_query, ())

    # Insert feedback
    feedback_id = f"FB-{uuid.uuid4().hex[:12].upper()}"
    insert_query = f"""
    INSERT INTO {LAKEBASE_SCHEMA}.recommendation_feedback
    (feedback_id, recommendation_id, member_id, action, feedback_type,
     feedback_value, agent_id, agent_notes, channel)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    return execute_insert(insert_query, (
        feedback_id, recommendation_id, member_id, action, feedback_type,
        feedback_value, agent_id, agent_notes, channel
    ))


def get_high_priority_members(limit: int = 10) -> List[Dict]:
    """Get members with highest priority recommendations."""
    # Demo mode: return mock priority queue
    if is_demo_mode():
        mock_members = get_mock_members()
        priority_list = []
        for m in mock_members:
            rec = get_mock_recommendation(m['member_id'])
            if rec:
                priority_list.append({
                    "member_id": m['member_id'],
                    "full_name": m['full_name'],
                    "action": rec['action'],
                    "priority_score": rec['priority_score'],
                    "preferred_channel": rec['preferred_channel'],
                    "churn_risk_score": m['churn_risk_score'],
                    "plan_type": m['plan_type'],
                    "member_segment": m['member_segment'],
                })
        # Sort by priority score descending
        priority_list.sort(key=lambda x: x['priority_score'], reverse=True)
        return priority_list[:limit]

    query = f"""
    SELECT
        r.member_id,
        r.full_name,
        r.action,
        r.priority_score,
        r.preferred_channel,
        f.churn_risk_score,
        f.plan_type,
        f.member_segment
    FROM {LAKEBASE_SCHEMA}.member_recommendations r
    JOIN {LAKEBASE_SCHEMA}.member_current_features f
        ON r.member_id = f.member_id
    ORDER BY r.priority_score DESC
    LIMIT %s
    """
    return execute_query(query, (limit,))


# =============================================================================
# UI Components
# =============================================================================

def render_member_search():
    """Render the member search interface."""
    st.subheader("Member Search")

    search_term = st.text_input(
        "Search by name or member ID",
        placeholder="Enter member name or ID...",
        key="member_search"
    )

    # Show sample searches when no search term entered
    if not search_term:
        st.markdown("---")
        st.markdown("**Try these sample searches:**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**By Name:**")
            st.code("John")
            st.code("Sarah")
            st.code("Michael")
        with col2:
            st.markdown("**By Member ID:**")
            st.code("MBR00000001")
            st.code("MBR00000025")
            st.code("MBR00000050")
        with col3:
            st.markdown("**Partial Match:**")
            st.code("Smith")
            st.code("Williams")
            st.code("MBR000000")

    if search_term and len(search_term) >= 2:
        results = search_members(search_term)

        if results:
            st.write(f"Found {len(results)} members")

            for member in results:
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.write(f"**{member['full_name']}**")
                    st.caption(f"ID: {member['member_id']}")
                with col2:
                    st.write(f"{member['plan_type']}")
                    st.caption(f"{member['region']} - {member['state']}")
                with col3:
                    if st.button("View", key=f"view_{member['member_id']}"):
                        st.session_state.selected_member = member['member_id']
                        st.rerun()
        else:
            st.info("No members found. Try a different search term.")


def render_member_360(member_id: str):
    """Render the Member 360 view with clear KNOW → PREDICT → ACT storytelling."""
    member = get_member_features(member_id)
    recommendation = get_member_recommendation(member_id)
    interactions = get_recent_interactions(member_id)

    if not member:
        st.error(f"Member not found: {member_id}")
        return

    # Header with back button
    col_back, col_title = st.columns([1, 11])
    with col_back:
        if st.button("← Back"):
            st.session_state.selected_member = None
            st.rerun()
    with col_title:
        st.title(f"{member['full_name']}")
        st.caption(f"Member ID: {member['member_id']} | {member['plan_type']} | {member['member_segment']}")

    # ==========================================================================
    # SECTION 1: MEMBER 360 - What we KNOW
    # ==========================================================================
    st.markdown("""
    <div style="background: linear-gradient(90deg, #1E3A5F 0%, #2E5077 100%); padding: 10px 20px; border-radius: 8px; margin: 20px 0 10px 0;">
        <h2 style="color: white; margin: 0; font-size: 1.3em;">📊 MEMBER 360 — What We Know</h2>
        <p style="color: #B8C9D9; margin: 5px 0 0 0; font-size: 0.9em;">Unified view from claims, interactions, digital engagement, and care management</p>
    </div>
    """, unsafe_allow_html=True)

    # Two-column layout for profile and features
    col1, col2 = st.columns([1, 1])

    with col1:
        render_member_profile(member)

    with col2:
        render_feature_snapshot(member)

    # ==========================================================================
    # SECTION 2: AI INSIGHTS - What we PREDICT
    # ==========================================================================
    st.markdown("""
    <div style="background: linear-gradient(90deg, #5B2C6F 0%, #7D3C98 100%); padding: 10px 20px; border-radius: 8px; margin: 30px 0 10px 0;">
        <h2 style="color: white; margin: 0; font-size: 1.3em;">🤖 AI INSIGHTS — What We Predict</h2>
        <p style="color: #D7BDE2; margin: 5px 0 0 0; font-size: 0.9em;">Powered by Databricks AI Functions: ai_analyze_sentiment, ai_classify, ai_summarize</p>
    </div>
    """, unsafe_allow_html=True)

    render_interactions_with_ai(interactions)

    # ==========================================================================
    # SECTION 3: NEXT BEST ACTION - What to DO
    # ==========================================================================
    st.markdown("""
    <div style="background: linear-gradient(90deg, #1D6F42 0%, #28A745 100%); padding: 10px 20px; border-radius: 8px; margin: 30px 0 10px 0;">
        <h2 style="color: white; margin: 0; font-size: 1.3em;">🎯 NEXT BEST ACTION — What To Do Now</h2>
        <p style="color: #C3E6CB; margin: 5px 0 0 0; font-size: 0.9em;">AI-generated recommendation with personalized explanation (ai_gen)</p>
    </div>
    """, unsafe_allow_html=True)

    render_recommendation_card(recommendation, member)

    # Expandable details
    with st.expander("📈 View All Action Scores"):
        if recommendation:
            render_recommendation_details(recommendation)
        else:
            st.info("No active recommendation for this member.")


def render_member_profile(member: Dict):
    """Render member profile card."""
    st.subheader("Member Profile")

    # Demographics
    st.markdown(f"""
    | Attribute | Value |
    |-----------|-------|
    | **Age** | {member.get('age', 'N/A')} ({member.get('age_band', 'N/A')}) |
    | **Gender** | {member.get('gender', 'N/A')} |
    | **Region** | {member.get('region', 'N/A')} - {member.get('state', 'N/A')} |
    | **Tenure** | {member.get('tenure_months', 0)} months |
    | **Language** | {member.get('language_preference', 'English')} |
    | **Comm Pref** | {member.get('communication_preference', 'N/A')} |
    """)

    # Flags
    flags = []
    if member.get('is_dual_eligible'):
        flags.append("Dual Eligible")
    if member.get('has_caregiver'):
        flags.append("Has Caregiver")
    if member.get('active_care_program'):
        flags.append(f"Care: {member['active_care_program']}")

    if flags:
        st.markdown("**Flags:** " + " | ".join(flags))


def render_recommendation_card(recommendation: Optional[Dict], member: Dict):
    """Render the primary recommendation card with AI-generated explanation."""

    if not recommendation:
        st.info("No active recommendation for this member.")
        return

    # Priority indicator
    priority = recommendation.get('priority_score', 0)
    if priority >= 0.8:
        priority_color = "#DC3545"
        priority_bg = "#F8D7DA"
        priority_label = "HIGH PRIORITY"
    elif priority >= 0.5:
        priority_color = "#FD7E14"
        priority_bg = "#FFE5D0"
        priority_label = "MEDIUM PRIORITY"
    else:
        priority_color = "#28A745"
        priority_bg = "#D4EDDA"
        priority_label = "STANDARD"

    # Two columns: Action card + AI Explanation
    col1, col2 = st.columns([1, 1.5])

    with col1:
        st.markdown(f"""
        <div style="background-color: {priority_bg}; padding: 20px; border-radius: 12px; border-left: 6px solid {priority_color}; height: 100%;">
            <span style="background-color: {priority_color}; color: white; padding: 4px 12px; border-radius: 12px; font-size: 0.75em; font-weight: bold;">
                {priority_label}
            </span>
            <h2 style="margin: 15px 0 10px 0; color: #1a1a1a;">{recommendation.get('action', 'N/A')}</h2>
            <p style="margin: 8px 0;"><strong>📞 Channel:</strong> {recommendation.get('preferred_channel', 'N/A')}</p>
            <p style="margin: 8px 0;"><strong>📊 Confidence:</strong> {recommendation.get('confidence_pct', 0)}%</p>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown("""
        <div style="background-color: #F3E8FF; padding: 15px 20px; border-radius: 12px; border: 1px solid #D8B4FE;">
            <p style="margin: 0 0 8px 0; font-size: 0.8em; color: #7C3AED; font-weight: 600;">
                🤖 AI-GENERATED EXPLANATION (powered by ai_gen)
            </p>
        </div>
        """, unsafe_allow_html=True)
        st.write(recommendation.get('explanation', 'No explanation available.'))

    # Feedback section
    st.markdown("---")
    st.markdown("**📝 Record Feedback** *(closes the loop for model improvement)*")

    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])

    with col1:
        if st.button("✅ Accepted", type="primary", key="accept"):
            if record_feedback(
                recommendation.get('recommendation_id'),
                member['member_id'],
                recommendation.get('action'),
                "action_taken",
                "accepted"
            ):
                st.success("Recorded!")
            else:
                st.error("Failed")

    with col2:
        if st.button("❌ Rejected", key="reject"):
            if record_feedback(
                recommendation.get('recommendation_id'),
                member['member_id'],
                recommendation.get('action'),
                "action_taken",
                "rejected"
            ):
                st.success("Recorded!")
            else:
                st.error("Failed")

    with col3:
        if st.button("✔️ Completed", key="complete"):
            if record_feedback(
                recommendation.get('recommendation_id'),
                member['member_id'],
                recommendation.get('action'),
                "outcome",
                "completed"
            ):
                st.success("Recorded!")
            else:
                st.error("Failed")

    with col4:
        st.caption("Feedback flows back to Unity Catalog for retraining")


def render_feature_snapshot(member: Dict):
    """Render member feature snapshot - computed scores and risk indicators."""
    st.markdown("**Computed Features**")

    # Key metrics in a compact grid
    col1, col2 = st.columns(2)

    with col1:
        churn = member.get('churn_risk_score', 0) * 100
        churn_color = "#28A745" if churn < 30 else "#FD7E14" if churn < 60 else "#DC3545"
        st.markdown(f"""
        <div style="background: #f8f9fa; padding: 12px; border-radius: 8px; margin-bottom: 10px;">
            <div style="font-size: 0.8em; color: #666;">Churn Risk</div>
            <div style="font-size: 1.8em; font-weight: bold; color: {churn_color};">{churn:.0f}%</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div style="background: #f8f9fa; padding: 12px; border-radius: 8px; margin-bottom: 10px;">
            <div style="font-size: 0.8em; color: #666;">Engagement Score</div>
            <div style="font-size: 1.8em; font-weight: bold; color: #1E3A5F;">{member.get('engagement_score', 0):.1f}<span style="font-size: 0.5em;">/10</span></div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div style="background: #f8f9fa; padding: 12px; border-radius: 8px; margin-bottom: 10px;">
            <div style="font-size: 0.8em; color: #666;">Claims (12M)</div>
            <div style="font-size: 1.4em; font-weight: bold; color: #1E3A5F;">{member.get('total_claims_12m', 0):,}</div>
            <div style="font-size: 0.8em; color: #666;">${member.get('total_paid_12m', 0):,.0f} paid</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div style="background: #f8f9fa; padding: 12px; border-radius: 8px; margin-bottom: 10px;">
            <div style="font-size: 0.8em; color: #666;">Channel Affinity</div>
            <div style="font-size: 1.2em; font-weight: bold; color: #1E3A5F;">{member.get('channel_affinity', 'N/A')}</div>
        </div>
        """, unsafe_allow_html=True)

    # Risk indicators row
    st.markdown("**Risk Indicators**")
    risk_cols = st.columns(4)
    risks = [
        ("Escalation", member.get('escalation_likelihood', 0)),
        ("Care Need", member.get('care_outreach_likelihood', 0)),
        ("Plan Switch", member.get('plan_switch_propensity', 0)),
        ("Churn", member.get('churn_risk_score', 0)),
    ]

    for i, (label, value) in enumerate(risks):
        with risk_cols[i]:
            color = "#28A745" if value < 0.3 else "#FD7E14" if value < 0.6 else "#DC3545"
            st.markdown(f"""
            <div style="text-align: center; background: #f8f9fa; padding: 8px; border-radius: 8px;">
                <div style="font-size: 1.3em; font-weight: bold; color: {color};">{value*100:.0f}%</div>
                <div style="font-size: 0.7em; color: #666;">{label}</div>
            </div>
            """, unsafe_allow_html=True)


def render_interactions(interactions: List[Dict]):
    """Render recent interactions (legacy - use render_interactions_with_ai instead)."""
    render_interactions_with_ai(interactions)


def render_interactions_with_ai(interactions: List[Dict]):
    """Render recent interactions with AI-powered insights prominently displayed."""

    if not interactions:
        st.info("No recent interactions found for this member.")
        return

    # Column headers
    cols = st.columns([2, 2, 1.5, 1.5, 4])
    cols[0].markdown("**Date / Channel**")
    cols[1].markdown("**Reason**")
    cols[2].markdown("**🤖 Sentiment**")
    cols[3].markdown("**🤖 Intent**")
    cols[4].markdown("**🤖 AI Summary**")

    st.markdown("<hr style='margin: 5px 0;'>", unsafe_allow_html=True)

    for interaction in interactions:
        sentiment = interaction.get('sentiment', 'unknown')
        intent = interaction.get('inferred_intent', 'unknown')

        # Sentiment badge colors
        sentiment_colors = {
            "positive": ("#28A745", "white"),
            "neutral": ("#FFC107", "black"),
            "negative": ("#DC3545", "white"),
            "mixed": ("#6C757D", "white")
        }
        bg_color, text_color = sentiment_colors.get(sentiment, ("#6C757D", "white"))

        # Intent badge (clean up the intent name)
        intent_display = intent.replace("_", " ").title() if intent else "Unknown"

        cols = st.columns([2, 2, 1.5, 1.5, 4])

        with cols[0]:
            st.markdown(f"**{interaction.get('interaction_date')}**")
            st.caption(f"{interaction.get('interaction_type')} via {interaction.get('channel')}")

        with cols[1]:
            st.write(interaction.get('reason', 'N/A'))
            if interaction.get('is_complaint'):
                st.markdown("⚠️ *Complaint*")

        with cols[2]:
            st.markdown(f"""
            <span style="background-color: {bg_color}; color: {text_color}; padding: 4px 12px; border-radius: 12px; font-size: 0.85em; font-weight: 500;">
                {sentiment.upper()}
            </span>
            """, unsafe_allow_html=True)

        with cols[3]:
            st.markdown(f"""
            <span style="background-color: #E9ECEF; color: #495057; padding: 4px 10px; border-radius: 8px; font-size: 0.8em;">
                {intent_display}
            </span>
            """, unsafe_allow_html=True)

        with cols[4]:
            summary = interaction.get('interaction_summary', 'No summary available.')
            st.markdown(f"*{summary}*")

        st.markdown("<hr style='margin: 8px 0; opacity: 0.3;'>", unsafe_allow_html=True)


def render_recommendation_details(recommendation: Dict):
    """Render detailed recommendation scoring."""

    st.markdown("**Alternative Action Scores:**")

    scores = [
        ("Send Benefit Reminder", recommendation.get('score_benefit_reminder', 0)),
        ("Route to Service Agent", recommendation.get('score_service_agent', 0)),
        ("Trigger Care Outreach", recommendation.get('score_care_outreach', 0)),
        ("Push Digital Nudge", recommendation.get('score_digital_nudge', 0)),
        ("Recommend Plan Education", recommendation.get('score_plan_education', 0)),
        ("Offer Retention Follow-up", recommendation.get('score_retention_followup', 0)),
    ]

    # Sort by score descending
    scores.sort(key=lambda x: x[1], reverse=True)

    for action, score in scores:
        pct = score * 100
        is_selected = action == recommendation.get('action')
        bar_color = "#1f77b4" if is_selected else "#cccccc"

        st.markdown(f"""
        <div style="margin: 5px 0;">
            <div style="display: flex; justify-content: space-between;">
                <span>{'**' if is_selected else ''}{action}{'**' if is_selected else ''}</span>
                <span>{pct:.0f}%</span>
            </div>
            <div style="background-color: #eee; height: 10px; border-radius: 5px;">
                <div style="background-color: {bar_color}; width: {pct}%; height: 10px; border-radius: 5px;"></div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"""
    **Model Info:**
    - Version: {recommendation.get('model_version', 'N/A')}
    - Generated: {recommendation.get('generated_at', 'N/A')}
    - Valid Until: {recommendation.get('valid_until', 'N/A')}
    """)


def render_priority_queue():
    """Render the high-priority member queue."""
    st.subheader("High Priority Members")

    members = get_high_priority_members(10)

    if not members:
        st.info("No high-priority members found.")
        return

    for member in members:
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

        with col1:
            st.write(f"**{member['full_name']}**")
            st.caption(f"{member['member_segment']}")

        with col2:
            st.write(f"{member['action']}")
            st.caption(f"via {member['preferred_channel']}")

        with col3:
            churn = member.get('churn_risk_score', 0) * 100
            st.write(f"Priority: {member['priority_score']*100:.0f}%")
            st.caption(f"Churn Risk: {churn:.0f}%")

        with col4:
            if st.button("View", key=f"queue_{member['member_id']}"):
                st.session_state.selected_member = member['member_id']
                st.rerun()

        st.divider()


def render_data_explorer():
    """Render the Genie-powered Data Explorer tab."""
    st.subheader("Ask Questions About Your Data")
    st.caption("Powered by Databricks Genie - natural language to SQL")

    # Initialize session state for Genie
    if 'genie_history' not in st.session_state:
        st.session_state.genie_history = []
    if 'genie_auto_run' not in st.session_state:
        st.session_state.genie_auto_run = None

    # Check if we need to auto-run a query (from suggested question click)
    auto_run_question = st.session_state.genie_auto_run
    if auto_run_question:
        st.session_state.genie_auto_run = None  # Clear it
        with st.spinner(f"Asking Genie: {auto_run_question}"):
            result = ask_genie(auto_run_question)
        if "error" in result and result["error"]:
            st.error(f"Error: {result['error']}")
        else:
            st.session_state.genie_history.insert(0, result)

    # Suggested questions
    st.markdown("**Try these questions:**")
    suggested = [
        "How many members do we have by segment?",
        "Which members have the highest churn risk?",
        "Show me claims by diagnosis category",
        "What's the average engagement score by plan type?",
        "List members with negative sentiment in recent interactions",
        "How many high priority recommendations are there?"
    ]

    cols = st.columns(3)
    for i, q in enumerate(suggested[:6]):
        with cols[i % 3]:
            if st.button(q, key=f"suggested_{i}", use_container_width=True):
                st.session_state.genie_auto_run = q
                st.rerun()

    st.markdown("---")

    # Question input
    question = st.text_input(
        "Or ask your own question:",
        placeholder="e.g., What are the top 10 members by total claims?",
        key="genie_input"
    )

    col1, col2 = st.columns([1, 5])
    with col1:
        ask_button = st.button("🔍 Ask Genie", type="primary", disabled=not question)
    with col2:
        if st.button("Clear History"):
            st.session_state.genie_history = []
            st.rerun()

    # Process question from text input
    if ask_button and question:
        with st.spinner("Genie is thinking..."):
            result = ask_genie(question)

        if "error" in result and result["error"]:
            st.error(f"Error: {result['error']}")
        else:
            st.session_state.genie_history.insert(0, result)
            st.rerun()

    # Display results
    if st.session_state.genie_history:
        for i, result in enumerate(st.session_state.genie_history[:5]):  # Show last 5
            render_genie_result(result, i)


def render_genie_result(result: Dict, index: int):
    """Render a single Genie query result."""

    with st.container():
        # Question header
        st.markdown(f"""
        <div style="background: linear-gradient(90deg, #4A90D9 0%, #357ABD 100%); padding: 10px 15px; border-radius: 8px 8px 0 0; margin-top: 20px;">
            <span style="color: white; font-weight: 600;">💬 {result.get('question', 'Query')}</span>
        </div>
        """, unsafe_allow_html=True)

        # Text response / summary
        if result.get("text_response"):
            st.markdown(f"""
            <div style="background: #F0F7FF; padding: 15px; border-left: 4px solid #4A90D9;">
                {result['text_response']}
            </div>
            """, unsafe_allow_html=True)

        # Results table
        if result.get("data") and result.get("columns"):
            st.markdown(f"**Results** ({result.get('row_count', len(result['data']))} rows)")

            # Convert to DataFrame for nice display
            df = pd.DataFrame(result["data"], columns=result["columns"])
            st.dataframe(df, use_container_width=True, hide_index=True)

        # SQL query (collapsible)
        if result.get("sql"):
            with st.expander("📝 View Generated SQL"):
                st.code(result["sql"], language="sql")
                if result.get("description"):
                    st.caption(f"*{result['description']}*")

        # Follow-up suggestions
        if result.get("suggested_questions"):
            st.markdown("**Follow-up questions:**")
            for sq_idx, sq in enumerate(result["suggested_questions"][:3]):
                if st.button(f"→ {sq}", key=f"followup_{index}_{sq_idx}_{hash(sq) % 10000}"):
                    st.session_state.genie_question = sq
                    st.rerun()

        st.markdown("---")


# =============================================================================
# Main App
# =============================================================================

def main():
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=PAGE_ICON,
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Initialize session state
    if 'selected_member' not in st.session_state:
        st.session_state.selected_member = None

    # Sidebar
    with st.sidebar:
        st.title(APP_TITLE)
        st.markdown("---")

        # Connection status
        conn = get_cached_connection()
        if conn:
            st.success("Connected to Lakebase")
            st.caption(f"Schema: {LAKEBASE_SCHEMA}")
        else:
            st.info("Demo Mode")
            st.caption("Using sample data (3 members)")
            st.caption("Try: John, Sarah, Michael")
            if _credential_cache.get("error"):
                with st.expander("Connection details"):
                    st.caption(f"{_credential_cache['error'][:100]}")

        st.markdown("---")

        # Navigation
        page = st.radio(
            "Navigation",
            ["Member Lookup", "Priority Queue", "Data Explorer", "About"],
            key="nav"
        )

    # Main content
    if st.session_state.selected_member:
        render_member_360(st.session_state.selected_member)
    elif page == "Member Lookup":
        st.title("Member Lookup")
        render_member_search()
    elif page == "Priority Queue":
        st.title("Priority Queue")
        render_priority_queue()
    elif page == "Data Explorer":
        st.title("Data Explorer")
        render_data_explorer()
    elif page == "About":
        st.title("About This Demo")

        st.markdown("""
        ## Healthcare Payer Next-Best-Action Console

        A complete Databricks solution for **operationalized AI** in healthcare member engagement.

        ---

        ### 🎯 The Demo Story

        | Step | What | Databricks Capability |
        |------|------|----------------------|
        | **KNOW** | Member 360 view | Unity Catalog + Lakebase |
        | **PREDICT** | AI-powered insights | AI Functions in SQL |
        | **ACT** | Next Best Action | Recommendation engine |
        | **LEARN** | Feedback loop | Delta Lake + retraining |

        ---

        ### 🤖 AI Capabilities

        This demo uses **Databricks AI Functions** and **Genie**:

        | Capability | Purpose | Location |
        |----------|---------|----------|
        | `ai_analyze_sentiment()` | Detect member sentiment | Interaction history |
        | `ai_classify()` | Classify member intent | Interaction history |
        | `ai_summarize()` | Generate interaction summaries | Interaction history |
        | `ai_gen()` | Create personalized explanations | Recommendations |
        | **Genie Space** | Natural language data exploration | Data Explorer tab |

        ---

        ### 🏗️ Architecture

        ```
        Unity Catalog (Governed)     →    Lakebase (Online)    →    App (UI)
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Bronze → Silver → Gold       →    PostgreSQL            →    Streamlit
        (batch, AI enrichment)            (sub-10ms lookups)         (real-time)
        ```

        **Key differentiators:**
        - **Unity Catalog**: Single governance layer for all data + AI
        - **Lakebase**: Managed PostgreSQL for online serving (no separate DB to manage)
        - **AI Functions**: LLM capabilities directly in SQL pipelines
        - **Asset Bundles**: One-command deployment (`./deploy.sh`)

        ---

        ### 📊 Data Scale

        - **5,000** synthetic members
        - **23** tables in medallion pipeline
        - **4** AI functions in production
        - **<10ms** member lookup latency
        """)


if __name__ == "__main__":
    main()
