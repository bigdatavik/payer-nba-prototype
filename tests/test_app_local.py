#!/usr/bin/env python3
"""
Local tests for the NBA Console app.

Run with: python -m pytest tests/test_app_local.py -v
"""

import os
import sys
from unittest.mock import patch, MagicMock

import pytest

# Add app to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "app"))


class TestDatabaseConnection:
    """Test database connection handling."""

    def test_connection_with_missing_env(self):
        """Should handle missing environment variables gracefully."""
        with patch.dict(os.environ, {}, clear=True):
            # Import after clearing env
            import importlib
            import app as app_module
            importlib.reload(app_module)

            conn = app_module.get_db_connection()
            assert conn is None, "Should return None when env vars missing"

    def test_connection_with_valid_env(self):
        """Should attempt connection with valid env vars."""
        test_env = {
            "PGHOST": "test-host",
            "PGDATABASE": "test-db",
            "PGUSER": "test-user",
            "PGPASSWORD": "test-pass",
            "PGPORT": "5432",
        }

        with patch.dict(os.environ, test_env):
            with patch("psycopg2.connect") as mock_connect:
                mock_connect.return_value = MagicMock()

                import importlib
                import app as app_module
                importlib.reload(app_module)

                # Clear cache to force reconnection
                app_module.get_db_connection.clear()

                conn = app_module.get_db_connection()
                mock_connect.assert_called_once()


class TestMemberSearch:
    """Test member search functionality."""

    @patch("app.execute_query")
    def test_search_returns_results(self, mock_query):
        """Should return member results for valid search."""
        mock_query.return_value = [
            {
                "member_id": "MBR00000001",
                "full_name": "John Doe",
                "plan_type": "Medicare Advantage",
                "member_segment": "High Value",
                "region": "Northeast",
                "state": "NY",
            }
        ]

        import app as app_module
        results = app_module.search_members("John")

        assert len(results) == 1
        assert results[0]["member_id"] == "MBR00000001"

    @patch("app.execute_query")
    def test_search_with_no_results(self, mock_query):
        """Should return empty list for no matches."""
        mock_query.return_value = []

        import app as app_module
        results = app_module.search_members("NonexistentMember")

        assert results == []


class TestMemberFeatures:
    """Test member feature retrieval."""

    @patch("app.execute_query")
    def test_get_member_features(self, mock_query):
        """Should return member features for valid ID."""
        mock_query.return_value = [
            {
                "member_id": "MBR00000001",
                "full_name": "John Doe",
                "churn_risk_score": 0.35,
                "engagement_score": 7.5,
            }
        ]

        import app as app_module
        member = app_module.get_member_features("MBR00000001")

        assert member is not None
        assert member["churn_risk_score"] == 0.35

    @patch("app.execute_query")
    def test_get_member_features_not_found(self, mock_query):
        """Should return None for invalid ID."""
        mock_query.return_value = []

        import app as app_module
        member = app_module.get_member_features("INVALID")

        assert member is None


class TestRecommendations:
    """Test recommendation functionality."""

    @patch("app.execute_query")
    def test_get_recommendation(self, mock_query):
        """Should return recommendation for member."""
        mock_query.return_value = [
            {
                "recommendation_id": "REC-MBR00000001-20240115",
                "member_id": "MBR00000001",
                "action": "Offer Retention Follow-up",
                "priority_score": 0.85,
                "explanation": "High churn risk detected",
            }
        ]

        import app as app_module
        rec = app_module.get_member_recommendation("MBR00000001")

        assert rec is not None
        assert rec["action"] == "Offer Retention Follow-up"
        assert rec["priority_score"] == 0.85


class TestFeedback:
    """Test feedback recording."""

    @patch("app.execute_insert")
    def test_record_feedback_success(self, mock_insert):
        """Should record feedback successfully."""
        mock_insert.return_value = True

        import app as app_module
        result = app_module.record_feedback(
            recommendation_id="REC-001",
            member_id="MBR00000001",
            action="Test Action",
            feedback_type="action_taken",
            feedback_value="accepted",
        )

        assert result is True
        assert mock_insert.call_count == 2  # Create table + insert

    @patch("app.execute_insert")
    def test_record_feedback_failure(self, mock_insert):
        """Should handle feedback recording failure."""
        mock_insert.return_value = False

        import app as app_module
        result = app_module.record_feedback(
            recommendation_id="REC-001",
            member_id="MBR00000001",
            action="Test Action",
            feedback_type="action_taken",
            feedback_value="accepted",
        )

        assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
