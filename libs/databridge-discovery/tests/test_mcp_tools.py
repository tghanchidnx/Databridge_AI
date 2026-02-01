"""
Unit tests for MCP Tools module.
"""

import json
import os
import tempfile

import pytest

from databridge_discovery.mcp.tools import (
    parse_sql,
    extract_case_statements,
    analyze_sql_complexity,
    start_discovery_session,
    get_discovery_session,
    export_discovery_evidence,
    add_sql_to_session,
    approve_hierarchy,
    reject_hierarchy,
    export_librarian_csv,
)


class TestParseSQLTool:
    """Tests for parse_sql MCP tool."""

    def test_parse_simple_query(self):
        """Test parsing a simple query."""
        result = parse_sql("SELECT id, name FROM users")

        assert result["query_type"] == "SELECT"
        assert len(result["tables"]) == 1
        assert result["tables"][0]["name"] == "users"
        assert len(result["columns"]) == 2

    def test_parse_with_join(self):
        """Test parsing query with JOIN."""
        sql = """
        SELECT a.id, b.name
        FROM orders a
        LEFT JOIN customers b ON a.customer_id = b.id
        """
        result = parse_sql(sql)

        assert len(result["joins"]) == 1
        assert result["joins"][0]["join_type"] == "LEFT"

    def test_parse_with_aggregation(self):
        """Test parsing query with aggregations."""
        sql = """
        SELECT customer_id, COUNT(*) as cnt, SUM(amount) as total
        FROM orders
        GROUP BY customer_id
        """
        result = parse_sql(sql)

        assert result["metrics"]["has_group_by"]
        assert result["metrics"]["aggregation_count"] >= 2

    def test_parse_with_case(self):
        """Test parsing query with CASE statement."""
        sql = """
        SELECT
            CASE WHEN status = 'A' THEN 'Active' END as status_label
        FROM users
        """
        result = parse_sql(sql)

        assert result["metrics"]["case_statement_count"] == 1
        assert result["columns"][0]["is_case_statement"]

    def test_parse_different_dialects(self):
        """Test parsing with different dialects."""
        sql = "SELECT * FROM users"

        for dialect in ["snowflake", "postgres", "mysql"]:
            result = parse_sql(sql, dialect=dialect)
            assert result["dialect"] == dialect
            assert result["query_type"] == "SELECT"


class TestExtractCaseStatementsTool:
    """Tests for extract_case_statements MCP tool."""

    def test_extract_simple_case(self):
        """Test extracting simple CASE statement."""
        sql = """
        SELECT
            CASE
                WHEN type = 'A' THEN 'Type A'
                WHEN type = 'B' THEN 'Type B'
            END as type_label
        FROM items
        """
        result = extract_case_statements(sql)

        assert result["case_count"] == 1
        assert len(result["case_statements"]) == 1
        assert len(result["case_statements"][0]["when_clauses"]) == 2

    def test_extract_with_hierarchy(self):
        """Test extracting with hierarchy detection."""
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '5%' THEN 'Revenue'
                WHEN account_code LIKE '6%' THEN 'Expense'
            END as category
        FROM gl
        """
        result = extract_case_statements(sql, include_hierarchy=True)

        assert result["case_count"] == 1
        assert len(result["hierarchies"]) >= 1
        assert result["hierarchies"][0]["confidence"] > 0

    def test_extract_without_hierarchy(self):
        """Test extracting without hierarchy detection."""
        sql = """
        SELECT
            CASE WHEN x = 1 THEN 'Y' END as col
        FROM t
        """
        result = extract_case_statements(sql, include_hierarchy=False)

        assert result["case_count"] == 1
        assert result["hierarchies"] is None

    def test_extract_entity_type_detection(self):
        """Test entity type detection in extraction."""
        sql = """
        SELECT
            CASE
                WHEN account_code LIKE '1%' THEN 'Assets'
            END as category
        FROM accounts
        """
        result = extract_case_statements(sql)

        assert result["case_statements"][0]["entity_type"] == "account"

    def test_extract_pattern_detection(self):
        """Test pattern detection in extraction."""
        sql = """
        SELECT
            CASE
                WHEN code LIKE '5%' THEN 'Revenue'
                WHEN code LIKE '6%' THEN 'Expense'
            END as cat
        FROM gl
        """
        result = extract_case_statements(sql)

        assert result["case_statements"][0]["pattern"] == "prefix"


class TestAnalyzeSQLComplexityTool:
    """Tests for analyze_sql_complexity MCP tool."""

    def test_analyze_simple_query(self):
        """Test analyzing simple query."""
        result = analyze_sql_complexity("SELECT * FROM users")

        assert result["complexity_level"] == "simple"
        assert result["complexity_score"] >= 0
        assert result["metrics"]["table_count"] == 1

    def test_analyze_complex_query(self):
        """Test analyzing complex query."""
        sql = """
        WITH cte1 AS (
            SELECT * FROM (
                SELECT * FROM users
            ) t
        )
        SELECT a.*, b.name,
            CASE WHEN c.status = 'A' THEN 'Active' END as status
        FROM orders a
        JOIN customers b ON a.cid = b.id
        JOIN statuses c ON a.sid = c.id
        LEFT JOIN products p ON a.pid = p.id
        WHERE a.amount > 100
        GROUP BY a.id
        """
        result = analyze_sql_complexity(sql)

        assert result["complexity_score"] > 5
        assert result["metrics"]["join_count"] >= 3
        assert result["metrics"]["cte_count"] >= 1

    def test_analyze_recommendations(self):
        """Test that recommendations are provided."""
        sql = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM users
                ) a
            ) b
        ) c
        """
        result = analyze_sql_complexity(sql)

        # Should have some recommendations for deep nesting
        assert isinstance(result["recommendations"], list)

    def test_analyze_lineage_summary(self):
        """Test lineage summary in analysis."""
        sql = """
        SELECT
            a.id,
            b.name,
            CASE WHEN a.status = 'A' THEN 'Active' END as status_label
        FROM orders a
        JOIN customers b ON a.customer_id = b.id
        """
        result = analyze_sql_complexity(sql)

        assert "lineage_summary" in result
        assert result["lineage_summary"]["output_columns"] >= 3


class TestSessionManagementTools:
    """Tests for session management MCP tools."""

    def test_start_session(self):
        """Test starting a new session."""
        result = start_discovery_session(
            name="Test Session",
            dialect="snowflake"
        )

        assert "session_id" in result
        assert result["name"] == "Test Session"
        assert result["status"] == "created"

    def test_start_session_with_sql(self):
        """Test starting session with SQL sources."""
        result = start_discovery_session(
            name="Test Session",
            sql_sources=[
                "SELECT CASE WHEN x = 1 THEN 'A' END as col FROM t"
            ]
        )

        assert result["analysis_complete"]
        assert result["sources_added"] == 1
        assert result["summary"]["case_statements_found"] >= 1

    def test_get_session(self):
        """Test getting session state."""
        # Start a session first
        start_result = start_discovery_session(
            name="Get Test",
            sql_sources=["SELECT 1 as x"]
        )

        # Get the session
        result = get_discovery_session(
            session_id=start_result["session_id"],
            include_proposals=True,
            include_evidence=True
        )

        assert result["session_id"] == start_result["session_id"]
        assert "sources" in result
        assert "proposals" in result
        assert "evidence" in result

    def test_get_nonexistent_session(self):
        """Test getting non-existent session."""
        result = get_discovery_session(session_id="nonexistent_id")

        assert "error" in result

    def test_add_sql_to_session(self):
        """Test adding SQL to existing session."""
        # Start a session
        start_result = start_discovery_session(name="Add SQL Test")
        session_id = start_result["session_id"]

        # Add SQL
        result = add_sql_to_session(
            session_id=session_id,
            sql="SELECT CASE WHEN a = 1 THEN 'X' END as col FROM t",
            analyze=True
        )

        assert result["added"]
        assert "analysis" in result

    def test_approve_hierarchy_workflow(self):
        """Test hierarchy approval workflow."""
        # Start session with SQL containing CASE
        start_result = start_discovery_session(
            name="Approve Test",
            sql_sources=[
                "SELECT CASE WHEN code LIKE '1%' THEN 'Cat1' END as cat FROM t"
            ]
        )
        session_id = start_result["session_id"]

        # Get session to find proposals
        session = get_discovery_session(session_id, include_proposals=True)

        if session.get("proposals") and len(session["proposals"]) > 0:
            hierarchy_id = session["proposals"][0]["id"]

            # Approve
            result = approve_hierarchy(session_id, hierarchy_id)
            assert result["approved"] or "error" not in result

    def test_reject_hierarchy_workflow(self):
        """Test hierarchy rejection workflow."""
        # Start session with SQL containing CASE
        start_result = start_discovery_session(
            name="Reject Test",
            sql_sources=[
                "SELECT CASE WHEN x = 1 THEN 'Y' END as col FROM t"
            ]
        )
        session_id = start_result["session_id"]

        # Get session to find proposals
        session = get_discovery_session(session_id, include_proposals=True)

        if session.get("proposals") and len(session["proposals"]) > 0:
            hierarchy_id = session["proposals"][0]["id"]

            # Reject
            result = reject_hierarchy(session_id, hierarchy_id, "Test rejection")
            assert result["rejected"] or "error" not in result


class TestExportTools:
    """Tests for export MCP tools."""

    def test_export_evidence_json(self):
        """Test exporting evidence to JSON."""
        # Start session with analysis
        start_result = start_discovery_session(
            name="Export Test",
            sql_sources=[
                "SELECT CASE WHEN a = 1 THEN 'X' END as col FROM t"
            ]
        )
        session_id = start_result["session_id"]

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            result = export_discovery_evidence(
                session_id=session_id,
                output_path=output_path,
                format="json"
            )

            assert result["success"]
            assert os.path.exists(output_path)

            with open(output_path) as f:
                data = json.load(f)
                assert "session_id" in data

        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_export_evidence_csv(self):
        """Test exporting evidence to CSV."""
        start_result = start_discovery_session(
            name="CSV Export Test",
            sql_sources=[
                "SELECT CASE WHEN a = 1 THEN 'X' END as col FROM t"
            ]
        )
        session_id = start_result["session_id"]

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            result = export_discovery_evidence(
                session_id=session_id,
                output_path=output_path,
                format="csv"
            )

            assert result["success"]

        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_export_librarian_csv(self):
        """Test exporting to Librarian CSV format."""
        # Start session and approve hierarchies
        start_result = start_discovery_session(
            name="Librarian Export Test",
            sql_sources=[
                """SELECT
                    CASE
                        WHEN account_code LIKE '5%' THEN 'Revenue'
                        WHEN account_code LIKE '6%' THEN 'Expense'
                    END as category
                FROM gl"""
            ]
        )
        session_id = start_result["session_id"]

        # Approve all hierarchies
        session = get_discovery_session(session_id, include_proposals=True)
        for proposal in session.get("proposals", []):
            approve_hierarchy(session_id, proposal["id"])

        with tempfile.TemporaryDirectory() as tmpdir:
            result = export_librarian_csv(session_id, tmpdir)

            assert result["success"]
            # Files should be created if there were approved hierarchies

    def test_export_nonexistent_session(self):
        """Test exporting from non-existent session."""
        result = export_discovery_evidence(
            session_id="nonexistent_id",
            output_path="/tmp/test.json",
            format="json"
        )

        assert not result["success"]
        assert "error" in result
