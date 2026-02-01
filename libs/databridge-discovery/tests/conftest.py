"""
Pytest configuration for databridge-discovery tests.
"""

import sys
from pathlib import Path

import pytest

# Add the src directory to Python path for development
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


@pytest.fixture
def sample_simple_sql():
    """Simple SQL for testing."""
    return "SELECT id, name FROM users WHERE active = 1"


@pytest.fixture
def sample_case_sql():
    """SQL with CASE statement for testing."""
    return """
    SELECT
        id,
        CASE
            WHEN status = 'A' THEN 'Active'
            WHEN status = 'I' THEN 'Inactive'
            ELSE 'Unknown'
        END as status_label
    FROM users
    """


@pytest.fixture
def sample_complex_sql():
    """Complex SQL for testing."""
    return """
    WITH active_users AS (
        SELECT id, name, department
        FROM users
        WHERE status = 'active'
    ),
    department_stats AS (
        SELECT
            department,
            COUNT(*) as user_count,
            AVG(salary) as avg_salary
        FROM active_users
        GROUP BY department
    )
    SELECT
        u.id,
        u.name,
        d.user_count,
        d.avg_salary,
        CASE
            WHEN d.avg_salary > 100000 THEN 'High'
            WHEN d.avg_salary > 50000 THEN 'Medium'
            ELSE 'Low'
        END as salary_tier
    FROM active_users u
    LEFT JOIN department_stats d ON u.department = d.department
    WHERE d.user_count > 5
    ORDER BY d.avg_salary DESC
    """


@pytest.fixture
def sample_fpa_sql():
    """Sample FP&A SQL with Oil & Gas patterns."""
    return """
    SELECT
        account_code,
        CASE
            WHEN account_code ILIKE '501%' THEN 'Oil Sales'
            WHEN account_code ILIKE '502%' THEN 'Gas Sales'
            WHEN account_code ILIKE '503%' THEN 'NGL Sales'
            WHEN account_code ILIKE '601%' THEN 'Oil Severance Taxes'
            WHEN account_code ILIKE '602%' THEN 'Gas Severance Taxes'
            WHEN account_code ILIKE '640%' THEN 'Lease Operating Expenses'
            WHEN account_code ILIKE '8%' THEN 'General & Administrative'
            WHEN account_code ILIKE '95%' THEN 'DD&A'
        END as gl_category
    FROM dim_account
    """


@pytest.fixture
def temp_dir(tmp_path):
    """Provide a temporary directory for tests."""
    return tmp_path


@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary database path for persistence tests."""
    return tmp_path / "test_discovery.db"
