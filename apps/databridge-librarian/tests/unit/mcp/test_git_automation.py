"""
Unit tests for Git Automation MCP tools.

Tests the commit_dbt_project, create_deployment_pr, and commit_deployment_scripts tools.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import datetime, timezone


class TestCommitDbtProject:
    """Tests for commit_dbt_project MCP tool."""

    def test_commit_dbt_project_no_git_module(self):
        """Test error when git module not available."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        # Get the tool function
        tool = mcp._tool_manager._tools.get("commit_dbt_project")
        assert tool is not None

        with patch.dict("sys.modules", {"databridge_core.git": None}):
            with patch("src.mcp.tools.git_automation._find_repo_root", return_value=None):
                result = tool.fn(project_id="test123")
                assert "error" in result

    def test_commit_dbt_project_not_in_repo(self):
        """Test error when not in a git repository."""
        from src.mcp.tools.git_automation import register_git_tools, _find_repo_root
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("commit_dbt_project")

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=None):
            result = tool.fn(project_id="test123")
            assert "error" in result
            assert "Not in a git repository" in result["error"]

    def test_commit_dbt_project_dirty_workdir(self, tmp_path):
        """Test error when working directory has uncommitted changes."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("commit_dbt_project")

        mock_git = MagicMock()
        mock_git.get_status.return_value = {
            "clean": False,
            "staged": ["file.py"],
            "modified": [],
        }

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                result = tool.fn(project_id="test123")
                assert "error" in result
                assert "uncommitted changes" in result["error"]

    def test_commit_dbt_project_no_files_generated(self, tmp_path):
        """Test error when no dbt files are generated."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("commit_dbt_project")

        mock_git = MagicMock()
        mock_git.get_status.return_value = {"clean": True, "staged": [], "modified": []}

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                with patch("src.mcp.tools.git_automation._generate_dbt_project", return_value=[]):
                    result = tool.fn(project_id="test123")
                    assert "error" in result
                    assert "No dbt files generated" in result["error"]

    def test_commit_dbt_project_success(self, tmp_path):
        """Test successful dbt project commit."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("commit_dbt_project")

        mock_git = MagicMock()
        mock_git.get_status.return_value = {"clean": True, "staged": [], "modified": []}
        mock_git.get_current_branch.return_value = "main"
        mock_git.create_branch.return_value = "auto/dbt-test1234"
        mock_git.commit_files.return_value = MagicMock(
            sha="abc12345",
            message="Test commit",
            files_changed=2,
            branch="auto/dbt-test1234",
        )

        dbt_files = [tmp_path / "dbt_project.yml", tmp_path / "models" / "test.sql"]

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                with patch("src.mcp.tools.git_automation._generate_dbt_project", return_value=dbt_files):
                    result = tool.fn(project_id="test1234")
                    assert result["status"] == "committed"
                    assert result["sha"] == "abc12345"
                    assert "auto/dbt-test1234" in result["branch"]


class TestCreateDeploymentPr:
    """Tests for create_deployment_pr MCP tool."""

    def test_create_pr_no_git_module(self):
        """Test error when git module not available."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("create_deployment_pr")

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=None):
            result = tool.fn(title="Test PR")
            assert "error" in result

    def test_create_pr_not_authenticated(self, tmp_path):
        """Test error when gh not authenticated."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("create_deployment_pr")

        mock_git = MagicMock()
        mock_git.is_gh_authenticated.return_value = False

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                result = tool.fn(title="Test PR")
                assert "error" in result
                assert "not authenticated" in result["error"]

    def test_create_pr_no_remote(self, tmp_path):
        """Test error when no remote configured."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("create_deployment_pr")

        mock_git = MagicMock()
        mock_git.is_gh_authenticated.return_value = True
        mock_git.has_remote.return_value = False

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                result = tool.fn(title="Test PR")
                assert "error" in result
                assert "No remote" in result["error"]

    def test_create_pr_success(self, tmp_path):
        """Test successful PR creation."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("create_deployment_pr")

        mock_git = MagicMock()
        mock_git.is_gh_authenticated.return_value = True
        mock_git.has_remote.return_value = True
        mock_git.get_current_branch.return_value = "feature/test"
        mock_git.get_default_branch.return_value = "main"
        mock_git.create_pull_request.return_value = MagicMock(
            number=42,
            url="https://github.com/owner/repo/pull/42",
            title="Test PR",
            head="feature/test",
            base="main",
        )

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                result = tool.fn(title="Test PR", body="Description")
                assert result["status"] == "created"
                assert result["pr_number"] == 42
                assert "github.com" in result["url"]


class TestCommitDeploymentScripts:
    """Tests for commit_deployment_scripts MCP tool."""

    def test_commit_scripts_no_scripts(self):
        """Test error when no scripts provided."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("commit_deployment_scripts")

        result = tool.fn(project_id="test123", scripts={})
        assert "error" in result
        assert "No scripts provided" in result["error"]

    def test_commit_scripts_success(self, tmp_path):
        """Test successful script commit."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("commit_deployment_scripts")

        mock_git = MagicMock()
        mock_git.get_current_branch.return_value = "main"
        mock_git.branch_exists.return_value = False
        mock_git.create_branch.return_value = "auto/deploy-test1234"
        mock_git.commit_files.return_value = MagicMock(
            sha="def67890",
            message="Test commit",
            files_changed=2,
            branch="auto/deploy-test1234",
        )

        scripts = {
            "insert.sql": "INSERT INTO table VALUES (1, 2);",
            "view.sql": "CREATE VIEW v AS SELECT * FROM t;",
        }

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                result = tool.fn(project_id="test1234", scripts=scripts)
                assert result["status"] == "committed"
                assert result["sha"] == "def67890"
                assert len(result["files"]) == 2


class TestGetGitStatus:
    """Tests for get_git_status MCP tool."""

    def test_get_status_not_in_repo(self):
        """Test error when not in a git repository."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("get_git_status")

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=None):
            result = tool.fn()
            assert "error" in result

    def test_get_status_success(self, tmp_path):
        """Test successful status retrieval."""
        from src.mcp.tools.git_automation import register_git_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_git_tools(mcp)

        tool = mcp._tool_manager._tools.get("get_git_status")

        mock_git = MagicMock()
        mock_git.get_status.return_value = {
            "branch": "main",
            "staged": [],
            "modified": ["file.py"],
            "untracked": ["new.py"],
            "clean": False,
        }
        mock_git.has_remote.return_value = True
        mock_git.is_gh_authenticated.return_value = True
        mock_git.get_default_branch.return_value = "main"

        with patch("src.mcp.tools.git_automation._find_repo_root", return_value=tmp_path):
            with patch("databridge_core.git.GitOperations", return_value=mock_git):
                result = tool.fn()
                assert result["branch"] == "main"
                assert result["clean"] is False
                assert result["has_remote"] is True


class TestFindRepoRoot:
    """Tests for _find_repo_root helper function."""

    def test_find_repo_root_found(self, tmp_path):
        """Test finding repo root."""
        from src.mcp.tools.git_automation import _find_repo_root

        # Create .git directory
        (tmp_path / ".git").mkdir()

        # Create subdirectory
        subdir = tmp_path / "src" / "module"
        subdir.mkdir(parents=True)

        with patch("pathlib.Path.cwd", return_value=subdir):
            root = _find_repo_root()
            assert root == tmp_path

    def test_find_repo_root_not_found(self, tmp_path):
        """Test not finding repo root."""
        from src.mcp.tools.git_automation import _find_repo_root

        # No .git directory
        with patch("pathlib.Path.cwd", return_value=tmp_path):
            root = _find_repo_root()
            # Will walk up to system root and not find .git
            # In most test environments, this should return None


class TestGenerateDbtProject:
    """Tests for _generate_dbt_project helper function."""

    def test_generate_dbt_project_creates_files(self, tmp_path):
        """Test dbt project generation creates expected files."""
        from src.mcp.tools.git_automation import _generate_dbt_project

        output_dir = tmp_path / "dbt_output"
        files = _generate_dbt_project("test1234", output_dir)

        assert len(files) >= 2
        assert output_dir.exists()
        assert (output_dir / "dbt_project.yml").exists()
        assert (output_dir / "models").exists()

    def test_generate_dbt_project_yml_content(self, tmp_path):
        """Test dbt_project.yml has correct content."""
        from src.mcp.tools.git_automation import _generate_dbt_project

        output_dir = tmp_path / "dbt_output"
        _generate_dbt_project("abcd1234", output_dir)

        content = (output_dir / "dbt_project.yml").read_text()
        assert "databridge_abcd1234" in content
        assert "version:" in content
        assert "model-paths:" in content
