"""
Tests for Git/CI-CD Integration module.

Tests cover:
- Types: GitConfig, DbtCIConfig, workflow models
- GitClient: Local git operations
- GitHubClient: GitHub API operations
- WorkflowGenerator: GitHub Actions workflow generation
- MCP Tools: All 12 registered tools
"""

import json
import os
import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.git_integration import (
    # Enums
    GitProvider,
    BranchStrategy,
    PRStatus,
    WorkflowTrigger,
    DbtCommand,
    # Config
    GitConfig,
    DbtCIConfig,
    # Git models
    CommitInfo,
    BranchInfo,
    PullRequest,
    GitOperationResult,
    # Workflow models
    WorkflowStep,
    WorkflowJob,
    GitHubActionsWorkflow,
    # Clients
    GitClient,
    GitHubClient,
    WorkflowGenerator,
)


# ========================================
# Fixtures
# ========================================

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def git_repo(temp_dir):
    """Create a temporary git repository."""
    import subprocess

    # Initialize git repo
    subprocess.run(["git", "init", "-b", "main"], cwd=temp_dir, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=temp_dir, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=temp_dir, capture_output=True)

    # Create initial commit
    test_file = Path(temp_dir) / "README.md"
    test_file.write_text("# Test Repo")
    subprocess.run(["git", "add", "README.md"], cwd=temp_dir, capture_output=True)
    subprocess.run(["git", "commit", "-m", "Initial commit"], cwd=temp_dir, capture_output=True)

    return temp_dir


@pytest.fixture
def git_config():
    """Create a sample git config."""
    return GitConfig(
        repo_path="/tmp/test-repo",
        remote_url="https://github.com/testorg/testrepo.git",
        username="testuser",
        email="test@example.com",
        token="ghp_testtoken",
        default_branch="main",
        branch_strategy=BranchStrategy.FEATURE,
    )


@pytest.fixture
def dbt_config():
    """Create a sample dbt CI config."""
    return DbtCIConfig(
        project_name="test_project",
        dbt_version="1.7.0",
        database_type="snowflake",
        run_commands=[DbtCommand.BUILD, DbtCommand.TEST],
        environments=["dev", "prod"],
    )


# ========================================
# Types Tests
# ========================================

class TestEnums:
    """Test enum values."""

    def test_git_provider_values(self):
        """Test GitProvider enum values."""
        assert GitProvider.GITHUB.value == "github"
        assert GitProvider.GITLAB.value == "gitlab"
        assert GitProvider.BITBUCKET.value == "bitbucket"

    def test_branch_strategy_values(self):
        """Test BranchStrategy enum values."""
        assert BranchStrategy.FEATURE.value == "feature"
        assert BranchStrategy.RELEASE.value == "release"
        assert BranchStrategy.HOTFIX.value == "hotfix"

    def test_pr_status_values(self):
        """Test PRStatus enum values."""
        assert PRStatus.OPEN.value == "open"
        assert PRStatus.CLOSED.value == "closed"
        assert PRStatus.MERGED.value == "merged"

    def test_dbt_command_values(self):
        """Test DbtCommand enum values."""
        assert DbtCommand.RUN.value == "run"
        assert DbtCommand.TEST.value == "test"
        assert DbtCommand.BUILD.value == "build"


class TestGitConfig:
    """Test GitConfig model."""

    def test_create_config(self):
        """Test creating a git config."""
        config = GitConfig(
            repo_path="/tmp/repo",
            remote_url="https://github.com/org/repo.git",
        )
        assert config.repo_path == "/tmp/repo"
        assert config.default_branch == "main"

    def test_config_with_branch_strategy(self):
        """Test config with branch strategy."""
        config = GitConfig(
            repo_path="/tmp/repo",
            branch_strategy=BranchStrategy.RELEASE,
        )
        assert config.branch_strategy == BranchStrategy.RELEASE

    def test_to_dict_excludes_token(self):
        """Test that to_dict excludes sensitive token."""
        config = GitConfig(
            repo_path="/tmp/repo",
            token="secret_token",
        )
        d = config.to_dict()
        assert "token" not in d
        assert d["has_token"] is True

    def test_protected_branches_default(self):
        """Test default protected branches."""
        config = GitConfig(repo_path="/tmp/repo")
        assert "main" in config.protected_branches
        assert "master" in config.protected_branches


class TestCommitInfo:
    """Test CommitInfo model."""

    def test_create_commit_info(self):
        """Test creating commit info."""
        from datetime import datetime

        commit = CommitInfo(
            sha="abc123def456",
            message="Test commit",
            author="Test User",
            email="test@example.com",
            timestamp=datetime.now(),
        )
        assert commit.sha == "abc123def456"
        assert commit.message == "Test commit"

    def test_to_dict(self):
        """Test commit to_dict."""
        from datetime import datetime

        commit = CommitInfo(
            sha="abc123",
            message="Test",
            author="User",
            email="user@example.com",
            timestamp=datetime.now(),
            files_changed=5,
        )
        d = commit.to_dict()
        assert d["sha"] == "abc123"
        assert d["files_changed"] == 5


class TestPullRequest:
    """Test PullRequest model."""

    def test_create_pr(self):
        """Test creating a pull request."""
        pr = PullRequest(
            title="Add feature",
            body="Description",
            source_branch="feature/test",
            target_branch="main",
        )
        assert pr.title == "Add feature"
        assert pr.status == PRStatus.DRAFT

    def test_pr_with_reviewers(self):
        """Test PR with reviewers."""
        pr = PullRequest(
            title="Test",
            body="Test",
            source_branch="test",
            target_branch="main",
            reviewers=["user1", "user2"],
            labels=["bug", "urgent"],
        )
        assert len(pr.reviewers) == 2
        assert len(pr.labels) == 2


class TestGitOperationResult:
    """Test GitOperationResult model."""

    def test_success_result(self):
        """Test successful operation result."""
        result = GitOperationResult(
            success=True,
            operation="commit",
            message="Created commit",
            commit_sha="abc123",
        )
        assert result.success is True
        assert result.commit_sha == "abc123"

    def test_failure_result(self):
        """Test failed operation result."""
        result = GitOperationResult(
            success=False,
            operation="push",
            message="Failed to push",
            error="Remote rejected",
        )
        assert result.success is False
        assert result.error == "Remote rejected"


# ========================================
# Workflow Types Tests
# ========================================

class TestWorkflowStep:
    """Test WorkflowStep model."""

    def test_create_step_with_uses(self):
        """Test step with action."""
        step = WorkflowStep(
            name="Checkout",
            uses="actions/checkout@v4",
        )
        d = step.to_yaml_dict()
        assert d["name"] == "Checkout"
        assert d["uses"] == "actions/checkout@v4"

    def test_create_step_with_run(self):
        """Test step with run command."""
        step = WorkflowStep(
            name="Install deps",
            run="pip install -r requirements.txt",
        )
        d = step.to_yaml_dict()
        assert "run" in d

    def test_step_with_env(self):
        """Test step with environment variables."""
        step = WorkflowStep(
            name="Test",
            run="pytest",
            env={"CI": "true"},
        )
        d = step.to_yaml_dict()
        assert d["env"]["CI"] == "true"


class TestWorkflowJob:
    """Test WorkflowJob model."""

    def test_create_job(self):
        """Test creating a job."""
        job = WorkflowJob(
            name="Build",
            runs_on="ubuntu-latest",
            steps=[
                WorkflowStep(name="Checkout", uses="actions/checkout@v4"),
            ],
        )
        assert job.name == "Build"
        assert len(job.steps) == 1

    def test_job_with_needs(self):
        """Test job with dependencies."""
        job = WorkflowJob(
            name="Deploy",
            runs_on="ubuntu-latest",
            needs=["build", "test"],
        )
        d = job.to_yaml_dict()
        assert "needs" in d
        assert len(d["needs"]) == 2

    def test_job_with_matrix(self):
        """Test job with matrix strategy."""
        job = WorkflowJob(
            name="Test",
            runs_on="ubuntu-latest",
            matrix={"python-version": ["3.10", "3.11", "3.12"]},
        )
        d = job.to_yaml_dict()
        assert "strategy" in d
        assert "matrix" in d["strategy"]


class TestGitHubActionsWorkflow:
    """Test GitHubActionsWorkflow model."""

    def test_create_workflow(self):
        """Test creating a workflow."""
        workflow = GitHubActionsWorkflow(
            name="CI",
            filename="ci.yml",
            on_push={"branches": ["main"]},
            jobs={
                "build": WorkflowJob(
                    name="Build",
                    runs_on="ubuntu-latest",
                    steps=[
                        WorkflowStep(name="Checkout", uses="actions/checkout@v4"),
                    ],
                ),
            },
        )
        assert workflow.name == "CI"
        assert len(workflow.jobs) == 1

    def test_to_yaml(self):
        """Test YAML generation."""
        workflow = GitHubActionsWorkflow(
            name="Test",
            filename="test.yml",
            on_push={"branches": ["main"]},
            on_workflow_dispatch=True,
            jobs={
                "test": WorkflowJob(
                    name="Test",
                    runs_on="ubuntu-latest",
                    steps=[
                        WorkflowStep(name="Echo", run="echo 'Hello'"),
                    ],
                ),
            },
        )
        yaml = workflow.to_yaml()
        assert "name: Test" in yaml
        assert "push:" in yaml
        assert "workflow_dispatch:" in yaml


# ========================================
# GitClient Tests
# ========================================

class TestGitClient:
    """Test GitClient class."""

    def test_init_client(self, git_config):
        """Test initializing client."""
        client = GitClient(config=git_config)
        assert client.config == git_config

    def test_is_repo(self, git_repo):
        """Test is_repo detection."""
        client = GitClient()
        client.set_repo_path(git_repo)
        assert client.is_repo() is True

    def test_is_not_repo(self, temp_dir):
        """Test is_repo for non-repo."""
        client = GitClient()
        client.set_repo_path(temp_dir)
        assert client.is_repo() is False

    def test_init_repo(self, temp_dir):
        """Test initializing a new repo."""
        client = GitClient()
        client.set_repo_path(temp_dir)

        result = client.init(initial_branch="main")
        assert result.success is True
        assert client.is_repo() is True

    def test_status(self, git_repo):
        """Test getting status."""
        client = GitClient()
        client.set_repo_path(git_repo)

        status = client.status()
        assert status["branch"] == "main"
        assert status["clean"] is True

    def test_add_and_commit(self, git_repo):
        """Test staging and committing."""
        client = GitClient()
        client.set_repo_path(git_repo)

        # Create a new file
        test_file = Path(git_repo) / "test.txt"
        test_file.write_text("Hello")

        # Commit
        result = client.commit(
            message="Add test file",
            files=["test.txt"],
        )

        assert result.success is True
        assert result.commit_sha is not None

    def test_create_branch(self, git_repo):
        """Test creating a branch."""
        client = GitClient()
        client.set_repo_path(git_repo)

        result = client.create_branch("test-branch", checkout=True)

        assert result.success is True
        assert "test-branch" in result.branch_name

    def test_list_branches(self, git_repo):
        """Test listing branches."""
        client = GitClient()
        client.set_repo_path(git_repo)

        branches = client.list_branches()
        assert len(branches) >= 1
        assert any(b.name == "main" for b in branches)

    def test_get_log(self, git_repo):
        """Test getting commit log."""
        client = GitClient()
        client.set_repo_path(git_repo)

        log = client.get_log(count=5)
        assert len(log) >= 1
        assert log[0].message == "Initial commit"


# ========================================
# GitHubClient Tests
# ========================================

class TestGitHubClient:
    """Test GitHubClient class."""

    def test_init_client(self, git_config):
        """Test initializing client."""
        client = GitHubClient(config=git_config)
        assert client.is_configured is True

    def test_parse_https_url(self):
        """Test parsing HTTPS remote URL."""
        client = GitHubClient()
        client.configure(
            token="test",
            remote_url="https://github.com/myorg/myrepo.git",
        )
        assert client._owner == "myorg"
        assert client._repo == "myrepo"

    def test_parse_ssh_url(self):
        """Test parsing SSH remote URL."""
        client = GitHubClient()
        client.configure(
            token="test",
            remote_url="git@github.com:myorg/myrepo.git",
        )
        assert client._owner == "myorg"
        assert client._repo == "myrepo"

    def test_not_configured(self):
        """Test client not configured."""
        client = GitHubClient()
        assert client.is_configured is False

    @patch("src.git_integration.github_client.requests")
    def test_create_pr(self, mock_requests, git_config):
        """Test creating a PR."""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "number": 42,
            "html_url": "https://github.com/org/repo/pull/42",
        }
        mock_response.text = '{"number": 42}'
        mock_requests.request.return_value = mock_response

        client = GitHubClient(config=git_config)
        result = client.create_pull_request(
            title="Test PR",
            body="Description",
            head="feature/test",
            base="main",
        )

        assert result.success is True
        assert result.pr_number == 42

    @patch("src.git_integration.github_client.requests")
    def test_get_pr(self, mock_requests, git_config):
        """Test getting a PR."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "number": 42,
            "title": "Test PR",
            "body": "Description",
            "state": "open",
            "head": {"ref": "feature/test"},
            "base": {"ref": "main"},
            "html_url": "https://github.com/org/repo/pull/42",
            "user": {"login": "testuser"},
            "labels": [],
            "created_at": "2024-01-01T00:00:00Z",
        }
        mock_response.text = '{"number": 42}'
        mock_requests.request.return_value = mock_response

        client = GitHubClient(config=git_config)
        pr = client.get_pull_request(42)

        assert pr is not None
        assert pr.number == 42
        assert pr.status == PRStatus.OPEN


# ========================================
# WorkflowGenerator Tests
# ========================================

class TestWorkflowGenerator:
    """Test WorkflowGenerator class."""

    def test_generate_dbt_ci_workflow(self, dbt_config):
        """Test generating dbt CI workflow."""
        gen = WorkflowGenerator()
        workflow = gen.generate_dbt_ci_workflow(dbt_config)

        assert workflow.name == f"dbt CI/CD - {dbt_config.project_name}"
        assert "lint" in workflow.jobs
        assert "build" in workflow.jobs
        assert "test" in workflow.jobs

    def test_workflow_has_correct_triggers(self, dbt_config):
        """Test workflow triggers."""
        gen = WorkflowGenerator()
        workflow = gen.generate_dbt_ci_workflow(dbt_config)

        assert workflow.on_push is not None
        assert workflow.on_pull_request is not None
        assert workflow.on_workflow_dispatch is True

    def test_workflow_yaml_output(self, dbt_config):
        """Test YAML output."""
        gen = WorkflowGenerator()
        workflow = gen.generate_dbt_ci_workflow(dbt_config)
        yaml = workflow.to_yaml()

        assert "name:" in yaml
        assert "on:" in yaml
        assert "jobs:" in yaml
        assert "lint:" in yaml

    def test_generate_deploy_workflow(self):
        """Test generating deploy workflow."""
        gen = WorkflowGenerator()
        workflow = gen.generate_databridge_deploy_workflow(
            project_name="test",
            environments=["dev", "prod"],
        )

        assert "validate" in workflow.jobs
        assert "deploy-dev" in workflow.jobs
        assert "deploy-prod" in workflow.jobs

    def test_generate_mart_workflow(self):
        """Test generating mart factory workflow."""
        gen = WorkflowGenerator()
        workflow = gen.generate_mart_factory_workflow(
            project_name="gross_los",
            hierarchy_table="DB.SCHEMA.HIERARCHY",
            mapping_table="DB.SCHEMA.MAPPING",
        )

        assert "generate" in workflow.jobs
        assert "deploy" in workflow.jobs

    def test_pr_template(self):
        """Test PR template generation."""
        gen = WorkflowGenerator()
        template = gen.generate_pr_template(project_type="dbt")

        assert "## Summary" in template
        assert "dbt compile" in template
        assert "DataBridge" in template


# ========================================
# MCP Tools Tests
# ========================================

class TestMCPTools:
    """Test MCP tool registration."""

    def test_register_tools(self):
        """Test that tools are registered correctly."""
        from src.git_integration.mcp_tools import register_git_integration_tools

        class MockMCP:
            def __init__(self):
                self.tools = []

            def tool(self):
                def decorator(func):
                    self.tools.append(func.__name__)
                    return func
                return decorator

        mcp = MockMCP()
        result = register_git_integration_tools(mcp)

        assert result["tools_registered"] == 12

        # Check specific tools
        assert "configure_git" in result["tools"]
        assert "git_status" in result["tools"]
        assert "git_commit" in result["tools"]
        assert "github_create_pr" in result["tools"]
        assert "generate_dbt_workflow" in result["tools"]

    def test_git_status_tool(self, git_repo):
        """Test git_status MCP tool."""
        from src.git_integration.mcp_tools import register_git_integration_tools

        class MockMCP:
            def __init__(self):
                self.registered = {}

            def tool(self):
                def decorator(func):
                    self.registered[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_git_integration_tools(mcp)

        result = mcp.registered["git_status"](repo_path=git_repo)
        assert result["success"] is True
        assert result["branch"] == "main"

    def test_generate_dbt_workflow_tool(self):
        """Test generate_dbt_workflow MCP tool."""
        from src.git_integration.mcp_tools import register_git_integration_tools

        class MockMCP:
            def __init__(self):
                self.registered = {}

            def tool(self):
                def decorator(func):
                    self.registered[func.__name__] = func
                    return func
                return decorator

        mcp = MockMCP()
        register_git_integration_tools(mcp)

        result = mcp.registered["generate_dbt_workflow"](
            project_name="test_project",
            dbt_version="1.7.0",
            database_type="snowflake",
            run_commands="build,test",
        )

        assert result["success"] is True
        assert "yaml" in result
        assert "lint" in result["jobs"]


# ========================================
# Integration Tests
# ========================================

class TestIntegration:
    """Integration tests for complete workflows."""

    def test_complete_git_workflow(self, git_repo):
        """Test complete git workflow."""
        client = GitClient()
        client.set_repo_path(git_repo)

        # Create a branch
        branch_result = client.create_branch("feature/test", checkout=True)
        assert branch_result.success is True

        # Create a file
        test_file = Path(git_repo) / "new_model.sql"
        test_file.write_text("SELECT 1")

        # Commit
        commit_result = client.commit(
            message="Add new model",
            files=["new_model.sql"],
        )
        assert commit_result.success is True

        # Check status
        status = client.status()
        assert status["branch"] == "feature/test"
        assert status["clean"] is True

    def test_workflow_to_file(self, temp_dir, dbt_config):
        """Test writing workflow to file."""
        gen = WorkflowGenerator()
        workflow = gen.generate_dbt_ci_workflow(dbt_config)

        output_path = Path(temp_dir) / ".github" / "workflows" / "dbt-ci.yml"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(workflow.to_yaml())

        assert output_path.exists()

        content = output_path.read_text()
        assert "name:" in content
        assert "jobs:" in content
