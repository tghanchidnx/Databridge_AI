"""
Git Integration Types.

Pydantic models for Git/GitHub operations and CI/CD workflow generation.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
import uuid


class GitProvider(str, Enum):
    """Supported Git providers."""
    GITHUB = "github"
    GITLAB = "gitlab"
    BITBUCKET = "bitbucket"
    AZURE_DEVOPS = "azure_devops"


class BranchStrategy(str, Enum):
    """Branch naming strategies."""
    FEATURE = "feature"  # feature/project-name
    RELEASE = "release"  # release/v1.0.0
    HOTFIX = "hotfix"    # hotfix/issue-123
    DEPLOY = "deploy"    # deploy/env-name


class PRStatus(str, Enum):
    """Pull request status."""
    OPEN = "open"
    CLOSED = "closed"
    MERGED = "merged"
    DRAFT = "draft"


class WorkflowTrigger(str, Enum):
    """GitHub Actions workflow triggers."""
    PUSH = "push"
    PULL_REQUEST = "pull_request"
    SCHEDULE = "schedule"
    WORKFLOW_DISPATCH = "workflow_dispatch"
    REPOSITORY_DISPATCH = "repository_dispatch"


class DbtCommand(str, Enum):
    """dbt commands for CI/CD."""
    RUN = "run"
    TEST = "test"
    BUILD = "build"
    COMPILE = "compile"
    DOCS_GENERATE = "docs generate"
    SOURCE_FRESHNESS = "source freshness"
    SEED = "seed"
    SNAPSHOT = "snapshot"


class GitConfig(BaseModel):
    """Git integration configuration."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])

    # Repository settings
    repo_path: str  # Local path to repository
    remote_url: Optional[str] = None  # GitHub/GitLab URL
    default_branch: str = "main"

    # Provider settings
    provider: GitProvider = GitProvider.GITHUB

    # Authentication
    username: Optional[str] = None
    email: Optional[str] = None
    token: Optional[str] = None  # Personal access token

    # Commit settings
    auto_commit: bool = False
    commit_prefix: str = "[DataBridge]"
    sign_commits: bool = False

    # Branch settings
    branch_strategy: BranchStrategy = BranchStrategy.FEATURE
    protected_branches: List[str] = Field(default_factory=lambda: ["main", "master"])

    # PR settings
    auto_create_pr: bool = False
    pr_template: Optional[str] = None
    reviewers: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excluding sensitive data)."""
        return {
            "id": self.id,
            "repo_path": self.repo_path,
            "remote_url": self.remote_url,
            "default_branch": self.default_branch,
            "provider": self.provider.value,
            "username": self.username,
            "email": self.email,
            "has_token": bool(self.token),
            "auto_commit": self.auto_commit,
            "commit_prefix": self.commit_prefix,
            "branch_strategy": self.branch_strategy.value,
            "auto_create_pr": self.auto_create_pr,
        }


class CommitInfo(BaseModel):
    """Information about a git commit."""
    sha: str
    message: str
    author: str
    email: str
    timestamp: datetime
    files_changed: int = 0
    insertions: int = 0
    deletions: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sha": self.sha,
            "message": self.message,
            "author": self.author,
            "timestamp": self.timestamp.isoformat(),
            "files_changed": self.files_changed,
        }


class BranchInfo(BaseModel):
    """Information about a git branch."""
    name: str
    is_current: bool = False
    is_remote: bool = False
    tracking: Optional[str] = None  # Remote tracking branch
    ahead: int = 0
    behind: int = 0
    last_commit: Optional[CommitInfo] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "is_current": self.is_current,
            "is_remote": self.is_remote,
            "tracking": self.tracking,
            "ahead": self.ahead,
            "behind": self.behind,
        }


class PullRequest(BaseModel):
    """Pull request information."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    number: Optional[int] = None
    title: str
    body: str
    source_branch: str
    target_branch: str
    status: PRStatus = PRStatus.DRAFT
    url: Optional[str] = None

    # Metadata
    author: Optional[str] = None
    reviewers: List[str] = Field(default_factory=list)
    labels: List[str] = Field(default_factory=list)

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    merged_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "number": self.number,
            "title": self.title,
            "source_branch": self.source_branch,
            "target_branch": self.target_branch,
            "status": self.status.value,
            "url": self.url,
            "author": self.author,
            "labels": self.labels,
        }


class WorkflowStep(BaseModel):
    """A step in a GitHub Actions workflow."""
    name: str
    uses: Optional[str] = None  # Action to use (e.g., actions/checkout@v4)
    run: Optional[str] = None   # Shell command to run
    with_params: Dict[str, Any] = Field(default_factory=dict)
    env: Dict[str, str] = Field(default_factory=dict)
    if_condition: Optional[str] = None
    continue_on_error: bool = False

    def to_yaml_dict(self) -> Dict[str, Any]:
        """Convert to YAML-compatible dictionary."""
        step = {"name": self.name}
        if self.uses:
            step["uses"] = self.uses
        if self.run:
            step["run"] = self.run
        if self.with_params:
            step["with"] = self.with_params
        if self.env:
            step["env"] = self.env
        if self.if_condition:
            step["if"] = self.if_condition
        if self.continue_on_error:
            step["continue-on-error"] = True
        return step


class WorkflowJob(BaseModel):
    """A job in a GitHub Actions workflow."""
    name: str
    runs_on: str = "ubuntu-latest"
    needs: List[str] = Field(default_factory=list)
    env: Dict[str, str] = Field(default_factory=dict)
    steps: List[WorkflowStep] = Field(default_factory=list)
    if_condition: Optional[str] = None
    timeout_minutes: int = 60

    # Matrix strategy
    matrix: Optional[Dict[str, List[Any]]] = None
    fail_fast: bool = True

    def to_yaml_dict(self) -> Dict[str, Any]:
        """Convert to YAML-compatible dictionary."""
        job = {
            "name": self.name,
            "runs-on": self.runs_on,
            "steps": [s.to_yaml_dict() for s in self.steps],
        }
        if self.needs:
            job["needs"] = self.needs
        if self.env:
            job["env"] = self.env
        if self.if_condition:
            job["if"] = self.if_condition
        if self.timeout_minutes != 60:
            job["timeout-minutes"] = self.timeout_minutes
        if self.matrix:
            job["strategy"] = {
                "matrix": self.matrix,
                "fail-fast": self.fail_fast,
            }
        return job


class GitHubActionsWorkflow(BaseModel):
    """A complete GitHub Actions workflow."""
    name: str
    filename: str  # e.g., "dbt-ci.yml"

    # Triggers
    on_push: Optional[Dict[str, Any]] = None
    on_pull_request: Optional[Dict[str, Any]] = None
    on_schedule: Optional[List[Dict[str, str]]] = None  # cron expressions
    on_workflow_dispatch: bool = False

    # Environment
    env: Dict[str, str] = Field(default_factory=dict)

    # Jobs
    jobs: Dict[str, WorkflowJob] = Field(default_factory=dict)

    # Concurrency
    concurrency_group: Optional[str] = None
    cancel_in_progress: bool = True

    def to_yaml(self) -> str:
        """Generate YAML string."""
        import yaml

        workflow = {"name": self.name}

        # Build 'on' trigger
        on_triggers = {}
        if self.on_push:
            on_triggers["push"] = self.on_push
        if self.on_pull_request:
            on_triggers["pull_request"] = self.on_pull_request
        if self.on_schedule:
            on_triggers["schedule"] = self.on_schedule
        if self.on_workflow_dispatch:
            on_triggers["workflow_dispatch"] = {}

        workflow["on"] = on_triggers

        if self.env:
            workflow["env"] = self.env

        if self.concurrency_group:
            workflow["concurrency"] = {
                "group": self.concurrency_group,
                "cancel-in-progress": self.cancel_in_progress,
            }

        workflow["jobs"] = {
            job_id: job.to_yaml_dict()
            for job_id, job in self.jobs.items()
        }

        return yaml.dump(workflow, sort_keys=False, default_flow_style=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "filename": self.filename,
            "job_count": len(self.jobs),
            "triggers": {
                "push": bool(self.on_push),
                "pull_request": bool(self.on_pull_request),
                "schedule": bool(self.on_schedule),
                "workflow_dispatch": self.on_workflow_dispatch,
            },
        }


class DbtCIConfig(BaseModel):
    """Configuration for dbt CI/CD workflow."""
    project_name: str

    # dbt settings
    dbt_version: str = "1.7.0"
    profiles_dir: str = ".dbt"
    target: str = "prod"

    # Database settings
    database_type: str = "snowflake"

    # Commands to run
    run_commands: List[DbtCommand] = Field(
        default_factory=lambda: [DbtCommand.BUILD, DbtCommand.TEST]
    )

    # Selectors
    selector: Optional[str] = None  # e.g., "state:modified+"
    exclude: Optional[str] = None

    # Environments
    environments: List[str] = Field(default_factory=lambda: ["dev", "prod"])

    # Secrets (names, not values)
    account_secret: str = "SNOWFLAKE_ACCOUNT"
    user_secret: str = "SNOWFLAKE_USER"
    password_secret: str = "SNOWFLAKE_PASSWORD"
    role_secret: str = "SNOWFLAKE_ROLE"
    warehouse_secret: str = "SNOWFLAKE_WAREHOUSE"
    database_secret: str = "SNOWFLAKE_DATABASE"
    schema_secret: str = "SNOWFLAKE_SCHEMA"

    # Artifacts
    upload_artifacts: bool = True
    artifact_paths: List[str] = Field(
        default_factory=lambda: ["target/manifest.json", "target/run_results.json"]
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "project_name": self.project_name,
            "dbt_version": self.dbt_version,
            "database_type": self.database_type,
            "run_commands": [c.value for c in self.run_commands],
            "environments": self.environments,
        }


class GitOperationResult(BaseModel):
    """Result of a git operation."""
    success: bool
    operation: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None

    # For commits
    commit_sha: Optional[str] = None

    # For branches
    branch_name: Optional[str] = None

    # For PRs
    pr_url: Optional[str] = None
    pr_number: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "success": self.success,
            "operation": self.operation,
            "message": self.message,
        }
        if self.commit_sha:
            result["commit_sha"] = self.commit_sha
        if self.branch_name:
            result["branch_name"] = self.branch_name
        if self.pr_url:
            result["pr_url"] = self.pr_url
        if self.pr_number:
            result["pr_number"] = self.pr_number
        if self.error:
            result["error"] = self.error
        if self.details:
            result["details"] = self.details
        return result
