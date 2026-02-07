"""
Git/CI-CD Integration Module.

Provides git operations, GitHub integration, and CI/CD workflow generation
for DataBridge AI projects.

Components:
- GitClient: Local git operations (commit, branch, push)
- GitHubClient: GitHub API for PRs, issues, releases
- WorkflowGenerator: GitHub Actions workflow generation

MCP Tools (12):
- Git Operations: configure_git, git_status, git_commit, git_create_branch, git_push
- GitHub: github_create_pr, github_get_pr_status, github_list_prs, github_merge_pr
- Workflows: generate_dbt_workflow, generate_deploy_workflow, generate_mart_workflow
"""

from .types import (
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
)

from .git_client import GitClient
from .github_client import GitHubClient
from .workflow_generator import WorkflowGenerator
from .mcp_tools import register_git_integration_tools

__all__ = [
    # Enums
    "GitProvider",
    "BranchStrategy",
    "PRStatus",
    "WorkflowTrigger",
    "DbtCommand",
    # Config
    "GitConfig",
    "DbtCIConfig",
    # Git models
    "CommitInfo",
    "BranchInfo",
    "PullRequest",
    "GitOperationResult",
    # Workflow models
    "WorkflowStep",
    "WorkflowJob",
    "GitHubActionsWorkflow",
    # Clients
    "GitClient",
    "GitHubClient",
    "WorkflowGenerator",
    # Registration
    "register_git_integration_tools",
]
