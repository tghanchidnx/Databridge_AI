"""
MCP Tools for Git/CI-CD Integration.

Provides 12 tools for git operations, GitHub integration, and CI/CD workflows:

Git Operations (5):
- configure_git
- git_status
- git_commit
- git_create_branch
- git_push

GitHub Operations (4):
- github_create_pr
- github_get_pr_status
- github_list_prs
- github_merge_pr

CI/CD Workflows (3):
- generate_dbt_workflow
- generate_deploy_workflow
- generate_mart_workflow
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import (
    BranchStrategy,
    DbtCIConfig,
    DbtCommand,
    GitConfig,
    GitProvider,
)
from .git_client import GitClient
from .github_client import GitHubClient
from .workflow_generator import WorkflowGenerator

logger = logging.getLogger(__name__)


def register_git_integration_tools(mcp, settings=None) -> Dict[str, Any]:
    """
    Register Git/CI-CD Integration MCP tools.

    Args:
        mcp: The FastMCP instance
        settings: Optional settings

    Returns:
        Dict with registration info
    """

    # Initialize components
    git_client = GitClient()
    github_client = GitHubClient()
    workflow_gen = WorkflowGenerator()

    # Store for configuration
    _config: Dict[str, GitConfig] = {}

    # ========================================
    # Git Operations (5 tools)
    # ========================================

    @mcp.tool()
    def configure_git(
        repo_path: str,
        remote_url: Optional[str] = None,
        username: Optional[str] = None,
        email: Optional[str] = None,
        token: Optional[str] = None,
        default_branch: str = "main",
        branch_strategy: str = "feature",
        auto_commit: bool = False,
        commit_prefix: str = "[DataBridge]",
    ) -> Dict[str, Any]:
        """
        Configure git integration settings.

        Sets up git client for a repository with optional GitHub authentication.

        Args:
            repo_path: Path to the git repository
            remote_url: GitHub/GitLab remote URL (e.g., https://github.com/owner/repo.git)
            username: Git username for commits
            email: Git email for commits
            token: GitHub personal access token (for PRs)
            default_branch: Default branch name
            branch_strategy: Branch naming strategy (feature, release, hotfix, deploy)
            auto_commit: Auto-commit generated files
            commit_prefix: Prefix for commit messages

        Returns:
            Configuration status

        Example:
            configure_git(
                repo_path="C:/projects/my-dbt",
                remote_url="https://github.com/myorg/my-dbt.git",
                username="databridge-bot",
                email="bot@example.com",
                token="ghp_xxxx",
                branch_strategy="feature"
            )
        """
        try:
            # Parse branch strategy
            try:
                strategy = BranchStrategy(branch_strategy.lower())
            except ValueError:
                strategy = BranchStrategy.FEATURE

            config = GitConfig(
                repo_path=repo_path,
                remote_url=remote_url,
                username=username,
                email=email,
                token=token,
                default_branch=default_branch,
                branch_strategy=strategy,
                auto_commit=auto_commit,
                commit_prefix=commit_prefix,
            )

            # Store config
            _config["default"] = config

            # Configure clients
            git_client.config = config
            git_client.set_repo_path(repo_path)

            if token and remote_url:
                github_client.configure(
                    token=token,
                    remote_url=remote_url,
                )

            return {
                "success": True,
                "config": config.to_dict(),
                "git_configured": True,
                "github_configured": github_client.is_configured,
                "message": f"Configured git for {repo_path}",
            }

        except Exception as e:
            logger.error(f"Failed to configure git: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def git_status(
        repo_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get git repository status.

        Shows current branch, staged/modified/untracked files, and sync status.

        Args:
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Repository status

        Example:
            git_status(repo_path="C:/projects/my-dbt")
        """
        try:
            if repo_path:
                git_client.set_repo_path(repo_path)

            status = git_client.status()

            if "error" in status:
                return {"success": False, "error": status["error"]}

            return {
                "success": True,
                **status,
            }

        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def git_commit(
        message: str,
        files: Optional[str] = None,
        all_files: bool = False,
        repo_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Commit changes to the repository.

        Stages and commits files with the specified message.

        Args:
            message: Commit message
            files: Comma-separated list of files to commit (or None for staged)
            all_files: Commit all changes (git add -A)
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Commit result with SHA

        Example:
            git_commit(
                message="Add dbt models for revenue hierarchy",
                files="models/staging/stg_revenue.sql,models/marts/fct_revenue.sql"
            )
        """
        try:
            if repo_path:
                git_client.set_repo_path(repo_path)

            file_list = None
            if files:
                file_list = [f.strip() for f in files.split(",")]

            result = git_client.commit(
                message=message,
                files=file_list,
                all_files=all_files,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to commit: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def git_create_branch(
        branch_name: str,
        checkout: bool = True,
        from_branch: Optional[str] = None,
        repo_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new git branch.

        Creates a branch with optional naming strategy prefix.

        Args:
            branch_name: Branch name (will be prefixed with strategy if configured)
            checkout: Switch to the new branch
            from_branch: Base branch to create from
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Branch creation result

        Example:
            git_create_branch(
                branch_name="add-revenue-hierarchy",
                from_branch="main"
            )
        """
        try:
            if repo_path:
                git_client.set_repo_path(repo_path)

            result = git_client.create_branch(
                name=branch_name,
                checkout=checkout,
                from_branch=from_branch,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to create branch: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def git_push(
        remote: str = "origin",
        branch: Optional[str] = None,
        set_upstream: bool = True,
        repo_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Push commits to remote repository.

        Pushes the current or specified branch to the remote.

        Args:
            remote: Remote name (default: origin)
            branch: Branch to push (current if not specified)
            set_upstream: Set upstream tracking (-u flag)
            repo_path: Repository path (uses configured path if not specified)

        Returns:
            Push result

        Example:
            git_push(remote="origin", set_upstream=True)
        """
        try:
            if repo_path:
                git_client.set_repo_path(repo_path)

            result = git_client.push(
                remote=remote,
                branch=branch,
                set_upstream=set_upstream,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to push: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # GitHub Operations (4 tools)
    # ========================================

    @mcp.tool()
    def github_create_pr(
        title: str,
        body: str,
        head_branch: str,
        base_branch: Optional[str] = None,
        draft: bool = False,
        reviewers: Optional[str] = None,
        labels: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a GitHub pull request.

        Creates a PR from head branch to base branch with optional reviewers and labels.

        Args:
            title: PR title
            body: PR description (supports markdown)
            head_branch: Source branch with changes
            base_branch: Target branch (default: main)
            draft: Create as draft PR
            reviewers: Comma-separated list of reviewer usernames
            labels: Comma-separated list of labels

        Returns:
            PR creation result with URL

        Example:
            github_create_pr(
                title="Add revenue hierarchy models",
                body="## Summary\\nAdds dbt models for revenue...",
                head_branch="feature/add-revenue-hierarchy",
                base_branch="main",
                reviewers="john,jane",
                labels="dbt,hierarchy"
            )
        """
        try:
            if not github_client.is_configured:
                return {
                    "success": False,
                    "error": "GitHub not configured. Call configure_git with token and remote_url first.",
                }

            reviewer_list = None
            if reviewers:
                reviewer_list = [r.strip() for r in reviewers.split(",")]

            label_list = None
            if labels:
                label_list = [l.strip() for l in labels.split(",")]

            result = github_client.create_pull_request(
                title=title,
                body=body,
                head=head_branch,
                base=base_branch,
                draft=draft,
                reviewers=reviewer_list,
                labels=label_list,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to create PR: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def github_get_pr_status(
        pr_number: int,
    ) -> Dict[str, Any]:
        """
        Get status of a pull request.

        Shows PR details, check status, and mergeability.

        Args:
            pr_number: Pull request number

        Returns:
            PR status with checks

        Example:
            github_get_pr_status(pr_number=42)
        """
        try:
            if not github_client.is_configured:
                return {
                    "success": False,
                    "error": "GitHub not configured",
                }

            status = github_client.get_pr_status(pr_number)

            if "error" in status:
                return {"success": False, "error": status["error"]}

            return {
                "success": True,
                **status,
            }

        except Exception as e:
            logger.error(f"Failed to get PR status: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def github_list_prs(
        state: str = "open",
        head_branch: Optional[str] = None,
        base_branch: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List pull requests.

        Lists PRs with optional filtering by state and branches.

        Args:
            state: PR state (open, closed, all)
            head_branch: Filter by source branch
            base_branch: Filter by target branch

        Returns:
            List of pull requests

        Example:
            github_list_prs(state="open", base_branch="main")
        """
        try:
            if not github_client.is_configured:
                return {
                    "success": False,
                    "error": "GitHub not configured",
                }

            prs = github_client.list_pull_requests(
                state=state,
                head=head_branch,
                base=base_branch,
            )

            return {
                "success": True,
                "count": len(prs),
                "pull_requests": [pr.to_dict() for pr in prs],
            }

        except Exception as e:
            logger.error(f"Failed to list PRs: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def github_merge_pr(
        pr_number: int,
        merge_method: str = "squash",
        commit_message: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Merge a pull request.

        Merges the PR using the specified method.

        Args:
            pr_number: Pull request number
            merge_method: Merge method (merge, squash, rebase)
            commit_message: Custom commit message for squash/merge

        Returns:
            Merge result

        Example:
            github_merge_pr(pr_number=42, merge_method="squash")
        """
        try:
            if not github_client.is_configured:
                return {
                    "success": False,
                    "error": "GitHub not configured",
                }

            result = github_client.merge_pull_request(
                pr_number=pr_number,
                merge_method=merge_method,
                commit_message=commit_message,
            )

            return result.to_dict()

        except Exception as e:
            logger.error(f"Failed to merge PR: {e}")
            return {"success": False, "error": str(e)}

    # ========================================
    # CI/CD Workflows (3 tools)
    # ========================================

    @mcp.tool()
    def generate_dbt_workflow(
        project_name: str,
        dbt_version: str = "1.7.0",
        database_type: str = "snowflake",
        run_commands: str = "build,test",
        environments: str = "dev,prod",
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a GitHub Actions workflow for dbt CI/CD.

        Creates a complete workflow with lint, build, test, and deploy jobs.

        Args:
            project_name: dbt project name
            dbt_version: dbt version to use
            database_type: Database type (snowflake, postgres, bigquery)
            run_commands: Comma-separated dbt commands (build, test, run, docs)
            environments: Comma-separated environments (dev, prod)
            output_path: Path to write workflow file (optional)

        Returns:
            Generated workflow YAML

        Example:
            generate_dbt_workflow(
                project_name="revenue_mart",
                dbt_version="1.7.0",
                database_type="snowflake",
                run_commands="build,test,docs generate",
                output_path=".github/workflows/dbt-ci.yml"
            )
        """
        try:
            # Parse commands
            commands = []
            for cmd in run_commands.split(","):
                cmd = cmd.strip().lower()
                try:
                    if cmd == "docs generate":
                        commands.append(DbtCommand.DOCS_GENERATE)
                    else:
                        commands.append(DbtCommand(cmd))
                except ValueError:
                    pass

            # Create config
            config = DbtCIConfig(
                project_name=project_name,
                dbt_version=dbt_version,
                database_type=database_type,
                run_commands=commands,
                environments=[e.strip() for e in environments.split(",")],
            )

            # Generate workflow
            workflow = workflow_gen.generate_dbt_ci_workflow(config)
            yaml_content = workflow.to_yaml()

            # Write to file if path specified
            if output_path:
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(yaml_content)

            return {
                "success": True,
                "workflow_name": workflow.name,
                "filename": workflow.filename,
                "job_count": len(workflow.jobs),
                "jobs": list(workflow.jobs.keys()),
                "output_path": output_path,
                "yaml": yaml_content[:2000] + "..." if len(yaml_content) > 2000 else yaml_content,
            }

        except Exception as e:
            logger.error(f"Failed to generate dbt workflow: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_deploy_workflow(
        project_name: str,
        environments: str = "dev,prod",
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a GitHub Actions workflow for DataBridge deployments.

        Creates a workflow to deploy hierarchy DDL scripts to multiple environments.

        Args:
            project_name: Project name
            environments: Comma-separated environments
            output_path: Path to write workflow file (optional)

        Returns:
            Generated workflow YAML

        Example:
            generate_deploy_workflow(
                project_name="upstream_gross",
                environments="dev,staging,prod",
                output_path=".github/workflows/deploy.yml"
            )
        """
        try:
            env_list = [e.strip() for e in environments.split(",")]

            workflow = workflow_gen.generate_databridge_deploy_workflow(
                project_name=project_name,
                environments=env_list,
            )
            yaml_content = workflow.to_yaml()

            # Write to file if path specified
            if output_path:
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(yaml_content)

            return {
                "success": True,
                "workflow_name": workflow.name,
                "filename": workflow.filename,
                "environments": env_list,
                "job_count": len(workflow.jobs),
                "jobs": list(workflow.jobs.keys()),
                "output_path": output_path,
                "yaml": yaml_content[:2000] + "..." if len(yaml_content) > 2000 else yaml_content,
            }

        except Exception as e:
            logger.error(f"Failed to generate deploy workflow: {e}")
            return {"success": False, "error": str(e)}

    @mcp.tool()
    def generate_mart_workflow(
        project_name: str,
        hierarchy_table: str,
        mapping_table: str,
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a GitHub Actions workflow for Mart Factory pipelines.

        Creates a workflow to auto-generate and deploy data mart DDL.

        Args:
            project_name: Project name
            hierarchy_table: Hierarchy table name (e.g., ANALYTICS.PUBLIC.TBL_0_HIERARCHY)
            mapping_table: Mapping table name
            output_path: Path to write workflow file (optional)

        Returns:
            Generated workflow YAML

        Example:
            generate_mart_workflow(
                project_name="upstream_gross",
                hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_",
                mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
                output_path=".github/workflows/mart-factory.yml"
            )
        """
        try:
            workflow = workflow_gen.generate_mart_factory_workflow(
                project_name=project_name,
                hierarchy_table=hierarchy_table,
                mapping_table=mapping_table,
            )
            yaml_content = workflow.to_yaml()

            # Write to file if path specified
            if output_path:
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(yaml_content)

            return {
                "success": True,
                "workflow_name": workflow.name,
                "filename": workflow.filename,
                "hierarchy_table": hierarchy_table,
                "mapping_table": mapping_table,
                "job_count": len(workflow.jobs),
                "jobs": list(workflow.jobs.keys()),
                "output_path": output_path,
                "yaml": yaml_content[:2000] + "..." if len(yaml_content) > 2000 else yaml_content,
            }

        except Exception as e:
            logger.error(f"Failed to generate mart workflow: {e}")
            return {"success": False, "error": str(e)}

    # Return registration info
    return {
        "tools_registered": 12,
        "tools": [
            # Git Operations
            "configure_git",
            "git_status",
            "git_commit",
            "git_create_branch",
            "git_push",
            # GitHub Operations
            "github_create_pr",
            "github_get_pr_status",
            "github_list_prs",
            "github_merge_pr",
            # CI/CD Workflows
            "generate_dbt_workflow",
            "generate_deploy_workflow",
            "generate_mart_workflow",
        ],
    }
