"""
Git Client.

Local git operations wrapper using subprocess.
Handles commits, branches, push/pull, and status.
"""

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .types import (
    BranchInfo,
    BranchStrategy,
    CommitInfo,
    GitConfig,
    GitOperationResult,
)

logger = logging.getLogger(__name__)


class GitClient:
    """Client for local git operations."""

    def __init__(self, config: Optional[GitConfig] = None):
        """
        Initialize the git client.

        Args:
            config: Git configuration
        """
        self.config = config
        self._repo_path: Optional[Path] = None
        if config and config.repo_path:
            self._repo_path = Path(config.repo_path)

    def set_repo_path(self, path: str) -> None:
        """Set the repository path."""
        self._repo_path = Path(path)

    @property
    def repo_path(self) -> Path:
        """Get the repository path."""
        if self._repo_path:
            return self._repo_path
        return Path.cwd()

    def _run_git(
        self,
        args: List[str],
        capture_output: bool = True,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """
        Run a git command.

        Args:
            args: Git command arguments
            capture_output: Whether to capture stdout/stderr
            check: Whether to raise on non-zero exit

        Returns:
            CompletedProcess result
        """
        cmd = ["git"] + args
        logger.debug(f"Running: {' '.join(cmd)}")

        env = os.environ.copy()
        if self.config:
            if self.config.username:
                env["GIT_AUTHOR_NAME"] = self.config.username
                env["GIT_COMMITTER_NAME"] = self.config.username
            if self.config.email:
                env["GIT_AUTHOR_EMAIL"] = self.config.email
                env["GIT_COMMITTER_EMAIL"] = self.config.email

        return subprocess.run(
            cmd,
            cwd=str(self.repo_path),
            capture_output=capture_output,
            text=True,
            check=check,
            env=env,
        )

    def is_repo(self) -> bool:
        """Check if the path is a git repository."""
        try:
            result = self._run_git(["rev-parse", "--git-dir"], check=False)
            return result.returncode == 0
        except Exception:
            return False

    def init(self, initial_branch: str = "main") -> GitOperationResult:
        """
        Initialize a new git repository.

        Args:
            initial_branch: Name of the initial branch

        Returns:
            Operation result
        """
        try:
            if self.is_repo():
                return GitOperationResult(
                    success=True,
                    operation="init",
                    message="Repository already initialized",
                )

            self._run_git(["init", "-b", initial_branch])

            # Configure user if provided
            if self.config:
                if self.config.username:
                    self._run_git(["config", "user.name", self.config.username])
                if self.config.email:
                    self._run_git(["config", "user.email", self.config.email])

            return GitOperationResult(
                success=True,
                operation="init",
                message=f"Initialized git repository with branch '{initial_branch}'",
                branch_name=initial_branch,
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="init",
                message="Failed to initialize repository",
                error=e.stderr,
            )

    def status(self) -> Dict[str, Any]:
        """
        Get repository status.

        Returns:
            Status dictionary
        """
        try:
            if not self.is_repo():
                return {"error": "Not a git repository"}

            # Get current branch
            branch_result = self._run_git(["branch", "--show-current"])
            current_branch = branch_result.stdout.strip()

            # Get status porcelain
            status_result = self._run_git(["status", "--porcelain"])
            status_lines = status_result.stdout.strip().split("\n") if status_result.stdout.strip() else []

            # Parse status
            staged = []
            modified = []
            untracked = []

            for line in status_lines:
                if len(line) < 3:
                    continue
                index_status = line[0]
                worktree_status = line[1]
                filename = line[3:]

                if index_status in "MADRC":
                    staged.append(filename)
                if worktree_status == "M":
                    modified.append(filename)
                if index_status == "?" and worktree_status == "?":
                    untracked.append(filename)

            # Get ahead/behind
            ahead = 0
            behind = 0
            try:
                ab_result = self._run_git(
                    ["rev-list", "--left-right", "--count", f"{current_branch}...@{{u}}"],
                    check=False
                )
                if ab_result.returncode == 0:
                    parts = ab_result.stdout.strip().split("\t")
                    if len(parts) == 2:
                        ahead = int(parts[0])
                        behind = int(parts[1])
            except Exception:
                pass

            # Get remote
            remote_url = None
            try:
                remote_result = self._run_git(["remote", "get-url", "origin"], check=False)
                if remote_result.returncode == 0:
                    remote_url = remote_result.stdout.strip()
            except Exception:
                pass

            return {
                "branch": current_branch,
                "remote_url": remote_url,
                "staged_count": len(staged),
                "modified_count": len(modified),
                "untracked_count": len(untracked),
                "staged": staged[:10],  # Limit for display
                "modified": modified[:10],
                "untracked": untracked[:10],
                "ahead": ahead,
                "behind": behind,
                "clean": len(staged) == 0 and len(modified) == 0,
            }

        except Exception as e:
            return {"error": str(e)}

    def add(
        self,
        files: Optional[List[str]] = None,
        all_files: bool = False,
    ) -> GitOperationResult:
        """
        Stage files for commit.

        Args:
            files: List of files to stage
            all_files: Stage all changes

        Returns:
            Operation result
        """
        try:
            if all_files:
                self._run_git(["add", "-A"])
                return GitOperationResult(
                    success=True,
                    operation="add",
                    message="Staged all changes",
                )
            elif files:
                self._run_git(["add"] + files)
                return GitOperationResult(
                    success=True,
                    operation="add",
                    message=f"Staged {len(files)} file(s)",
                    details={"files": files},
                )
            else:
                return GitOperationResult(
                    success=False,
                    operation="add",
                    message="No files specified",
                )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="add",
                message="Failed to stage files",
                error=e.stderr,
            )

    def commit(
        self,
        message: str,
        files: Optional[List[str]] = None,
        all_files: bool = False,
    ) -> GitOperationResult:
        """
        Commit staged changes.

        Args:
            message: Commit message
            files: Files to commit (will stage first)
            all_files: Commit all changes

        Returns:
            Operation result
        """
        try:
            # Stage files if specified
            if files or all_files:
                add_result = self.add(files=files, all_files=all_files)
                if not add_result.success:
                    return add_result

            # Add prefix if configured
            if self.config and self.config.commit_prefix:
                if not message.startswith(self.config.commit_prefix):
                    message = f"{self.config.commit_prefix} {message}"

            # Commit
            self._run_git(["commit", "-m", message])

            # Get commit SHA
            sha_result = self._run_git(["rev-parse", "HEAD"])
            sha = sha_result.stdout.strip()

            return GitOperationResult(
                success=True,
                operation="commit",
                message=f"Created commit {sha[:8]}",
                commit_sha=sha,
            )

        except subprocess.CalledProcessError as e:
            error_msg = e.stderr if e.stderr else str(e)
            if "nothing to commit" in error_msg.lower():
                return GitOperationResult(
                    success=True,
                    operation="commit",
                    message="Nothing to commit, working tree clean",
                )
            return GitOperationResult(
                success=False,
                operation="commit",
                message="Failed to commit",
                error=error_msg,
            )

    def create_branch(
        self,
        name: str,
        checkout: bool = True,
        from_branch: Optional[str] = None,
    ) -> GitOperationResult:
        """
        Create a new branch.

        Args:
            name: Branch name
            checkout: Whether to switch to the new branch
            from_branch: Base branch to create from

        Returns:
            Operation result
        """
        try:
            # Build branch name with strategy prefix
            if self.config and self.config.branch_strategy:
                strategy = self.config.branch_strategy.value
                if not name.startswith(f"{strategy}/"):
                    name = f"{strategy}/{name}"

            args = ["checkout", "-b", name] if checkout else ["branch", name]
            if from_branch:
                args.append(from_branch)

            self._run_git(args)

            return GitOperationResult(
                success=True,
                operation="create_branch",
                message=f"Created branch '{name}'",
                branch_name=name,
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="create_branch",
                message="Failed to create branch",
                error=e.stderr,
            )

    def checkout(self, branch: str) -> GitOperationResult:
        """
        Switch to a branch.

        Args:
            branch: Branch name

        Returns:
            Operation result
        """
        try:
            self._run_git(["checkout", branch])
            return GitOperationResult(
                success=True,
                operation="checkout",
                message=f"Switched to branch '{branch}'",
                branch_name=branch,
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="checkout",
                message="Failed to checkout branch",
                error=e.stderr,
            )

    def push(
        self,
        remote: str = "origin",
        branch: Optional[str] = None,
        set_upstream: bool = False,
        force: bool = False,
    ) -> GitOperationResult:
        """
        Push to remote.

        Args:
            remote: Remote name
            branch: Branch to push (current if None)
            set_upstream: Set upstream tracking
            force: Force push (use with caution)

        Returns:
            Operation result
        """
        try:
            args = ["push"]

            if set_upstream:
                args.extend(["-u", remote])
            else:
                args.append(remote)

            if branch:
                args.append(branch)

            if force:
                args.append("--force")

            self._run_git(args)

            return GitOperationResult(
                success=True,
                operation="push",
                message=f"Pushed to {remote}",
                branch_name=branch,
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="push",
                message="Failed to push",
                error=e.stderr,
            )

    def pull(
        self,
        remote: str = "origin",
        branch: Optional[str] = None,
        rebase: bool = False,
    ) -> GitOperationResult:
        """
        Pull from remote.

        Args:
            remote: Remote name
            branch: Branch to pull
            rebase: Use rebase instead of merge

        Returns:
            Operation result
        """
        try:
            args = ["pull"]
            if rebase:
                args.append("--rebase")
            args.append(remote)
            if branch:
                args.append(branch)

            self._run_git(args)

            return GitOperationResult(
                success=True,
                operation="pull",
                message=f"Pulled from {remote}",
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="pull",
                message="Failed to pull",
                error=e.stderr,
            )

    def list_branches(self, include_remote: bool = False) -> List[BranchInfo]:
        """
        List branches.

        Args:
            include_remote: Include remote branches

        Returns:
            List of branch info
        """
        try:
            args = ["branch", "-v"]
            if include_remote:
                args.append("-a")

            result = self._run_git(args)
            branches = []

            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                is_current = line.startswith("*")
                line = line.lstrip("* ").strip()

                parts = line.split()
                if len(parts) >= 2:
                    name = parts[0]
                    is_remote = name.startswith("remotes/")

                    branches.append(BranchInfo(
                        name=name,
                        is_current=is_current,
                        is_remote=is_remote,
                    ))

            return branches

        except Exception as e:
            logger.error(f"Failed to list branches: {e}")
            return []

    def get_log(self, count: int = 10) -> List[CommitInfo]:
        """
        Get commit log.

        Args:
            count: Number of commits to return

        Returns:
            List of commit info
        """
        try:
            result = self._run_git([
                "log",
                f"-{count}",
                "--format=%H|%s|%an|%ae|%ai",
            ])

            commits = []
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) >= 5:
                    commits.append(CommitInfo(
                        sha=parts[0],
                        message=parts[1],
                        author=parts[2],
                        email=parts[3],
                        timestamp=datetime.fromisoformat(parts[4].replace(" ", "T").rsplit("+", 1)[0]),
                    ))

            return commits

        except Exception as e:
            logger.error(f"Failed to get log: {e}")
            return []

    def add_remote(
        self,
        name: str,
        url: str,
    ) -> GitOperationResult:
        """
        Add a remote.

        Args:
            name: Remote name
            url: Remote URL

        Returns:
            Operation result
        """
        try:
            # Check if remote exists
            check_result = self._run_git(["remote", "get-url", name], check=False)
            if check_result.returncode == 0:
                # Update existing
                self._run_git(["remote", "set-url", name, url])
                return GitOperationResult(
                    success=True,
                    operation="add_remote",
                    message=f"Updated remote '{name}' URL",
                )
            else:
                # Add new
                self._run_git(["remote", "add", name, url])
                return GitOperationResult(
                    success=True,
                    operation="add_remote",
                    message=f"Added remote '{name}'",
                )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="add_remote",
                message="Failed to add remote",
                error=e.stderr,
            )

    def diff(
        self,
        staged: bool = False,
        file_path: Optional[str] = None,
    ) -> str:
        """
        Get diff output.

        Args:
            staged: Show staged changes
            file_path: Specific file to diff

        Returns:
            Diff output
        """
        try:
            args = ["diff"]
            if staged:
                args.append("--staged")
            if file_path:
                args.append(file_path)

            result = self._run_git(args)
            return result.stdout

        except Exception as e:
            return f"Error: {e}"

    def stash(self, message: Optional[str] = None) -> GitOperationResult:
        """
        Stash changes.

        Args:
            message: Stash message

        Returns:
            Operation result
        """
        try:
            args = ["stash", "push"]
            if message:
                args.extend(["-m", message])

            self._run_git(args)

            return GitOperationResult(
                success=True,
                operation="stash",
                message="Stashed changes",
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="stash",
                message="Failed to stash",
                error=e.stderr,
            )

    def stash_pop(self) -> GitOperationResult:
        """
        Pop stashed changes.

        Returns:
            Operation result
        """
        try:
            self._run_git(["stash", "pop"])

            return GitOperationResult(
                success=True,
                operation="stash_pop",
                message="Applied stashed changes",
            )

        except subprocess.CalledProcessError as e:
            return GitOperationResult(
                success=False,
                operation="stash_pop",
                message="Failed to pop stash",
                error=e.stderr,
            )
