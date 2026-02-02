"""
Git operations module for DataBridge AI.

Provides a high-level interface for git and GitHub CLI operations,
including branch creation, commits, pushes, and pull requests.
"""

import subprocess
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class GitOperationError(Exception):
    """Raised when a git operation fails."""

    def __init__(self, message: str, returncode: int = 1, stderr: str = ""):
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr


@dataclass
class CommitResult:
    """Result of a git commit operation."""

    sha: str
    message: str
    files_changed: int
    branch: str


@dataclass
class PullRequestResult:
    """Result of a pull request creation."""

    number: int
    url: str
    title: str
    head: str
    base: str


class GitOperations:
    """
    High-level interface for git and GitHub CLI operations.

    Uses subprocess to call git and gh CLI commands. Requires git
    to be installed and gh to be authenticated for PR operations.
    """

    def __init__(self, repo_path: Path):
        """
        Initialize GitOperations with a repository path.

        Args:
            repo_path: Path to the git repository root.

        Raises:
            GitOperationError: If the path is not a git repository.
        """
        self.repo_path = Path(repo_path).resolve()

        # Verify git is available
        if not shutil.which("git"):
            raise GitOperationError("git command not found in PATH")

        # Verify it's a git repository
        if not (self.repo_path / ".git").exists():
            raise GitOperationError(f"Not a git repository: {self.repo_path}")

    def _run_git(
        self,
        args: List[str],
        capture_output: bool = True,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """
        Run a git command.

        Args:
            args: Git command arguments (without 'git' prefix).
            capture_output: Whether to capture stdout/stderr.
            check: Whether to raise on non-zero exit.

        Returns:
            CompletedProcess result.

        Raises:
            GitOperationError: If the command fails and check=True.
        """
        cmd = ["git"] + args
        logger.debug(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                cwd=self.repo_path,
                capture_output=capture_output,
                text=True,
                check=False,
            )

            if check and result.returncode != 0:
                raise GitOperationError(
                    f"Git command failed: {' '.join(args)}",
                    returncode=result.returncode,
                    stderr=result.stderr,
                )

            return result

        except FileNotFoundError:
            raise GitOperationError("git command not found")

    def _run_gh(
        self,
        args: List[str],
        capture_output: bool = True,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """
        Run a GitHub CLI command.

        Args:
            args: gh command arguments (without 'gh' prefix).
            capture_output: Whether to capture stdout/stderr.
            check: Whether to raise on non-zero exit.

        Returns:
            CompletedProcess result.

        Raises:
            GitOperationError: If the command fails and check=True.
        """
        if not shutil.which("gh"):
            raise GitOperationError("gh (GitHub CLI) command not found in PATH")

        cmd = ["gh"] + args
        logger.debug(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                cwd=self.repo_path,
                capture_output=capture_output,
                text=True,
                check=False,
            )

            if check and result.returncode != 0:
                raise GitOperationError(
                    f"GitHub CLI command failed: {' '.join(args)}",
                    returncode=result.returncode,
                    stderr=result.stderr,
                )

            return result

        except FileNotFoundError:
            raise GitOperationError("gh command not found")

    def get_current_branch(self) -> str:
        """
        Get the current branch name.

        Returns:
            Current branch name.
        """
        result = self._run_git(["branch", "--show-current"])
        return result.stdout.strip()

    def get_default_branch(self) -> str:
        """
        Get the default branch name (main or master).

        Returns:
            Default branch name.
        """
        # Try to get from remote
        result = self._run_git(
            ["symbolic-ref", "refs/remotes/origin/HEAD"],
            check=False,
        )

        if result.returncode == 0:
            # Parse refs/remotes/origin/main -> main
            ref = result.stdout.strip()
            return ref.split("/")[-1]

        # Fallback: check if main or master exists
        for branch in ["main", "master"]:
            result = self._run_git(
                ["rev-parse", "--verify", f"refs/heads/{branch}"],
                check=False,
            )
            if result.returncode == 0:
                return branch

        return "main"  # Default fallback

    def branch_exists(self, branch_name: str, remote: bool = False) -> bool:
        """
        Check if a branch exists.

        Args:
            branch_name: Name of the branch.
            remote: Check remote branches instead of local.

        Returns:
            True if branch exists.
        """
        ref = f"refs/remotes/origin/{branch_name}" if remote else f"refs/heads/{branch_name}"
        result = self._run_git(["rev-parse", "--verify", ref], check=False)
        return result.returncode == 0

    def create_branch(self, branch_name: str, base: Optional[str] = None) -> str:
        """
        Create a new branch and switch to it.

        Args:
            branch_name: Name for the new branch.
            base: Base branch/commit to branch from (default: current HEAD).

        Returns:
            The new branch name.

        Raises:
            GitOperationError: If branch already exists or creation fails.
        """
        if self.branch_exists(branch_name):
            raise GitOperationError(f"Branch already exists: {branch_name}")

        args = ["checkout", "-b", branch_name]
        if base:
            args.append(base)

        self._run_git(args)
        logger.info(f"Created and switched to branch: {branch_name}")
        return branch_name

    def switch_branch(self, branch_name: str) -> str:
        """
        Switch to an existing branch.

        Args:
            branch_name: Name of the branch to switch to.

        Returns:
            The branch name.

        Raises:
            GitOperationError: If branch doesn't exist.
        """
        self._run_git(["checkout", branch_name])
        return branch_name

    def add_files(self, files: List[Path]) -> int:
        """
        Stage files for commit.

        Args:
            files: List of file paths to stage.

        Returns:
            Number of files staged.

        Raises:
            GitOperationError: If staging fails.
        """
        if not files:
            return 0

        # Convert to relative paths
        rel_files = []
        for f in files:
            path = Path(f)
            if path.is_absolute():
                try:
                    path = path.relative_to(self.repo_path)
                except ValueError:
                    pass
            rel_files.append(str(path))

        self._run_git(["add"] + rel_files)
        return len(rel_files)

    def commit_files(
        self,
        files: List[Path],
        message: str,
        author: Optional[str] = None,
    ) -> CommitResult:
        """
        Stage and commit specified files.

        Args:
            files: List of file paths to commit.
            message: Commit message.
            author: Optional author string (format: "Name <email>").

        Returns:
            CommitResult with commit details.

        Raises:
            GitOperationError: If commit fails.
        """
        # Stage files
        self.add_files(files)

        # Build commit command
        args = ["commit", "-m", message]
        if author:
            args.extend(["--author", author])

        self._run_git(args)

        # Get commit info
        sha = self._run_git(["rev-parse", "HEAD"]).stdout.strip()
        branch = self.get_current_branch()

        return CommitResult(
            sha=sha[:8],
            message=message,
            files_changed=len(files),
            branch=branch,
        )

    def push_branch(
        self,
        branch_name: Optional[str] = None,
        set_upstream: bool = True,
        force: bool = False,
    ) -> bool:
        """
        Push a branch to remote.

        Args:
            branch_name: Branch to push (default: current branch).
            set_upstream: Set upstream tracking reference.
            force: Force push (use with caution).

        Returns:
            True if push succeeded.

        Raises:
            GitOperationError: If push fails.
        """
        branch = branch_name or self.get_current_branch()

        args = ["push"]
        if set_upstream:
            args.extend(["-u", "origin", branch])
        else:
            args.extend(["origin", branch])

        if force:
            args.insert(1, "--force")

        self._run_git(args)
        logger.info(f"Pushed branch to origin: {branch}")
        return True

    def create_pull_request(
        self,
        title: str,
        body: str,
        base: Optional[str] = None,
        head: Optional[str] = None,
        draft: bool = False,
    ) -> PullRequestResult:
        """
        Create a pull request using GitHub CLI.

        Args:
            title: PR title.
            body: PR body/description.
            base: Base branch (default: repository default).
            head: Head branch (default: current branch).
            draft: Create as draft PR.

        Returns:
            PullRequestResult with PR details.

        Raises:
            GitOperationError: If PR creation fails.
        """
        head_branch = head or self.get_current_branch()
        base_branch = base or self.get_default_branch()

        args = [
            "pr", "create",
            "--title", title,
            "--body", body,
            "--base", base_branch,
            "--head", head_branch,
        ]

        if draft:
            args.append("--draft")

        result = self._run_gh(args)

        # Parse PR URL from output
        pr_url = result.stdout.strip()

        # Extract PR number from URL
        # Format: https://github.com/owner/repo/pull/123
        pr_number = int(pr_url.split("/")[-1])

        return PullRequestResult(
            number=pr_number,
            url=pr_url,
            title=title,
            head=head_branch,
            base=base_branch,
        )

    def get_status(self) -> dict:
        """
        Get repository status.

        Returns:
            Dictionary with status information.
        """
        # Get porcelain status
        result = self._run_git(["status", "--porcelain"])
        lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

        staged = []
        modified = []
        untracked = []

        for line in lines:
            if not line:
                continue
            status = line[:2]
            filename = line[3:]

            if status[0] in "MADRC":
                staged.append(filename)
            if status[1] in "MD":
                modified.append(filename)
            if status == "??":
                untracked.append(filename)

        return {
            "branch": self.get_current_branch(),
            "staged": staged,
            "modified": modified,
            "untracked": untracked,
            "clean": len(staged) == 0 and len(modified) == 0,
        }

    def get_diff(
        self,
        staged: bool = False,
        path: Optional[str] = None,
    ) -> str:
        """
        Get diff output.

        Args:
            staged: Get staged changes (--cached).
            path: Limit to specific path.

        Returns:
            Diff output as string.
        """
        args = ["diff"]
        if staged:
            args.append("--cached")
        if path:
            args.extend(["--", path])

        result = self._run_git(args)
        return result.stdout

    def get_log(
        self,
        count: int = 10,
        oneline: bool = True,
    ) -> List[dict]:
        """
        Get commit log.

        Args:
            count: Number of commits to retrieve.
            oneline: Use oneline format.

        Returns:
            List of commit dictionaries.
        """
        if oneline:
            args = ["log", f"-{count}", "--oneline"]
            result = self._run_git(args)

            commits = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    parts = line.split(" ", 1)
                    commits.append({
                        "sha": parts[0],
                        "message": parts[1] if len(parts) > 1 else "",
                    })
            return commits
        else:
            args = [
                "log",
                f"-{count}",
                "--format=%H|%s|%an|%ai",
            ]
            result = self._run_git(args)

            commits = []
            for line in result.stdout.strip().split("\n"):
                if line:
                    parts = line.split("|")
                    commits.append({
                        "sha": parts[0],
                        "message": parts[1] if len(parts) > 1 else "",
                        "author": parts[2] if len(parts) > 2 else "",
                        "date": parts[3] if len(parts) > 3 else "",
                    })
            return commits

    def has_remote(self) -> bool:
        """
        Check if repository has a remote configured.

        Returns:
            True if remote 'origin' exists.
        """
        result = self._run_git(["remote"], check=False)
        return "origin" in result.stdout

    def is_gh_authenticated(self) -> bool:
        """
        Check if GitHub CLI is authenticated.

        Returns:
            True if gh is authenticated.
        """
        try:
            result = self._run_gh(["auth", "status"], check=False)
            return result.returncode == 0
        except GitOperationError:
            return False
