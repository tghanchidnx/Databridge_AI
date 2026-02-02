"""
Unit tests for GitOperations class.

Tests git and GitHub CLI operations with mocked subprocess calls.
"""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

from databridge_core.git.operations import (
    GitOperations,
    GitOperationError,
    CommitResult,
    PullRequestResult,
)


@pytest.fixture
def mock_repo(tmp_path):
    """Create a mock git repository."""
    git_dir = tmp_path / ".git"
    git_dir.mkdir()
    return tmp_path


@pytest.fixture
def git_ops(mock_repo):
    """Create GitOperations instance with mocked repo."""
    with patch("shutil.which", return_value="/usr/bin/git"):
        return GitOperations(mock_repo)


class TestGitOperationsInit:
    """Tests for GitOperations initialization."""

    def test_init_with_valid_repo(self, mock_repo):
        """Test initialization with a valid git repository."""
        with patch("shutil.which", return_value="/usr/bin/git"):
            ops = GitOperations(mock_repo)
            assert ops.repo_path == mock_repo

    def test_init_with_invalid_repo(self, tmp_path):
        """Test initialization fails without .git directory."""
        with patch("shutil.which", return_value="/usr/bin/git"):
            with pytest.raises(GitOperationError) as exc:
                GitOperations(tmp_path)
            assert "Not a git repository" in str(exc.value)

    def test_init_without_git(self, mock_repo):
        """Test initialization fails when git is not installed."""
        with patch("shutil.which", return_value=None):
            with pytest.raises(GitOperationError) as exc:
                GitOperations(mock_repo)
            assert "git command not found" in str(exc.value)


class TestRunGit:
    """Tests for _run_git method."""

    def test_run_git_success(self, git_ops):
        """Test successful git command execution."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "status"],
            returncode=0,
            stdout="On branch main\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            result = git_ops._run_git(["status"])
            assert result.returncode == 0
            assert "main" in result.stdout

    def test_run_git_failure(self, git_ops):
        """Test git command failure raises error."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "invalid"],
            returncode=1,
            stdout="",
            stderr="error: unknown command",
        )

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(GitOperationError) as exc:
                git_ops._run_git(["invalid"])
            assert exc.value.returncode == 1
            assert "unknown command" in exc.value.stderr

    def test_run_git_no_check(self, git_ops):
        """Test git command with check=False doesn't raise."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "status"],
            returncode=1,
            stdout="",
            stderr="error",
        )

        with patch("subprocess.run", return_value=mock_result):
            result = git_ops._run_git(["status"], check=False)
            assert result.returncode == 1


class TestRunGh:
    """Tests for _run_gh method."""

    def test_run_gh_success(self, git_ops):
        """Test successful gh command execution."""
        mock_result = subprocess.CompletedProcess(
            args=["gh", "auth", "status"],
            returncode=0,
            stdout="Logged in",
            stderr="",
        )

        with patch("shutil.which", return_value="/usr/bin/gh"):
            with patch("subprocess.run", return_value=mock_result):
                result = git_ops._run_gh(["auth", "status"])
                assert result.returncode == 0

    def test_run_gh_not_installed(self, git_ops):
        """Test gh command fails when not installed."""
        with patch("shutil.which", return_value=None):
            with pytest.raises(GitOperationError) as exc:
                git_ops._run_gh(["auth", "status"])
            assert "gh (GitHub CLI) command not found" in str(exc.value)


class TestGetCurrentBranch:
    """Tests for get_current_branch method."""

    def test_get_current_branch(self, git_ops):
        """Test getting current branch name."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "branch", "--show-current"],
            returncode=0,
            stdout="feature/test\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            branch = git_ops.get_current_branch()
            assert branch == "feature/test"


class TestGetDefaultBranch:
    """Tests for get_default_branch method."""

    def test_get_default_branch_from_remote(self, git_ops):
        """Test getting default branch from remote HEAD."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
            returncode=0,
            stdout="refs/remotes/origin/main\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            branch = git_ops.get_default_branch()
            assert branch == "main"

    def test_get_default_branch_fallback_main(self, git_ops):
        """Test falling back to main branch."""
        mock_results = [
            subprocess.CompletedProcess(
                args=["git", "symbolic-ref"],
                returncode=1,
                stdout="",
                stderr="not found",
            ),
            subprocess.CompletedProcess(
                args=["git", "rev-parse"],
                returncode=0,
                stdout="abc123\n",
                stderr="",
            ),
        ]

        with patch("subprocess.run", side_effect=mock_results):
            branch = git_ops.get_default_branch()
            assert branch == "main"


class TestBranchExists:
    """Tests for branch_exists method."""

    def test_branch_exists_local(self, git_ops):
        """Test checking if local branch exists."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "rev-parse"],
            returncode=0,
            stdout="abc123\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            assert git_ops.branch_exists("feature/test") is True

    def test_branch_not_exists(self, git_ops):
        """Test checking if branch doesn't exist."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "rev-parse"],
            returncode=1,
            stdout="",
            stderr="fatal: not found",
        )

        with patch("subprocess.run", return_value=mock_result):
            assert git_ops.branch_exists("nonexistent") is False

    def test_branch_exists_remote(self, git_ops):
        """Test checking if remote branch exists."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "rev-parse"],
            returncode=0,
            stdout="abc123\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            git_ops.branch_exists("feature/test", remote=True)
            # Verify it checked remote ref
            call_args = mock_run.call_args[0][0]
            assert "refs/remotes/origin/feature/test" in call_args


class TestCreateBranch:
    """Tests for create_branch method."""

    def test_create_branch_success(self, git_ops):
        """Test creating a new branch."""
        mock_results = [
            # branch_exists check
            subprocess.CompletedProcess(
                args=["git", "rev-parse"],
                returncode=1,
                stdout="",
                stderr="",
            ),
            # checkout -b
            subprocess.CompletedProcess(
                args=["git", "checkout"],
                returncode=0,
                stdout="Switched to new branch",
                stderr="",
            ),
        ]

        with patch("subprocess.run", side_effect=mock_results):
            branch = git_ops.create_branch("feature/new")
            assert branch == "feature/new"

    def test_create_branch_already_exists(self, git_ops):
        """Test creating branch that already exists fails."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "rev-parse"],
            returncode=0,
            stdout="abc123\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            with pytest.raises(GitOperationError) as exc:
                git_ops.create_branch("existing")
            assert "already exists" in str(exc.value)


class TestSwitchBranch:
    """Tests for switch_branch method."""

    def test_switch_branch_success(self, git_ops):
        """Test switching to an existing branch."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "checkout"],
            returncode=0,
            stdout="Switched to branch 'main'\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            branch = git_ops.switch_branch("main")
            assert branch == "main"


class TestAddFiles:
    """Tests for add_files method."""

    def test_add_files_success(self, git_ops, mock_repo):
        """Test staging files."""
        files = [mock_repo / "file1.py", mock_repo / "file2.py"]

        mock_result = subprocess.CompletedProcess(
            args=["git", "add"],
            returncode=0,
            stdout="",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            count = git_ops.add_files(files)
            assert count == 2

    def test_add_files_empty(self, git_ops):
        """Test adding empty file list."""
        count = git_ops.add_files([])
        assert count == 0


class TestCommitFiles:
    """Tests for commit_files method."""

    def test_commit_files_success(self, git_ops, mock_repo):
        """Test committing files."""
        files = [mock_repo / "file.py"]

        mock_results = [
            # add
            subprocess.CompletedProcess(args=["git", "add"], returncode=0, stdout="", stderr=""),
            # commit
            subprocess.CompletedProcess(args=["git", "commit"], returncode=0, stdout="", stderr=""),
            # rev-parse HEAD
            subprocess.CompletedProcess(args=["git", "rev-parse"], returncode=0, stdout="abc12345\n", stderr=""),
            # branch --show-current
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="main\n", stderr=""),
        ]

        with patch("subprocess.run", side_effect=mock_results):
            result = git_ops.commit_files(files, "Test commit")
            assert isinstance(result, CommitResult)
            assert result.sha == "abc12345"
            assert result.branch == "main"
            assert result.files_changed == 1

    def test_commit_files_with_author(self, git_ops, mock_repo):
        """Test committing with custom author."""
        files = [mock_repo / "file.py"]

        mock_results = [
            subprocess.CompletedProcess(args=["git", "add"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["git", "commit"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["git", "rev-parse"], returncode=0, stdout="abc12345\n", stderr=""),
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="main\n", stderr=""),
        ]

        with patch("subprocess.run", side_effect=mock_results) as mock_run:
            git_ops.commit_files(files, "Test", author="Test <test@example.com>")
            # Check author flag was passed
            commit_call = mock_run.call_args_list[1]
            assert "--author" in commit_call[0][0]


class TestPushBranch:
    """Tests for push_branch method."""

    def test_push_branch_success(self, git_ops):
        """Test pushing branch to remote."""
        mock_results = [
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="main\n", stderr=""),
            subprocess.CompletedProcess(args=["git", "push"], returncode=0, stdout="", stderr=""),
        ]

        with patch("subprocess.run", side_effect=mock_results):
            result = git_ops.push_branch()
            assert result is True

    def test_push_branch_with_upstream(self, git_ops):
        """Test pushing with upstream set."""
        mock_results = [
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="feature\n", stderr=""),
            subprocess.CompletedProcess(args=["git", "push"], returncode=0, stdout="", stderr=""),
        ]

        with patch("subprocess.run", side_effect=mock_results) as mock_run:
            git_ops.push_branch(set_upstream=True)
            push_call = mock_run.call_args_list[1]
            assert "-u" in push_call[0][0]


class TestCreatePullRequest:
    """Tests for create_pull_request method."""

    def test_create_pr_success(self, git_ops):
        """Test creating a pull request."""
        mock_results = [
            # get_current_branch
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="feature\n", stderr=""),
            # get_default_branch
            subprocess.CompletedProcess(args=["git", "symbolic-ref"], returncode=0, stdout="refs/remotes/origin/main\n", stderr=""),
            # gh pr create
            subprocess.CompletedProcess(args=["gh", "pr"], returncode=0, stdout="https://github.com/owner/repo/pull/42\n", stderr=""),
        ]

        with patch("shutil.which", return_value="/usr/bin/gh"):
            with patch("subprocess.run", side_effect=mock_results):
                result = git_ops.create_pull_request(
                    title="Test PR",
                    body="PR description",
                )
                assert isinstance(result, PullRequestResult)
                assert result.number == 42
                assert "github.com" in result.url
                assert result.title == "Test PR"


class TestGetStatus:
    """Tests for get_status method."""

    def test_get_status_clean(self, git_ops):
        """Test getting status of clean repo."""
        mock_results = [
            subprocess.CompletedProcess(args=["git", "status"], returncode=0, stdout="", stderr=""),
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="main\n", stderr=""),
        ]

        with patch("subprocess.run", side_effect=mock_results):
            status = git_ops.get_status()
            assert status["clean"] is True
            assert status["branch"] == "main"

    def test_get_status_with_changes(self, git_ops):
        """Test getting status with modified files."""
        mock_results = [
            subprocess.CompletedProcess(
                args=["git", "status"],
                returncode=0,
                stdout="M  file.py\n?? new.py\n",
                stderr="",
            ),
            subprocess.CompletedProcess(args=["git", "branch"], returncode=0, stdout="main\n", stderr=""),
        ]

        with patch("subprocess.run", side_effect=mock_results):
            status = git_ops.get_status()
            assert status["clean"] is False
            assert "file.py" in status["staged"]
            assert "new.py" in status["untracked"]


class TestGetDiff:
    """Tests for get_diff method."""

    def test_get_diff(self, git_ops):
        """Test getting diff output."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "diff"],
            returncode=0,
            stdout="diff --git a/file.py b/file.py\n+new line\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            diff = git_ops.get_diff()
            assert "file.py" in diff
            assert "+new line" in diff

    def test_get_diff_staged(self, git_ops):
        """Test getting staged diff."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "diff"],
            returncode=0,
            stdout="staged changes\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            git_ops.get_diff(staged=True)
            call_args = mock_run.call_args[0][0]
            assert "--cached" in call_args


class TestGetLog:
    """Tests for get_log method."""

    def test_get_log_oneline(self, git_ops):
        """Test getting log in oneline format."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "log"],
            returncode=0,
            stdout="abc1234 First commit\ndef5678 Second commit\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            commits = git_ops.get_log(count=2)
            assert len(commits) == 2
            assert commits[0]["sha"] == "abc1234"
            assert commits[0]["message"] == "First commit"

    def test_get_log_full(self, git_ops):
        """Test getting detailed log."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "log"],
            returncode=0,
            stdout="abc123|First commit|Author|2024-01-01\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            commits = git_ops.get_log(count=1, oneline=False)
            assert len(commits) == 1
            assert commits[0]["author"] == "Author"


class TestHasRemote:
    """Tests for has_remote method."""

    def test_has_remote_true(self, git_ops):
        """Test detecting remote origin."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "remote"],
            returncode=0,
            stdout="origin\nupstream\n",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            assert git_ops.has_remote() is True

    def test_has_remote_false(self, git_ops):
        """Test detecting no remote."""
        mock_result = subprocess.CompletedProcess(
            args=["git", "remote"],
            returncode=0,
            stdout="",
            stderr="",
        )

        with patch("subprocess.run", return_value=mock_result):
            assert git_ops.has_remote() is False


class TestIsGhAuthenticated:
    """Tests for is_gh_authenticated method."""

    def test_gh_authenticated(self, git_ops):
        """Test gh authenticated status."""
        mock_result = subprocess.CompletedProcess(
            args=["gh", "auth", "status"],
            returncode=0,
            stdout="Logged in to github.com",
            stderr="",
        )

        with patch("shutil.which", return_value="/usr/bin/gh"):
            with patch("subprocess.run", return_value=mock_result):
                assert git_ops.is_gh_authenticated() is True

    def test_gh_not_authenticated(self, git_ops):
        """Test gh not authenticated status."""
        mock_result = subprocess.CompletedProcess(
            args=["gh", "auth", "status"],
            returncode=1,
            stdout="",
            stderr="not logged in",
        )

        with patch("shutil.which", return_value="/usr/bin/gh"):
            with patch("subprocess.run", return_value=mock_result):
                assert git_ops.is_gh_authenticated() is False

    def test_gh_not_installed(self, git_ops):
        """Test gh not installed returns False."""
        with patch("shutil.which", return_value=None):
            assert git_ops.is_gh_authenticated() is False
