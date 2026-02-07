"""
GitHub Client.

GitHub API client for pull requests, issues, and repository operations.
Uses the GitHub REST API v3.
"""

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

from .types import (
    GitConfig,
    GitOperationResult,
    PRStatus,
    PullRequest,
)

logger = logging.getLogger(__name__)


class GitHubClient:
    """Client for GitHub API operations."""

    BASE_URL = "https://api.github.com"

    def __init__(self, config: Optional[GitConfig] = None):
        """
        Initialize the GitHub client.

        Args:
            config: Git configuration with token
        """
        self.config = config
        self._token: Optional[str] = None
        self._owner: Optional[str] = None
        self._repo: Optional[str] = None

        if config:
            self._token = config.token
            if config.remote_url:
                self._parse_remote_url(config.remote_url)

    def configure(
        self,
        token: str,
        owner: Optional[str] = None,
        repo: Optional[str] = None,
        remote_url: Optional[str] = None,
    ) -> None:
        """
        Configure the client.

        Args:
            token: GitHub personal access token
            owner: Repository owner
            repo: Repository name
            remote_url: Remote URL to parse owner/repo from
        """
        self._token = token
        if owner:
            self._owner = owner
        if repo:
            self._repo = repo
        if remote_url:
            self._parse_remote_url(remote_url)

    def _parse_remote_url(self, url: str) -> None:
        """Parse owner and repo from remote URL."""
        # Handle HTTPS URLs: https://github.com/owner/repo.git
        # Handle SSH URLs: git@github.com:owner/repo.git
        patterns = [
            r"github\.com[/:]([^/]+)/([^/\.]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                self._owner = match.group(1)
                self._repo = match.group(2).replace(".git", "")
                return

    @property
    def is_configured(self) -> bool:
        """Check if client is properly configured."""
        return bool(self._token and self._owner and self._repo)

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an API request.

        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request body

        Returns:
            Response JSON
        """
        if not REQUESTS_AVAILABLE:
            raise ImportError("requests library is required for GitHub API")

        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_headers()

        logger.debug(f"GitHub API: {method} {url}")

        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data,
            timeout=30,
        )

        if response.status_code >= 400:
            error_data = response.json() if response.text else {}
            raise Exception(
                f"GitHub API error {response.status_code}: {error_data.get('message', 'Unknown error')}"
            )

        if response.text:
            return response.json()
        return {}

    def get_repo(self) -> Dict[str, Any]:
        """
        Get repository information.

        Returns:
            Repository data
        """
        if not self.is_configured:
            raise ValueError("GitHub client not configured")

        return self._request("GET", f"/repos/{self._owner}/{self._repo}")

    def create_pull_request(
        self,
        title: str,
        body: str,
        head: str,
        base: Optional[str] = None,
        draft: bool = False,
        reviewers: Optional[List[str]] = None,
        labels: Optional[List[str]] = None,
    ) -> GitOperationResult:
        """
        Create a pull request.

        Args:
            title: PR title
            body: PR description
            head: Source branch
            base: Target branch (default: main)
            draft: Create as draft PR
            reviewers: Requested reviewers
            labels: Labels to add

        Returns:
            Operation result with PR details
        """
        if not self.is_configured:
            return GitOperationResult(
                success=False,
                operation="create_pr",
                message="GitHub client not configured",
                error="Missing token, owner, or repo",
            )

        try:
            base = base or (self.config.default_branch if self.config else "main")

            # Create PR
            pr_data = self._request(
                "POST",
                f"/repos/{self._owner}/{self._repo}/pulls",
                data={
                    "title": title,
                    "body": body,
                    "head": head,
                    "base": base,
                    "draft": draft,
                },
            )

            pr_number = pr_data.get("number")
            pr_url = pr_data.get("html_url")

            # Add reviewers if specified
            if reviewers and pr_number:
                try:
                    self._request(
                        "POST",
                        f"/repos/{self._owner}/{self._repo}/pulls/{pr_number}/requested_reviewers",
                        data={"reviewers": reviewers},
                    )
                except Exception as e:
                    logger.warning(f"Failed to add reviewers: {e}")

            # Add labels if specified
            if labels and pr_number:
                try:
                    self._request(
                        "POST",
                        f"/repos/{self._owner}/{self._repo}/issues/{pr_number}/labels",
                        data={"labels": labels},
                    )
                except Exception as e:
                    logger.warning(f"Failed to add labels: {e}")

            return GitOperationResult(
                success=True,
                operation="create_pr",
                message=f"Created PR #{pr_number}",
                pr_url=pr_url,
                pr_number=pr_number,
                details={
                    "title": title,
                    "head": head,
                    "base": base,
                    "draft": draft,
                },
            )

        except Exception as e:
            return GitOperationResult(
                success=False,
                operation="create_pr",
                message="Failed to create pull request",
                error=str(e),
            )

    def get_pull_request(self, pr_number: int) -> Optional[PullRequest]:
        """
        Get pull request details.

        Args:
            pr_number: PR number

        Returns:
            PullRequest object or None
        """
        if not self.is_configured:
            return None

        try:
            pr_data = self._request(
                "GET",
                f"/repos/{self._owner}/{self._repo}/pulls/{pr_number}",
            )

            # Map status
            status = PRStatus.OPEN
            if pr_data.get("merged"):
                status = PRStatus.MERGED
            elif pr_data.get("state") == "closed":
                status = PRStatus.CLOSED
            elif pr_data.get("draft"):
                status = PRStatus.DRAFT

            return PullRequest(
                number=pr_data.get("number"),
                title=pr_data.get("title"),
                body=pr_data.get("body") or "",
                source_branch=pr_data.get("head", {}).get("ref", ""),
                target_branch=pr_data.get("base", {}).get("ref", ""),
                status=status,
                url=pr_data.get("html_url"),
                author=pr_data.get("user", {}).get("login"),
                labels=[l.get("name") for l in pr_data.get("labels", [])],
                created_at=datetime.fromisoformat(
                    pr_data.get("created_at", "").replace("Z", "+00:00")
                ),
            )

        except Exception as e:
            logger.error(f"Failed to get PR: {e}")
            return None

    def list_pull_requests(
        self,
        state: str = "open",
        head: Optional[str] = None,
        base: Optional[str] = None,
    ) -> List[PullRequest]:
        """
        List pull requests.

        Args:
            state: PR state (open, closed, all)
            head: Filter by head branch
            base: Filter by base branch

        Returns:
            List of pull requests
        """
        if not self.is_configured:
            return []

        try:
            endpoint = f"/repos/{self._owner}/{self._repo}/pulls?state={state}"
            if head:
                endpoint += f"&head={self._owner}:{head}"
            if base:
                endpoint += f"&base={base}"

            prs_data = self._request("GET", endpoint)
            prs = []

            for pr_data in prs_data:
                status = PRStatus.OPEN
                if pr_data.get("merged_at"):
                    status = PRStatus.MERGED
                elif pr_data.get("state") == "closed":
                    status = PRStatus.CLOSED
                elif pr_data.get("draft"):
                    status = PRStatus.DRAFT

                prs.append(PullRequest(
                    number=pr_data.get("number"),
                    title=pr_data.get("title"),
                    body=pr_data.get("body") or "",
                    source_branch=pr_data.get("head", {}).get("ref", ""),
                    target_branch=pr_data.get("base", {}).get("ref", ""),
                    status=status,
                    url=pr_data.get("html_url"),
                    author=pr_data.get("user", {}).get("login"),
                ))

            return prs

        except Exception as e:
            logger.error(f"Failed to list PRs: {e}")
            return []

    def get_pr_status(self, pr_number: int) -> Dict[str, Any]:
        """
        Get PR status including checks.

        Args:
            pr_number: PR number

        Returns:
            Status information
        """
        if not self.is_configured:
            return {"error": "GitHub client not configured"}

        try:
            # Get PR info
            pr = self.get_pull_request(pr_number)
            if not pr:
                return {"error": f"PR #{pr_number} not found"}

            result = {
                "number": pr.number,
                "title": pr.title,
                "status": pr.status.value,
                "url": pr.url,
                "source_branch": pr.source_branch,
                "target_branch": pr.target_branch,
            }

            # Get check runs
            try:
                pr_data = self._request(
                    "GET",
                    f"/repos/{self._owner}/{self._repo}/pulls/{pr_number}",
                )
                head_sha = pr_data.get("head", {}).get("sha")

                if head_sha:
                    checks = self._request(
                        "GET",
                        f"/repos/{self._owner}/{self._repo}/commits/{head_sha}/check-runs",
                    )

                    check_runs = checks.get("check_runs", [])
                    result["checks"] = {
                        "total": len(check_runs),
                        "completed": sum(1 for c in check_runs if c.get("status") == "completed"),
                        "success": sum(1 for c in check_runs if c.get("conclusion") == "success"),
                        "failure": sum(1 for c in check_runs if c.get("conclusion") == "failure"),
                        "pending": sum(1 for c in check_runs if c.get("status") in ("queued", "in_progress")),
                    }

                    # Add individual check names
                    result["check_details"] = [
                        {
                            "name": c.get("name"),
                            "status": c.get("status"),
                            "conclusion": c.get("conclusion"),
                        }
                        for c in check_runs[:10]  # Limit to 10
                    ]

            except Exception as e:
                logger.warning(f"Failed to get checks: {e}")

            # Get mergeable status
            result["mergeable"] = pr_data.get("mergeable") if "pr_data" in dir() else None
            result["mergeable_state"] = pr_data.get("mergeable_state") if "pr_data" in dir() else None

            return result

        except Exception as e:
            return {"error": str(e)}

    def merge_pull_request(
        self,
        pr_number: int,
        merge_method: str = "squash",
        commit_message: Optional[str] = None,
    ) -> GitOperationResult:
        """
        Merge a pull request.

        Args:
            pr_number: PR number
            merge_method: merge, squash, or rebase
            commit_message: Custom commit message

        Returns:
            Operation result
        """
        if not self.is_configured:
            return GitOperationResult(
                success=False,
                operation="merge_pr",
                message="GitHub client not configured",
            )

        try:
            data = {"merge_method": merge_method}
            if commit_message:
                data["commit_message"] = commit_message

            result = self._request(
                "PUT",
                f"/repos/{self._owner}/{self._repo}/pulls/{pr_number}/merge",
                data=data,
            )

            return GitOperationResult(
                success=True,
                operation="merge_pr",
                message=f"Merged PR #{pr_number}",
                pr_number=pr_number,
                commit_sha=result.get("sha"),
            )

        except Exception as e:
            return GitOperationResult(
                success=False,
                operation="merge_pr",
                message="Failed to merge PR",
                error=str(e),
            )

    def add_comment(
        self,
        pr_number: int,
        body: str,
    ) -> GitOperationResult:
        """
        Add a comment to a PR.

        Args:
            pr_number: PR number
            body: Comment body

        Returns:
            Operation result
        """
        if not self.is_configured:
            return GitOperationResult(
                success=False,
                operation="add_comment",
                message="GitHub client not configured",
            )

        try:
            result = self._request(
                "POST",
                f"/repos/{self._owner}/{self._repo}/issues/{pr_number}/comments",
                data={"body": body},
            )

            return GitOperationResult(
                success=True,
                operation="add_comment",
                message=f"Added comment to PR #{pr_number}",
                pr_number=pr_number,
                details={"comment_id": result.get("id")},
            )

        except Exception as e:
            return GitOperationResult(
                success=False,
                operation="add_comment",
                message="Failed to add comment",
                error=str(e),
            )

    def create_release(
        self,
        tag_name: str,
        name: str,
        body: str,
        target_branch: Optional[str] = None,
        draft: bool = False,
        prerelease: bool = False,
    ) -> GitOperationResult:
        """
        Create a release.

        Args:
            tag_name: Git tag name
            name: Release name
            body: Release notes
            target_branch: Target branch for tag
            draft: Create as draft
            prerelease: Mark as pre-release

        Returns:
            Operation result
        """
        if not self.is_configured:
            return GitOperationResult(
                success=False,
                operation="create_release",
                message="GitHub client not configured",
            )

        try:
            data = {
                "tag_name": tag_name,
                "name": name,
                "body": body,
                "draft": draft,
                "prerelease": prerelease,
            }
            if target_branch:
                data["target_commitish"] = target_branch

            result = self._request(
                "POST",
                f"/repos/{self._owner}/{self._repo}/releases",
                data=data,
            )

            return GitOperationResult(
                success=True,
                operation="create_release",
                message=f"Created release {tag_name}",
                details={
                    "id": result.get("id"),
                    "url": result.get("html_url"),
                    "tag": tag_name,
                },
            )

        except Exception as e:
            return GitOperationResult(
                success=False,
                operation="create_release",
                message="Failed to create release",
                error=str(e),
            )
