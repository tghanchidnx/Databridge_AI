"""
CI/CD Pipeline Generator.

Generates CI/CD workflows for dbt projects:
- GitHub Actions
- GitLab CI
- Azure DevOps
"""

import logging
from typing import Any, Dict, Optional
import yaml

from .types import (
    CiCdConfig,
    CiCdPlatform,
)

logger = logging.getLogger(__name__)


class CiCdGenerator:
    """Generates CI/CD pipeline configurations."""

    def __init__(self):
        pass

    def generate_pipeline(
        self,
        config: CiCdConfig,
        project_name: str,
    ) -> str:
        """
        Generate CI/CD pipeline configuration.

        Args:
            config: CI/CD configuration
            project_name: dbt project name

        Returns:
            Pipeline configuration content
        """
        if config.platform == CiCdPlatform.GITHUB_ACTIONS:
            return self.generate_github_actions(config, project_name)
        elif config.platform == CiCdPlatform.GITLAB_CI:
            return self.generate_gitlab_ci(config, project_name)
        elif config.platform == CiCdPlatform.AZURE_DEVOPS:
            return self.generate_azure_devops(config, project_name)
        else:
            raise ValueError(f"Unsupported platform: {config.platform}")

    def generate_github_actions(
        self,
        config: CiCdConfig,
        project_name: str,
    ) -> str:
        """Generate GitHub Actions workflow."""
        workflow = {
            "name": f"dbt CI - {project_name}",
            "on": {
                "push": {
                    "branches": config.trigger_branches,
                    "paths": config.trigger_paths,
                },
                "pull_request": {
                    "branches": config.trigger_branches,
                },
            },
            "env": {
                "DBT_PROFILES_DIR": ".",
                "SNOWFLAKE_ACCOUNT": f"${{{{ secrets.{config.snowflake_account_secret} }}}}",
                "SNOWFLAKE_USER": f"${{{{ secrets.{config.snowflake_user_secret} }}}}",
                "SNOWFLAKE_PASSWORD": f"${{{{ secrets.{config.snowflake_password_secret} }}}}",
                "SNOWFLAKE_ROLE": f"${{{{ secrets.{config.snowflake_role_secret} }}}}",
                "SNOWFLAKE_WAREHOUSE": f"${{{{ secrets.{config.snowflake_warehouse_secret} }}}}",
                "SNOWFLAKE_DATABASE": f"${{{{ secrets.{config.snowflake_database_secret} }}}}",
            },
            "jobs": {
                "dbt-ci": {
                    "runs-on": "ubuntu-latest",
                    "steps": [
                        {
                            "name": "Checkout",
                            "uses": "actions/checkout@v4",
                        },
                        {
                            "name": "Set up Python",
                            "uses": "actions/setup-python@v5",
                            "with": {
                                "python-version": config.python_version,
                            },
                        },
                        {
                            "name": "Install dependencies",
                            "run": "pip install dbt-snowflake==" + config.dbt_version,
                        },
                        {
                            "name": "Install dbt packages",
                            "run": "dbt deps",
                        },
                        {
                            "name": "Run dbt debug",
                            "run": "dbt debug",
                        },
                        {
                            "name": "Run dbt build",
                            "run": "dbt build --fail-fast",
                        },
                    ],
                },
            },
        }

        # Add test step if enabled
        if config.run_tests:
            workflow["jobs"]["dbt-ci"]["steps"].append({
                "name": "Run dbt tests",
                "run": "dbt test",
            })

        # Add docs generation if enabled
        if config.run_docs:
            workflow["jobs"]["dbt-ci"]["steps"].append({
                "name": "Generate docs",
                "run": "dbt docs generate",
            })

            if config.deploy_docs:
                workflow["jobs"]["dbt-ci"]["steps"].append({
                    "name": "Deploy docs to GitHub Pages",
                    "uses": "peaceiris/actions-gh-pages@v3",
                    "with": {
                        "github_token": "${{ secrets.GITHUB_TOKEN }}",
                        "publish_dir": "./target",
                    },
                })

        # Add Slack notification if configured
        if config.slack_webhook_secret:
            workflow["jobs"]["dbt-ci"]["steps"].append({
                "name": "Notify Slack on failure",
                "if": "failure()",
                "uses": "rtCamp/action-slack-notify@v2",
                "env": {
                    "SLACK_WEBHOOK": f"${{{{ secrets.{config.slack_webhook_secret} }}}}",
                    "SLACK_MESSAGE": f"dbt CI failed for {project_name}",
                    "SLACK_COLOR": "danger",
                },
            })

        return yaml.dump(workflow, default_flow_style=False, sort_keys=False)

    def generate_gitlab_ci(
        self,
        config: CiCdConfig,
        project_name: str,
    ) -> str:
        """Generate GitLab CI configuration."""
        pipeline = {
            "image": f"python:{config.python_version}",
            "variables": {
                "DBT_PROFILES_DIR": ".",
                "SNOWFLAKE_ACCOUNT": "${SNOWFLAKE_ACCOUNT}",
                "SNOWFLAKE_USER": "${SNOWFLAKE_USER}",
                "SNOWFLAKE_PASSWORD": "${SNOWFLAKE_PASSWORD}",
                "SNOWFLAKE_ROLE": "${SNOWFLAKE_ROLE}",
                "SNOWFLAKE_WAREHOUSE": "${SNOWFLAKE_WAREHOUSE}",
                "SNOWFLAKE_DATABASE": "${SNOWFLAKE_DATABASE}",
            },
            "stages": ["install", "build", "test", "docs"],
            "cache": {
                "paths": [".venv/"],
            },
            "before_script": [
                "pip install --upgrade pip",
                f"pip install dbt-snowflake=={config.dbt_version}",
                "dbt deps",
            ],
            "install": {
                "stage": "install",
                "script": [
                    "dbt debug",
                ],
                "only": {
                    "refs": config.trigger_branches,
                    "changes": config.trigger_paths,
                },
            },
            "build": {
                "stage": "build",
                "script": [
                    "dbt build --fail-fast",
                ],
                "only": {
                    "refs": config.trigger_branches,
                },
            },
        }

        if config.run_tests:
            pipeline["test"] = {
                "stage": "test",
                "script": [
                    "dbt test",
                ],
                "only": {
                    "refs": config.trigger_branches,
                },
            }

        if config.run_docs:
            pipeline["docs"] = {
                "stage": "docs",
                "script": [
                    "dbt docs generate",
                ],
                "artifacts": {
                    "paths": ["target/"],
                    "expire_in": "1 week",
                },
                "only": {
                    "refs": ["main"],
                },
            }

        return yaml.dump(pipeline, default_flow_style=False, sort_keys=False)

    def generate_azure_devops(
        self,
        config: CiCdConfig,
        project_name: str,
    ) -> str:
        """Generate Azure DevOps pipeline."""
        pipeline = {
            "trigger": {
                "branches": {
                    "include": config.trigger_branches,
                },
                "paths": {
                    "include": config.trigger_paths,
                },
            },
            "pool": {
                "vmImage": "ubuntu-latest",
            },
            "variables": [
                {"name": "DBT_PROFILES_DIR", "value": "."},
                {"name": "SNOWFLAKE_ACCOUNT", "value": f"$(snowflake-account)"},
                {"name": "SNOWFLAKE_USER", "value": f"$(snowflake-user)"},
                {"name": "SNOWFLAKE_PASSWORD", "value": f"$(snowflake-password)"},
                {"name": "SNOWFLAKE_ROLE", "value": f"$(snowflake-role)"},
                {"name": "SNOWFLAKE_WAREHOUSE", "value": f"$(snowflake-warehouse)"},
                {"name": "SNOWFLAKE_DATABASE", "value": f"$(snowflake-database)"},
            ],
            "stages": [
                {
                    "stage": "Build",
                    "jobs": [
                        {
                            "job": "dbt_build",
                            "displayName": "dbt Build",
                            "steps": [
                                {
                                    "task": "UsePythonVersion@0",
                                    "inputs": {
                                        "versionSpec": config.python_version,
                                    },
                                },
                                {
                                    "script": f"pip install dbt-snowflake=={config.dbt_version}",
                                    "displayName": "Install dbt",
                                },
                                {
                                    "script": "dbt deps",
                                    "displayName": "Install dbt packages",
                                },
                                {
                                    "script": "dbt debug",
                                    "displayName": "dbt debug",
                                },
                                {
                                    "script": "dbt build --fail-fast",
                                    "displayName": "dbt build",
                                },
                            ],
                        },
                    ],
                },
            ],
        }

        if config.run_tests:
            test_stage = {
                "stage": "Test",
                "dependsOn": "Build",
                "jobs": [
                    {
                        "job": "dbt_test",
                        "displayName": "dbt Test",
                        "steps": [
                            {
                                "task": "UsePythonVersion@0",
                                "inputs": {
                                    "versionSpec": config.python_version,
                                },
                            },
                            {
                                "script": f"pip install dbt-snowflake=={config.dbt_version}",
                                "displayName": "Install dbt",
                            },
                            {
                                "script": "dbt deps",
                                "displayName": "Install dbt packages",
                            },
                            {
                                "script": "dbt test",
                                "displayName": "dbt test",
                            },
                        ],
                    },
                ],
            }
            pipeline["stages"].append(test_stage)

        if config.run_docs:
            docs_stage = {
                "stage": "Docs",
                "dependsOn": "Test" if config.run_tests else "Build",
                "condition": "eq(variables['Build.SourceBranch'], 'refs/heads/main')",
                "jobs": [
                    {
                        "job": "dbt_docs",
                        "displayName": "dbt Docs",
                        "steps": [
                            {
                                "task": "UsePythonVersion@0",
                                "inputs": {
                                    "versionSpec": config.python_version,
                                },
                            },
                            {
                                "script": f"pip install dbt-snowflake=={config.dbt_version}",
                                "displayName": "Install dbt",
                            },
                            {
                                "script": "dbt deps && dbt docs generate",
                                "displayName": "Generate docs",
                            },
                            {
                                "task": "PublishBuildArtifacts@1",
                                "inputs": {
                                    "PathtoPublish": "target",
                                    "ArtifactName": "dbt-docs",
                                },
                            },
                        ],
                    },
                ],
            }
            pipeline["stages"].append(docs_stage)

        return yaml.dump(pipeline, default_flow_style=False, sort_keys=False)

    def get_pipeline_path(self, platform: CiCdPlatform) -> str:
        """Get the file path for the pipeline configuration."""
        if platform == CiCdPlatform.GITHUB_ACTIONS:
            return ".github/workflows/dbt_ci.yml"
        elif platform == CiCdPlatform.GITLAB_CI:
            return ".gitlab-ci.yml"
        elif platform == CiCdPlatform.AZURE_DEVOPS:
            return "azure-pipelines.yml"
        else:
            return "ci-pipeline.yml"
