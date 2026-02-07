"""
GitHub Actions Workflow Generator.

Generates CI/CD workflows for dbt projects and DataBridge deployments.
"""

import logging
from typing import Any, Dict, List, Optional

from .types import (
    DbtCIConfig,
    DbtCommand,
    GitHubActionsWorkflow,
    WorkflowJob,
    WorkflowStep,
)

logger = logging.getLogger(__name__)


class WorkflowGenerator:
    """Generates GitHub Actions workflows."""

    def generate_dbt_ci_workflow(
        self,
        config: DbtCIConfig,
    ) -> GitHubActionsWorkflow:
        """
        Generate a dbt CI/CD workflow.

        Args:
            config: dbt CI configuration

        Returns:
            GitHub Actions workflow
        """
        # Build jobs
        jobs = {}

        # Lint job
        lint_job = self._create_lint_job(config)
        jobs["lint"] = lint_job

        # Build job (depends on lint)
        build_job = self._create_dbt_build_job(config)
        build_job.needs = ["lint"]
        jobs["build"] = build_job

        # Test job (depends on build)
        if DbtCommand.TEST in config.run_commands:
            test_job = self._create_dbt_test_job(config)
            test_job.needs = ["build"]
            jobs["test"] = test_job

        # Docs job (optional)
        if DbtCommand.DOCS_GENERATE in config.run_commands:
            docs_job = self._create_dbt_docs_job(config)
            docs_job.needs = ["build"]
            jobs["docs"] = docs_job

        # Deploy job (production only)
        deploy_job = self._create_deploy_job(config)
        deploy_job.needs = ["build"]
        if DbtCommand.TEST in config.run_commands:
            deploy_job.needs.append("test")
        jobs["deploy"] = deploy_job

        workflow = GitHubActionsWorkflow(
            name=f"dbt CI/CD - {config.project_name}",
            filename="dbt-ci.yml",
            on_push={
                "branches": ["main", "master"],
                "paths": ["models/**", "macros/**", "seeds/**", "dbt_project.yml"],
            },
            on_pull_request={
                "branches": ["main", "master"],
                "paths": ["models/**", "macros/**", "seeds/**", "dbt_project.yml"],
            },
            on_workflow_dispatch=True,
            env={
                "DBT_PROFILES_DIR": config.profiles_dir,
            },
            concurrency_group="${{ github.workflow }}-${{ github.ref }}",
            cancel_in_progress=True,
            jobs=jobs,
        )

        return workflow

    def generate_databridge_deploy_workflow(
        self,
        project_name: str,
        environments: Optional[List[str]] = None,
    ) -> GitHubActionsWorkflow:
        """
        Generate a DataBridge deployment workflow.

        Args:
            project_name: Project name
            environments: Target environments

        Returns:
            GitHub Actions workflow
        """
        environments = environments or ["dev", "prod"]

        jobs = {}

        # Validate job
        validate_job = WorkflowJob(
            name="Validate",
            runs_on="ubuntu-latest",
            steps=[
                WorkflowStep(
                    name="Checkout",
                    uses="actions/checkout@v4",
                ),
                WorkflowStep(
                    name="Setup Python",
                    uses="actions/setup-python@v5",
                    with_params={"python-version": "3.11"},
                ),
                WorkflowStep(
                    name="Install dependencies",
                    run="pip install -r requirements.txt",
                ),
                WorkflowStep(
                    name="Validate DDL scripts",
                    run="python -m src.validation.ddl_validator deployment/",
                ),
                WorkflowStep(
                    name="Validate hierarchy configuration",
                    run="python -m src.validation.config_validator config/",
                ),
            ],
        )
        jobs["validate"] = validate_job

        # Deploy jobs per environment
        for i, env in enumerate(environments):
            deploy_job = WorkflowJob(
                name=f"Deploy to {env.upper()}",
                runs_on="ubuntu-latest",
                needs=["validate"] if i == 0 else ["validate", f"deploy-{environments[i-1]}"],
                if_condition=f"github.ref == 'refs/heads/main'" if env == "prod" else None,
                env={
                    "ENVIRONMENT": env,
                    "SNOWFLAKE_ACCOUNT": f"${{{{ secrets.SNOWFLAKE_ACCOUNT_{env.upper()} }}}}",
                    "SNOWFLAKE_USER": f"${{{{ secrets.SNOWFLAKE_USER_{env.upper()} }}}}",
                    "SNOWFLAKE_PASSWORD": f"${{{{ secrets.SNOWFLAKE_PASSWORD_{env.upper()} }}}}",
                },
                steps=[
                    WorkflowStep(
                        name="Checkout",
                        uses="actions/checkout@v4",
                    ),
                    WorkflowStep(
                        name="Setup Python",
                        uses="actions/setup-python@v5",
                        with_params={"python-version": "3.11"},
                    ),
                    WorkflowStep(
                        name="Install dependencies",
                        run="pip install -r requirements.txt",
                    ),
                    WorkflowStep(
                        name="Deploy hierarchy DDL",
                        run=f"python -m src.deployment.executor --env {env} --scripts deployment/*.sql",
                    ),
                    WorkflowStep(
                        name="Verify deployment",
                        run=f"python -m src.deployment.verifier --env {env}",
                    ),
                ],
            )
            jobs[f"deploy-{env}"] = deploy_job

        workflow = GitHubActionsWorkflow(
            name=f"DataBridge Deploy - {project_name}",
            filename="databridge-deploy.yml",
            on_push={
                "branches": ["main", "master"],
                "paths": ["deployment/**", "config/**"],
            },
            on_workflow_dispatch=True,
            concurrency_group="${{ github.workflow }}-${{ github.ref }}",
            cancel_in_progress=True,
            jobs=jobs,
        )

        return workflow

    def generate_mart_factory_workflow(
        self,
        project_name: str,
        hierarchy_table: str,
        mapping_table: str,
    ) -> GitHubActionsWorkflow:
        """
        Generate a Mart Factory pipeline workflow.

        Args:
            project_name: Project name
            hierarchy_table: Hierarchy table name
            mapping_table: Mapping table name

        Returns:
            GitHub Actions workflow
        """
        jobs = {}

        # Generate pipeline job
        generate_job = WorkflowJob(
            name="Generate Pipeline",
            runs_on="ubuntu-latest",
            env={
                "SNOWFLAKE_ACCOUNT": "${{ secrets.SNOWFLAKE_ACCOUNT }}",
                "SNOWFLAKE_USER": "${{ secrets.SNOWFLAKE_USER }}",
                "SNOWFLAKE_PASSWORD": "${{ secrets.SNOWFLAKE_PASSWORD }}",
            },
            steps=[
                WorkflowStep(
                    name="Checkout",
                    uses="actions/checkout@v4",
                ),
                WorkflowStep(
                    name="Setup Python",
                    uses="actions/setup-python@v5",
                    with_params={"python-version": "3.11"},
                ),
                WorkflowStep(
                    name="Install DataBridge",
                    run="pip install -r requirements.txt",
                ),
                WorkflowStep(
                    name="Discover hierarchy patterns",
                    run=f"""python -c "
from src.mart_factory import CortexDiscoveryAgent
agent = CortexDiscoveryAgent()
result = agent.discover_hierarchy('{hierarchy_table}', '{mapping_table}')
print(result)
"
""",
                ),
                WorkflowStep(
                    name="Generate DDL pipeline",
                    run=f"""python -c "
from src.mart_factory import MartPipelineGenerator
gen = MartPipelineGenerator()
gen.generate_all('{project_name}')
"
""",
                ),
                WorkflowStep(
                    name="Upload DDL artifacts",
                    uses="actions/upload-artifact@v4",
                    with_params={
                        "name": "ddl-scripts",
                        "path": "output/*.sql",
                    },
                ),
            ],
        )
        jobs["generate"] = generate_job

        # Deploy pipeline job
        deploy_job = WorkflowJob(
            name="Deploy Pipeline",
            runs_on="ubuntu-latest",
            needs=["generate"],
            if_condition="github.ref == 'refs/heads/main'",
            env={
                "SNOWFLAKE_ACCOUNT": "${{ secrets.SNOWFLAKE_ACCOUNT }}",
                "SNOWFLAKE_USER": "${{ secrets.SNOWFLAKE_USER }}",
                "SNOWFLAKE_PASSWORD": "${{ secrets.SNOWFLAKE_PASSWORD }}",
            },
            steps=[
                WorkflowStep(
                    name="Download DDL artifacts",
                    uses="actions/download-artifact@v4",
                    with_params={"name": "ddl-scripts"},
                ),
                WorkflowStep(
                    name="Deploy VW_1",
                    run=f"python -m src.deployment.executor output/VW_1_{project_name.upper()}.sql",
                ),
                WorkflowStep(
                    name="Deploy DT_2",
                    run=f"python -m src.deployment.executor output/DT_2_{project_name.upper()}.sql",
                ),
                WorkflowStep(
                    name="Deploy DT_3A",
                    run=f"python -m src.deployment.executor output/DT_3A_{project_name.upper()}.sql",
                ),
                WorkflowStep(
                    name="Deploy DT_3",
                    run=f"python -m src.deployment.executor output/DT_3_{project_name.upper()}.sql",
                ),
            ],
        )
        jobs["deploy"] = deploy_job

        workflow = GitHubActionsWorkflow(
            name=f"Mart Factory - {project_name}",
            filename=f"mart-factory-{project_name.lower()}.yml",
            on_push={
                "branches": ["main", "master"],
                "paths": [f"hierarchies/{project_name}/**"],
            },
            on_workflow_dispatch=True,
            jobs=jobs,
        )

        return workflow

    def _create_lint_job(self, config: DbtCIConfig) -> WorkflowJob:
        """Create a dbt lint job."""
        return WorkflowJob(
            name="Lint",
            runs_on="ubuntu-latest",
            steps=[
                WorkflowStep(
                    name="Checkout",
                    uses="actions/checkout@v4",
                ),
                WorkflowStep(
                    name="Setup Python",
                    uses="actions/setup-python@v5",
                    with_params={"python-version": "3.11"},
                ),
                WorkflowStep(
                    name="Install sqlfluff",
                    run="pip install sqlfluff sqlfluff-templater-dbt",
                ),
                WorkflowStep(
                    name="Lint SQL files",
                    run="sqlfluff lint models/ --dialect snowflake",
                    continue_on_error=True,
                ),
            ],
        )

    def _create_dbt_build_job(self, config: DbtCIConfig) -> WorkflowJob:
        """Create a dbt build job."""
        steps = [
            WorkflowStep(
                name="Checkout",
                uses="actions/checkout@v4",
            ),
            WorkflowStep(
                name="Setup Python",
                uses="actions/setup-python@v5",
                with_params={"python-version": "3.11"},
            ),
            WorkflowStep(
                name="Install dbt",
                run=f"pip install dbt-{config.database_type}=={config.dbt_version}",
            ),
            WorkflowStep(
                name="Setup dbt profile",
                run=f"""mkdir -p {config.profiles_dir}
cat > {config.profiles_dir}/profiles.yml << EOF
{config.project_name}:
  target: {config.target}
  outputs:
    {config.target}:
      type: {config.database_type}
      account: ${{{{ secrets.{config.account_secret} }}}}
      user: ${{{{ secrets.{config.user_secret} }}}}
      password: ${{{{ secrets.{config.password_secret} }}}}
      role: ${{{{ secrets.{config.role_secret} }}}}
      warehouse: ${{{{ secrets.{config.warehouse_secret} }}}}
      database: ${{{{ secrets.{config.database_secret} }}}}
      schema: ${{{{ secrets.{config.schema_secret} }}}}
      threads: 4
EOF
""",
            ),
            WorkflowStep(
                name="Install dbt packages",
                run="dbt deps",
            ),
            WorkflowStep(
                name="dbt compile",
                run=f"dbt compile --target {config.target}",
            ),
        ]

        # Add build command if specified
        if DbtCommand.BUILD in config.run_commands or DbtCommand.RUN in config.run_commands:
            cmd = "dbt build" if DbtCommand.BUILD in config.run_commands else "dbt run"
            if config.selector:
                cmd += f" --selector {config.selector}"
            if config.exclude:
                cmd += f" --exclude {config.exclude}"
            cmd += f" --target {config.target}"

            steps.append(WorkflowStep(
                name="dbt build" if DbtCommand.BUILD in config.run_commands else "dbt run",
                run=cmd,
            ))

        # Upload artifacts
        if config.upload_artifacts:
            steps.append(WorkflowStep(
                name="Upload artifacts",
                uses="actions/upload-artifact@v4",
                with_params={
                    "name": "dbt-artifacts",
                    "path": "\n".join(config.artifact_paths),
                },
                if_condition="always()",
            ))

        return WorkflowJob(
            name="Build",
            runs_on="ubuntu-latest",
            steps=steps,
        )

    def _create_dbt_test_job(self, config: DbtCIConfig) -> WorkflowJob:
        """Create a dbt test job."""
        return WorkflowJob(
            name="Test",
            runs_on="ubuntu-latest",
            steps=[
                WorkflowStep(
                    name="Checkout",
                    uses="actions/checkout@v4",
                ),
                WorkflowStep(
                    name="Setup Python",
                    uses="actions/setup-python@v5",
                    with_params={"python-version": "3.11"},
                ),
                WorkflowStep(
                    name="Install dbt",
                    run=f"pip install dbt-{config.database_type}=={config.dbt_version}",
                ),
                WorkflowStep(
                    name="Download artifacts",
                    uses="actions/download-artifact@v4",
                    with_params={"name": "dbt-artifacts"},
                ),
                WorkflowStep(
                    name="dbt test",
                    run=f"dbt test --target {config.target}",
                ),
            ],
        )

    def _create_dbt_docs_job(self, config: DbtCIConfig) -> WorkflowJob:
        """Create a dbt docs job."""
        return WorkflowJob(
            name="Generate Docs",
            runs_on="ubuntu-latest",
            steps=[
                WorkflowStep(
                    name="Checkout",
                    uses="actions/checkout@v4",
                ),
                WorkflowStep(
                    name="Setup Python",
                    uses="actions/setup-python@v5",
                    with_params={"python-version": "3.11"},
                ),
                WorkflowStep(
                    name="Install dbt",
                    run=f"pip install dbt-{config.database_type}=={config.dbt_version}",
                ),
                WorkflowStep(
                    name="dbt docs generate",
                    run=f"dbt docs generate --target {config.target}",
                ),
                WorkflowStep(
                    name="Upload docs",
                    uses="actions/upload-pages-artifact@v3",
                    with_params={"path": "target"},
                ),
            ],
        )

    def _create_deploy_job(self, config: DbtCIConfig) -> WorkflowJob:
        """Create a deploy job."""
        return WorkflowJob(
            name="Deploy to Production",
            runs_on="ubuntu-latest",
            if_condition="github.ref == 'refs/heads/main' && github.event_name == 'push'",
            steps=[
                WorkflowStep(
                    name="Checkout",
                    uses="actions/checkout@v4",
                ),
                WorkflowStep(
                    name="Setup Python",
                    uses="actions/setup-python@v5",
                    with_params={"python-version": "3.11"},
                ),
                WorkflowStep(
                    name="Install dbt",
                    run=f"pip install dbt-{config.database_type}=={config.dbt_version}",
                ),
                WorkflowStep(
                    name="dbt run (production)",
                    run=f"dbt run --target prod --full-refresh",
                    env={
                        "DBT_TARGET": "prod",
                    },
                ),
                WorkflowStep(
                    name="Notify success",
                    uses="slackapi/slack-github-action@v1.26.0",
                    with_params={
                        "channel-id": "${{ secrets.SLACK_CHANNEL_ID }}",
                        "slack-message": f"dbt deploy for {config.project_name} completed successfully!",
                    },
                    env={
                        "SLACK_BOT_TOKEN": "${{ secrets.SLACK_BOT_TOKEN }}",
                    },
                    if_condition="success()",
                    continue_on_error=True,
                ),
            ],
        )

    def generate_pr_template(
        self,
        project_type: str = "dbt",
    ) -> str:
        """
        Generate a PR template.

        Args:
            project_type: Type of project

        Returns:
            PR template markdown
        """
        if project_type == "dbt":
            return """## Summary
<!-- Brief description of changes -->

## Changes
<!-- List the models/sources affected -->
- [ ] New models added
- [ ] Existing models modified
- [ ] Tests added/updated
- [ ] Documentation updated

## Testing
<!-- How were these changes tested? -->
- [ ] `dbt compile` passes
- [ ] `dbt run` completes successfully
- [ ] `dbt test` passes

## Checklist
- [ ] Code follows SQL style guide
- [ ] Models have appropriate tests
- [ ] Documentation is updated
- [ ] No sensitive data exposed

## Related Issues
<!-- Link any related issues -->
Closes #

---
Generated by DataBridge AI
"""
        else:
            return """## Summary
<!-- Brief description of changes -->

## Changes
<!-- List the changes made -->

## Testing
<!-- How were these changes tested? -->

## Checklist
- [ ] Code follows project conventions
- [ ] Tests pass
- [ ] Documentation updated

## Related Issues
Closes #

---
Generated by DataBridge AI
"""
