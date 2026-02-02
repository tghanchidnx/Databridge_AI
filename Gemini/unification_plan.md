# Prompt and Plan of Action for Unifying DataBridge Librarian & Researcher

This document contains a detailed prompt for an AI assistant and a corresponding plan of action to unify the `librarian` and `researcher` applications into a single, cohesive monorepo.

---

## AI Prompt for Unification

"Hello! Your task is to refactor the DataBridge AI platform by unifying the existing `librarian` and `researcher` applications into a single, streamlined monorepo. The goal is to reduce code duplication, improve maintainability, and create a more cohesive developer and user experience.

Please follow these key requirements:

1.  **Create a Monorepo Structure:** Establish a new directory structure with `apps` and `libs` folders. The `librarian` and `researcher` applications will be moved into the `apps` folder, and shared code will be extracted into libraries within the `libs` folder.

2.  **Extract Shared Libraries:** Identify and move duplicated code from `librarian` and `researcher` into new shared libraries. This includes:
    *   **`databridge-core`:** For database connectors, MCP server utilities, shared CLI helpers, and configuration models.
    *   **`databridge-models`:** For shared SQLAlchemy and Pydantic models.

3.  **Refactor Applications:** Modify the `librarian` and `researcher` applications to remove the extracted code and instead use the new shared libraries as dependencies.

4.  **Unify the Command-Line Interface (CLI):** Merge the two separate CLIs (`databridge` and `databridge-analytics`) into a single entry point named `databridge`. The commands from `librarian` and `researcher` should be organized under logical subcommands (e.g., `databridge hierarchy ...`, `databridge analytics ...`).

5.  **Update the CI/CD Pipeline:** Refactor the existing GitHub Actions workflows (`ci.yml` and `release.yml`) to work with the new monorepo structure. Use matrix strategies to reduce duplication in the CI pipeline.

6.  **Ensure Functionality:** Throughout this process, ensure that all existing tests for both applications continue to pass and that the overall functionality of the platform remains intact.

Please follow the detailed plan of action provided below to execute this task systematically."

---

## Plan of Action for Unification

This plan breaks down the unification process into six distinct phases.

### Phase 1: Project Scaffolding & Setup

**Goal:** Create the new monorepo directory structure and initialize it as a Python project workspace.

1.  **Create Root Structure:**
    *   Create a new root-level `apps` directory.
    *   Create a new root-level `libs` directory.
    *   Create a root-level `pyproject.toml` to manage the workspace (e.g., using Poetry or a similar tool).

2.  **Move Applications:**
    *   Move the entire contents of the `librarian` directory into `apps/databridge-librarian`.
    *   Move the entire contents of the `researcher` directory into `apps/databridge-researcher`.

3.  **Create Shared Library Scaffolds:**
    *   Create `libs/databridge-core` with a `pyproject.toml` and a `src/databridge_core` directory.
    *   Create `libs/databridge-models` with a `pyproject.toml` and a `src/databridge_models` directory.

4.  **Update Configuration:**
    *   Configure the root `pyproject.toml` to recognize `apps/*` and `libs/*` as part of the workspace.

### Phase 2: Core Library Extraction (`databridge-core`)

**Goal:** Consolidate shared, non-domain-specific code into the `databridge-core` library.

1.  **Connection Management:**
    *   Identify the common database connection logic in `librarian/src/connections` and `researcher/src/connectors`.
    *   Move the generic connector interface and specific implementations (Snowflake, PostgreSQL, etc.) into `libs/databridge-core/src/databridge_core/connections`.
    *   Refactor them to be agnostic of Librarian or Researcher.

2.  **MCP Server Utilities:**
    *   Move the FastMCP server setup and utility functions (e.g., tool registration helpers) from `librarian/src/mcp` and `researcher/src/mcp` into `libs/databridge-core/src/databridge_core/mcp`.

3.  **CLI Helpers:**
    *   Extract shared CLI utilities, such as `rich` table formatters, console objects, and common validators, into `libs/databridge-core/src/databridge_core/cli`.

4.  **Configuration:**
    *   Move the core Pydantic settings management (`Settings` class, `.env` loading) into `libs/databridge-core/src/databridge_core/config`.

### Phase 3: Application Refactoring

**Goal:** Modify the Librarian and Researcher applications to depend on the new shared libraries.

1.  **Update Dependencies:**
    *   In `apps/databridge-librarian/pyproject.toml`, add `databridge-core` and `databridge-models` as local, editable dependencies.
    *   In `apps/databridge-researcher/pyproject.toml`, do the same.

2.  **Refactor Librarian:**
    *   Delete the `src/connections`, `src/mcp`, and other duplicated directories that have been moved.
    *   Update all imports in the Librarian codebase to reference the shared libraries (e.g., `from databridge_core.connections import ...`).
    *   Run Librarian tests to ensure no functionality is broken.

3.  **Refactor Researcher:**
    *   Delete the `src/connectors`, `src/mcp`, and other duplicated code.
    *   Update all imports to use the shared libraries.
    *   Run Researcher tests to ensure no functionality is broken.

### Phase 4: CLI Unification

**Goal:** Create a single, user-friendly CLI.

1.  **Create a New CLI Entry Point:**
    *   Create a new application or directory, e.g., `apps/databridge-cli`, that will serve as the main entry point.
    *   This new app will depend on both `databridge-librarian` and `databridge-researcher`.

2.  **Merge Typer Apps:**
    *   Modify the main `typer` app in the new CLI to import and mount the Typer apps from Librarian and Researcher as subcommands.
    *   Example:
        ```python
        # in databridge-cli/main.py
        import typer
        from databridge_librarian.cli.app import app as librarian_app
        from databridge_researcher.cli.app import app as researcher_app

        main_app = typer.Typer(name="databridge")
        main_app.add_typer(librarian_app, name="hierarchy")
        main_app.add_typer(researcher_app, name="analytics")

        if __name__ == "__main__":
            main_app()
        ```

3.  **Adjust `pyproject.toml` Scripts:**
    *   Update the `[tool.poetry.scripts]` or equivalent in the root `pyproject.toml` to point to the new unified CLI entry point (e.g., `databridge = "databridge_cli.main:main_app"`).

### Phase 5: CI/CD Refactoring

**Goal:** Update the GitHub Actions to align with the new monorepo structure.

1.  **Refactor `ci.yml`:**
    *   Modify the file paths to reflect the new `apps/` and `libs/` structure.
    *   Use a **matrix strategy** to run linting and testing jobs across `[librarian, researcher]`, eliminating duplicated job definitions.
    *   Add a new job to lint and test the shared libraries in `libs/`.
    *   Ensure that coverage reports are still generated and uploaded correctly for each application.

2.  **Refactor `release.yml`:**
    *   Update the Docker build contexts to point to `apps/databridge-librarian` and `apps/databridge-researcher`.
    *   Ensure that Docker images are still built and tagged correctly.
    *   Update the changelog generation script to correctly detect changes within the `apps/` and `libs/` directories.

### Phase 6: Finalization and Documentation

**Goal:** Clean up, document the new structure, and validate the final result.

1.  **Remove Old Files:**
    *   Delete the original `librarian` and `researcher` directories from the root.
    *   Clean up any redundant configuration files.

2.  **Update Documentation:**
    *   Update `README.md` to reflect the new unified structure and single CLI.
    *   Update all user guides and developer documentation to refer to the new commands and file paths.
    *   Add documentation for the new shared libraries.

3.  **End-to-End Validation:**
    *   Run all tests for the entire monorepo one last time (`pytest apps/ libs/`).
    *   Manually test the key CLI commands for both hierarchy and analytics functionality to ensure a smooth user experience.
    *   Manually trigger a test run of the updated CI/CD pipelines.