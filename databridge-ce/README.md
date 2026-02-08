# Databridge AI

*A modern, AI-powered framework for intelligent data integration, analysis, and workflow orchestration.*

[![PyPI version](https://badge.fury.io/py/databridge-ai.svg)](https://badge.fury.io/py/databridge-ai)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

**Databridge AI** is a Python-based framework designed to streamline complex data engineering tasks. It combines a powerful **Multi-Capability Plane (MCP)** for tool integration with a suite of intelligent agents to automate data analysis, schema matching, and workflow execution.

At its core is an interactive web UI, the **Databridge Workbench**, that provides a real-time interface for discovering tools, managing projects, and building complex data processing workflows.

## Key Features

- **Plugin-Based Architecture:** Easily extend the system by creating new "Tools" and "Agents" as simple Python modules.
- **Interactive Web UI:** A local, Flask-based web application (the "Workbench") for real-time interaction with all system capabilities.
- **AI-Powered Agents:** Includes a `UnifiedAgent` that can plan and delegate tasks to specialized agents for SQL generation, data analysis, and more.
- **Workflow Orchestrator:** A simple but powerful engine for chaining tool calls together to create complex, reproducible data pipelines.
- **Extensible and Introspective:** All registered tools and their parameters are automatically discoverable through the backend API and visible in the UI.

## Installation

You can install the stable version of Databridge AI directly from PyPI:

```sh
pip install databridge-ai
```

To install the latest development version directly from the `dev` branch on GitHub:

```sh
pip install git+https://github.com/your-username/Databridge_AI.git@dev
```

## Quick Start

Get the Databridge AI Workbench up and running in a few simple steps:

1.  **Install the package:**
    ```sh
    pip install databridge-ai
    ```

2.  **Run the UI:**
    ```sh
    python -m databridge_ai.run_ui
    ```
    *(Note: The exact entry point may vary based on package structure. This is a common pattern.)*

3.  **Open your browser:** Navigate to `http://127.0.0.1:5050` to access the Workbench.

## Documentation

For more detailed information on the architecture, API, and advanced usage, please visit the [**Official Project Wiki**](https://github.com/your-username/Databridge_AI/wiki).

## Contributing

We welcome contributions from the community! Whether it's reporting a bug, suggesting a feature, or writing code, your help is valued.

Please read our [**Contributing Guide**](CONTRIBUTING.md) to learn how you can get involved and understand our development workflow.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.