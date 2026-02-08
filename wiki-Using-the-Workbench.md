# Using the Workbench

The Databridge AI Workbench is the primary user interface for interacting with the framework's tools and agents. It is organized into several pages, accessible from the left-hand navigation pane.

## Dashboard

This is the main landing page. It provides a high-level, at-a-glance overview of your Databridge AI instance, including:

- **Stat Cards:** Key metrics showing the total number of tools, projects, and workflow steps currently registered in the system.
- **Recent Activity:** A log of the most recent actions performed, pulled from the system's audit logs.

## Tool Workbench

This is the primary page for experimenting with and executing individual tools.

- **Select Tool:** This dropdown is automatically populated with every tool discovered by the plugin loader.
- **Parameters:** Once you select a tool, this section dynamically generates a form with input fields for all of the tool's required parameters. Descriptions and types are shown to guide you.
- **Tool History:** This panel on the right automatically saves a record of every tool you successfully run. Clicking on a history item will instantly pre-fill the tool and its parameters, allowing you to easily re-run or modify previous commands.
- **Tool Output:** The results of your tool execution are displayed here in a user-friendly, collapsible tree format for easy inspection of complex JSON data.

## Hierarchy Projects

This page is designed for managing more complex, multi-file projects, particularly those related to hierarchy management.

- **Projects List:** On the left, you'll find a list of all projects discovered within the `use_cases_by_claude` directory.
- **Editor:** Clicking a project allows you to view and edit its main `README.md` file directly within the UI.

## Workflow Editor

This page allows you to chain multiple tool calls together into a single, reproducible workflow.

- **Toolbox:** A list of all available tools that can be added as steps to your workflow.
- **Workflow Canvas:** The main area where your workflow steps are displayed in order.
- **Adding a Step:** Click a tool in the Toolbox to open a modal window. Here, you can give the step a descriptive name and fill in the tool's parameters.
- **Loading an Example:** Click the "Load Example" button to see a pre-built workflow on the canvas. This is a great way to learn how workflows are structured.
- **Saving/Loading:** You can load a previously saved workflow from the backend or clear the canvas to start fresh.

## Administration

This page provides access to the system's core configuration. You can view the current settings (`config.py`) as a JSON object and edit them. Note that in the current version, saving is simulated and does not persist after a server restart.

## Help / Guide

This page contains all the essential documentation for the project, including the architectural diagram, user guides, and a list of all discoverable example use cases.
