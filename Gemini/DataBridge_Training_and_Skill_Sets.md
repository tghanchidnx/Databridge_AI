# DataBridge AI: Training Plan & Required Skill Sets

## Introduction

This document outlines the recommended skill sets and a corresponding training plan for organizations adopting the DataBridge AI platform. The philosophy of DataBridge is to empower business users by automating complex technical tasks. Therefore, our approach to training differentiates between the skills required for **End Users** (the "drivers" of the platform) and the **Implementation Team** (the "mechanics" who maintain the engine).

---

## Section 1: Skill Sets for End Users

*(Target Audience: Financial Analysts, FP&A Teams, Operations Analysts, Business Analysts)*

The primary goal for an End User is to leverage their deep business knowledge. DataBridge AI handles the underlying technical complexity, allowing the user to focus on a `what` and `why`, not the `how`.

### Core Skill Sets:

1.  **Deep Domain Expertise:**
    *   This is the most critical requirement. The user must intimately understand their area of the business (e.g., financial closing processes, operational KPIs, sales pipeline stages). The AI's value is directly proportional to the quality of the business logic the user provides it with.

2.  **Logical and Structural Thinking:**
    *   Users must be able to think about their business processes in terms of hierarchies, relationships, and logical rules.
    *   *Example:* An analyst should be able to whiteboard a simple Chart of Accounts, showing how individual GL accounts roll up into parent categories, even if they don't know how to model it in a database.

3.  **Data Literacy:**
    *   A solid understanding of basic data concepts is necessary. Users should be comfortable with concepts like tables, columns, rows, and the general idea of filtering and joining data.
    *   They do **not** need to be SQL experts, but they should be able to read a simple `SELECT` statement and understand its intent.

4.  **Effective Communication:**
    *   The ability to ask clear, unambiguous questions and describe business processes in plain English is essential for interacting with the platform's AI agents. The more precise the prompt, the better the result.

### What End Users *Don't* Need:

*   Expertise in Python, dbt, or Git.
*   Database administration skills.
*   Deep experience with data modeling or ETL development.

---

## Section 2: Skill Sets for the Implementation Team

*(Target Audience: Data Engineers, BI Developers, Platform Administrators)*

This team is responsible for installing, configuring, managing, and extending the DataBridge AI platform. They build the foundation upon which the End Users operate.

### Core Skill Sets:

1.  **Platform & Data Engineering:**
    *   **Python:** Strong, idiomatic Python skills are mandatory for managing the core application, scripting interactions, and potentially extending the platform with new tools or agents.
    *   **Database Administration:** Experience with PostgreSQL (for "The Librarian" and "The Historian's" structured log) and a Document Database like MongoDB is highly recommended.
    *   **Data Warehousing:** Expert-level knowledge of the target data warehouse (e.g., Snowflake), including data loading, performance tuning, and security management.

2.  **DevOps & Automation:**
    *   **Git/GitHub:** Proficiency in version control is essential, especially for managing the "Advanced" dbt-based workflows.
    *   **CI/CD:** Experience with GitHub Actions or a similar tool to automate the testing and deployment of DataBridge projects.
    *   **Docker:** Ability to build, manage, and deploy the containerized components of the DataBridge application.

3.  **Data Modeling & BI Integration:**
    *   **Advanced SQL:** Expert-level SQL is non-negotiable for validating AI-generated code and troubleshooting complex data issues.
    *   **dbt (Recommended):** For organizations using the "Advanced" workflow, strong dbt skills are required to manage and customize the AI-generated dbt projects.
    *   **BI Tool Connectivity:** A deep understanding of how BI tools like Power BI, Tableau, and SSAS connect to data sources and build their semantic models. This is crucial for ensuring the DataBridge output is optimized for these downstream tools.

---

## Section 3: Recommended Training Plan

This modular plan can be tailored to different audiences.

### Module 1: The DataBridge Philosophy (1 Day - All Audiences)
*   **Objective:** To align everyone on the strategic value of DataBridge AI.
*   **Topics:**
    *   Introduction: Why DataBridge? The "BI Accelerator" Strategy.
    *   Meet the Agents: Understanding the roles of "The Librarian," "The Researcher," and "The Historian."
    *   Core Workflow: From legacy assets (SQL, CSV) to a business-ready data mart.
    *   Lab: A complete, end-to-end demonstration of the platform's capabilities.

### Module 2: A User's Guide to DataBridge (2 Days - End Users)
*   **Objective:** To empower business users to effectively translate their knowledge into AI-driven results.
*   **Topics:**
    *   Thinking in Hierarchies: Deconstructing business processes into structured data.
    *   Communicating with the AI: The art of the effective prompt.
    *   Hands-On Lab 1: Using "The Librarian" to create and manage a new business hierarchy from a spreadsheet.
    *   Hands-On Lab 2: Using "The Researcher" to ask natural language questions against the new data model.
    *   Understanding the Output: How to use the auto-generated documentation and data marts in Excel or a connected BI tool.

### Module 3: Platform Administration & Architecture (3 Days - Implementation Team)
*   **Objective:** To provide engineers with the skills to install, manage, and maintain the platform.
*   **Topics:**
    *   Architecture Deep Dive: The monorepo, microservices, and data flow.
    *   Installation & Configuration: Setting up the Python environment, Docker containers, and database connections (PostgreSQL, MongoDB, Vector DB).
    *   The MCP Toolkit: How to manage, enable, and configure tools for the AI agents.
    *   "The Historian": Understanding the event log and how to query the platform's memory.
    *   Hands-On Lab: A full end-to-end installation and configuration of a new DataBridge instance.

### Module 4: Advanced Implementation Workflows (2 Days - Implementation Team)
*   **Objective:** To master the advanced, dbt-centric capabilities of the platform.
*   **Topics:**
    *   dbt Integration Deep Dive: How the AI generates dbt models and projects.
    *   Version Control with Git: Managing and branching AI-generated projects.
    *   CI/CD for DataBridge Projects: Building a GitHub Actions pipeline to automatically test and deploy changes to Snowflake.
    *   Extending the Platform: A primer on creating a new custom MCP tool.
    *   Hands-On Lab: Taking an AI-generated dbt project, committing it to Git, and running it through an automated deployment pipeline.