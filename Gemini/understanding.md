# Understanding the DataBridge AI Application

Based on the contents of the `v3` and `v4` directories, the DataBridge AI application is a sophisticated, two-part platform for Financial Planning & Analysis (FP&A). It is designed to be "headless," meaning it operates without a graphical user interface, relying instead on a command-line interface (CLI) and integration with an AI assistant (Claude) via an MCP server.

The platform is split into two distinct but complementary components:

## Librarian: Headless DataBridge AI (The Hierarchy & Reconciliation Engine)

Version 3 is the foundational layer of the platform. Its primary role is to build and manage the semantic layer that gives business context to raw data. It is focused on **hierarchy management and data reconciliation**.

### Key Responsibilities of Librarian:

*   **Hierarchy Management:** Users can build, edit, and manage complex business hierarchies, such as:
    *   Chart of Accounts for financial reporting (P&L, Balance Sheet).
    *   Organizational structures (cost centers, departments, regions).
    *   Product or asset hierarchies.
*   **Data Mapping:** It allows users to map these abstract hierarchies to concrete data sources in various databases (e.g., mapping a "Revenue" node in a P&L hierarchy to specific general ledger account numbers in a database table).
*   **Data Reconciliation:** It provides tools to compare datasets from different sources to identify discrepancies. This includes:
    *   **Hash-based comparison** to find exact matches, orphans (records in one source but not the other), and conflicts (records with the same key but different values).
    *   **Fuzzy matching** to identify and de-duplicate similar but not identical records (e.g., "Acme Inc." vs. "Acme, Inc.").
*   **Deployment:** It can generate and deploy SQL scripts (e.g., for views or data insertion) to a data warehouse like Snowflake, effectively operationalizing the created hierarchies.
*   **Headless Operation:** It functions as a pure Python CLI application, intended for technical users or for programmatic use by the AI assistant.

In essence, **Librarian is used to define the "what" and "how" of the data**—what the business concepts are and how they map to the underlying data systems.

## Researcher: Headless DataBridge Analytics (The Analytics & Insights Engine)

Version 4 is the analytical engine that sits on top of the foundation laid by Librarian. It leverages the hierarchies and mappings created in Librarian to perform powerful, context-aware analysis on large datasets.

### Key Responsibilities of Researcher:

*   **Compute Pushdown:** This is the core architectural principle of Researcher. Instead of pulling massive datasets out of a data warehouse to be analyzed locally, Researcher generates and sends optimized SQL queries to the data warehouse (like Snowflake or Databricks) for execution. Only the much smaller, aggregated results are returned. This is highly efficient and scalable.
*   **Natural Language to SQL (NL-to-SQL):** It provides a natural language interface that allows business users to ask questions in plain English (e.g., "What were our total sales by region last quarter?") and have them translated into complex SQL queries.
*   **Insight Generation:** Researcher goes beyond just answering questions. It can proactively analyze data to:
    *   Detect anomalies and outliers.
    *   Identify trends and patterns.
    *   Perform variance analysis (e.g., budget vs. actual).
    *   Generate executive summaries.
*   **FP&A Workflows:** It is specifically designed to support common financial analysis workflows, such as month-end close, forecasting, and management reporting.

In essence, **Researcher is used to analyze the data and answer the "why"**—why did sales increase, what are the key drivers of variance, what are the emerging trends.

## Synergy and Overall Purpose

The two versions work in tandem to create a comprehensive, AI-driven FP&A platform:

1.  **Librarian** is used by data stewards, financial systems analysts, or developers to build and maintain the logical data model (the hierarchies).
2.  **Researcher** is used by financial analysts, business users, and executives to query and analyze data in a self-service manner, using the business terms defined in Librarian, often through a natural language interface.

Together, they aim to bridge the gap between raw enterprise data and actionable business insights, making complex financial analysis more accessible, efficient, and scalable. The "headless" nature of both components indicates a modern, API-first approach, where the primary user interface is intended to be an AI assistant rather than a traditional web application.
