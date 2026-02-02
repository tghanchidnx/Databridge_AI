"""
Business Glossary and Metric Definitions for DataBridge Analytics Researcher.

Pre-built definitions for common FP&A and accounting terms.
"""

from typing import List, Dict, Any

from .store import KnowledgeBaseStore, KBCollectionType, KBDocument


# Business Glossary Terms
BUSINESS_GLOSSARY: List[Dict[str, Any]] = [
    # Accounting Terms
    {
        "term": "GAAP",
        "category": "accounting",
        "definition": "Generally Accepted Accounting Principles - The standard framework of accounting rules and standards used in the United States for financial reporting.",
        "related_terms": ["IFRS", "SEC", "Financial Reporting"],
    },
    {
        "term": "Accrual Accounting",
        "category": "accounting",
        "definition": "An accounting method that records revenues and expenses when they are incurred, regardless of when cash is exchanged.",
        "related_terms": ["Cash Accounting", "Revenue Recognition", "Matching Principle"],
    },
    {
        "term": "Depreciation",
        "category": "accounting",
        "definition": "The systematic allocation of the cost of a tangible asset over its useful life. Common methods include straight-line and accelerated depreciation.",
        "related_terms": ["Amortization", "Book Value", "Useful Life"],
    },
    {
        "term": "Chart of Accounts",
        "category": "accounting",
        "definition": "A listing of all accounts used in an organization's general ledger, organized by account type (assets, liabilities, equity, revenue, expenses).",
        "related_terms": ["General Ledger", "Account Structure", "Trial Balance"],
    },
    {
        "term": "Journal Entry",
        "category": "accounting",
        "definition": "A recording of a business transaction in the accounting system, with at least one debit and one credit that balance.",
        "related_terms": ["General Ledger", "Double-Entry Accounting", "Posting"],
    },

    # Finance Terms
    {
        "term": "Working Capital",
        "category": "finance",
        "definition": "The difference between current assets and current liabilities, measuring a company's short-term liquidity and operational efficiency.",
        "related_terms": ["Current Ratio", "Liquidity", "Cash Conversion Cycle"],
    },
    {
        "term": "EBITDA",
        "category": "finance",
        "definition": "Earnings Before Interest, Taxes, Depreciation, and Amortization - A measure of operating performance that excludes non-operating expenses.",
        "related_terms": ["Operating Income", "EBIT", "Cash Flow"],
    },
    {
        "term": "Cost of Capital",
        "category": "finance",
        "definition": "The required return necessary to make a capital budgeting project worthwhile. Includes cost of debt and cost of equity.",
        "related_terms": ["WACC", "Discount Rate", "Hurdle Rate"],
    },
    {
        "term": "Budget Variance",
        "category": "finance",
        "definition": "The difference between budgeted and actual figures. Favorable variance indicates better-than-expected performance; unfavorable indicates worse.",
        "related_terms": ["Variance Analysis", "Favorable", "Unfavorable"],
    },
    {
        "term": "Rolling Forecast",
        "category": "finance",
        "definition": "A continuously updated financial forecast that extends a set number of periods into the future, replacing outdated periods with new projections.",
        "related_terms": ["Budget", "Forecast", "Planning Horizon"],
    },

    # FP&A Terms
    {
        "term": "Driver-Based Planning",
        "category": "fpa",
        "definition": "A planning methodology that focuses on key business drivers (units, headcount, capacity) rather than line-item budgeting to create more accurate forecasts.",
        "related_terms": ["Business Drivers", "Operational Planning", "Modeling"],
    },
    {
        "term": "Scenario Analysis",
        "category": "fpa",
        "definition": "The process of analyzing possible future events by considering alternative outcomes (scenarios) such as base case, upside, and downside.",
        "related_terms": ["Sensitivity Analysis", "What-If Analysis", "Monte Carlo"],
    },
    {
        "term": "Month-End Close",
        "category": "fpa",
        "definition": "The process of finalizing all financial transactions for a month, reconciling accounts, and preparing financial statements.",
        "related_terms": ["Close Calendar", "Reconciliation", "Financial Statements"],
    },
    {
        "term": "Flux Analysis",
        "category": "fpa",
        "definition": "The analysis of changes (fluctuations) in financial data between periods, identifying and explaining significant variances.",
        "related_terms": ["Variance Analysis", "MoM", "YoY"],
    },
    {
        "term": "Bridge Analysis",
        "category": "fpa",
        "definition": "A visualization and analysis technique showing how a value changes from one point to another through contributing factors.",
        "related_terms": ["Waterfall Chart", "Variance Bridge", "Walk"],
    },

    # Analytics Terms
    {
        "term": "KPI",
        "category": "analytics",
        "definition": "Key Performance Indicator - A measurable value that demonstrates how effectively an organization is achieving key business objectives.",
        "related_terms": ["Metrics", "OKR", "Scorecard"],
    },
    {
        "term": "Trend Analysis",
        "category": "analytics",
        "definition": "The analysis of data over time to identify patterns, direction, and rate of change in key metrics.",
        "related_terms": ["Time Series", "Seasonality", "Moving Average"],
    },
    {
        "term": "Cohort Analysis",
        "category": "analytics",
        "definition": "Grouping subjects by shared characteristics over time to analyze behavior patterns and track performance.",
        "related_terms": ["Customer Analytics", "Retention", "LTV"],
    },
]

# Metric Definitions
METRIC_DEFINITIONS: List[Dict[str, Any]] = [
    # Revenue Metrics
    {
        "name": "Revenue",
        "category": "revenue",
        "definition": "Total income generated from the sale of goods or services",
        "formula": "SUM(sales_amount)",
        "unit": "currency",
        "related_metrics": ["Net Revenue", "Gross Sales"],
    },
    {
        "name": "Net Revenue",
        "category": "revenue",
        "definition": "Revenue after deducting returns, allowances, and discounts",
        "formula": "Gross Revenue - Returns - Allowances - Discounts",
        "unit": "currency",
        "related_metrics": ["Gross Revenue", "Revenue"],
    },
    {
        "name": "ARR",
        "category": "revenue",
        "definition": "Annual Recurring Revenue - The annualized value of recurring subscription revenue",
        "formula": "MRR * 12",
        "unit": "currency",
        "related_metrics": ["MRR", "Bookings", "Churn"],
    },
    {
        "name": "MRR",
        "category": "revenue",
        "definition": "Monthly Recurring Revenue - The predictable revenue from subscriptions on a monthly basis",
        "formula": "SUM(monthly_subscription_value)",
        "unit": "currency",
        "related_metrics": ["ARR", "Net MRR", "Expansion MRR"],
    },

    # Profitability Metrics
    {
        "name": "Gross Margin",
        "category": "profitability",
        "definition": "The percentage of revenue remaining after cost of goods sold",
        "formula": "(Revenue - COGS) / Revenue * 100",
        "unit": "percentage",
        "related_metrics": ["Net Margin", "COGS", "Gross Profit"],
    },
    {
        "name": "Operating Margin",
        "category": "profitability",
        "definition": "The percentage of revenue remaining after operating expenses",
        "formula": "Operating Income / Revenue * 100",
        "unit": "percentage",
        "related_metrics": ["EBITDA Margin", "Net Margin"],
    },
    {
        "name": "EBITDA Margin",
        "category": "profitability",
        "definition": "EBITDA as a percentage of revenue, measuring operating cash efficiency",
        "formula": "EBITDA / Revenue * 100",
        "unit": "percentage",
        "related_metrics": ["Operating Margin", "EBITDA"],
    },
    {
        "name": "Net Profit Margin",
        "category": "profitability",
        "definition": "The percentage of revenue that translates to profit after all expenses",
        "formula": "Net Income / Revenue * 100",
        "unit": "percentage",
        "related_metrics": ["Gross Margin", "Operating Margin"],
    },

    # Efficiency Metrics
    {
        "name": "Inventory Turnover",
        "category": "efficiency",
        "definition": "How many times inventory is sold and replaced over a period",
        "formula": "COGS / Average Inventory",
        "unit": "ratio",
        "related_metrics": ["Days Inventory Outstanding", "Asset Turnover"],
    },
    {
        "name": "DSO",
        "category": "efficiency",
        "definition": "Days Sales Outstanding - Average days to collect payment from customers",
        "formula": "(Accounts Receivable / Revenue) * 365",
        "unit": "days",
        "related_metrics": ["DPO", "Cash Conversion Cycle"],
    },
    {
        "name": "DPO",
        "category": "efficiency",
        "definition": "Days Payable Outstanding - Average days to pay suppliers",
        "formula": "(Accounts Payable / COGS) * 365",
        "unit": "days",
        "related_metrics": ["DSO", "Cash Conversion Cycle"],
    },
    {
        "name": "Cash Conversion Cycle",
        "category": "efficiency",
        "definition": "Days to convert inventory investment into cash from sales",
        "formula": "DIO + DSO - DPO",
        "unit": "days",
        "related_metrics": ["DSO", "DPO", "Working Capital"],
    },

    # Liquidity Metrics
    {
        "name": "Current Ratio",
        "category": "liquidity",
        "definition": "Ability to pay short-term obligations with current assets",
        "formula": "Current Assets / Current Liabilities",
        "unit": "ratio",
        "related_metrics": ["Quick Ratio", "Working Capital"],
    },
    {
        "name": "Quick Ratio",
        "category": "liquidity",
        "definition": "Ability to meet short-term obligations without selling inventory",
        "formula": "(Current Assets - Inventory) / Current Liabilities",
        "unit": "ratio",
        "related_metrics": ["Current Ratio", "Cash Ratio"],
    },

    # Growth Metrics
    {
        "name": "Revenue Growth Rate",
        "category": "growth",
        "definition": "Percentage increase in revenue over a period",
        "formula": "(Current Revenue - Prior Revenue) / Prior Revenue * 100",
        "unit": "percentage",
        "related_metrics": ["YoY Growth", "CAGR"],
    },
    {
        "name": "CAGR",
        "category": "growth",
        "definition": "Compound Annual Growth Rate - Annualized average growth rate over multiple years",
        "formula": "(Ending Value / Beginning Value)^(1/n) - 1",
        "unit": "percentage",
        "related_metrics": ["Revenue Growth", "YoY Growth"],
    },
    {
        "name": "Customer Growth Rate",
        "category": "growth",
        "definition": "Rate of new customer acquisition over a period",
        "formula": "(New Customers - Lost Customers) / Beginning Customers * 100",
        "unit": "percentage",
        "related_metrics": ["Churn Rate", "Net Customer Adds"],
    },
]

# FP&A Concepts
FPA_CONCEPTS: List[Dict[str, Any]] = [
    {
        "concept": "Zero-Based Budgeting",
        "category": "budgeting",
        "description": "A budgeting approach where every expense must be justified for each new period, starting from a zero base rather than using prior year actuals.",
        "use_cases": ["Cost reduction initiatives", "New business units", "Restructuring"],
    },
    {
        "concept": "Top-Down vs Bottom-Up Planning",
        "category": "planning",
        "description": "Top-down sets targets from leadership; bottom-up builds from operational details. Most organizations use a hybrid approach.",
        "use_cases": ["Annual planning", "Strategic targets", "Operational budgeting"],
    },
    {
        "concept": "Three Statement Model",
        "category": "modeling",
        "description": "An integrated financial model connecting the Income Statement, Balance Sheet, and Cash Flow Statement through linked formulas.",
        "use_cases": ["Financial forecasting", "M&A analysis", "Valuation"],
    },
    {
        "concept": "Price/Volume/Mix Analysis",
        "category": "variance",
        "description": "Decomposition of revenue variance into price changes, volume changes, and product/customer mix shifts.",
        "use_cases": ["Revenue variance", "Margin analysis", "Sales performance"],
    },
    {
        "concept": "Contribution Margin Analysis",
        "category": "profitability",
        "description": "Analysis of the portion of revenue that exceeds variable costs, showing contribution to fixed costs and profit.",
        "use_cases": ["Product profitability", "Pricing decisions", "Break-even analysis"],
    },
]


class GlossaryLoader:
    """Loader for glossary and metric definitions."""

    def __init__(self, kb_store: KnowledgeBaseStore):
        """Initialize with knowledge base store."""
        self.kb = kb_store

    def load_business_glossary(self) -> Dict[str, Any]:
        """Load all business glossary terms."""
        documents = []
        for term in BUSINESS_GLOSSARY:
            content = f"{term['term']}: {term['definition']}"
            if term.get("related_terms"):
                content += f" Related: {', '.join(term['related_terms'])}"

            documents.append(KBDocument(
                id=f"term_{term['term'].lower().replace(' ', '_')}",
                content=content,
                category=term["category"],
                title=term["term"],
                metadata={"related_terms": term.get("related_terms", [])},
            ))

        result = self.kb.add_documents(KBCollectionType.BUSINESS_GLOSSARY, documents)
        return {
            "type": "business_glossary",
            "loaded": len(documents),
            "success": result.success,
        }

    def load_metric_definitions(self) -> Dict[str, Any]:
        """Load all metric definitions."""
        documents = []
        for metric in METRIC_DEFINITIONS:
            content = f"{metric['name']}: {metric['definition']}. Formula: {metric['formula']}"
            if metric.get("related_metrics"):
                content += f" Related: {', '.join(metric['related_metrics'])}"

            documents.append(KBDocument(
                id=f"metric_{metric['name'].lower().replace(' ', '_')}",
                content=content,
                category=metric["category"],
                title=metric["name"],
                metadata={
                    "formula": metric["formula"],
                    "unit": metric["unit"],
                    "related_metrics": metric.get("related_metrics", []),
                },
            ))

        result = self.kb.add_documents(KBCollectionType.METRIC_DEFINITIONS, documents)
        return {
            "type": "metric_definitions",
            "loaded": len(documents),
            "success": result.success,
        }

    def load_fpa_concepts(self) -> Dict[str, Any]:
        """Load FP&A concepts."""
        documents = []
        for concept in FPA_CONCEPTS:
            content = f"{concept['concept']}: {concept['description']}"
            if concept.get("use_cases"):
                content += f" Use cases: {', '.join(concept['use_cases'])}"

            documents.append(KBDocument(
                id=f"concept_{concept['concept'].lower().replace(' ', '_').replace('/', '_')}",
                content=content,
                category=concept["category"],
                title=concept["concept"],
                metadata={"use_cases": concept.get("use_cases", [])},
            ))

        result = self.kb.add_documents(KBCollectionType.FPA_CONCEPTS, documents)
        return {
            "type": "fpa_concepts",
            "loaded": len(documents),
            "success": result.success,
        }

    def load_all(self) -> Dict[str, Any]:
        """Load all knowledge base content."""
        results = {
            "glossary": self.load_business_glossary(),
            "metrics": self.load_metric_definitions(),
            "concepts": self.load_fpa_concepts(),
        }

        total_loaded = sum(r["loaded"] for r in results.values())
        return {
            "success": all(r["success"] for r in results.values()),
            "total_loaded": total_loaded,
            "details": results,
        }


def get_glossary_terms() -> List[Dict[str, Any]]:
    """Get all glossary terms."""
    return BUSINESS_GLOSSARY.copy()


def get_metric_definitions() -> List[Dict[str, Any]]:
    """Get all metric definitions."""
    return METRIC_DEFINITIONS.copy()


def get_fpa_concepts() -> List[Dict[str, Any]]:
    """Get all FP&A concepts."""
    return FPA_CONCEPTS.copy()
