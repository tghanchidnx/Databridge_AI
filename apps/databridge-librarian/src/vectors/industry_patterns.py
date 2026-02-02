"""
Industry Patterns for DataBridge AI Librarian.

Pre-built patterns for common industries including hierarchy structures,
typical metrics, and best practices.
"""

from typing import List, Dict, Any

# Industry pattern definitions
INDUSTRY_PATTERNS: List[Dict[str, Any]] = [
    # Oil & Gas Industry
    {
        "id": "oil_gas_upstream_pl",
        "industry": "oil_gas",
        "name": "Upstream Oil & Gas P&L",
        "description": "Income statement structure for exploration and production companies with focus on lease operating statements and field-level economics.",
        "hierarchy_type": "profit_and_loss",
        "typical_levels": [
            "Total Revenue",
            "Production Revenue",
            "Oil Revenue",
            "Gas Revenue",
            "NGL Revenue",
            "Lease Operating Expenses",
            "LOE - Labor",
            "LOE - Repairs & Maintenance",
            "LOE - Chemicals",
            "LOE - Utilities",
            "Production Taxes",
            "Severance Tax",
            "Ad Valorem Tax",
            "DD&A",
            "G&A Expense",
            "Operating Income",
        ],
        "key_metrics": [
            "LOE per BOE",
            "Finding & Development Cost",
            "Reserve Replacement Ratio",
            "Production per Well",
            "Netback per BOE",
        ],
        "formulas": [
            {"name": "LOE Total", "type": "SUM", "children": ["LOE - Labor", "LOE - Repairs & Maintenance", "LOE - Chemicals", "LOE - Utilities"]},
            {"name": "Total Revenue", "type": "SUM", "children": ["Oil Revenue", "Gas Revenue", "NGL Revenue"]},
        ],
    },
    {
        "id": "oil_gas_asset_hierarchy",
        "industry": "oil_gas",
        "name": "Upstream Asset Hierarchy",
        "description": "Geographic and operational hierarchy for upstream oil and gas assets from basin to well level.",
        "hierarchy_type": "operational",
        "typical_levels": [
            "Company",
            "Region",
            "Basin",
            "Area",
            "Field",
            "Lease",
            "Well",
        ],
        "key_metrics": [
            "Production BOE",
            "Working Interest %",
            "Net Revenue Interest %",
            "Reserve Estimates",
        ],
        "dimension_attributes": [
            "Well Type",
            "Well Status",
            "Operator",
            "Formation",
            "Vintage Year",
        ],
    },

    # Manufacturing Industry
    {
        "id": "manufacturing_pl",
        "industry": "manufacturing",
        "name": "Manufacturing P&L",
        "description": "Income statement for manufacturing companies with detailed COGS breakdown and variance analysis.",
        "hierarchy_type": "profit_and_loss",
        "typical_levels": [
            "Net Sales",
            "Product Sales",
            "Service Revenue",
            "Cost of Goods Sold",
            "Direct Materials",
            "Direct Labor",
            "Manufacturing Overhead",
            "Variable Overhead",
            "Fixed Overhead",
            "Gross Profit",
            "Operating Expenses",
            "Selling Expense",
            "G&A Expense",
            "R&D Expense",
            "Operating Income",
        ],
        "key_metrics": [
            "Gross Margin %",
            "COGS per Unit",
            "Labor Efficiency",
            "Overhead Absorption Rate",
            "Yield Rate",
            "Scrap Rate",
        ],
        "formulas": [
            {"name": "COGS", "type": "SUM", "children": ["Direct Materials", "Direct Labor", "Manufacturing Overhead"]},
            {"name": "Gross Profit", "type": "SUBTRACT", "operands": ["Net Sales", "COGS"]},
        ],
    },
    {
        "id": "manufacturing_plant_hierarchy",
        "industry": "manufacturing",
        "name": "Manufacturing Plant Hierarchy",
        "description": "Operational hierarchy for manufacturing facilities from region to work center.",
        "hierarchy_type": "operational",
        "typical_levels": [
            "Enterprise",
            "Region",
            "Plant",
            "Production Line",
            "Work Center",
            "Machine",
        ],
        "key_metrics": [
            "OEE (Overall Equipment Effectiveness)",
            "Throughput",
            "Cycle Time",
            "Downtime %",
            "Quality Rate",
        ],
        "dimension_attributes": [
            "Plant Type",
            "Product Family",
            "Shift",
            "Cost Center",
        ],
    },

    # Healthcare Industry
    {
        "id": "healthcare_pl",
        "industry": "healthcare",
        "name": "Healthcare Provider P&L",
        "description": "Income statement for healthcare providers with service line and payer mix detail.",
        "hierarchy_type": "profit_and_loss",
        "typical_levels": [
            "Gross Patient Revenue",
            "Inpatient Revenue",
            "Outpatient Revenue",
            "Emergency Revenue",
            "Ancillary Services",
            "Contractual Adjustments",
            "Net Patient Revenue",
            "Other Operating Revenue",
            "Total Operating Revenue",
            "Operating Expenses",
            "Salaries & Wages",
            "Benefits",
            "Supplies",
            "Purchased Services",
            "Depreciation",
            "Operating Income",
        ],
        "key_metrics": [
            "Net Revenue per Discharge",
            "Cost per Adjusted Discharge",
            "Case Mix Index",
            "Length of Stay",
            "Payer Mix",
            "Bad Debt %",
        ],
        "formulas": [
            {"name": "Net Patient Revenue", "type": "SUBTRACT", "operands": ["Gross Patient Revenue", "Contractual Adjustments"]},
        ],
    },
    {
        "id": "healthcare_service_hierarchy",
        "industry": "healthcare",
        "name": "Healthcare Service Line Hierarchy",
        "description": "Service line and departmental hierarchy for healthcare organizations.",
        "hierarchy_type": "operational",
        "typical_levels": [
            "Health System",
            "Facility",
            "Service Line",
            "Department",
            "Cost Center",
        ],
        "key_metrics": [
            "Patient Volume",
            "Revenue per FTE",
            "Cost per Procedure",
            "Utilization Rate",
        ],
        "dimension_attributes": [
            "Facility Type",
            "Service Type",
            "Payer",
            "Provider",
        ],
    },

    # Private Equity Industry
    {
        "id": "private_equity_pl",
        "industry": "private_equity",
        "name": "Private Equity Portfolio P&L",
        "description": "Consolidated income statement view for private equity portfolio companies.",
        "hierarchy_type": "profit_and_loss",
        "typical_levels": [
            "Total Revenue",
            "Portfolio Company Revenue",
            "Management Fees",
            "Carried Interest",
            "Total Expenses",
            "Portfolio Company OpEx",
            "Fund Operating Expenses",
            "Investment Professional Comp",
            "EBITDA",
            "Depreciation & Amortization",
            "Interest Expense",
            "Net Income",
        ],
        "key_metrics": [
            "MOIC (Multiple on Invested Capital)",
            "IRR",
            "DPI (Distributions to Paid-In)",
            "TVPI (Total Value to Paid-In)",
            "Revenue CAGR",
            "EBITDA Margin",
        ],
        "formulas": [
            {"name": "EBITDA", "type": "SUBTRACT", "operands": ["Total Revenue", "Total Expenses"]},
        ],
    },
    {
        "id": "private_equity_portfolio_hierarchy",
        "industry": "private_equity",
        "name": "PE Portfolio Hierarchy",
        "description": "Investment hierarchy for private equity fund and portfolio company management.",
        "hierarchy_type": "operational",
        "typical_levels": [
            "Fund Family",
            "Fund",
            "Vintage Year",
            "Portfolio Company",
            "Business Unit",
        ],
        "key_metrics": [
            "Investment Amount",
            "Current Valuation",
            "Ownership %",
            "Board Seats",
        ],
        "dimension_attributes": [
            "Industry Sector",
            "Investment Stage",
            "Geography",
            "Deal Type",
        ],
    },

    # Retail Industry
    {
        "id": "retail_pl",
        "industry": "retail",
        "name": "Retail P&L",
        "description": "Income statement for retail companies with store-level and category detail.",
        "hierarchy_type": "profit_and_loss",
        "typical_levels": [
            "Net Sales",
            "In-Store Sales",
            "E-Commerce Sales",
            "Cost of Goods Sold",
            "Product Cost",
            "Freight In",
            "Inventory Shrinkage",
            "Gross Margin",
            "Store Operating Expenses",
            "Labor",
            "Occupancy",
            "Marketing",
            "Corporate Overhead",
            "Operating Income",
        ],
        "key_metrics": [
            "Comp Store Sales",
            "Sales per Square Foot",
            "Inventory Turnover",
            "Gross Margin %",
            "Labor % of Sales",
            "Four-Wall EBITDA",
        ],
        "formulas": [
            {"name": "Gross Margin", "type": "SUBTRACT", "operands": ["Net Sales", "COGS"]},
            {"name": "COGS", "type": "SUM", "children": ["Product Cost", "Freight In", "Inventory Shrinkage"]},
        ],
    },
    {
        "id": "retail_store_hierarchy",
        "industry": "retail",
        "name": "Retail Store Hierarchy",
        "description": "Geographic and format hierarchy for retail store operations.",
        "hierarchy_type": "operational",
        "typical_levels": [
            "Company",
            "Division",
            "Region",
            "District",
            "Store",
            "Department",
        ],
        "key_metrics": [
            "Traffic",
            "Conversion Rate",
            "Average Transaction",
            "Units per Transaction",
        ],
        "dimension_attributes": [
            "Store Format",
            "Store Size",
            "Urban/Suburban",
            "Vintage",
            "Franchise/Corporate",
        ],
    },

    # Construction Industry
    {
        "id": "construction_pl",
        "industry": "construction",
        "name": "Construction P&L",
        "description": "Income statement for construction companies with job cost accounting structure.",
        "hierarchy_type": "profit_and_loss",
        "typical_levels": [
            "Contract Revenue",
            "Earned Revenue",
            "Change Orders",
            "Cost of Revenue",
            "Direct Labor",
            "Subcontractor Costs",
            "Materials",
            "Equipment Costs",
            "Job Overhead",
            "Gross Profit",
            "G&A Expense",
            "Operating Income",
        ],
        "key_metrics": [
            "Gross Margin %",
            "Backlog",
            "Book-to-Bill Ratio",
            "Days in WIP",
            "Overbilling/Underbilling",
            "Job Margin",
        ],
        "formulas": [
            {"name": "Cost of Revenue", "type": "SUM", "children": ["Direct Labor", "Subcontractor Costs", "Materials", "Equipment Costs", "Job Overhead"]},
        ],
    },
    {
        "id": "construction_project_hierarchy",
        "industry": "construction",
        "name": "Construction Project Hierarchy",
        "description": "Project-based hierarchy for construction job cost tracking.",
        "hierarchy_type": "operational",
        "typical_levels": [
            "Company",
            "Division",
            "Project Type",
            "Project",
            "Phase",
            "Cost Code",
        ],
        "key_metrics": [
            "Contract Value",
            "Cost to Date",
            "Estimated Cost at Completion",
            "% Complete",
            "Earned Value",
        ],
        "dimension_attributes": [
            "Project Type",
            "Customer",
            "Region",
            "Contract Type",
            "Project Manager",
        ],
    },
]

# Group patterns by industry
PATTERNS_BY_INDUSTRY: Dict[str, List[Dict[str, Any]]] = {}
for pattern in INDUSTRY_PATTERNS:
    industry = pattern["industry"]
    if industry not in PATTERNS_BY_INDUSTRY:
        PATTERNS_BY_INDUSTRY[industry] = []
    PATTERNS_BY_INDUSTRY[industry].append(pattern)

# Industry metadata
INDUSTRIES: Dict[str, Dict[str, Any]] = {
    "oil_gas": {
        "name": "Oil & Gas",
        "description": "Upstream, midstream, and oilfield services companies",
        "sub_industries": ["upstream", "midstream", "oilfield_services"],
        "pattern_count": len(PATTERNS_BY_INDUSTRY.get("oil_gas", [])),
    },
    "manufacturing": {
        "name": "Manufacturing",
        "description": "Discrete and process manufacturing companies",
        "sub_industries": ["discrete", "process", "aerospace", "automotive"],
        "pattern_count": len(PATTERNS_BY_INDUSTRY.get("manufacturing", [])),
    },
    "healthcare": {
        "name": "Healthcare",
        "description": "Healthcare providers, hospitals, and health systems",
        "sub_industries": ["hospitals", "clinics", "diagnostic", "pharma"],
        "pattern_count": len(PATTERNS_BY_INDUSTRY.get("healthcare", [])),
    },
    "private_equity": {
        "name": "Private Equity",
        "description": "Private equity funds and portfolio company management",
        "sub_industries": ["buyout", "growth", "venture", "distressed"],
        "pattern_count": len(PATTERNS_BY_INDUSTRY.get("private_equity", [])),
    },
    "retail": {
        "name": "Retail",
        "description": "Retail companies including brick-and-mortar and e-commerce",
        "sub_industries": ["specialty", "department", "grocery", "ecommerce"],
        "pattern_count": len(PATTERNS_BY_INDUSTRY.get("retail", [])),
    },
    "construction": {
        "name": "Construction",
        "description": "General contractors and construction management",
        "sub_industries": ["commercial", "residential", "infrastructure", "specialty"],
        "pattern_count": len(PATTERNS_BY_INDUSTRY.get("construction", [])),
    },
}


def get_all_patterns() -> List[Dict[str, Any]]:
    """Get all industry patterns."""
    return INDUSTRY_PATTERNS.copy()


def get_patterns_by_industry(industry: str) -> List[Dict[str, Any]]:
    """Get patterns for a specific industry."""
    return PATTERNS_BY_INDUSTRY.get(industry, [])


def get_pattern_by_id(pattern_id: str) -> Dict[str, Any]:
    """Get a specific pattern by ID."""
    for pattern in INDUSTRY_PATTERNS:
        if pattern["id"] == pattern_id:
            return pattern
    return {}


def get_industries() -> Dict[str, Dict[str, Any]]:
    """Get all industries with metadata."""
    return INDUSTRIES.copy()


def get_industry_info(industry: str) -> Dict[str, Any]:
    """Get information about a specific industry."""
    return INDUSTRIES.get(industry, {})


class IndustryPatternLoader:
    """
    Loader for industry patterns into the vector store.
    """

    def __init__(self, rag_pipeline):
        """
        Initialize the loader.

        Args:
            rag_pipeline: HierarchyRAG instance for indexing.
        """
        self.rag = rag_pipeline

    def load_all_patterns(self) -> Dict[str, Any]:
        """
        Load all industry patterns into the vector store.

        Returns:
            Summary of loaded patterns.
        """
        results = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "by_industry": {},
        }

        for pattern in INDUSTRY_PATTERNS:
            result = self.rag.index_industry_pattern(pattern)

            results["total"] += 1
            industry = pattern["industry"]

            if industry not in results["by_industry"]:
                results["by_industry"][industry] = {"success": 0, "failed": 0}

            if result.success:
                results["success"] += 1
                results["by_industry"][industry]["success"] += 1
            else:
                results["failed"] += 1
                results["by_industry"][industry]["failed"] += 1

        return results

    def load_patterns_by_industry(self, industry: str) -> Dict[str, Any]:
        """
        Load patterns for a specific industry.

        Args:
            industry: Industry identifier.

        Returns:
            Summary of loaded patterns.
        """
        patterns = get_patterns_by_industry(industry)
        results = {
            "industry": industry,
            "total": len(patterns),
            "success": 0,
            "failed": 0,
        }

        for pattern in patterns:
            result = self.rag.index_industry_pattern(pattern)
            if result.success:
                results["success"] += 1
            else:
                results["failed"] += 1

        return results
