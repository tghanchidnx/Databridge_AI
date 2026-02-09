"""DataBridge AI Examples & Tests Package.

Provides use-case tutorials and test suites for DataBridge AI.
Requires a Pro license for full access.

Sub-modules:
    - use_cases: Step-by-step tutorials (beginner, financial, faux objects)
    - tests.ce: Test suite for Community Edition modules
    - tests.pro: Test suite for Pro modules (requires databridge-ai-pro)
"""

__version__ = "0.40.0"

USE_CASE_CATEGORIES = {
    'beginner': {
        'description': 'Simple introductory use cases (01-04)',
        'cases': [
            '01_pizza_shop_sales_check',
            '02_find_my_friends',
            '03_school_report_card_hierarchy',
            '04_sports_league_comparison',
        ],
    },
    'financial': {
        'description': 'SEC EDGAR financial analysis (05-11)',
        'cases': [
            '05_apple_money_checkup',
            '06_apple_money_tree',
            '07_apple_vs_microsoft',
            '08_apple_time_machine',
            '09_balance_sheet_detective',
            '10_full_financial_pipeline',
            '11_wall_street_analyst',
        ],
    },
    'faux_objects': {
        'description': 'Domain persona tutorials (12-19)',
        'cases': [
            '12_financial_analyst',
            '13_oil_gas_analyst',
            '14_operations_analyst',
            '15_cost_analyst',
            '16_manufacturing_analyst',
            '17_saas_analyst',
            '18_transportation_analyst',
            '19_sql_translator',
        ],
    },
}


def register_examples(mcp=None):
    """Register examples plugin with DataBridge AI.

    Called automatically via the databridge.plugins entry point
    when the package is installed.

    Args:
        mcp: Optional MCP server instance for tool registration.
    """
    from src.plugins import get_license_manager

    mgr = get_license_manager()
    if not mgr.is_pro_examples():
        return {
            'status': 'skipped',
            'reason': 'Pro license required for examples & tests',
        }

    return {
        'status': 'registered',
        'version': __version__,
        'use_case_count': sum(len(cat['cases']) for cat in USE_CASE_CATEGORIES.values()),
        'categories': list(USE_CASE_CATEGORIES.keys()),
    }
