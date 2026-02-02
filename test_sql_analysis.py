"""
Standalone SQL Hierarchy Analysis Script
Analyzes SQL CASE statements and exports to CSV files.

Features:
- Extracts CASE statements from SQL queries
- Auto-generates intelligent file names based on SQL content
- Exports hierarchy trees with parent-child relationships
- Creates source mappings for each condition
"""
import csv
import os
import re
import hashlib
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ==============================================================================
# INTELLIGENT FILE NAMING - Auto-generates names from SQL content
# ==============================================================================

def generate_export_name(sql: str, case_columns: List[str] = None) -> str:
    """
    Generate an intelligent export filename based on SQL content analysis.

    Analyzes:
    1. Key CASE output aliases (Alloc_Code, Segment, fund, state, etc.)
    2. FROM clause table names
    3. Business context keywords
    4. Query patterns (detail, summary, report, etc.)

    Args:
        sql: The SQL query string
        case_columns: Optional list of CASE statement output column names

    Returns:
        A descriptive filename like "marketing_segment_analysis" or "upstream_loe_state_analysis"
    """
    sql_lower = sql.lower()
    name_parts = []

    # Priority keywords to look for in CASE aliases and columns
    business_keywords = {
        # Segments/Divisions
        'segment': 'segment',
        'marketing': 'marketing',
        'upstream': 'upstream',
        'midstream': 'midstream',
        'services': 'services',
        'downstream': 'downstream',

        # Financial
        'fund': 'fund',
        'stake': 'stake',
        'financial': 'financial',
        'consolidated': 'consolidated',
        'gl': 'gl',

        # Operations
        'state': 'state',
        'loe': 'loe',
        'los': 'los',
        'alloc': 'alloc',
        'operated': 'op',

        # Categories
        'billcat': 'billing',
        'adjbillcat': 'adjbilling',
        'productid': 'product',
    }

    # Check CASE column aliases for context
    if case_columns:
        for col in case_columns:
            col_lower = col.lower()
            for keyword, short_name in business_keywords.items():
                if keyword in col_lower and short_name not in name_parts:
                    name_parts.append(short_name)

    # Check FROM clause for table context
    from_pattern = r'FROM\s+[\w.]+\.(\w+)'
    from_matches = re.findall(from_pattern, sql, re.IGNORECASE)
    for table in from_matches:
        table_lower = table.lower()
        if 'financial' in table_lower and 'financial' not in name_parts:
            name_parts.insert(0, 'financial')
        elif 'loe' in table_lower and 'loe' not in name_parts:
            name_parts.append('loe')
        elif 'account' in table_lower and 'account' not in name_parts:
            name_parts.append('account')

    # Check for specific business patterns in SQL
    if 'corp_code' in sql_lower:
        if 'marketing' not in name_parts and "'marketing'" in sql_lower:
            name_parts.insert(0, 'marketing')
        elif 'services' not in name_parts and "'services'" in sql_lower:
            name_parts.insert(0, 'services')
        elif 'upstream' not in name_parts and "'upstream'" in sql_lower:
            name_parts.insert(0, 'upstream')
        elif 'segment' not in name_parts:
            name_parts.append('segment')

    # Check for state inference patterns
    if 'cost_center' in sql_lower and ('tx' in sql_lower or 'la' in sql_lower or 'wy' in sql_lower):
        if 'state' not in name_parts:
            name_parts.append('state')

    # Check for LOE/operating patterns
    if 'operating' in sql_lower or 'loe' in sql_lower or 'los_map' in sql_lower:
        if 'loe' not in name_parts:
            name_parts.append('loe')

    # Check for allocation patterns
    if 'alloc' in sql_lower:
        if 'alloc' not in name_parts:
            name_parts.append('alloc')

    # Check for detail vs summary report types
    if 'detail' in sql_lower:
        name_parts.append('detail')

    # Default fallback if no keywords found
    if not name_parts:
        # Try to get something from the first alias
        alias_pattern = r'AS\s+(\w+)'
        aliases = re.findall(alias_pattern, sql, re.IGNORECASE)
        if aliases:
            first_meaningful = next((a for a in aliases if len(a) > 3 and a.lower() not in ('null', 'case')), None)
            if first_meaningful:
                name_parts.append(first_meaningful.lower())

        # Ultimate fallback
        if not name_parts:
            name_parts.append('sql_hierarchy')

    # Build final name
    name = '_'.join(name_parts[:4])  # Limit to 4 parts for readability
    name = name + '_analysis'

    # Clean up the name
    name = re.sub(r'[^a-z0-9_]', '', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')

    return name


class EntityType(str, Enum):
    ACCOUNT = "account"
    COST_CENTER = "cost_center"
    ENTITY = "entity"
    UNKNOWN = "unknown"


class ConditionOperator(str, Enum):
    EQUALS = "="
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    BETWEEN = "BETWEEN"


@dataclass
class CaseCondition:
    column: str
    operator: ConditionOperator
    values: List[str]
    raw_condition: str


@dataclass
class CaseWhen:
    condition: CaseCondition
    result_value: str
    position: int
    raw_sql: str


@dataclass
class ExtractedCase:
    id: str
    source_column: str
    input_column: str
    input_table: Optional[str]
    when_clauses: List[CaseWhen]
    else_value: Optional[str]
    entity_type: EntityType
    pattern_type: Optional[str]
    unique_results: List[str]


class SimpleCaseExtractor:
    def extract_from_sql(self, sql: str) -> List[ExtractedCase]:
        cases = []
        case_pattern = r"CASE\s+(.*?)\s+END(?:\s+AS\s+(\w+))?"

        for idx, match in enumerate(re.finditer(case_pattern, sql, re.IGNORECASE | re.DOTALL)):
            case_body = match.group(1)
            alias = match.group(2) or f"case_column_{idx}"

            case_stmt = self._parse_case_body(case_body, alias, idx, match.group(0))
            if case_stmt:
                cases.append(case_stmt)

        return cases

    def _parse_case_body(self, case_body: str, alias: str, position: int, raw_sql: str) -> Optional[ExtractedCase]:
        when_clauses = []
        else_value = None
        input_column = None

        # Extract WHEN clauses
        when_pattern = r"WHEN\s+(.*?)\s+THEN\s+['\"]?([^'\"]+?)['\"]?\s*(?=WHEN|ELSE|$)"

        for when_idx, when_match in enumerate(re.finditer(when_pattern, case_body, re.IGNORECASE | re.DOTALL)):
            condition_str = when_match.group(1).strip()
            result_value = when_match.group(2).strip().strip("'\"")

            condition = self._parse_condition(condition_str)
            if condition and not input_column:
                input_column = condition.column

            when_clauses.append(CaseWhen(
                condition=condition or CaseCondition(
                    column="unknown",
                    operator=ConditionOperator.EQUALS,
                    values=[],
                    raw_condition=condition_str,
                ),
                result_value=result_value,
                position=when_idx,
                raw_sql=when_match.group(0),
            ))

        # Extract ELSE
        else_pattern = r"ELSE\s+['\"]?([^'\"]+?)['\"]?\s*$"
        else_match = re.search(else_pattern, case_body, re.IGNORECASE)
        if else_match:
            else_value = else_match.group(1).strip().strip("'\"")

        if not when_clauses:
            return None

        case_id = hashlib.md5(raw_sql.encode()).hexdigest()[:12]
        entity_type = self._detect_entity_type(input_column)
        pattern_type = self._detect_pattern_type(when_clauses)
        unique_results = list(set(w.result_value for w in when_clauses))
        if else_value:
            unique_results.append(else_value)

        return ExtractedCase(
            id=case_id,
            source_column=alias,
            input_column=input_column or "unknown",
            input_table=None,
            when_clauses=when_clauses,
            else_value=else_value,
            entity_type=entity_type,
            pattern_type=pattern_type,
            unique_results=unique_results,
        )

    def _parse_condition(self, condition_str: str) -> Optional[CaseCondition]:
        condition_str = condition_str.strip()

        # ILIKE ANY pattern
        ilike_any_pattern = r"(\w+)\s+ILIKE\s+ANY\s*\(([^)]+)\)"
        ilike_any_match = re.match(ilike_any_pattern, condition_str, re.IGNORECASE)
        if ilike_any_match:
            values = [v.strip().strip("'\"") for v in ilike_any_match.group(2).split(",")]
            return CaseCondition(
                column=ilike_any_match.group(1),
                operator=ConditionOperator.ILIKE,
                values=values,
                raw_condition=condition_str,
            )

        # ILIKE pattern
        ilike_pattern = r"(\w+)\s+ILIKE\s+['\"]([^'\"]+)['\"]"
        ilike_match = re.match(ilike_pattern, condition_str, re.IGNORECASE)
        if ilike_match:
            return CaseCondition(
                column=ilike_match.group(1),
                operator=ConditionOperator.ILIKE,
                values=[ilike_match.group(2)],
                raw_condition=condition_str,
            )

        # IN pattern
        in_pattern = r"(\w+)\s+IN\s*\(([^)]+)\)"
        in_match = re.match(in_pattern, condition_str, re.IGNORECASE)
        if in_match:
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(",")]
            return CaseCondition(
                column=in_match.group(1),
                operator=ConditionOperator.IN,
                values=values,
                raw_condition=condition_str,
            )

        # BETWEEN pattern
        between_pattern = r"(\w+)\s+BETWEEN\s+['\"]?(\d+)['\"]?\s+AND\s+['\"]?(\d+)['\"]?"
        between_match = re.match(between_pattern, condition_str, re.IGNORECASE)
        if between_match:
            return CaseCondition(
                column=between_match.group(1),
                operator=ConditionOperator.BETWEEN,
                values=[between_match.group(2), between_match.group(3)],
                raw_condition=condition_str,
            )

        # Equals pattern
        eq_pattern = r"(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?"
        eq_match = re.match(eq_pattern, condition_str, re.IGNORECASE)
        if eq_match:
            return CaseCondition(
                column=eq_match.group(1),
                operator=ConditionOperator.EQUALS,
                values=[eq_match.group(2).strip()],
                raw_condition=condition_str,
            )

        return None

    def _detect_entity_type(self, column_name: Optional[str]) -> EntityType:
        if not column_name:
            return EntityType.UNKNOWN
        col_lower = column_name.lower()
        if "account" in col_lower or "acct" in col_lower or "code" in col_lower:
            return EntityType.ACCOUNT
        if "corp" in col_lower or "entity" in col_lower:
            return EntityType.ENTITY
        if "cost" in col_lower or "center" in col_lower:
            return EntityType.COST_CENTER
        return EntityType.UNKNOWN

    def _detect_pattern_type(self, when_clauses: List[CaseWhen]) -> Optional[str]:
        if not when_clauses:
            return None
        pattern_counts = defaultdict(int)
        for when in when_clauses:
            cond = when.condition
            if cond.operator in (ConditionOperator.LIKE, ConditionOperator.ILIKE):
                for v in cond.values:
                    if v.endswith("%") and not v.startswith("%"):
                        pattern_counts["prefix"] += 1
                    else:
                        pattern_counts["pattern"] += 1
            elif cond.operator == ConditionOperator.IN:
                pattern_counts["exact_list"] += 1
            elif cond.operator == ConditionOperator.EQUALS:
                pattern_counts["exact"] += 1
            elif cond.operator == ConditionOperator.BETWEEN:
                pattern_counts["range"] += 1
        if not pattern_counts:
            return None
        return max(pattern_counts.items(), key=lambda x: x[1])[0]


# The SQL to analyze - Marketing Segment Report with Alloc_Code
sql = """
SELECT
    account_code AS acctcode,
    accts.account_name AS acctdesc,
    DATE_FROM_PARTS(
        SUBSTR(entries.accounting_date_key, 1, 4),
        SUBSTR(entries.accounting_date_key, 5, 2),
        1
    ) AS acctdate,
    DATE_FROM_PARTS(
        SUBSTR(entries.service_date_key_m, 1, 4),
        SUBSTR(entries.service_date_key_m, 5, 2),
        1
    ) AS svcdate,
    ROUND(SUM(entries.net_volume), 2) AS Vol,
    ROUND(SUM(entries.amount_gl), 2) AS Val,
    CASE
        WHEN account_code ILIKE '51%' THEN '1 - Gas Sales'
        WHEN account_code ILIKE ANY ('65%', '66%') THEN '3 - Fees'
        ELSE '2 - COGP'
    END AS Alloc_Code
FROM
    edw.financial.fact_financial_details AS entries
    LEFT JOIN (
        SELECT
            account_hid,
            account_code,
            account_name,
            account_class_code,
            account_accrual_flag,
            CASE
                WHEN account_code ILIKE '101%' THEN 'Cash'
                WHEN account_code ILIKE '125%' THEN 'Affiliate AR'
                WHEN account_code ILIKE ANY ('11%', '12%') THEN 'AR'
                WHEN account_code ILIKE '13%' THEN 'Prepaid Expenses'
                WHEN account_code ILIKE '14%' THEN 'Inventory'
                WHEN account_code ILIKE ANY ('15%', '16%') THEN 'Other Current Assets'
                WHEN account_code ILIKE '17%' THEN 'Derivative Assets'
                WHEN account_code ILIKE '18%' THEN 'Deferred Tax Assets'
                WHEN account_code IN (
                    '205-102',
                    '205-106',
                    '205-112',
                    '205-116',
                    '205-117',
                    '205-152',
                    '205-190',
                    '205-202',
                    '205-206',
                    '205-252',
                    '205-990',
                    '210-110',
                    '210-140',
                    '210-990',
                    '215-110',
                    '215-990',
                    '220-110',
                    '220-990',
                    '225-110',
                    '225-140',
                    '225-990',
                    '230-110',
                    '230-140',
                    '230-990',
                    '232-200',
                    '232-210',
                    '235-105',
                    '235-110',
                    '235-115',
                    '235-116',
                    '235-120',
                    '235-250',
                    '235-275',
                    '240-110',
                    '240-140',
                    '240-990',
                    '242-150',
                    '242-160',
                    '244-110',
                    '244-140',
                    '244-410',
                    '244-440',
                    '244-560',
                    '244-590',
                    '244-610',
                    '244-640',
                    '244-990',
                    '244-995',
                    '244-998',
                    '245-202',
                    '245-206',
                    '245-227',
                    '245-252',
                    '245-302',
                    '245-306',
                    '245-402',
                    '245-412',
                    '245-602',
                    '245-902',
                    '245-906',
                    '246-312',
                    '246-346',
                    '246-322',
                    '246-316'
                ) THEN 'Capex'
                WHEN account_code ILIKE ANY ('25%', '26%', '27%', '28%') then 'Other Assets'
                WHEN account_code ILIKE '29%' THEN 'Accumulated DD&A'
                WHEN account_code ILIKE ANY ('30%', '31%', '32%', '33%', '34%') THEN 'AP'
                WHEN account_code ILIKE ANY ('35%', '45%') THEN 'Total Debt'
                WHEN account_code ILIKE ANY ('36%', '37%', '38%') THEN 'Other Current Liabilities'
                WHEN account_code ILIKE ANY ('44%', '46%', '47%', '48%') THEN 'Other Liabilities'
                WHEN account_code ILIKE '49%' THEN 'Equity'
                WHEN account_code ILIKE '501%' then 'Oil Sales'
                WHEN account_code ILIKE ANY ('502%', '510%') THEN 'Gas Sales'
                WHEN account_code ILIKE ANY ('503%', '512%', '513%') THEN 'NGL Sales'
                WHEN account_code ILIKE ANY (
                    '504%',
                    '520%',
                    '570%',
                    '590-100',
                    '590-110',
                    '590-410',
                    '590-510',
                    '590-900'
                ) THEN 'Other Income'
                WHEN account_code ILIKE ANY ('514%', '519%', '518-120', '590-710', '674%', '695%') THEN 'Cashouts'
                WHEN account_code IN ('515-100', '515-990') THEN 'Service Revenue'
                WHEN account_code IN (
                    '515-110',
                    '515-199',
                    '610-110',
                    '610-120',
                    '610-130'
                ) THEN 'Gathering Fees'
                WHEN account_code IN ('515-120', '613-110', '613-120') THEN 'Compression Fees'
                WHEN account_code IN (
                    '515-130',
                    '515-140',
                    '612-110',
                    '612-120',
                    '614-110',
                    '614-120',
                    '619-990'
                ) THEN 'Treating Fees'
                WHEN account_code IN ('515-210', '610-210', '610-220') THEN 'Capital Recovery Fees'
                WHEN account_code = '517-110' THEN 'Demand Fees'
                WHEN account_code ILIKE ANY (
                    '517%',
                    '611-210',
                    '611-220',
                    '613-130',
                    '613-140',
                    '619-110',
                    '619-120',
                    '619-275',
                    '619-991'
                ) THEN 'Transportation Fees'
                WHEN account_code ILIKE '518%' THEN 'Gas Sales'
                WHEN account_code = '530-100' THEN 'Service Income'
                WHEN account_code IN ('530-110', '530-111', '530-990') THEN 'Sand Sales'
                WHEN account_code IN (
                    '515-205',
                    '530-120',
                    '530-140',
                    '530-720',
                    '530-990',
                    '530-991',
                    '530-993',
                    '590-310'
                ) THEN 'Rental Income'
                WHEN account_code IN ('530-150', '530-994', '590-620') THEN 'Water Income'
                WHEN account_code IN ('530-160', '530-995', '590-610') THEN 'SWD Income'
                WHEN account_code IN ('530-170', '530-996') THEN 'Consulting Income'
                WHEN account_code IN ('530-180', '530-997') THEN 'Fuel Income'
                WHEN account_code ILIKE '580%' then 'Hedge Gains'
                WHEN account_code = '581-540' THEN 'Interest Hedge Gains'
                WHEN account_code ILIKE '581%' then 'Unrealized Hedge Gains'
                WHEN account_code IN ('590-210', '590-211', '590-990', '640-992') THEN 'COPAS'
                WHEN account_code = '590-555' THEN 'Compressor Recovery Income'
                WHEN account_code = '590-850' THEN 'Rig Termination Penalties'
                WHEN account_code = '590-991' THEN 'Gathering Fee Income'
                WHEN account_code IN (
                    '601-100',
                    '601-110',
                    '601-113',
                    '601-120',
                    '601-123',
                    '601-275',
                    '601-990'
                ) THEN 'Oil Severance Taxes'
                WHEN account_code IN (
                    '602-100',
                    '602-110',
                    '602-113',
                    '602-120',
                    '602-123',
                    '602-275',
                    '602-990'
                ) THEN 'Gas Severance Taxes'
                WHEN account_code IN (
                    '603-100',
                    '603-110',
                    '603-113',
                    '603-120',
                    '603-123',
                    '603-275',
                    '603-990'
                ) THEN 'NGL Severance Taxes'
                WHEN account_code IN (
                    '601-112',
                    '601-122',
                    '602-112',
                    '602-122',
                    '603-112',
                    '603-122',
                    '640-120',
                    '640-991'
                ) THEN 'Ad Valorem Taxes'
                WHEN account_code IN ('601-111', '601-121') THEN 'Oil Conservation Taxes'
                WHEN account_code IN ('602-111', '602-121') THEN 'Gas Conservation Taxes'
                WHEN account_code IN ('603-111', '603-121') THEN 'NGL Conservation Taxes'
                WHEN account_code = '611-110' THEN 'Commodity Fees'
                WHEN account_code ILIKE ANY ('611-120', '62%') THEN 'Non-Op Fees'
                WHEN account_code ILIKE ANY ('630-1%', '710-170', '710-996') THEN 'Consulting Expenses'
                WHEN account_code IN (
                    '640-110',
                    '640-100',
                    '640-275',
                    '640-300',
                    '640-990',
                    '641-110',
                    '641-100',
                    '641-990'
                ) THEN 'Lease Operating Expenses'
                WHEN account_code IN ('641-150', '963-100') THEN 'Accretion Expense'
                WHEN account_code ILIKE '642%' THEN 'Leasehold Expenses'
                WHEN account_code ILIKE '645%' THEN 'Exploration Expenses'
                WHEN account_code ILIKE ANY ('650%', '660%', '667%', '668%') THEN 'Third Party Fees Paid'
                WHEN account_code ILIKE ANY ('665%', '666%', '669%') THEN 'Midstream Operating Expenses'
                WHEN account_code ILIKE ANY ('670%', '680%', '690%', '691%') THEN 'Cost of Purchased Gas'
                WHEN account_code IN (
                    '700-100',
                    '700-110',
                    '700-800',
                    '700-990',
                    '701-100',
                    '701-110'
                ) THEN 'Sand Purchases'
                WHEN account_code IN ('700-150', '700-994') THEN 'Water Purchases'
                WHEN account_code IN ('700-180', '700-997') THEN 'Fuel Purchases'
                WHEN account_code IN (
                    '710-100',
                    '710-120',
                    '710-140',
                    '710-300',
                    '710-301',
                    '710-991',
                    '710-992',
                    '710-993',
                    '720-120',
                    '720-985'
                ) THEN 'Rental Expenses'
                WHEN account_code IN ('710-110', '710-990') THEN 'Sand Expenses'
                WHEN account_code IN ('710-150', '710-994') THEN 'Water Expenses'
                WHEN account_code IN ('710-160', '710-995') THEN 'SWD Expenses'
                WHEN account_code IN ('710-180', '710-997') THEN 'Fuel Expenses'
                WHEN account_code ILIKE '8%' THEN 'General & Administrative'
                WHEN account_code IN ('910-100', '920-100') THEN 'Interest Income'
                WHEN account_code ILIKE ANY ('93%', '94%') THEN 'Other Gains/Losses'
                WHEN account_code ILIKE ANY ('950-1%', '950-300') THEN 'Interest Expense'
                WHEN account_code ILIKE ANY ('950-8%', '955%', '970%', '972%', '974%', '975%') THEN 'DD&A'
                WHEN account_code = '971-210' THEN 'Impairment Expense'
                WHEN account_code = '960-100' THEN 'Bad Debt Expense'
                WHEN account_code = '965-200' THEN 'Other Expenses'
            END AS gl
        FROM
            edw.financial.dim_account
        WHERE
            account_code <> '700-800'
        GROUP BY
            account_hid,
            account_code,
            account_name,
            account_class_code,
            account_accrual_flag
    ) AS accts ON accts.account_hid = entries.account_hid
    LEFT JOIN (
        SELECT
            Corp_HID,
            Corp_code,
            corp_name,
            CASE
                WHEN corp_code IN (551, 561, 565, 578, 586, 587, 588, 598, 702, 755) THEN 'A3'
                WHEN corp_code IN (
                    410,
                    420,
                    550,
                    560,
                    580,
                    585,
                    586,
                    590,
                    595,
                    599,
                    600,
                    650,
                    700,
                    701,
                    750,
                    751
                ) THEN 'AU'
                WHEN corp_code IN (012, 043, 049, 052) THEN '4HC'
                ELSE NULL
            END AS fund,
            CASE
                WHEN corp_code IN (12, 43, 44, 540, 550, 551, 560, 561, 565, 650, 750, 751, 755) THEN 'Upstream'
                WHEN corp_code IN (41, 410, 578, 580, 585, 586, 587, 588, 590, 595) THEN 'Midstream'
                WHEN corp_code = 600 THEN 'Marketing'
                WHEN corp_code BETWEEN 700
                AND 702 THEN 'Services'
                WHEN corp_code BETWEEN 597
                AND 599 THEN 'Elim'
                ELSE NULL
            END AS Segment,
            CASE
                WHEN corp_code IN (
                    550,
                    560,
                    580,
                    585,
                    590,
                    595,
                    599,
                    600,
                    650,
                    700,
                    701,
                    750,
                    751
                ) THEN 1
                WHEN corp_code IN (410, 420) THEN 0.9
                WHEN corp_code = 586 THEN 0.225
                ELSE 0
            END AS AU_Stake,
            CASE
                WHEN corp_code IN (551, 561, 565, 587, 598, 702, 755) THEN 1
                WHEN corp_code IN (578, 587) THEN 0.9
                WHEN corp_code = 586 THEN 0.675
                ELSE 0
            END AS A3_Stake
        FROM
            edw.financial.dim_corp
        WHERE
            corp_hid > 1
    ) AS corps ON corps.corp_hid = entries.corp_hid
    LEFT JOIN edw.financial.dim_counter_party as vendors ON vendors.counter_party_hid = entries.counter_party_hid
WHERE
    (
        accts.account_class_code BETWEEN 4
        AND 6
        OR accts.gl ILIKE 'Capex'
    )
    AND entries.transaction_description NOT ILIKE '%Generated%'
    AND Corps.corp_code = 600
    AND accts.account_code ILIKE ANY ('51%', '6%')
    AND acctdate > '2021-12-31'
GROUP BY
    acctdate,
    svcdate,
    acctcode,
    acctdesc,
    batch_number
HAVING
    SUM(entries.amount_gl) <> 0
ORDER BY
    acctcode,
    svcdate DESC;
"""


def run_analysis(custom_sql: str = None, custom_name: str = None):
    """
    Run the SQL analysis and export results.

    Args:
        custom_sql: Optional SQL string to analyze (defaults to embedded SQL)
        custom_name: Optional custom export name (auto-generates if not provided)
    """
    # Use provided SQL or default embedded SQL
    sql_to_analyze = custom_sql if custom_sql else sql

    extractor = SimpleCaseExtractor()
    cases = extractor.extract_from_sql(sql_to_analyze)

    # Get CASE column names for intelligent naming
    case_columns = [case.source_column for case in cases]

    # Auto-generate intelligent filename based on SQL content
    if custom_name:
        export_name = custom_name
    else:
        export_name = generate_export_name(sql_to_analyze, case_columns)

    print("=" * 70)
    print(f"SQL HIERARCHY ANALYSIS")
    print(f"Export Name: {export_name}")
    print("=" * 70)

    print(f"\nFound {len(cases)} CASE statements\n")

    if not cases:
        print("No CASE statements found!")
        return {
            "export_name": export_name,
            "export_path": None,
            "files": {},
            "statistics": {
                "total_hierarchies": 0,
                "total_nodes": 0,
                "total_mappings": 0,
            },
            "case_columns": [],
        }

    export_path = "./result_export"
    Path(export_path).mkdir(parents=True, exist_ok=True)

    all_hierarchy_rows = []
    all_mapping_rows = []
    summary_rows = []

    for idx, case in enumerate(cases):
        print(f"--- CASE {idx + 1}: {case.source_column} ---")
        print(f"    Input Column: {case.input_column}")
        print(f"    Entity Type: {case.entity_type.value}")
        print(f"    Pattern Type: {case.pattern_type}")
        print(f"    Conditions: {len(case.when_clauses)}")
        print(f"    Unique Results: {len(case.unique_results)}")
        print(f"    Has ELSE: {case.else_value is not None}")
        print()

        hier_name = case.source_column
        sort_idx = 0

        # Parent node
        parent_id = f"{hier_name.upper().replace(' ', '_')}"
        all_hierarchy_rows.append({
            "HIERARCHY_ID": parent_id,
            "HIERARCHY_NAME": hier_name,
            "PARENT_ID": "",
            "DESCRIPTION": f"Auto-extracted from SQL on {case.input_column}",
            "LEVEL_1": hier_name,
            "LEVEL_2": "",
            "SOURCE_COLUMN": case.input_column,
            "IS_LEAF_NODE": "false",
            "SORT_ORDER": sort_idx,
        })
        sort_idx += 1

        # Group by result
        result_groups = defaultdict(list)
        for when in case.when_clauses:
            result_groups[when.result_value].append(when)

        # Child nodes
        for result_value, when_clauses in result_groups.items():
            child_id = f"{parent_id}_{result_value.upper().replace(' ', '_').replace('/', '_').replace('&', '').replace('-', '_')[:25]}"

            all_hierarchy_rows.append({
                "HIERARCHY_ID": child_id,
                "HIERARCHY_NAME": result_value,
                "PARENT_ID": parent_id,
                "DESCRIPTION": f"Maps {case.input_column} to '{result_value}'",
                "LEVEL_1": hier_name,
                "LEVEL_2": result_value,
                "SOURCE_COLUMN": case.input_column,
                "IS_LEAF_NODE": "true",
                "SORT_ORDER": sort_idx,
            })
            sort_idx += 1

            # Mappings
            for when in when_clauses:
                cond = when.condition
                for value in cond.values:
                    all_mapping_rows.append({
                        "HIERARCHY_ID": child_id,
                        "HIERARCHY_NAME": result_value,
                        "PARENT_HIERARCHY": hier_name,
                        "SOURCE_COLUMN": case.input_column,
                        "CONDITION_TYPE": cond.operator.value,
                        "CONDITION_VALUE": value,
                        "MAPPED_VALUE": result_value,
                    })

        # ELSE
        if case.else_value:
            else_id = f"{parent_id}_ELSE"
            all_hierarchy_rows.append({
                "HIERARCHY_ID": else_id,
                "HIERARCHY_NAME": str(case.else_value),
                "PARENT_ID": parent_id,
                "DESCRIPTION": "ELSE - default when no conditions match",
                "LEVEL_1": hier_name,
                "LEVEL_2": str(case.else_value),
                "SOURCE_COLUMN": case.input_column,
                "IS_LEAF_NODE": "true",
                "SORT_ORDER": sort_idx,
            })
            all_mapping_rows.append({
                "HIERARCHY_ID": else_id,
                "HIERARCHY_NAME": str(case.else_value),
                "PARENT_HIERARCHY": hier_name,
                "SOURCE_COLUMN": case.input_column,
                "CONDITION_TYPE": "ELSE",
                "CONDITION_VALUE": "*",
                "MAPPED_VALUE": str(case.else_value),
            })

        # Summary
        confidence = 0.5
        if len(case.when_clauses) >= 10:
            confidence += 0.2
        if case.entity_type != EntityType.UNKNOWN:
            confidence += 0.15
        if case.pattern_type:
            confidence += 0.1

        summary_rows.append({
            "HIERARCHY_NAME": hier_name,
            "SOURCE_COLUMN": case.input_column,
            "ENTITY_TYPE": case.entity_type.value,
            "PATTERN_TYPE": case.pattern_type or "mixed",
            "TOTAL_CONDITIONS": len(case.when_clauses),
            "UNIQUE_VALUES": len(case.unique_results),
            "HAS_ELSE": "Yes" if case.else_value else "No",
            "CONFIDENCE": f"{confidence:.0%}",
        })

    # Export CSVs
    print("=" * 70)
    print("EXPORTING CSV FILES")
    print("=" * 70)

    # Summary
    summary_file = os.path.join(export_path, f"{export_name}_SUMMARY.csv")
    with open(summary_file, "w", newline="", encoding="utf-8") as f:
        if summary_rows:
            writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
            writer.writeheader()
            writer.writerows(summary_rows)
    print(f"\n1. {summary_file}")
    print(f"   -> {len(summary_rows)} hierarchies summarized")

    # Hierarchy
    hierarchy_file = os.path.join(export_path, f"{export_name}_HIERARCHY.csv")
    with open(hierarchy_file, "w", newline="", encoding="utf-8") as f:
        if all_hierarchy_rows:
            writer = csv.DictWriter(f, fieldnames=all_hierarchy_rows[0].keys())
            writer.writeheader()
            writer.writerows(all_hierarchy_rows)
    print(f"\n2. {hierarchy_file}")
    print(f"   -> {len(all_hierarchy_rows)} hierarchy nodes")

    # Mapping
    mapping_file = os.path.join(export_path, f"{export_name}_MAPPING.csv")
    with open(mapping_file, "w", newline="", encoding="utf-8") as f:
        if all_mapping_rows:
            writer = csv.DictWriter(f, fieldnames=all_mapping_rows[0].keys())
            writer.writeheader()
            writer.writerows(all_mapping_rows)
    print(f"\n3. {mapping_file}")
    print(f"   -> {len(all_mapping_rows)} source mappings")

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print(f"\nTotal Hierarchies: {len(cases)}")
    print(f"Total Nodes: {len(all_hierarchy_rows)}")
    print(f"Total Mappings: {len(all_mapping_rows)}")

    # Return statistics for programmatic use
    return {
        "export_name": export_name,
        "export_path": export_path,
        "files": {
            "summary": summary_file,
            "hierarchy": hierarchy_file,
            "mapping": mapping_file,
        },
        "statistics": {
            "total_hierarchies": len(cases),
            "total_nodes": len(all_hierarchy_rows),
            "total_mappings": len(all_mapping_rows),
        },
        "case_columns": case_columns,
    }


def analyze_sql(sql_query: str, name: str = None) -> dict:
    """
    Analyze a SQL query and export hierarchies to CSV files.

    This is the main entry point for SQL hierarchy analysis.
    It automatically generates intelligent file names based on SQL content.

    Args:
        sql_query: The SQL query string containing CASE statements
        name: Optional custom export name (auto-generated if not provided)

    Returns:
        dict with export statistics and file paths

    Example:
        >>> result = analyze_sql('''
        ...     SELECT CASE WHEN corp_code = 600 THEN 'Marketing'
        ...            ELSE 'Other' END AS Segment
        ...     FROM financial.dim_corp
        ... ''')
        >>> print(result['export_name'])  # 'marketing_segment_analysis'
    """
    return run_analysis(custom_sql=sql_query, custom_name=name)


if __name__ == "__main__":
    # Run analysis on the embedded SQL
    # The filename will be auto-generated based on SQL content
    run_analysis()

    # Example of how to use with custom SQL:
    # analyze_sql("SELECT CASE WHEN x THEN 'A' ELSE 'B' END AS category FROM table1")
