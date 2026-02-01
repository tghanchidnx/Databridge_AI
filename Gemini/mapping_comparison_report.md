# Hierarchy Mapping Comparison Report

This report compares the hierarchy mappings defined in the `FP&A Queries.sql` file against the mappings provided in the `tbl_0_net_los_report_hierarchy_MAPPING.csv` and `DIM_ACCOUNT.csv` files.

## Summary of Findings

The analysis revealed several discrepancies between the two sources. The primary source of truth is considered to be the SQL query's `CASE` statements. The `tbl_0_net_los_report_hierarchy_MAPPING.csv` file represents the new, proposed hierarchy structure.

---

## Discrepancies

The following items have different hierarchy mappings between the SQL query and the uploaded CSV file.

| ID Type | ID | Description | Hierarchy from CSV | Hierarchy from SQL |
| :--- | :--- | :--- | :--- | :--- |
| ACCOUNT_CODE | 513-990 | OIL & GAS SALES ACCRUAL | Volumes > Oil (BBL) | Revenue |
| MINOR_CODE | 4-10-150-513 | MS-OIL SALES REVENUE | Volumes > Oil (BBL) | Revenue |
| MINOR_CODE | 4-10-100-501 | OIL SALES REVENUE | Volumes > Oil (BBL) | Revenue |
| MINOR_CODE | 4-10-100-502 | GAS SALES REVENUE | Volumes > Gas (MCF) | Revenue |
| MINOR_CODE | 4-10-100-503 | NGL SALES REVENUE | Volumes > NGL (GAL) | Revenue |
| MINOR_CODE | 4-10-150-512 | MS-NGL SALES REVENUE | Volumes > NGL (GAL) | Revenue |
| BILLING_CATEGORY_CODE | MOE750 | Ad Valorem Taxes | Operating Expense > Recurring > Ad Valorem Taxes | Deducts |
| BILLING_CATEGORY_CODE | NLOE750 | NLOE-Non-Op Ad Valorem Taxess | Operating Expense > Recurring > Ad Valorem Taxes | Deducts |
| BILLING_CATEGORY_CODE | MOE700 | Gas Mktg and Transport Fee | Operating Expense > Recurring > Gas Marketing | Deducts |
| BILLING_CATEGORY_CODE | NLOE852 | NLOE-Non-Operated COPAS Overhead | Operating Expense > Recurring > COPAS | LOE |
| BILLING_CATEGORY_CODE | LOE600 | LOE-Operated COPAS Overhead | Operating Expense > Recurring > Operated | LOE |

---

## Items Missing from SQL Logic

The following items were found in the uploaded CSV mapping but are not explicitly handled in the `CASE` statements of the SQL query.

- **ID:** `4-10-150-510`, **Type:** `MINOR_CODE`, **Hierarchy:** `Volumes > Gas (MCF)`
- **ID:** `GAS`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Ad Valorem > Gas Ad Val`
- **ID:** `NGL`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Ad Valorem > NGL Ad Val`
- **ID:** `OIL`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Ad Valorem > Oil Ad Val`
- **ID:** `GAS`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Severance Tax > Gas Sev Tax`
- **ID:** `NGL`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Severance Tax > NGL Sev Tax`
- **ID:** `OIL`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Severance Tax > Oil Sev Tax`
- **ID:** `GAS`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Conservation Taxes > Gas Conservation Tax`
- **ID:** `NGL`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Conservation Taxes > NGL Conservation Tax`
- **ID:** `OIL`, **Type:** `PRODUCT_CODE`, **Hierarchy:** `Taxes > Conservation Taxes > Oil Conservation Tax`
- **ID:** `MOX`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Operating Expense > NonRecurring > MOX`
- **ID:** `WOX`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Operating Expense > NonRecurring > WOX Expense`
- **ID:** `PAC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Operating Expense > NonRecurring > PAC Expense`
- **ID:** `NIDC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > Intangible Drilling Cost`
- **ID:** `TDC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > Tangible Drilling Cost`
- **ID:** `NICC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > Intangible Completion Cost`
- **ID:** `TCC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > Tangible Completion Cost`
- **ID:** `IFC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > Facility Cost`
- **ID:** `TFC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > Facility Cost`
- **ID:** `MFC`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Capital Spend > Midstream CAPEX > Midstream facility costs`
- **ID:** `LHX`, **Type:** `BILLING_CATEGORY_TYPE_CODE`, **Hierarchy:** `Operating Expense > Lease Expense > Operated`
- **ID:** `MOP260`, **Type:** `BILLING_CATEGRY_CODE`, **Hierarchy:** `Capital Spend > Upstream CAPEX > MOP`

---

## Items Missing from CSV Mapping

The following items were found in the SQL `CASE` statements but were not present in the uploaded CSV mapping files.

- **ID:** `5-10-100-501` (Revenue), **Type:** `MINOR_CODE`
- **ID:** `5-10-100-502` (Revenue), **Type:** `MINOR_CODE`
- **ID:** `5-10-100-503` (Revenue), **Type:** `MINOR_CODE`
- **ID:** `5-10-150-512` (Revenue), **Type:** `MINOR_CODE`
- **ID:** `5-10-150-513` (Revenue), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-610` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-611` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-612` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-613` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-619` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-620` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-621` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `5-10-200-622` (Deducts), **Type:** `MINOR_CODE`
- **ID:** `601-110` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `601-111` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `601-112` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `601-120` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `601-121` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `601-122` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `601-990` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-110` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-111` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-112` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-113` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-120` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-121` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-122` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-123` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `602-990` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-110` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-111` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-112` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-120` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-121` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-122` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `603-990` (Taxes), **Type:** `ACCOUNT_CODE`
- **ID:** `640-110` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-120` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-130` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-140` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-150` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-160` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-210` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-220` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-230` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-240` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-250` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-260` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-270` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-280` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-310` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-320` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-330` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-340` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-350` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-360` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-370` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-380` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-410` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-420` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-430` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-440` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-510` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-520` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-530` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-540` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-610` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-620` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-710` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-720` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-730` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-810` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-820` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-830` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `640-850` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-110` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-120` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-130` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-140` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-210` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-220` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-230` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-310` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-410` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-510` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-610` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-710` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-810` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-820` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-830` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-840` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-850` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-860` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `641-991` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-110` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-120` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-130` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-140` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-150` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-160` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-210` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-220` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-230` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-240` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-310` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-320` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-410` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-420` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-510` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-520` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-530` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-540` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-610` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-710` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-810` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-820` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-830` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-990` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-991` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `642-992` (LOE), **Type:** `ACCOUNT_CODE`
- **ID:** `MKTE705` (Deducts), **Type:** `BILLING_CATEGORY_CODE`
- **ID:** `MOE705` (Deducts), **Type:** `BILLING_CATEGORY_CODE`
- **ID:** `LOE705` (Deducts), **Type:** `BILLING_CATEGORY_CODE`

---

This report should serve as a clear guide for reconciling the hierarchy definitions between the legacy SQL logic and the new mapping files.
