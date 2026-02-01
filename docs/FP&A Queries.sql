SELECT
    account_code AS acctcode,
    accts.account_name AS acctdesc,
    REPLACE(entries.batch_number, 'N/A', NULL) AS batchnum,
    entries.txn_src_code AS transsrccode,
    TO_DATE(entries.txn_date) AS transdate,
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
    CASE
        WHEN entries.billing_category_code = 'N/A' THEN NULL
        ELSE entries.billing_category_code
    END AS billcat,
    billcats.billcatdesc,
    ROUND(entries.amount_gl, 2) AS totalval,
    entries.transaction_description AS transdesc,
    corps.corp_code AS corpcode,
    corps.corp_name AS corpname,
    corps.fund AS Fund,
    corps.AU_Stake AS AU_Stake,
    corps.A3_Stake AS A3_Stake,
    ROUND(entries.amount_gl * corps.AU_Stake, 2) AS Net_AU_Val,
    ROUND(entries.amount_gl * corps.A3_Stake, 2) AS Net_A3_Val,
    CASE
        WHEN props.cost_center_code = 'UNKNOWN' THEN NULL
        ELSE props.cost_center_code
    END AS propcode,
    CASE
        WHEN props.cost_center_name = 'UNKNOWN' THEN NULL
        ELSE props.cost_center_name
    END AS propname,
    CASE
        WHEN vendors.business_associate_code = 'N/A' THEN NULL
        ELSE vendors.business_associate_code
    END AS vendorcode,
    CASE
        WHEN vendors.business_associate_name = 'N/A' THEN NULL
        ELSE vendors.business_associate_name
    END AS vendorname,
    'Alloc Group ID' AS Alloc_Code,
    'Alloc Group Name' As Alloc_Name
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
            account_billing_category_code AS billcat,
            account_billing_category_description AS billcatdesc,
            account_billing_category_type_code AS billcode,
            CASE
                WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', 'FC85%', 'NICC%', 'NIDC%') THEN 'CNOP'
                WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
                WHEN account_billing_category_code ILIKE ANY ('%950%', '%960%') THEN 'CACQ'
                WHEN account_billing_category_type_code IN ('ICC', 'TCC') THEN 'CFRC'
                WHEN account_billing_category_type_code IN ('IDC', 'TDC') THEN 'CDRL'
                WHEN account_billing_category_type_code IN ('IFC', 'TFC') THEN 'CFAC'
                WHEN account_billing_category_type_code IN ('LHD', 'LHU') THEN 'CLHD'
                WHEN account_billing_category_type_code = 'LHX' THEN 'LHD'
                WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%', 'MOE330', 'MOE345', 'MOE625') THEN 'LBR'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE11%',
                    'LOE320',
                    'LOE321',
                    'LOE330',
                    'LOE331',
                    'LOE335',
                    'LOE336',
                    'LOE340',
                    'LOE341',
                    'LOE343',
                    'LOE345',
                    'LOE346',
                    'LOE355',
                    'LOE375',
                    'LOE405',
                    'LOE410',
                    'LOE411',
                    'LOE420',
                    'LOE421',
                    'LOE425',
                    'LOE426',
                    'LOE430',
                    'LOE431',
                    'LOE722',
                    'LOE725',
                    'LOE726',
                    'LOE727',
                    'LOE730',
                    'LOE731',
                    'LOE735',
                    'LOE740',
                    'LOE743',
                    'LOE745',
                    'LOE746',
                    'LOE760',
                    'LOE761',
                    'LOE763',
                    'LOE765',
                    'LOE766',
                    'LOE770',
                    'LOE771',
                    'LOE800',
                    'MOE315',
                    'MOE316',
                    'MOE325',
                    'MOE326',
                    'MOE335',
                    'MOE336',
                    'MOE350',
                    'MOE351',
                    'MOE375',
                    'MOE410',
                    'MOE425',
                    'MOE700',
                    'MOE760',
                    'MOE800'
                ) THEN 'OHD'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE140',
                    'LOE160',
                    'LOE161',
                    'LOE165',
                    'LOE166',
                    'LOE170',
                    'LOE171',
                    'LOE175',
                    'LOE176',
                    'LOE180',
                    'LOE181',
                    'LOE185',
                    'LOE190',
                    'LOE195',
                    'LOE196',
                    'LOE200',
                    'LOE205',
                    'LOE210',
                    'LOE215',
                    'LOE216',
                    'LOE220',
                    'LOE250',
                    'LOE251',
                    'LOE265',
                    'LOE266',
                    'LOE270',
                    'LOE271',
                    'LOE315',
                    'LOE316',
                    'LOE350',
                    'LOE351',
                    'LOE700',
                    'LOE701',
                    'LOE705',
                    'LOE706',
                    'LOE710',
                    'LOE715'
                ) THEN 'SVC'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE24%',
                    'LOE25%',
                    'LOE26%',
                    'MOE24%',
                    'MOE26%',
                    'MOE27%'
                ) THEN 'CHM'
                WHEN account_billing_category_code IN ('LOE273', 'LOE274', 'LOE275', 'LOE276') THEN 'SWD'
                WHEN account_billing_category_code IN (
                    'LOE295',
                    'LOE300',
                    'LOE301',
                    'LOE302',
                    'LOE303',
                    'LOE304',
                    'MOE295',
                    'MOE300',
                    'MOE301',
                    'MOE304',
                    'MOE320',
                    'MOE321',
                    'MOE420',
                    'MOE421'
                ) THEN 'RNM'
                WHEN account_billing_category_code IN ('MOE555', 'MOE556') THEN 'CMP'
                WHEN account_billing_category_code IN ('MOE340', 'MOE341') THEN 'HSE'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE305',
                    'LOE306',
                    'LOE310',
                    'LOE311',
                    'LOE312',
                    'LOE325',
                    'LOE326',
                    'LOE5%',
                    'MOE5%',
                    'MOE110',
                    'MOE111',
                    'MOE115',
                    'MOE305',
                    'MOE306',
                    'MOE310',
                    'MOE311',
                    'MOE705'
                ) THEN 'EQU'
                WHEN account_billing_category_code ILIKE 'LOE6%' THEN 'COPAS'
                WHEN account_billing_category_code IN ('LOE720', 'LOE721') THEN 'SEV'
                WHEN account_billing_category_code IN (
                    'LOE750',
                    'LOE751',
                    'NLOE750',
                    'MOE750',
                    'MOE751',
                    'MOE755'
                ) THEN 'ADV'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE850',
                    'LOE851',
                    'LOE852',
                    'LOE853',
                    'LOE990',
                    'NLOE%'
                ) THEN 'NLOE'
                WHEN account_billing_category_type_code IN ('PAC', 'WOX', 'MOX') THEN account_billing_category_type_code
            END AS los_map
        FROM
            edw.financial.dim_account
        WHERE
            account_billing_category_code <> 'N/A'
            AND account_billing_category_code IS NOT NULL
        GROUP BY
            account_billing_category_code,
            account_billing_category_description,
            account_billing_category_type_code
    ) AS billcats ON billcats.billcat = entries.billing_category_code
    LEFT JOIN (
        SELECT
            dim_cost_center.cost_center_hid AS cost_center_hid,
            dim_cost_center.cost_center_code AS cost_center_code,
            dim_cost_center.cost_center_name AS Cost_center_name,
            dim_cost_center.cost_center_area_code AS cost_center_area_code,
            dim_cost_center.cost_center_area_name AS cost_center_area_name,
            dim_cost_center.cost_center_district_code AS cost_center_district_code,
            dim_cost_center.cost_center_district_name AS cost_center_district_name,
            dim_cost_center.cost_center_field_code AS cost_center_field_code,
            dim_cost_center.cost_center_field_name AS cost_center_field_name,
            dim_cost_center.cost_center_unit_code AS cost_center_unit_code,
            dim_cost_center.cost_center_unit_name AS cost_center_unit_name,
            dim_cost_center.cost_center_gathfac_code,
            dim_cost_center.cost_center_gathfac_name,
            dim_cost_center.cost_center_mssystem_code,
            dim_cost_center.cost_center_mssystem_name,
            dim_cost_center.cost_center_pipelinems_code,
            dim_cost_center.cost_center_pipelinems_name,
            dim_cost_center.cost_center_state,
        FROM
            edw.financial.dim_cost_center
    ) AS props ON props.cost_center_hid = entries.cost_center_hid
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
    LEFT JOIN edw.financial.dim_business_associate as vendors ON vendors.business_associate_hid = entries.business_associate_hid
WHERE
    (
        accts.account_class_code BETWEEN 4
        AND 6
        OR accts.gl ILIKE 'Capex'
    )
    AND entries.transaction_description NOT ILIKE '%Generated%'
    AND accts.gl = 'General & Administrative'
    AND fund IN ('AU', 'A3')
    AND acctdate > '2022-12-31'
ORDER BY
    acctdate DESC,
    corpcode ASC;






// GL:
SELECT
    DATE_FROM_PARTS(
        SUBSTR(entries.accounting_date_key, 1, 4),
        SUBSTR(entries.accounting_date_key, 5, 2),
        1
    ) AS acctdate,
    account_code AS acctcode,
    LISTAGG(DISTINCT accts.account_name) AS acctdesc,
    corps.corp_name AS corpname,
    - ROUND(SUM(entries.net_volume), 2) AS net_volume,
    ROUND(SUM(entries.amount_gl), 2) AS totalval,
    LISTAGG(DISTINCT corps.fund) AS Fund,
    LISTAGG(DISTINCT corps.segment) AS Segment,
    LISTAGG(DISTINCT accts.gl) AS gl,
    MAX(corps.AU_Stake) AS AU_Stake,
    MAX(corps.A3_Stake) AS A3_Stake,
    ROUND(SUM(entries.Net_volume) * MAX(corps.AU_Stake), 2) AS Net_AU_Vol,
    ROUND(SUM(entries.Net_volume) * MAX(corps.A3_Stake), 2) AS Net_A3_Vol,
    ROUND(SUM(entries.amount_gl) * MAX(corps.AU_Stake), 2) AS Net_AU_Val,
    ROUND(SUM(entries.amount_gl) * MAX(corps.A3_Stake), 2) AS Net_A3_Val
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
        ORDER BY
            account_code
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
                WHEN corp_code IN (578, 588) THEN 0.9
                WHEN corp_code = 586 THEN 0.675
                ELSE 0
            END AS A3_Stake
        FROM
            edw.financial.dim_corp
        WHERE
            corp_hid > 1
    ) AS corps ON corps.corp_hid = entries.corp_hid
WHERE
    (
        accts.account_class_code BETWEEN 4
        AND 6
        OR accts.gl ILIKE 'Capex'
    )
    AND entries.transaction_description NOT ILIKE '%Generated%'
GROUP BY
    accounting_date_key,
    accts.account_code,
    corps.corp_name
ORDER BY
    acctdate DESC,
    fund,
    acctcode;
LOS:
SELECT
    CASE
        WHEN accts.account_code = '640-990'
        AND entries.transaction_description ILIKE '%COPAS%' THEN '640-992'
        ELSE accts.account_code
    END AS acctcode,
    LISTAGG(DISTINCT accts.account_name) AS acctdesc,
    corps.fund AS Fund,
    accts.gl AS gl,
    CASE
        WHEN entries.transaction_description LIKE '%PAC %'
        AND accts.account_code = '641-990' THEN 'PAC990'
        WHEN entries.transaction_description LIKE '%NONOP LOE %'
        AND accts.account_code IN ('641-990', '640-990') THEN 'NLOE990'
        WHEN entries.transaction_description ILIKE ANY ('%WOX %', '% LOE %')
        AND accts.account_code = '641-990' THEN 'WOX990'
        WHEN accts.account_code = '641-990' THEN 'WOX990'
        WHEN entries.transaction_description LIKE '%REST FEE%'
        AND accts.account_code = '640-990' THEN 'LOE722'
        WHEN (
            entries.transaction_description LIKE '% LOE %'
            AND accts.account_code = '641-990'
        )
        OR (
            accts.account_code = '640-990'
            AND entries.transaction_description NOT LIKE '%COPAS%'
        ) THEN 'LOE990'
        ELSE billcats.billcat
    END AS adjbillcat,
    LISTAGG(DISTINCT billcats.billcatdesc) AS billcatdesc,
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
    CASE
        WHEN props.cost_center_state ILIKE ANY ('%UNKNOWN%', '%N/A%') THEN NULL
        WHEN props.cost_center_state IN ('AR', 'MS') THEN 'LA'
        ELSE props.cost_center_state
    END AS state,
    CASE
        WHEN entries.product_code = 'N/A' THEN NULL
        ELSE entries.product_code
    END AS productid,
    - ROUND(SUM(entries.net_volume), 2) AS total_vol,
    ROUND(SUM(entries.amount_gl), 2) AS total_val
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
            account_billing_category_code AS billcat,
            account_billing_category_description AS billcatdesc,
            account_billing_category_type_code AS billcode,
            CASE
                WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', 'FC85%', 'NICC%', 'NIDC%') THEN 'CNOP'
                WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
                WHEN account_billing_category_code ILIKE ANY ('%950%', '%960%') THEN 'CACQ'
                WHEN account_billing_category_type_code IN ('ICC', 'TCC') THEN 'CFRC'
                WHEN account_billing_category_type_code IN ('IDC', 'TDC') THEN 'CDRL'
                WHEN account_billing_category_type_code IN ('IFC', 'TFC') THEN 'CFAC'
                WHEN account_billing_category_type_code IN ('LHD', 'LHU') THEN 'CLHD'
                WHEN account_billing_category_type_code = 'LHX' THEN 'LHD'
                WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%', 'MOE330', 'MOE345', 'MOE625') THEN 'LBR'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE11%',
                    'LOE320',
                    'LOE321',
                    'LOE330',
                    'LOE331',
                    'LOE335',
                    'LOE336',
                    'LOE340',
                    'LOE341',
                    'LOE343',
                    'LOE345',
                    'LOE346',
                    'LOE355',
                    'LOE375',
                    'LOE405',
                    'LOE410',
                    'LOE411',
                    'LOE420',
                    'LOE421',
                    'LOE425',
                    'LOE426',
                    'LOE430',
                    'LOE431',
                    'LOE722',
                    'LOE725',
                    'LOE726',
                    'LOE727',
                    'LOE730',
                    'LOE731',
                    'LOE735',
                    'LOE740',
                    'LOE743',
                    'LOE745',
                    'LOE746',
                    'LOE760',
                    'LOE761',
                    'LOE763',
                    'LOE765',
                    'LOE766',
                    'LOE770',
                    'LOE771',
                    'LOE800',
                    'MOE315',
                    'MOE316',
                    'MOE325',
                    'MOE326',
                    'MOE335',
                    'MOE336',
                    'MOE350',
                    'MOE351',
                    'MOE375',
                    'MOE410',
                    'MOE425',
                    'MOE700',
                    'MOE760',
                    'MOE800'
                ) THEN 'OHD'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE140',
                    'LOE160',
                    'LOE161',
                    'LOE165',
                    'LOE166',
                    'LOE170',
                    'LOE171',
                    'LOE175',
                    'LOE176',
                    'LOE180',
                    'LOE181',
                    'LOE185',
                    'LOE190',
                    'LOE195',
                    'LOE196',
                    'LOE200',
                    'LOE205',
                    'LOE210',
                    'LOE215',
                    'LOE216',
                    'LOE220',
                    'LOE250',
                    'LOE251',
                    'LOE265',
                    'LOE266',
                    'LOE270',
                    'LOE271',
                    'LOE315',
                    'LOE316',
                    'LOE350',
                    'LOE351',
                    'LOE700',
                    'LOE701',
                    'LOE705',
                    'LOE706',
                    'LOE710',
                    'LOE715'
                ) THEN 'SVC'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE24%',
                    'LOE25%',
                    'LOE26%',
                    'MOE24%',
                    'MOE26%',
                    'MOE27%'
                ) THEN 'CHM'
                WHEN account_billing_category_code IN ('LOE273', 'LOE274', 'LOE275', 'LOE276') THEN 'SWD'
                WHEN account_billing_category_code IN (
                    'LOE295',
                    'LOE300',
                    'LOE301',
                    'LOE302',
                    'LOE303',
                    'LOE304',
                    'MOE295',
                    'MOE300',
                    'MOE301',
                    'MOE304',
                    'MOE320',
                    'MOE321',
                    'MOE420',
                    'MOE421'
                ) THEN 'RNM'
                WHEN account_billing_category_code IN ('MOE555', 'MOE556') THEN 'CMP'
                WHEN account_billing_category_code IN ('MOE340', 'MOE341') THEN 'HSE'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE305',
                    'LOE306',
                    'LOE310',
                    'LOE311',
                    'LOE312',
                    'LOE325',
                    'LOE326',
                    'LOE5%',
                    'MOE5%',
                    'MOE110',
                    'MOE111',
                    'MOE115',
                    'MOE305',
                    'MOE306',
                    'MOE310',
                    'MOE311',
                    'MOE705'
                ) THEN 'EQU'
                WHEN account_billing_category_code ILIKE 'LOE6%' THEN 'COPAS'
                WHEN account_billing_category_code IN ('LOE720', 'LOE721') THEN 'SEV'
                WHEN account_billing_category_code IN (
                    'LOE750',
                    'LOE751',
                    'NLOE750',
                    'MOE750',
                    'MOE751',
                    'MOE755'
                ) THEN 'ADV'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE850',
                    'LOE851',
                    'LOE852',
                    'LOE853',
                    'LOE990',
                    'NLOE%'
                ) THEN 'NLOE'
                WHEN account_billing_category_type_code IN ('PAC', 'WOX', 'MOX') THEN account_billing_category_type_code
            END AS los_map
        FROM
            edw.financial.dim_account
        WHERE
            account_billing_category_code <> 'N/A'
            AND account_billing_category_code IS NOT NULL
        GROUP BY
            account_billing_category_code,
            account_billing_category_description,
            account_billing_category_type_code
    ) AS billcats ON billcats.billcat = entries.billing_category_code
    LEFT JOIN (
        SELECT
            dim_cost_center.cost_center_hid AS cost_center_hid,
            dim_cost_center.cost_center_code AS cost_center_code,
            dim_cost_center.cost_center_name AS Cost_center_name,
            dim_cost_center.cost_center_area_code AS cost_center_area_code,
            dim_cost_center.cost_center_area_name AS cost_center_area_name,
            dim_cost_center.cost_center_district_code AS cost_center_district_code,
            dim_cost_center.cost_center_district_name AS cost_center_district_name,
            dim_cost_center.cost_center_field_code AS cost_center_field_code,
            dim_cost_center.cost_center_field_name AS cost_center_field_name,
            dim_cost_center.cost_center_unit_code AS cost_center_unit_code,
            dim_cost_center.cost_center_unit_name AS cost_center_unit_name,
            dim_cost_center.cost_center_gathfac_code,
            dim_cost_center.cost_center_gathfac_name,
            dim_cost_center.cost_center_mssystem_code,
            dim_cost_center.cost_center_mssystem_name,
            dim_cost_center.cost_center_pipelinems_code,
            dim_cost_center.cost_center_pipelinems_name,
            CASE
                WHEN dim_cost_center.cost_center_state = 'UNKNOWN' THEN CASE
                    WHEN cost_center_code IN (
                        'W007388-1',
                        'W007389-1',
                        'DP044',
                        'W007386-1',
                        'W007541-1',
                        'W005820-1',
                        'PR0020',
                        'PR0017',
                        'PR307',
                        'PR260',
                        'PR265',
                        'PR0167',
                        'PR0064',
                        'PR292',
                        'PR309',
                        'FAC0355',
                        'FAC0353',
                        'FAC0374',
                        'W001618-1',
                        'PR0162',
                        'W005116-1',
                        'FAC0329',
                        'PR214',
                        'PR0061',
                        'PR304',
                        'PR185',
                        'PR0194',
                        'PR0195',
                        'W001641-1',
                        'W001586-1',
                        'W001623-1',
                        'PR297',
                        'PR211',
                        'PR261',
                        'PR299',
                        'PR304',
                        'PR333',
                        'CSW100',
                        'PR337',
                        'PR0036',
                        'PR300',
                        'CSW125',
                        'WH100',
                        'PR0063'
                    )
                    OR cost_center_area_code IN ('AREA0200', 'AREA0220', 'AREA0210')
                    OR cost_center_field_code IN ('F095', 'F308') THEN 'TX'
                    WHEN cost_center_code IN (
                        'W007390-1',
                        'W007391-1',
                        'DP043',
                        'W006788-1',
                        'PR0014',
                        'PR0016',
                        'PR0018',
                        'W007213-1',
                        'PR303',
                        'PR237',
                        'PR309',
                        'PR310',
                        'FAC0373',
                        'FAC0356',
                        'FAC0355',
                        'FAC0354',
                        'W004943-1',
                        'PR217',
                        'W007387-1',
                        'PR288',
                        'PR0086',
                        'FAC0118',
                        'FAC0188',
                        'FAC0226',
                        'FAC0216',
                        'FAC0208',
                        'FAC0192',
                        'FAC0319',
                        'W000017-1',
                        'FAC0323',
                        'FAC0216',
                        'PR258',
                        'PR274',
                        'PR311',
                        'PR262',
                        'PR284',
                        'PR0089',
                        'PR293',
                        'PR287',
                        'PR234',
                        'PR189',
                        'PR178',
                        'PR0123',
                        'PR302',
                        'PR296',
                        'PR263',
                        'PR0055',
                        'PR278',
                        'PR0087',
                        'PR295',
                        'PR291',
                        'PR334',
                        'PR0235',
                        'WH310',
                        'CSW300',
                        'WH355',
                        'CSW200',
                        'CSW250',
                        'CSW275',
                        'CSW325',
                        'CSW350',
                        'CSW375',
                        'PR0024',
                        'FAC0330',
                        'FLDOFF210',
                        'W007603-1',
                        'W007212-1',
                        'PR238'
                    )
                    OR cost_center_area_code IN (
                        'AREA0300',
                        'AREA0400',
                        'AREA0330',
                        'AREA0350',
                        'AREA0420',
                        'AREA0440',
                        'AREA0320',
                        'AREA0340'
                    ) THEN 'LA'
                    WHEN cost_center_code IN (
                        'W001170-1',
                        'PR0015',
                        'PR0058',
                        'GF0062',
                        'PR259',
                        'FAC0027',
                        'GF0050',
                        'FAC0118',
                        'FAC0056',
                        'FAC0009',
                        'FAC0012'
                    )
                    OR cost_center_area_code IN ('AREA0100', 'AREA0125') THEN 'WY'
                    WHEN cost_center_code ILIKE ANY ('VE%', 'DP00560', 'DP00565', 'PR301', 'PR342') THEN 'Multiple'
                    ELSE NULL
                END
                ELSE dim_cost_center.cost_center_state
            END AS cost_center_state,
            actenum.propnum,
            actenum.well,
            actenum.source,
            actenum.play,
            actenum.Subplay,
            actenum.state,
            actenum.WI,
            actenum.NRI,
            actenum.AFECode,
            actenum.pad,
            actenum.pod,
            actenum.development,
            actenum.gross_T_cost_exp,
            actenum.gross_D_cost_exp,
            actenum.gross_C_cost_exp,
            actenum.gross_F_cost_exp,
            actenum.gross_P_cost_exp,
            CASE
                WHEN cost_center_area_code IN (
                    'AREA0100',
                    'AREA0200',
                    'AREA0220',
                    'AREA0300',
                    'AREA0301',
                    'AREA0430',
                    'AREA0470',
                    'AREA0600',
                    'AREA0625'
                ) THEN TRUE
                ELSE FALSE
            END AS AU_Op,
            CASE
                WHEN cost_center_area_code IN (
                    'AREA0210',
                    'AREA0320',
                    'AREA0440',
                    'AREA0475',
                    'AREA0650'
                ) THEN TRUE
                ELSE FALSE
            END AS A3_Op,
            CASE
                WHEN cost_center_area_code IN ('AREA0250', 'AREA0350', 'AREA0440', 'AREA0475') THEN TRUE
                ELSE FALSE
            END AS AU_Nop,
            CASE
                WHEN cost_center_area_code IN ('AREA0330', 'AREA0340', 'AREA0430', 'AREA0470') THEN TRUE
                ELSE FALSE
            END AS A3_Nop
        FROM
            edw.financial.dim_cost_center
            LEFT JOIN (
                SELECT
                    LISTAGG(DISTINCT propnum) AS Propnum,
                    LISTAGG(DISTINCT well_name) AS Well,
                    enertia_id,
                    LISTAGG(DISTINCT schedule_type) AS Source,
                    LISTAGG(DISTINCT area) AS Play,
                    LISTAGG(DISTINCT sub_area) AS Subplay,
                    LISTAGG(DISTINCT state) AS State,
                    MAX(working_interest) AS WI,
                    MAX(net_revenue_interest) AS NRI,
                    LISTAGG(DISTINCT afe_number_drilling) AS AFECode,
                    LISTAGG(DISTINCT pad_name) AS Pad,
                    LISTAGG(DISTINCT pod_name) AS Pod,
                    LISTAGG(DISTINCT development_name) AS Development,
                    MAX(gross_well_cost_expected) * 1000 AS Gross_T_Cost_Exp,
                    MAX(gross_drilling_cost_expected) * 1000 AS gross_D_Cost_Exp,
                    MAX(gross_completion_cost_expected) * 1000 AS Gross_C_Cost_Exp,
                    MAX(gross_facility_cost_expected) * 1000 AS Gross_F_Cost_Exp,
                    MAX(gross_production_cost_expected) * 1000 AS Gross_P_Cost_Exp
                FROM
                    edw.analytics.dim_drill_schedules_well
                WHERE
                    SCHEDULE_TYPE IN ('HSVLBSSR', 'OTHERH', 'OTHERV', 'NONOP')
                    AND enertia_id IS NOT NULL
                    AND TO_DATE(created_date) = (
                        SELECT
                            MAX(TO_DATE(created_date))
                        FROM
                            edw.analytics.dim_drill_schedules_well
                    )
                GROUP BY
                    enertia_id
                HAVING
                    MAX(Net_revenue_interest) > 0
            ) AS actenum ON Actenum.Enertia_ID = cost_center_code
    ) AS props ON props.cost_center_hid = entries.cost_center_hid
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
    AND svcdate > '2021-12-31'
    AND acctdate BETWEEN '2022-12-31'
    AND '2025-06-01'
    AND cost_center_code IS NOT NULL
    AND gl IS NOT NULL
    AND segment = 'Upstream'
    AND fund IN ('AU', 'A3')
    AND gl NOT IN (
        'Omit',
        'Hedge Gains',
        'General & Administrative',
        'Impairment Expense',
        'DD&A',
        'Accretion Expense',
        'Interest Expense',
        'Interest Income',
        'Other Gains/Losses',
        'Interest Hedge Gains',
        'Unrealized Hedge Gains'
    )
    AND account_code <> '570-115'
    AND Account_code NOT LIKE '242%'
GROUP BY
    fund,
    adjbillcat,
    acctdate,
    svcdate,
    cost_center_state,
    acctcode,
    productid,
    gl
HAVING
    total_val <> 0
ORDER BY
    fund,
    state,
    gl,
    adjbillcat,
    svcdate ASC;
SCONA:
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
    END AS Alloc_Code,
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
Brahma:
SELECT
    account_code AS acctcode,
    LISTAGG(DISTINCT accts.account_name) AS acctdesc,
    LISTAGG(DISTINCT accts.gl) AS gl,
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
    CASE
        WHEN entries.billing_category_code = 'N/A' THEN NULL
        ELSE entries.billing_category_code
    END AS billcat,
    LISTAGG(DISTINCT billcats.billcatdesc) AS billcatdesc,
    LISTAGG(DISTINCT billcats.billcode) AS billcode,
    LISTAGG(DISTINCT billcats.los_map) AS Los_map,
    ROUND(SUM(entries.net_volume), 2) AS net_vol,
    ROUND(SUM(entries.amount_gl), 2) AS net_val,
    CASE
        WHEN SUM(entries.net_volume) <> 0
        AND SUM(entries.amount_gl) <> 0 THEN ROUND(
            ABS(SUM(entries.amount_gl) / SUM(entries.net_volume)),
            2
        )
        ELSE NULL
    END AS Impl_price,
    corps.corp_code AS corpcode,
    LISTAGG(DISTINCT corps.corp_name) AS corpname
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
            account_billing_category_code AS billcat,
            account_billing_category_description AS billcatdesc,
            account_billing_category_type_code AS billcode,
            CASE
                WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', 'FC85%', 'NICC%', 'NIDC%') THEN 'CNOP'
                WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
                WHEN account_billing_category_code ILIKE ANY ('%950%', '%960%') THEN 'CACQ'
                WHEN account_billing_category_type_code IN ('ICC', 'TCC') THEN 'CFRC'
                WHEN account_billing_category_type_code IN ('IDC', 'TDC') THEN 'CDRL'
                WHEN account_billing_category_type_code IN ('IFC', 'TFC') THEN 'CFAC'
                WHEN account_billing_category_type_code IN ('LHD', 'LHU') THEN 'CLHD'
                WHEN account_billing_category_type_code = 'LHX' THEN 'LHD'
                WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%', 'MOE330', 'MOE345', 'MOE625') THEN 'LBR'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE11%',
                    'LOE320',
                    'LOE321',
                    'LOE330',
                    'LOE331',
                    'LOE335',
                    'LOE336',
                    'LOE340',
                    'LOE341',
                    'LOE343',
                    'LOE345',
                    'LOE346',
                    'LOE355',
                    'LOE375',
                    'LOE405',
                    'LOE410',
                    'LOE411',
                    'LOE420',
                    'LOE421',
                    'LOE425',
                    'LOE426',
                    'LOE430',
                    'LOE431',
                    'LOE722',
                    'LOE725',
                    'LOE726',
                    'LOE727',
                    'LOE730',
                    'LOE731',
                    'LOE735',
                    'LOE740',
                    'LOE743',
                    'LOE745',
                    'LOE746',
                    'LOE760',
                    'LOE761',
                    'LOE763',
                    'LOE765',
                    'LOE766',
                    'LOE770',
                    'LOE771',
                    'LOE800',
                    'MOE315',
                    'MOE316',
                    'MOE325',
                    'MOE326',
                    'MOE335',
                    'MOE336',
                    'MOE350',
                    'MOE351',
                    'MOE375',
                    'MOE410',
                    'MOE425',
                    'MOE700',
                    'MOE760',
                    'MOE800'
                ) THEN 'OHD'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE140',
                    'LOE160',
                    'LOE161',
                    'LOE165',
                    'LOE166',
                    'LOE170',
                    'LOE171',
                    'LOE175',
                    'LOE176',
                    'LOE180',
                    'LOE181',
                    'LOE185',
                    'LOE190',
                    'LOE195',
                    'LOE196',
                    'LOE200',
                    'LOE205',
                    'LOE210',
                    'LOE215',
                    'LOE216',
                    'LOE220',
                    'LOE250',
                    'LOE251',
                    'LOE265',
                    'LOE266',
                    'LOE270',
                    'LOE271',
                    'LOE315',
                    'LOE316',
                    'LOE350',
                    'LOE351',
                    'LOE700',
                    'LOE701',
                    'LOE705',
                    'LOE706',
                    'LOE710',
                    'LOE715'
                ) THEN 'SVC'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE24%',
                    'LOE25%',
                    'LOE26%',
                    'MOE24%',
                    'MOE26%',
                    'MOE27%'
                ) THEN 'CHM'
                WHEN account_billing_category_code IN ('LOE273', 'LOE274', 'LOE275', 'LOE276') THEN 'SWD'
                WHEN account_billing_category_code IN (
                    'LOE295',
                    'LOE300',
                    'LOE301',
                    'LOE302',
                    'LOE303',
                    'LOE304',
                    'MOE295',
                    'MOE300',
                    'MOE301',
                    'MOE304',
                    'MOE320',
                    'MOE321',
                    'MOE420',
                    'MOE421'
                ) THEN 'RNM'
                WHEN account_billing_category_code IN ('MOE555', 'MOE556') THEN 'CMP'
                WHEN account_billing_category_code IN ('MOE340', 'MOE341') THEN 'HSE'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE305',
                    'LOE306',
                    'LOE310',
                    'LOE311',
                    'LOE312',
                    'LOE325',
                    'LOE326',
                    'LOE5%',
                    'MOE5%',
                    'MOE110',
                    'MOE111',
                    'MOE115',
                    'MOE305',
                    'MOE306',
                    'MOE310',
                    'MOE311',
                    'MOE705'
                ) THEN 'EQU'
                WHEN account_billing_category_code ILIKE 'LOE6%' THEN 'COPAS'
                WHEN account_billing_category_code IN ('LOE720', 'LOE721') THEN 'SEV'
                WHEN account_billing_category_code IN (
                    'LOE750',
                    'LOE751',
                    'NLOE750',
                    'MOE750',
                    'MOE751',
                    'MOE755'
                ) THEN 'ADV'
                WHEN account_billing_category_code ILIKE ANY (
                    'LOE850',
                    'LOE851',
                    'LOE852',
                    'LOE853',
                    'LOE990',
                    'NLOE%'
                ) THEN 'NLOE'
                WHEN account_billing_category_type_code IN ('PAC', 'WOX', 'MOX') THEN account_billing_category_type_code
            END AS los_map
        FROM
            edw.financial.dim_account
        WHERE
            account_billing_category_code <> 'N/A'
            AND account_billing_category_code IS NOT NULL
        GROUP BY
            account_billing_category_code,
            account_billing_category_description,
            account_billing_category_type_code
    ) AS billcats ON billcats.billcat = entries.billing_category_code
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
WHERE
    (
        accts.account_class_code BETWEEN 4
        AND 6
        OR accts.gl ILIKE 'Capex'
    )
    AND entries.transaction_description NOT ILIKE '%Generated%'
    AND segment = 'Services'
    AND accts.account_code <> '700-800'
    AND gl NOT IN (
        'Omit',
        'General & Administrative',
        'DD&A',
        'Accretion Expense',
        'Interest Expense',
        'Interest Income',
        'Other Gains/Losses'
    )
    AND acctdate > '2021-12-31'
GROUP BY
    acctcode,
    acctdate,
    svcdate,
    corpcode,
    entries.billing_category_code
HAVING
    SUM(entries.amount_gl) <> 0
ORDER BY
    acctdate DESC,
    corpcode,
    svcdate DESC,
    acctcode;