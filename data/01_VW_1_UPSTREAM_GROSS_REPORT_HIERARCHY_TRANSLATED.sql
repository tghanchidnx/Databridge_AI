create or replace view TRANSFORMATION.CORE_FINANCIAL.VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED(
	HIERARCHY_GROUP_NAME,
	LEVEL_1,
	LEVEL_2,
	LEVEL_3,
	LEVEL_4,
	LEVEL_5,
	LEVEL_6,
	LEVEL_7,
	LEVEL_8,
	LEVEL_9,
	LOS_EXT_LEVEL_1,
	LOS_EXT_LEVEL_2,
	LOS_EXT_LEVEL_3,
	LOS_EXT_LEVEL_4,
	LOS_EXT_LEVEL_5,
	LOS_EXT_LEVEL_6,
	LOS_EXT_LEVEL_7,
	LOS_EXT_LEVEL_8,
	LOS_EXT_LEVEL_9,
	LEVEL_1_SORT,
	LEVEL_2_SORT,
	LEVEL_3_SORT,
	LEVEL_4_SORT,
	LEVEL_5_SORT,
	LEVEL_6_SORT,
	LEVEL_7_SORT,
	LEVEL_8_SORT,
	LEVEL_9_SORT,
	LOS_EXT_LEVEL_1_SORT,
	LOS_EXT_LEVEL_2_SORT,
	LOS_EXT_LEVEL_3_SORT,
	LOS_EXT_LEVEL_4_SORT,
	LOS_EXT_LEVEL_5_SORT,
	LOS_EXT_LEVEL_6_SORT,
	LOS_EXT_LEVEL_7_SORT,
	LOS_EXT_LEVEL_8_SORT,
	LOS_EXT_LEVEL_9_SORT,
	SIGN_CHANGE_FLAG,
	VOLUME_FLAG,
	EXCLUSION_FLAG,
	ACTIVE_FLAG,
	CALCULATION_FLAG,
	ID_UNPIVOT_FLAG,
	ID_ROW_FLAG,
	DO_NOT_EXPAND_FLAG,
	CREATE_NEW_COLUMN,
	FILTER_GROUP_1,
	FILTER_GROUP_2,
	FILTER_GROUP_3,
	FILTER_GROUP_4,
	FORMULA_GROUP,
	FORMULA_PARAM_REF,
	FORMULA_PARAM2_CONST_NUMBER,
	ARITHMETIC_LOGIC,
	FORMULA_PRECEDENCE,
	ID_DATABASE,
	ID_SCHEMA,
	ID_TABLE,
	ID_SOURCE,
	PIVOT_SOURCE_COLUMN_NAME,
	PIVOT_SOURCE_TABLE_NAME,
	PIVOT_SOURCE_SCHEMA_NAME,
	PIVOT_SOURCE_DATABASE_NAME,
	LOS_ACCOUNT_ID_FILTER,
	LOS_PRODUCT_CODE_FILTER,
	LOS_DEDUCT_CODE_FILTER,
	LOS_ROYALTY_FILTER
) as

/********************************************************************************
Author:			Telha Ghanchi	
Create date:	5/23/2024
Description:	Returns MIDSTREAM LOS REPORT TABLE RESULTS FROM MULTIPLE TABLES
                This query depends on other dynamic tables or other hierarchy or group tables generated through Data Nexum's Financial Analysis Builder to perform and produce results specific to business user requirement
                Those tables are all listed in the from clause or subqueries (if any), and are as follows:
                    1.      TBL_0_MOS_REPORT_HIERARCHY
                    2.      CONFIGURATION.TBL_0_MOS_REPORT_HIERARCHY_MAPPING

Modification Log:
                TELHA GHANCHI,  10/7/2024    ADDED EXTENTSION CAPABILITIES FOR GROSS_LOS
                BJ CUMMINGS     03/29/2025  Added explicit conversion to VARCHAR
                AHAMZA          1/23/2026 REMOVED 'AETHON' FROM HIERARCHY_GROUP_NAME FILTER i.e  'AETHON Operation Team Financial Group' TO 'Operation Team Financial Group'
*********************************************************************************/
    
    SELECT DISTINCT 
    
              MAIN.HIERARCHY_GROUP_NAME                 AS HIERARCHY_GROUP_NAME            
            , MAIN.LEVEL_1                              AS LEVEL_1
            , MAIN.LEVEL_2                              AS LEVEL_2
            , MAIN.LEVEL_3                              AS LEVEL_3
            , MAIN.LEVEL_4                              AS LEVEL_4
            , MAIN.LEVEL_5                              AS LEVEL_5
            , MAIN.LEVEL_6                              AS LEVEL_6
            , MAIN.LEVEL_7                              AS LEVEL_7
            , MAIN.LEVEL_8                              AS LEVEL_8
            , MAIN.LEVEL_9                              AS LEVEL_9
            
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_1, LOS_EXT_2.LEVEL_1, LOS_EXT_3.LEVEL_1 ))                          AS LOS_EXT_LEVEL_1
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_2, LOS_EXT_2.LEVEL_2, LOS_EXT_3.LEVEL_2 ))                          AS LOS_EXT_LEVEL_2
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_3, LOS_EXT_2.LEVEL_3, LOS_EXT_3.LEVEL_3 ))                          AS LOS_EXT_LEVEL_3
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_4, LOS_EXT_2.LEVEL_4, LOS_EXT_3.LEVEL_4 ))                          AS LOS_EXT_LEVEL_4
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_5, LOS_EXT_2.LEVEL_5, LOS_EXT_3.LEVEL_5 ))                          AS LOS_EXT_LEVEL_5
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_6, LOS_EXT_2.LEVEL_6, LOS_EXT_3.LEVEL_6 ))                          AS LOS_EXT_LEVEL_6
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_7, LOS_EXT_2.LEVEL_7, LOS_EXT_3.LEVEL_7 ))                          AS LOS_EXT_LEVEL_7
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_8, LOS_EXT_2.LEVEL_8, LOS_EXT_3.LEVEL_8 ))                          AS LOS_EXT_LEVEL_8
            , TO_VARCHAR(COALESCE(LOS_EXT_1.LEVEL_9, LOS_EXT_2.LEVEL_9, LOS_EXT_3.LEVEL_9 ))                          AS LOS_EXT_LEVEL_9
            
            
            , 1                                         AS LEVEL_1_SORT
            , MAIN.LEVEL_2_SORT                         AS LEVEL_2_SORT
            , MAIN.LEVEL_3_SORT                         AS LEVEL_3_SORT
            , MAIN.LEVEL_4_SORT                         AS LEVEL_4_SORT
            , MAIN.LEVEL_5_SORT                         AS LEVEL_5_SORT
            , MAIN.LEVEL_6_SORT                         AS LEVEL_6_SORT
            , MAIN.LEVEL_7_SORT                         AS LEVEL_7_SORT
            , MAIN.LEVEL_8_SORT                         AS LEVEL_8_SORT
            , MAIN.LEVEL_9_SORT                         AS LEVEL_9_SORT
            
            , COALESCE(LOS_EXT_1.LEVEL_1_SORT, LOS_EXT_2.LEVEL_1_SORT, LOS_EXT_3.LEVEL_1_SORT )                          AS LOS_EXT_LEVEL_1_SORT
            , COALESCE(LOS_EXT_1.LEVEL_2_SORT, LOS_EXT_2.LEVEL_2_SORT, LOS_EXT_3.LEVEL_2_SORT )                          AS LOS_EXT_LEVEL_2_SORT
            , COALESCE(LOS_EXT_1.LEVEL_3_SORT, LOS_EXT_2.LEVEL_3_SORT, LOS_EXT_3.LEVEL_3_SORT )                          AS LOS_EXT_LEVEL_3_SORT
            , COALESCE(LOS_EXT_1.LEVEL_4_SORT, LOS_EXT_2.LEVEL_4_SORT, LOS_EXT_3.LEVEL_4_SORT )                          AS LOS_EXT_LEVEL_4_SORT
            , COALESCE(LOS_EXT_1.LEVEL_5_SORT, LOS_EXT_2.LEVEL_5_SORT, LOS_EXT_3.LEVEL_5_SORT )                          AS LOS_EXT_LEVEL_5_SORT
            , COALESCE(LOS_EXT_1.LEVEL_6_SORT, LOS_EXT_2.LEVEL_6_SORT, LOS_EXT_3.LEVEL_6_SORT )                          AS LOS_EXT_LEVEL_6_SORT
            , COALESCE(LOS_EXT_1.LEVEL_7_SORT, LOS_EXT_2.LEVEL_7_SORT, LOS_EXT_3.LEVEL_7_SORT )                          AS LOS_EXT_LEVEL_7_SORT
            , COALESCE(LOS_EXT_1.LEVEL_8_SORT, LOS_EXT_2.LEVEL_8_SORT, LOS_EXT_3.LEVEL_8_SORT )                          AS LOS_EXT_LEVEL_8_SORT
            , COALESCE(LOS_EXT_1.LEVEL_9_SORT, LOS_EXT_2.LEVEL_9_SORT, LOS_EXT_3.LEVEL_9_SORT )                          AS LOS_EXT_LEVEL_9_SORT
            
            , MAIN.SIGN_CHANGE_FLAG                     AS SIGN_CHANGE_FLAG
            , TBL.VOLUME_FLAG                           AS VOLUME_FLAG
            , TBL.EXCLUSION_FLAG                        AS EXCLUSION_FLAG    
            , TBL.ACTIVE_FLAG                           AS ACTIVE_FLAG       
            , MAIN.CALCULATION_FLAG                     AS CALCULATION_FLAG  
            , MAIN.ID_UNPIVOT_FLAG                      AS ID_UNPIVOT_FLAG   
            , MAIN.ID_ROW_FLAG                          AS ID_ROW_FLAG       
            , MAIN.DO_NOT_EXPAND_FLAG                   AS DO_NOT_EXPAND_FLAG
            , TBL.CREATE_NEW_COLUMN                     AS CREATE_NEW_COLUMN
            
            , TBL.FILTER_GROUP_1                        AS FILTER_GROUP_1
            , TBL.FILTER_GROUP_2                        AS FILTER_GROUP_2
            , TBL.FILTER_GROUP_3                        AS FILTER_GROUP_3
            , TBL.FILTER_GROUP_4                        AS FILTER_GROUP_4

            , MAIN.FORMULA_GROUP                        AS FORMULA_GROUP
            , MAIN.FORMULA_PARAM_REF                    AS FORMULA_PARAM_REF
            , MAIN.FORMULA_PARAM2_CONST_NUMBER          AS FORMULA_PARAM2_CONST_NUMBER
            , MAIN.ARITHMETIC_LOGIC                     AS ARITHMETIC_LOGIC
            
            , MAIN.FORMULA_PRECEDENCE                   AS FORMULA_PRECEDENCE
            
            
            , TBL.ID_DATABASE                           AS ID_DATABASE
            , TBL.ID_SCHEMA                             AS ID_SCHEMA  
            , TBL.ID_TABLE                              AS ID_TABLE
            , TBL.ID_SOURCE                             AS ID_SOURCE

            , IFNULL(TBL_X.ID_SOURCE,   'N/A')          AS PIVOT_SOURCE_COLUMN_NAME
            , IFNULL(TBL_X.ID_TABLE,    'N/A')          AS PIVOT_SOURCE_TABLE_NAME
            , IFNULL(TBL_X.ID_SCHEMA,   'N/A')          AS PIVOT_SOURCE_SCHEMA_NAME
            , IFNULL(TBL_X.ID_DATABASE, 'N/A')          AS PIVOT_SOURCE_DATABASE_NAME
            
            , IFNULL(TBL_1.ID, 'N/A')                   AS LOS_ACCOUNT_ID_FILTER
            , TBL_2.ID                                  AS LOS_PRODUCT_CODE_FILTER
            , TBL_3.ID                                  AS LOS_DEDUCT_CODE_FILTER
            , TBL_4.ID                                  AS LOS_ROYALTY_FILTER
            
    FROM    TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY AS MAIN
    INNER JOIN  
            TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING AS TBL
    ON      MAIN.XREF_HIERARCHY_KEY = TBL.FK_REPORT_KEY
    AND     TBL.ACTIVE_FLAG =  TRUE
    LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING AS TBL_1
    ON      MAIN.XREF_HIERARCHY_KEY = TBL_1.FK_REPORT_KEY
    AND     TBL.GROUP_FILTER_PRECEDENCE = TBL_1.GROUP_FILTER_PRECEDENCE
    AND     TBL.FILTER_GROUP_1 = TBL_1.FILTER_GROUP_1
    AND     TBL.FILTER_GROUP_2 = TBL_1.FILTER_GROUP_2
    AND     TBL_1.ID_TABLE = 'DIM_ACCOUNT'
    AND     TBL_1.ID_ROW_FLAG = TRUE
    AND     TBL_1.ACTIVE_FLAG = TRUE
   
    LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING AS TBL_2
    ON      TBL_2.ID_TABLE = 'DIM_PRODUCT'
    AND     TBL.GROUP_FILTER_PRECEDENCE = TBL_2.GROUP_FILTER_PRECEDENCE
    AND     MAIN.XREF_HIERARCHY_KEY = TBL_2.FK_REPORT_KEY
    AND     TBL.FILTER_GROUP_1 = TBL_2.FILTER_GROUP_1
    AND     TBL.FILTER_GROUP_2 = TBL_2.FILTER_GROUP_2
    AND     TBL_2.ID_ROW_FLAG = TRUE
    AND     TBL_2.ACTIVE_FLAG = TRUE
    -- AND     TBL.ID_SOURCE = TBL_2.ID_SOURCE
    -- AND     TBL.ID_TABLE = TBL_2.ID_TABLE
    -- AND     TBL.ID_SCHEMA = TBL_2.ID_SCHEMA
    LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING AS TBL_3
    ON      TBL_3.ID_TABLE = 'DIM_DEDUCT'
    AND     TBL.GROUP_FILTER_PRECEDENCE = TBL_3.GROUP_FILTER_PRECEDENCE
    AND     TBL.FILTER_GROUP_1 = TBL_3.FILTER_GROUP_1
    AND     TBL.FILTER_GROUP_2 = TBL_3.FILTER_GROUP_2
    AND     MAIN.XREF_HIERARCHY_KEY = TBL_3.FK_REPORT_KEY
    AND     TBL_3.ID_ROW_FLAG = TRUE
    AND     TBL_3.ACTIVE_FLAG = TRUE
   
    LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING AS TBL_4
    ON      TBL_4.ID_TABLE = 'DT_1_FACT_FINANCIAL_ACTUALS'
    AND     TBL.GROUP_FILTER_PRECEDENCE = TBL_4.GROUP_FILTER_PRECEDENCE
    AND     TBL.FILTER_GROUP_1 = TBL_4.FILTER_GROUP_1
    AND     TBL.FILTER_GROUP_2 = TBL_4.FILTER_GROUP_2
    AND     MAIN.XREF_HIERARCHY_KEY = TBL_4.FK_REPORT_KEY
    AND     TBL_4.ID_ROW_FLAG = TRUE
    AND     TBL_4.ACTIVE_FLAG = TRUE
    
   LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING AS TBL_X
    ON      MAIN.XREF_HIERARCHY_KEY = TBL_X.FK_REPORT_KEY
    AND     TBL.FILTER_GROUP_2 = TBL_X.FILTER_GROUP_2
    AND     TBL_X.ID_ROW_FLAG = FALSE
    AND     TBL_X.ACTIVE_FLAG = TRUE
    AND     TBL.ID_SOURCE = TBL_X.ID_SOURCE
    AND     TBL.ID_TABLE = TBL_X.ID_TABLE
    AND     TBL.ID_SCHEMA = TBL_X.ID_SCHEMA

    LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROUP_TABLE                         AS LOS_EXT_1
    ON      LOS_EXT_1.ID = TBL_1.XREF_HIERARCHY_KEY ::VARCHAR
    AND     LOS_EXT_1.ACTIVE_FLAG = TRUE
    AND     LOS_EXT_1.HIERARCHY_GROUP_NAME = 'Operation Team Financial Group'    ---1/23/2026
    LEFT OUTER JOIN
            TRANSFORMATION.CONFIGURATION.TBL_0_GROUP_TABLE                         AS LOS_EXT_2
    ON      LOS_EXT_2.ID = TBL_2.XREF_HIERARCHY_KEY ::VARCHAR
    AND     LOS_EXT_2.ACTIVE_FLAG = TRUE
    AND     LOS_EXT_2.HIERARCHY_GROUP_NAME = 'Operation Team Financial Group'  ---1/23/2026
    LEFT OUTER JOIN
            CONFIGURATION.TBL_0_GROUP_TABLE                         AS LOS_EXT_3
    ON      LOS_EXT_3.ID = TBL_3.XREF_HIERARCHY_KEY ::VARCHAR
    AND     LOS_EXT_3.ACTIVE_FLAG = TRUE
    AND     LOS_EXT_3.HIERARCHY_GROUP_NAME = 'Operation Team Financial Group'   ---1/23/2026
    
    WHERE   MAIN.ACTIVE_FLAG = 1
    AND     MAIN.CALCULATION_FLAG = FALSE
    ORDER BY 
              MAIN.LEVEL_2_SORT
            , MAIN.LEVEL_3_SORT
            , MAIN.LEVEL_4_SORT
            
            ;