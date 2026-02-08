create or replace dynamic table TRANSFORMATION.CORE_FINANCIAL.DT_3A_UPSTREAM_GROSS_LOS(
	GROSS_AMOUNT,
	GROSS_VOLUME,
	GROSS_MCFE,
	GROSS_VOLUME_MMBTU,
	GROSS_MCFE_MMBTU,
	ACCOUNTING_DATE_KEY,
	SERVICE_DATE_KEY_M,
	SERVICE_DATE_KEY,
	CORP_HID,
	ALLOCATION_HID,
	COST_CENTER_HID,
	ACCOUNT_ID,
	BUSINESS_ASSOCIATE_HID,
	AFE_HID,
	COUNTER_PARTY_HID,
	PURCHASER_HID,
	DEDUCT_CODE,
	PRODUCT_CODE,
	PROD_CODE_COMPONENT,
	FORMULA_GROUP,
	HIERARCHY_GROUP_NAME,
	XREF_HIERARCHY_KEY
) target_lag = '6 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = TRANSFORMING_WH
 as 
/***********************************************************************************************************************************************
Author:			Telha Ghanchi
Create date:	6/9/2024
Description:	Returns summarized values from FFA for all FORMULA_PRECEDENCE levels and details

Modification Log:
                    --Time to run - X-Small - 7/5/2024 - < 1m

***********************************************************************************************************************************************/
            --GROSS VALUES FOR ACCOUNT AND ACCOUNTS WITH PROD CODES 
            SELECT          
                      IFF(LOS.VOLUME_FLAG = TRUE, NULL,  SUM(FFA.GROSS_AMOUNT) 
                                    * IFF( IFNULL(LOS.SIGN_CHANGE_FLAG,FALSE) = TRUE, -1,1))                AS GROSS_AMOUNT
                    , SUM(FFA.GROSS_VOLUME )* IFF(LOS.VOLUME_FLAG = TRUE, 
                                CASE UPPER(LOS.ARITHMETIC_LOGIC)
                                    WHEN 'MULTIPLY'
                                    THEN LOS.FORMULA_PARAM2_CONST_NUMBER
                                    WHEN 'DIVIDE'
                                    THEN 1/LOS.FORMULA_PARAM2_CONST_NUMBER
                                    ELSE 1
                                END
                            , NULL --IFF ELSE
                            )                                                                               AS GROSS_VOLUME
                    , SUM(FFA.GROSS_VOLUME )* IFF(LOS.VOLUME_FLAG = TRUE, 
                                CASE UPPER(LOS.ARITHMETIC_LOGIC)
                                    WHEN 'MULTIPLY'
                                    THEN LOS.FORMULA_PARAM2_CONST_NUMBER
                                    WHEN 'DIVIDE'
                                    THEN 1/LOS.FORMULA_PARAM2_CONST_NUMBER
                                    ELSE 1
                                END
                            , NULL --IFF ELSE
                            )                                                                               AS GROSS_MCFE
                    , SUM(GROSS_VOLUME_MMBTU)                                                               AS GROSS_VOLUME_MMBTU
                    , NULL                                                                                  AS GROSS_MCFE_MMBTU
                                                                    
                    , FFA.ACCOUNTING_DATE_KEY                                                               AS ACCOUNTING_DATE_KEY
                    , FFA.SERVICE_DATE_KEY_M                                                                AS SERVICE_DATE_KEY_M
                    , FFA.SERVICE_DATE_KEY                                                                  AS SERVICE_DATE_KEY
                    , FFA.CORP_HID                                                                          AS CORP_HID
                    , FFA.ALLOCATION_HID AS ALLOCATION_HID
                    , FFA.COST_CENTER_HID                                                                   AS COST_CENTER_HID
                    , FFA.ACCOUNT_ID                                                                        AS ACCOUNT_ID
                    , FFA.BUSINESS_ASSOCIATE_HID                                                            AS BUSINESS_ASSOCIATE_HID
                    , FFA.AFE_HID                                                                           AS AFE_HID
                    , FFA.COUNTER_PARTY_HID                                                                 AS COUNTER_PARTY_HID
                    , FFA.PURCHASER_HID                                                                     AS PURCHASER_HID
                                                                
                    , FFA.DEDUCT_CODE                                                                       AS DEDUCT_CODE
                    , FFA.PRODUCT_CODE                                                                      AS PRODUCT_CODE
                    , FFA.PROD_CODE_COMPONENT                                                               AS PROD_CODE_COMPONENT
                                                                
                    , LOS.FORMULA_GROUP                                                                     AS FORMULA_GROUP
                    , LOS.HIERARCHY_GROUP_NAME                                                              AS HIERARCHY_GROUP_NAME
                    , LOS_CALCS.XREF_HIERARCHY_KEY                                                          AS XREF_HIERARCHY_KEY
                    
            FROM    CORE_FINANCIAL.DT_1_FACT_FINANCIAL_ACTUALS AS FFA
            INNER JOIN
                    CORE_FINANCIAL.DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY AS LOS
            ON      FFA.ACCOUNT_ID      =   LOS.ACCOUNT_ID
            AND     TRIM(UPPER(FFA.PRODUCT_CODE))   =   TRIM(UPPER(IFNULL(LOS.PRODUCT_CODE, FFA.PRODUCT_CODE )))
            AND     (           
                    FFA.GROSS_AMOUNT        IS NOT NULL
                OR  FFA.GROSS_VOLUME        IS NOT NULL
                OR  FFA.GROSS_VOLUME_MMBTU  IS NOT NULL
            )
            INNER JOIN 
            --        CORE_FINANCIAL.DT_2_UPSTREAM_LOS_REPORT_HIERARCHY AS LOS_CALCS
                (
                    SELECT        COUNT(1)
                                , FORMULA_GROUP
                                , HIERARCHY_GROUP_NAME
                                , MAX(LOS_CALCS.XREF_HIERARCHY_KEY)     AS XREF_HIERARCHY_KEY
                               
                                , LOS_CALCS.ACTIVE_FLAG                 AS ACTIVE_FLAG
                    FROM    CORE_FINANCIAL.DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY AS LOS_CALCS
                    WHERE 
                                LOS_CALCS.CALCULATION_FLAG      = TRUE
                        AND     LOS_CALCS.ACTIVE_FLAG           = TRUE
                        AND     LOS_CALCS.FORMULA_PRECEDENCE    = 1
                    GROUP BY 
                              FORMULA_GROUP
                            , HIERARCHY_GROUP_NAME
                           
                            , LOS_CALCS.ACTIVE_FLAG
                ) AS LOS_CALCS
            ON      LOS.FORMULA_GROUP = LOS_CALCS.FORMULA_GROUP
            AND     LOS.HIERARCHY_GROUP_NAME = LOS_CALCS.HIERARCHY_GROUP_NAME
            
            WHERE   LOS.CALCULATION_FLAG        = FALSE
            AND     LOS.ACTIVE_FLAG             = TRUE
            AND     LOS_CALCS.ACTIVE_FLAG       = TRUE
            AND     FFA.ROYALTY_FILTER          = 'N/A'
            AND     (           
                            FFA.GROSS_AMOUNT        IS NOT NULL
                        OR  FFA.GROSS_VOLUME        IS NOT NULL
                        OR  FFA.GROSS_VOLUME_MMBTU  IS NOT NULL
                     )
            GROUP BY
                      FFA.ACCOUNTING_DATE_KEY           
                    , FFA.SERVICE_DATE_KEY_M            
                    , FFA.SERVICE_DATE_KEY              
                    , FFA.CORP_HID  
                    , FFA.ALLOCATION_HID
                    , FFA.COST_CENTER_HID               
                    , FFA.ACCOUNT_ID                   
                    , FFA.BUSINESS_ASSOCIATE_HID        
                    , FFA.AFE_HID                       
                    , FFA.COUNTER_PARTY_HID             
                    , FFA.PURCHASER_HID                 
            
                    , FFA.DEDUCT_CODE                   
                    , FFA.PRODUCT_CODE                  
                    , FFA.PROD_CODE_COMPONENT           
                    , LOS.FORMULA_GROUP               
                    , LOS_CALCS.XREF_HIERARCHY_KEY
                    , LOS.HIERARCHY_GROUP_NAME
                    
                    , LOS.VOLUME_FLAG
                    , LOS.ARITHMETIC_LOGIC
                    , LOS.FORMULA_PARAM2_CONST_NUMBER
                    , LOS.SIGN_CHANGE_FLAG
           
            UNION ALL
            --GROSS VALUES FOR DEDUCT CODES
            SELECT          
                      IFF(LOS.VOLUME_FLAG = TRUE, NULL,  SUM(FFA.GROSS_AMOUNT) 
                                    * IFF( IFNULL(LOS.SIGN_CHANGE_FLAG,FALSE) = TRUE, -1,1))                AS GROSS_AMOUNT
                    , SUM(FFA.GROSS_VOLUME )* IFF(LOS.VOLUME_FLAG = TRUE, 
                                CASE UPPER(LOS.ARITHMETIC_LOGIC)
                                    WHEN 'MULTIPLY'
                                    THEN LOS.FORMULA_PARAM2_CONST_NUMBER
                                    WHEN 'DIVIDE'
                                    THEN 1/LOS.FORMULA_PARAM2_CONST_NUMBER
                                    ELSE 1
                                END
                            , NULL --IFF ELSE
                            )                                                                               AS GROSS_VOLUME
                    , SUM(FFA.GROSS_VOLUME )* IFF(LOS.VOLUME_FLAG = TRUE, 
                                CASE UPPER(LOS.ARITHMETIC_LOGIC)
                                    WHEN 'MULTIPLY'
                                    THEN LOS.FORMULA_PARAM2_CONST_NUMBER
                                    WHEN 'DIVIDE'
                                    THEN 1/LOS.FORMULA_PARAM2_CONST_NUMBER
                                    ELSE 1
                                END
                            , NULL --IFF ELSE
                            )                                                                               AS GROSS_MCFE
                    , SUM(GROSS_VOLUME_MMBTU)                                                               AS GROSS_VOLUME_MMBTU
                    , NULL                                                                                  AS GROSS_MCFE_MMBTU
                                                                       
                    , FFA.ACCOUNTING_DATE_KEY                                                               AS ACCOUNTING_DATE_KEY
                    , FFA.SERVICE_DATE_KEY_M                                                                AS SERVICE_DATE_KEY_M
                    , FFA.SERVICE_DATE_KEY                                                                  AS SERVICE_DATE_KEY
                    , FFA.CORP_HID                                                                          AS CORP_HID
                    , FFA.ALLOCATION_HID AS ALLOCATION_HID
                    , FFA.COST_CENTER_HID                                                                   AS COST_CENTER_HID
                    , FFA.ACCOUNT_ID                                                                        AS ACCOUNT_ID
                    , FFA.BUSINESS_ASSOCIATE_HID                                                            AS BUSINESS_ASSOCIATE_HID
                    , FFA.AFE_HID                                                                           AS AFE_HID
                    , FFA.COUNTER_PARTY_HID                                                                 AS COUNTER_PARTY_HID
                    , FFA.PURCHASER_HID                                                                     AS PURCHASER_HID                                         
                    , FFA.DEDUCT_CODE                                                                       AS DEDUCT_CODE
                    , FFA.PRODUCT_CODE                                                                      AS PRODUCT_CODE
                    , FFA.PROD_CODE_COMPONENT                                                               AS PROD_CODE_COMPONENT
                                                                
                    , LOS.FORMULA_GROUP                                                                     AS FORMULA_GROUP
                    , LOS.HIERARCHY_GROUP_NAME                                                              AS HIERARCHY_GROUP_NAME
                    , LOS_CALCS.XREF_HIERARCHY_KEY                                                          AS LOS_XREF_HIERARCHY_KEY
                    
            FROM    CORE_FINANCIAL.DT_1_FACT_FINANCIAL_ACTUALS AS FFA
            INNER JOIN
                    CORE_FINANCIAL.DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY AS LOS
            ON      TRIM(UPPER(FFA.DEDUCT_CODE))     =   TRIM(UPPER(LOS.DEDUCT_CODE))
            AND     TRIM(UPPER(FFA.PRODUCT_CODE))    =   TRIM(UPPER(IFNULL(LOS.PRODUCT_CODE, FFA.PRODUCT_CODE )))
            
            AND     (           
                            FFA.GROSS_AMOUNT        IS NOT NULL
                        OR  FFA.GROSS_VOLUME        IS NOT NULL
                        OR  FFA.GROSS_VOLUME_MMBTU  IS NOT NULL
                    )
            INNER JOIN 
            --        CORE_FINANCIAL.DT_2_UPSTREAM_LOS_REPORT_HIERARCHY AS LOS_CALCS
                (
                    SELECT        COUNT(1)
                                , FORMULA_GROUP
                                , HIERARCHY_GROUP_NAME
                                , MAX(LOS_CALCS.XREF_HIERARCHY_KEY)     AS XREF_HIERARCHY_KEY
                               
                                , LOS_CALCS.ACTIVE_FLAG                 AS ACTIVE_FLAG
                    FROM    CORE_FINANCIAL.DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY AS LOS_CALCS
                    WHERE 
                                LOS_CALCS.CALCULATION_FLAG      = TRUE
                        AND     LOS_CALCS.ACTIVE_FLAG           = TRUE
                        AND     LOS_CALCS.FORMULA_PRECEDENCE    = 1
                    GROUP BY 
                              FORMULA_GROUP
                            , HIERARCHY_GROUP_NAME
                         
                            , LOS_CALCS.ACTIVE_FLAG
                ) AS LOS_CALCS
            ON      LOS.FORMULA_GROUP = LOS_CALCS.FORMULA_GROUP
            AND     LOS.HIERARCHY_GROUP_NAME = LOS_CALCS.HIERARCHY_GROUP_NAME
            
            WHERE   LOS.CALCULATION_FLAG        = FALSE
            AND     LOS.ACTIVE_FLAG             = TRUE
            AND     LOS_CALCS.ACTIVE_FLAG       = TRUE
            AND     FFA.ROYALTY_FILTER          = 'N/A'
            GROUP BY
                      FFA.ACCOUNTING_DATE_KEY           
                    , FFA.SERVICE_DATE_KEY_M            
                    , FFA.SERVICE_DATE_KEY              
                    , FFA.CORP_HID                      
                    , FFA.COST_CENTER_HID               
                    , FFA.ACCOUNT_ID                 
                    , FFA.BUSINESS_ASSOCIATE_HID        
                    , FFA.AFE_HID 
                    , FFA.ALLOCATION_HID
                    , FFA.COUNTER_PARTY_HID             
                    , FFA.PURCHASER_HID                 
            
                    , FFA.DEDUCT_CODE                   
                    , FFA.PRODUCT_CODE                  
                    , FFA.PROD_CODE_COMPONENT           
                    , LOS.FORMULA_GROUP               
                    , LOS_CALCS.XREF_HIERARCHY_KEY
                    , LOS.HIERARCHY_GROUP_NAME  
                    
                    , LOS.VOLUME_FLAG
                    , LOS.ARITHMETIC_LOGIC
                    , LOS.FORMULA_PARAM2_CONST_NUMBER
                    , LOS.SIGN_CHANGE_FLAG
            UNION ALL
             --GROSS VALUES FOR ROYALTY PRODUCT CODES
            SELECT          
                      IFF(LOS.VOLUME_FLAG = TRUE, NULL,  SUM(FFA.GROSS_AMOUNT) 
                                    * IFF( IFNULL(LOS.SIGN_CHANGE_FLAG,FALSE) = TRUE, -1,1))                AS GROSS_AMOUNT
                    , SUM(FFA.GROSS_VOLUME )* IFF(LOS.VOLUME_FLAG = TRUE, 
                                CASE UPPER(LOS.ARITHMETIC_LOGIC)
                                    WHEN 'MULTIPLY'
                                    THEN LOS.FORMULA_PARAM2_CONST_NUMBER
                                    WHEN 'DIVIDE'
                                    THEN 1/LOS.FORMULA_PARAM2_CONST_NUMBER
                                    ELSE 1
                                END
                            , NULL --IFF ELSE
                            )                                                                               AS GROSS_VOLUME
                    , SUM(FFA.GROSS_VOLUME )* IFF(LOS.VOLUME_FLAG = TRUE, 
                                CASE UPPER(LOS.ARITHMETIC_LOGIC)
                                    WHEN 'MULTIPLY'
                                    THEN LOS.FORMULA_PARAM2_CONST_NUMBER
                                    WHEN 'DIVIDE'
                                    THEN 1/LOS.FORMULA_PARAM2_CONST_NUMBER
                                    ELSE 1
                                END
                            , NULL --IFF ELSE
                            )                                                                               AS GROSS_MCFE
                    , SUM(GROSS_VOLUME_MMBTU)                                                               AS GROSS_VOLUME_MMBTU
                    , NULL                                                                                  AS GROSS_MCFE_MMBTU
                                                                       
                    , FFA.ACCOUNTING_DATE_KEY                                                               AS ACCOUNTING_DATE_KEY
                    , FFA.SERVICE_DATE_KEY_M                                                                AS SERVICE_DATE_KEY_M
                    , FFA.SERVICE_DATE_KEY                                                                  AS SERVICE_DATE_KEY
                    , FFA.CORP_HID                                                                          AS CORP_HID
                    , FFA.ALLOCATION_HID AS ALLOCATION_HID
                    , FFA.COST_CENTER_HID                                                                   AS COST_CENTER_HID
                    , FFA.ACCOUNT_ID                                                                        AS ACCOUNT_ID
                    , FFA.BUSINESS_ASSOCIATE_HID                                                            AS BUSINESS_ASSOCIATE_HID
                    , FFA.AFE_HID                                                                           AS AFE_HID
                    , FFA.COUNTER_PARTY_HID                                                                 AS COUNTER_PARTY_HID
                    , FFA.PURCHASER_HID                                                                     AS PURCHASER_HID                                         
                    , FFA.DEDUCT_CODE                                                                       AS DEDUCT_CODE
                    , FFA.PRODUCT_CODE                                                                      AS PRODUCT_CODE
                    , FFA.PROD_CODE_COMPONENT                                                               AS PROD_CODE_COMPONENT
                                                                
                    , LOS.FORMULA_GROUP                                                                     AS FORMULA_GROUP
                    , LOS.HIERARCHY_GROUP_NAME                                                              AS HIERARCHY_GROUP_NAME
                    , LOS_CALCS.XREF_HIERARCHY_KEY                                                          AS LOS_XREF_HIERARCHY_KEY
                    
            FROM    CORE_FINANCIAL.DT_1_FACT_FINANCIAL_ACTUALS AS FFA
            INNER JOIN
                    CORE_FINANCIAL.DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY AS LOS
            ON      TRIM(UPPER(FFA.PRODUCT_CODE))   =   TRIM(UPPER(LOS.PRODUCT_CODE))
            AND     LOS.LOS_ROYALTY_FILTER          = FFA.ROYALTY_FILTER
            AND     (           
                            FFA.GROSS_AMOUNT        IS NOT NULL
                        OR  FFA.GROSS_VOLUME        IS NOT NULL
                        OR  FFA.GROSS_VOLUME_MMBTU  IS NOT NULL
                    )
            INNER JOIN 
            --        CORE_FINANCIAL.DT_2_UPSTREAM_LOS_REPORT_HIERARCHY AS LOS_CALCS
                (
                    SELECT        COUNT(1)
                                , FORMULA_GROUP
                                , HIERARCHY_GROUP_NAME
                                , MAX(LOS_CALCS.XREF_HIERARCHY_KEY)     AS XREF_HIERARCHY_KEY
                               
                                , LOS_CALCS.ACTIVE_FLAG                 AS ACTIVE_FLAG
                    FROM    CORE_FINANCIAL.DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY AS LOS_CALCS
                    WHERE 
                                LOS_CALCS.CALCULATION_FLAG      = TRUE
                        AND     LOS_CALCS.ACTIVE_FLAG           = TRUE
                        AND     LOS_CALCS.FORMULA_PRECEDENCE    = 1
                    GROUP BY 
                              FORMULA_GROUP
                            , HIERARCHY_GROUP_NAME
                         
                            , LOS_CALCS.ACTIVE_FLAG
                ) AS LOS_CALCS
            ON      LOS.FORMULA_GROUP = LOS_CALCS.FORMULA_GROUP
            AND     LOS.HIERARCHY_GROUP_NAME = LOS_CALCS.HIERARCHY_GROUP_NAME
            
            WHERE   LOS.CALCULATION_FLAG        = FALSE
            AND     LOS.ACTIVE_FLAG             = TRUE
            AND     LOS_CALCS.ACTIVE_FLAG       = TRUE
            AND     FFA.ROYALTY_FILTER          = 'Y'

            GROUP BY
                      FFA.ACCOUNTING_DATE_KEY           
                    , FFA.SERVICE_DATE_KEY_M            
                    , FFA.SERVICE_DATE_KEY              
                    , FFA.CORP_HID           
                    , FFA.ALLOCATION_HID
                    , FFA.COST_CENTER_HID               
                    , FFA.ACCOUNT_ID                 
                    , FFA.BUSINESS_ASSOCIATE_HID        
                    , FFA.AFE_HID                       
                    , FFA.COUNTER_PARTY_HID             
                    , FFA.PURCHASER_HID                 
            
                    , FFA.DEDUCT_CODE                   
                    , FFA.PRODUCT_CODE                  
                    , FFA.PROD_CODE_COMPONENT           
                    , LOS.FORMULA_GROUP               
                    , LOS_CALCS.XREF_HIERARCHY_KEY
                    , LOS.HIERARCHY_GROUP_NAME  
                    
                    , LOS.VOLUME_FLAG
                    , LOS.ARITHMETIC_LOGIC
                    , LOS.FORMULA_PARAM2_CONST_NUMBER
                    , LOS.SIGN_CHANGE_FLAG
                
;