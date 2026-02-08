create or replace dynamic table TRANSFORMATION.CORE_FINANCIAL.(
	XREF_HIERARCHY_KEY,
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
	LOS_EXT_LEVEL_1_SORT,
	LOS_EXT_LEVEL_2_SORT,
	LOS_EXT_LEVEL_3_SORT,
	LOS_EXT_LEVEL_4_SORT,
	LOS_EXT_LEVEL_5_SORT,
	LOS_EXT_LEVEL_6_SORT,
	LOS_EXT_LEVEL_7_SORT,
	LOS_EXT_LEVEL_8_SORT,
	LOS_EXT_LEVEL_9_SORT,
	VOLUME_FLAG,
	ACCOUNT_ID,
	PRODUCT_CODE,
	DEDUCT_CODE,
	LOS_ROYALTY_FILTER,
	LEVEL_2_SORT,
	LEVEL_3_SORT,
	LEVEL_4_SORT,
	LEVEL_5_SORT,
	LEVEL_6_SORT,
	LEVEL_7_SORT,
	LEVEL_8_SORT,
	LEVEL_9_SORT,
	ACTIVE_FLAG,
	EXCLUSION_FLAG,
	CALCULATION_FLAG,
	ID_UNPIVOT_FLAG,
	ACCOUNT_MAJOR,
	ACCOUNT_MAJOR_CODE,
	ACCOUNT_MINOR,
	ACCOUNT_MINOR_CODE,
	ACCOUNT_CODE,
	ACCOUNT_NAME,
	ACCOUNT_CODE_PLUS_NAME,
	ACCOUNT_BILLING_CATEGORY_CODE,
	ACCOUNT_BILLING_CATEGORY_DESCRIPTION,
	ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME,
	FILTER_GROUP_1,
	FILTER_GROUP_2,
	FILTER_GROUP_3,
	FILTER_GROUP_4,
	FORMULA_GROUP,
	FORMULA_PRECEDENCE,
	FORMULA_PARAM_REF,
	ARITHMETIC_LOGIC,
	FORMULA_PARAM2_CONST_NUMBER,
	CREATE_NEW_COLUMN,
	SIGN_CHANGE_FLAG
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = TRANSFORMING_WH
 as 
    
    SELECT 
            ROW_NUMBER() OVER (ORDER BY 
                                              LEVEL_1
                                            , LEVEL_2
                                            , LEVEL_3
                                            , LEVEL_4
                                            , LEVEL_5
                                            , LEVEL_6
                                            , VOLUME_FLAG
                                            , ACCOUNT_ID
                                                                        ) AS XREF_HIERARCHY_KEY,
    	    HIERARCHY_GROUP_NAME,
            LEVEL_1,
        	LEVEL_2,
        	IFNULL(  LEVEL_3,            
            IFF( LEVEL_2 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME 
                    )
                )
                    AS LEVEL_3,
            IFNULL( LEVEL_4,       
            IFF( LEVEL_3 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME     
                    )
                )
                    AS LEVEL_4,
            IFNULL( LEVEL_5,         
            IFF( LEVEL_4 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME  
                    )
                )
                    AS LEVEL_5,
            IFNULL( LEVEL_6,          
            IFF( LEVEL_5 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME     
                    )
                )
                    AS LEVEL_6,
            IFNULL( LEVEL_7, IFF( LEVEL_6 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME 
                    )
                )      
            
                    AS LEVEL_7,
                IFNULL( LEVEL_8, IFF( LEVEL_7 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME 
                    )
                )      
            
                    AS LEVEL_8,
                 IFNULL( LEVEL_9, IFF( LEVEL_8 IS NULL
                        , ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                                , ACCOUNT_CODE_PLUS_NAME 
                    )
                )      
            
                    AS LEVEL_9,

            LOS_EXT_LEVEL_1,
            LOS_EXT_LEVEL_2,
            LOS_EXT_LEVEL_3,
            LOS_EXT_LEVEL_4,
            LOS_EXT_LEVEL_5,
            LOS_EXT_LEVEL_6,
            LOS_EXT_LEVEL_7,
            LOS_EXT_LEVEL_8,
            LOS_EXT_LEVEL_9,
            
            LOS_EXT_LEVEL_1_SORT,
            LOS_EXT_LEVEL_2_SORT,
            LOS_EXT_LEVEL_3_SORT,
            LOS_EXT_LEVEL_4_SORT,
            LOS_EXT_LEVEL_5_SORT,
            LOS_EXT_LEVEL_6_SORT,
            LOS_EXT_LEVEL_7_SORT,
            LOS_EXT_LEVEL_8_SORT,
            LOS_EXT_LEVEL_9_SORT,
            
        	VOLUME_FLAG,
        	ACCOUNT_ID,
            UPPER(TRIM(PRODUCT_CODE))                  AS PRODUCT_CODE,
            UPPER(TRIM(DEDUCT_CODE))                   AS DEDUCT_CODE,
            LOS_ROYALTY_FILTER                         AS LOS_ROYALTY_FILTER,
        	DENSE_RANK() OVER (ORDER BY LEVEL_2_SORT)  AS LEVEL_2_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_3_SORT)  AS LEVEL_3_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_4_SORT)  AS LEVEL_4_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_5_SORT)  AS LEVEL_5_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_6_SORT)  AS LEVEL_6_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_7_SORT)  AS LEVEL_7_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_8_SORT)  AS LEVEL_8_SORT,
        	DENSE_RANK() OVER (ORDER BY LEVEL_9_SORT)  AS LEVEL_9_SORT,
        	ACTIVE_FLAG,       	EXCLUSION_FLAG,
        	CALCULATION_FLAG,
        	ID_UNPIVOT_FLAG,
        	ACCOUNT_MAJOR,
        	ACCOUNT_MAJOR_CODE,
        	ACCOUNT_MINOR,
        	ACCOUNT_MINOR_CODE,
        	ACCOUNT_CODE,
        	ACCOUNT_NAME,
        	ACCOUNT_CODE_PLUS_NAME,
        	ACCOUNT_BILLING_CATEGORY_CODE,
        	ACCOUNT_BILLING_CATEGORY_DESCRIPTION,
        	ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME,
        	FILTER_GROUP_1,
        	FILTER_GROUP_2,
        	FILTER_GROUP_3,
        	FILTER_GROUP_4,
            FORMULA_GROUP,
            FORMULA_PRECEDENCE,	
            FORMULA_PARAM_REF,	
            ARITHMETIC_LOGIC,
            FORMULA_PARAM2_CONST_NUMBER,
            CREATE_NEW_COLUMN,
            SIGN_CHANGE_FLAG

    FROM 
        (
                SELECT  DISTINCT
                          HIERARCHY_GROUP_NAME
                        , LOS_R_H.LEVEL_1
                        , LOS_R_H.LEVEL_2
                        , LOS_R_H.LEVEL_3
                        , LOS_R_H.LEVEL_4
                        , LOS_R_H.LEVEL_5
                        , LOS_R_H.LEVEL_6
                        , LOS_R_H.LEVEL_7
                        , LOS_R_H.LEVEL_8
                        , LOS_R_H.LEVEL_9
                        
                        , LOS_R_H.VOLUME_FLAG
                        , ACCT.ACCOUNT_ID                       AS ACCOUNT_ID
                        , PROD.PRODUCT_CODE                     AS PRODUCT_CODE
                        , LOS_R_H.LOS_DEDUCT_CODE_FILTER        AS DEDUCT_CODE
                        , LOS_R_H.LOS_ROYALTY_FILTER            AS LOS_ROYALTY_FILTER
                        , LOS_R_H.LEVEL_2_SORT
                        , LOS_R_H.LEVEL_3_SORT
                        , LOS_R_H.LEVEL_4_SORT
                        , LOS_R_H.LEVEL_5_SORT
                        , LOS_R_H.LEVEL_6_SORT
                        , LOS_R_H.LEVEL_7_SORT
                        , LOS_R_H.LEVEL_8_SORT
                        , LOS_R_H.LEVEL_9_SORT
                        , LOS_R_H.ACTIVE_FLAG
                        , LOS_R_H.EXCLUSION_FLAG
                        , LOS_R_H.CALCULATION_FLAG
                        , LOS_R_H.ID_UNPIVOT_FLAG
                        , ACCT.ACCOUNT_MAJOR
                        , ACCT.ACCOUNT_MAJOR_CODE
                        , ACCT.ACCOUNT_MINOR
                        , ACCT.ACCOUNT_MINOR_CODE
                        , ACCT.ACCOUNT_CODE
                        , ACCT.ACCOUNT_NAME
                        , CONCAT(ACCT.ACCOUNT_CODE, ' - ', ACCT.ACCOUNT_NAME) AS ACCOUNT_CODE_PLUS_NAME
                        , ACCT.ACCOUNT_BILLING_CATEGORY_CODE
                        , ACCT.ACCOUNT_BILLING_CATEGORY_DESCRIPTION
                        , CONCAT(ACCT.ACCOUNT_BILLING_CATEGORY_CODE, ' - ', ACCT.ACCOUNT_BILLING_CATEGORY_DESCRIPTION) AS ACCOUNT_BILLING_CATEGORY_CODE_PLUS_NAME
                        , FILTER_GROUP_1
                        , FILTER_GROUP_2
                        , FILTER_GROUP_3
                        , FILTER_GROUP_4
                        , FORMULA_GROUP
                        , FORMULA_PRECEDENCE
                        , FORMULA_PARAM_REF
                        , ARITHMETIC_LOGIC
                        , FORMULA_PARAM2_CONST_NUMBER
                        , CREATE_NEW_COLUMN
                        , SIGN_CHANGE_FLAG

                        , LOS_EXT_LEVEL_1
                        , LOS_EXT_LEVEL_2
                        , LOS_EXT_LEVEL_3
                        , LOS_EXT_LEVEL_4
                        , LOS_EXT_LEVEL_5
                        , LOS_EXT_LEVEL_6
                        , LOS_EXT_LEVEL_7
                        , LOS_EXT_LEVEL_8
                        , LOS_EXT_LEVEL_9

                        , LOS_EXT_LEVEL_1_SORT
                        , LOS_EXT_LEVEL_2_SORT
                        , LOS_EXT_LEVEL_3_SORT
                        , LOS_EXT_LEVEL_4_SORT
                        , LOS_EXT_LEVEL_5_SORT
                        , LOS_EXT_LEVEL_6_SORT
                        , LOS_EXT_LEVEL_7_SORT
                        , LOS_EXT_LEVEL_8_SORT
                        , LOS_EXT_LEVEL_9_SORT

                FROM    
                        CORE_FINANCIAL.VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED   AS LOS_R_H
                LEFT OUTER JOIN
                        CORE_FINANCIAL.DT_1_DIM_ACCOUNT              AS ACCT
                ON      CASE    UPPER(LOS_R_H.ID_SOURCE)
                                    WHEN    'MAJOR_CODE'
                                    THEN    ACCT.ACCOUNT_MAJOR_CODE
                                    WHEN    'MINOR_CODE'
                                    THEN    ACCT.ACCOUNT_MINOR_CODE
                                  
                                    WHEN    'BILLING_CATEGORY_CODE'
                                    THEN    ACCT.ACCOUNT_BILLING_CATEGORY_CODE
                                    WHEN    'BILLING_CATEGORY_TYPE_CODE'
                                    THEN    ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE
                                    ELSE    ACCT.ACCOUNT_CODE
                                END 
                                                                            = LOS_R_H.LOS_ACCOUNT_ID_FILTER
                AND     UPPER(ACCT.ACCOUNT_NET_GROSS) = 'GROSS'
                LEFT OUTER JOIN
                        CORE_FINANCIAL.PRODUCT AS PROD
                ON      TRIM(UPPER(LOS_R_H.LOS_PRODUCT_CODE_FILTER)) = TRIM(UPPER(PROD.PRODUCT_CODE))
                
               
                WHERE
                        LOS_R_H.ACTIVE_FLAG = TRUE
                AND     LOS_R_H.EXCLUSION_FLAG = FALSE
                AND     LOS_R_H.CALCULATION_FLAG = FALSE
                AND     LOS_R_H.ID_UNPIVOT_FLAG = FALSE
                AND     LOS_R_H.ID_ROW_FLAG = TRUE
                AND     LOS_R_H.DO_NOT_EXPAND_FLAG = FALSE
                AND     IFNULL(LOS_R_H.PIVOT_SOURCE_COLUMN_NAME,'N/A') = 'N/A'
            
                AND     ACCT.ACCOUNT_ID 
                        NOT IN 
                        (
                                SELECT  
                                        DISTINCT ACCT2.ACCOUNT_ID
                                FROM    CORE_FINANCIAL.VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED       AS INTERNAL_LOS_R_H
                                JOIN
                                        CORE_FINANCIAL.DT_1_DIM_ACCOUNT              AS ACCT2
                                ON     CASE    UPPER(INTERNAL_LOS_R_H.ID_SOURCE)
                                            WHEN    'MAJOR_CODE'
                                            THEN    ACCT2.ACCOUNT_MAJOR_CODE
                                            WHEN    'MINOR_CODE'
                                            THEN    ACCT2.ACCOUNT_MINOR_CODE
                                            
                                            WHEN    'BILLING_CATEGORY_CODE'
                                            THEN    ACCT2.ACCOUNT_BILLING_CATEGORY_CODE
                                            WHEN    'BILLING_CATEGORY_TYPE_CODE'
                                            THEN    ACCT2.ACCOUNT_BILLING_CATEGORY_TYPE_CODE
                                            ELSE    ACCT2.ACCOUNT_CODE
                                        END 
                                                                                    = INTERNAL_LOS_R_H.LOS_ACCOUNT_ID_FILTER 
                                        
                                WHERE 
                                        INTERNAL_LOS_R_H.EXCLUSION_FLAG = TRUE
                                AND     INTERNAL_LOS_R_H.ACTIVE_FLAG = TRUE
                                AND     INTERNAL_LOS_R_H.CALCULATION_FLAG = FALSE
                                AND     INTERNAL_LOS_R_H.ID_UNPIVOT_FLAG = FALSE
                                AND     INTERNAL_LOS_R_H.ID_ROW_FLAG = TRUE
                                AND     INTERNAL_LOS_R_H.DO_NOT_EXPAND_FLAG = FALSE
                                AND     UPPER(INTERNAL_LOS_R_H.ID_TABLE) = UPPER(LOS_R_H.ID_TABLE)
                                AND     UPPER(INTERNAL_LOS_R_H.LEVEL_1) = UPPER(LOS_R_H.LEVEL_1)
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_2), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_2), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_3), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_3), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_4), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_4), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_5), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_5), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_6), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_6), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_7), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_7), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_8), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_8), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_9), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_9), 'X' )
                        )
               AND     PROD.PRODUCT_CODE NOT IN 
                        (
                                SELECT  
                                        DISTINCT PROD2.PRODUCT_CODE
                                FROM    CORE_FINANCIAL.VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED       AS INTERNAL_LOS_R_H
                                JOIN
                                        CORE_FINANCIAL.PRODUCT AS PROD2
                                ON      INTERNAL_LOS_R_H.LOS_PRODUCT_CODE_FILTER = PROD2.PRODUCT_CODE
                                        
                                WHERE 
                                        INTERNAL_LOS_R_H.EXCLUSION_FLAG = TRUE
                                AND     INTERNAL_LOS_R_H.ACTIVE_FLAG = TRUE
                                AND     INTERNAL_LOS_R_H.CALCULATION_FLAG = FALSE
                                AND     INTERNAL_LOS_R_H.ID_UNPIVOT_FLAG = FALSE
                                AND     INTERNAL_LOS_R_H.ID_ROW_FLAG = TRUE
                                AND     INTERNAL_LOS_R_H.DO_NOT_EXPAND_FLAG = FALSE
                                AND     UPPER(INTERNAL_LOS_R_H.ID_TABLE) = UPPER(LOS_R_H.ID_TABLE)
                                AND     UPPER(INTERNAL_LOS_R_H.LEVEL_1) = UPPER(LOS_R_H.LEVEL_1)
                                AND     INTERNAL_LOS_R_H.HIERARCHY_GROUP_NAME = LOS_R_H.HIERARCHY_GROUP_NAME
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_2), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_2), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_3), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_3), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_4), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_4), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_5), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_5), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_6), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_6), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_7), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_7), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_8), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_8), 'X' )
                                AND     IFNULL( UPPER(INTERNAL_LOS_R_H.LEVEL_9), 'X' ) = IFNULL( UPPER(LOS_R_H.LEVEL_9), 'X' )
                                
                        )  
                        

                UNION ALL  
                --ADDING ALL CALCULATION ROWS
                SELECT  DISTINCT
                          HIERARCHY_GROUP_NAME
                        , LOS_R_H.LEVEL_1
                        , LOS_R_H.LEVEL_2
                        , LOS_R_H.LEVEL_3
                        , LOS_R_H.LEVEL_4
                        , LOS_R_H.LEVEL_5
                        , LOS_R_H.LEVEL_6
                        , LOS_R_H.LEVEL_7
                        , LOS_R_H.LEVEL_8
                        , LOS_R_H.LEVEL_9
                        
                        , LOS_R_H.VOLUME_FLAG
                        , NULL AS ACCOUNT_ID
                        , NULL AS PRODUCT_CODE
                        , NULL AS DEDUCT_CODE
                        , NULL AS LOS_ROYALTY_FILTER
                        , LOS_R_H.LEVEL_2_SORT
                        , LOS_R_H.LEVEL_3_SORT
                        , LOS_R_H.LEVEL_4_SORT
                        , LOS_R_H.LEVEL_5_SORT
                        , LOS_R_H.LEVEL_6_SORT
                        , LOS_R_H.LEVEL_7_SORT
                        , LOS_R_H.LEVEL_8_SORT
                        , LOS_R_H.LEVEL_9_SORT
                        , LOS_R_H.ACTIVE_FLAG
                        , LOS_R_H.EXCLUSION_FLAG
                        , LOS_R_H.CALCULATION_FLAG
                        , LOS_R_H.ID_UNPIVOT_FLAG
                        , NULL AS MAJOR
                        , NULL AS MAJOR_CODE
                        , NULL AS MINOR
                        , NULL AS MINOR_CODE
                        , NULL AS ACCOUNT_CODE
                        , NULL AS ACCOUNT_NAME
                        , NULL AS ACCOUNT_CODE_PLUS_NAME
                        , NULL AS BILLING_CATEGORY_CODE
                        , NULL AS BILLING_CATEGORY_DESCRIPTION
                        , NULL AS BILLING_CATEGORY_CODE_PLUS_NAME
                        , FILTER_GROUP_1
                        , FILTER_GROUP_2
                        , FILTER_GROUP_3
                        , FILTER_GROUP_4
                        , FORMULA_GROUP
                        , FORMULA_PRECEDENCE
                        , FORMULA_PARAM_REF
                        , ARITHMETIC_LOGIC
                        , FORMULA_PARAM2_CONST_NUMBER
                        , CREATE_NEW_COLUMN
                        , SIGN_CHANGE_FLAG
                        
                        , 'N/A'     AS LOS_EXT_LEVEL_1
                        , 'N/A'     AS LOS_EXT_LEVEL_2
                        , 'N/A'     AS LOS_EXT_LEVEL_3
                        , 'N/A'     AS LOS_EXT_LEVEL_4
                        , 'N/A'     AS LOS_EXT_LEVEL_5
                        , 'N/A'     AS LOS_EXT_LEVEL_6
                        , 'N/A'     AS LOS_EXT_LEVEL_7
                        , 'N/A'     AS LOS_EXT_LEVEL_8
                        , 'N/A'     AS LOS_EXT_LEVEL_9
                        
                        , 0         AS LOS_EXT_LEVEL_1_SORT
                        , 0         AS LOS_EXT_LEVEL_2_SORT
                        , 0         AS LOS_EXT_LEVEL_3_SORT
                        , 0         AS LOS_EXT_LEVEL_4_SORT
                        , 0         AS LOS_EXT_LEVEL_5_SORT
                        , 0         AS LOS_EXT_LEVEL_6_SORT
                        , 0         AS LOS_EXT_LEVEL_7_SORT
                        , 0         AS LOS_EXT_LEVEL_8_SORT
                        , 0         AS LOS_EXT_LEVEL_9_SORT
                    
                FROM    
                        CONFIGURATION.TBL_0_GROSS_LOS_REPORT_HIERARCHY   AS LOS_R_H
            
                WHERE
                        LOS_R_H.ACTIVE_FLAG = TRUE
                AND     LOS_R_H.CALCULATION_FLAG = TRUE
        
        ) AS LOS
;