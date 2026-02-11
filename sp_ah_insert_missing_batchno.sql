CREATE
OR REPLACE PROCEDURE sp_ah_insert_missing_batchno
AS
/******************************************************************************

NAME:       sp_ah_insert_missing_batchno
PURPOSE:    Insert missing batchno from daily. checking with monthly 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        02/09/2026           Francis              1. Create sp_ah_insert_missing_batchno



NOTES:

 ******************************************************************************/
BEGIN 


    BEGIN

        
        BEGIN --STEP 1 INSERT ALL THE DATA OF THE MISSING BATCHNO

            --FOR TRAVEL
            INSERT INTO
            temp_missing_batchno (
            batchno,
            line_pref,
            creation_date,
            last_update_date
            
            )
            SELECT batchno ,'GA' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM nlr_portfolio_pisc_eis_iris 
            WHERE acctg_year = EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1))
            AND acctg_mon = EXTRACT(MONTH FROM ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1))
            AND line_pref = 'GA'
        
            MINUS
        
            SELECT batchno,'GA' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM travel_transactions_daily 
        
            MINUS
        
            SELECT batchno,'GA' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM NLR_PORTFOLIO_PISC_EIS_DAILY
            WHERE line_pref = 'GA'

            MINUS 
            
            SELECT batchno,'GA' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM temp_missing_batchno
            WHERE line_pref = 'GA'
            ;

            commit;
    

            --FOR AH   
            INSERT INTO
            temp_missing_batchno (
            batchno,
            line_pref,
            creation_date,
            last_update_date
            
            )
            SELECT batchno ,'AC' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM nlr_portfolio_pisc_eis_iris 
            WHERE acctg_year = EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1))
            AND acctg_mon = EXTRACT(MONTH FROM ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1))
            AND line_pref = 'AC'
        
            MINUS
        
            SELECT batchno,'AC' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM ah_transactions_daily 
        
            MINUS
        
            SELECT batchno,'AC' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM NLR_PORTFOLIO_PISC_EIS_DAILY
            WHERE line_pref = 'AC'
            -- AND TRUNC(trandate) >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -2) + 25
            -- AND TRUNC(trandate) < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1) + 25

            MINUS 
            
            SELECT batchno,'AC' AS line_pref,sysdate as creation_date,sysdate as last_update_date
            FROM temp_missing_batchno
            WHERE line_pref = 'AC'

            ;

            commit;

        END;

    END;    


END sp_ah_insert_missing_batchno;        