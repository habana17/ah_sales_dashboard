CREATE
OR REPLACE PROCEDURE sp_ah_add_missing_batchno  
 
AS
/******************************************************************************

NAME:       sp_ah_add_missing_batchno
PURPOSE:    Insert missing batchno from daily. checking with monthly 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        02/09/2026           Francis              1. Create sp_ah_add_missing_batchno



NOTES:

 ******************************************************************************/
BEGIN 


    BEGIN

        BEGIN--STEP 1 INSERT ALL THE DATA OF THE MISSING BATCHNO TO temp_missing_batchno
            sp_ah_insert_missing_batchno; 
            sp_ah_update_flag_missing; --update flag if existing 1ST UPDATE
        END;

        BEGIN--STEP 2 INSERT ALL DATA OF TRAVEL AND AH  to TEMP_NLR_PORTFOLIO_PISC_DAILY_MIS
            DELETE FROM  TEMP_NLR_PORTFOLIO_PISC_DAILY_MIS;   
            commit;
            sp_ah_missing_ctrip; --PROCESS CTRIP TRAVEL DATA
            sp_ah_missing_othertravel; --PROCESS OTHER TRAVEL DATA
            sp_ah_missing_ac; --PROCESS AC DATA
        END;

        BEGIN --INSERT DATA TO NLR_PORTFOLIO_PISC_EIS_DAILY
            sp_ah_load_eis_missing; --PROCESS EIS DATA 
            sp_ah_update_flag_missing; --update flag if existing 2ND UPDATE IF ITS NOW EXISTING IN EIS 
        END;

        BEGIN --INSERT ALL MISSING DATA TO AH_TRANSACTIONS_DAILY AND TRAVEL_TRANSACTIONS_DAILY
            sp_ah_load_missing_tran; --PROCESS TO LOAD TO PROD TABLES 
            sp_ah_update_flag_missing; --update flag if existing 3RD UPDATE IF ITS NOW EXISTING IN PROD  
        END;

    END;    


END sp_ah_add_missing_batchno;