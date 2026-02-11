CREATE
OR REPLACE PROCEDURE sp_ah_update_flag_missing
AS
/******************************************************************************

NAME:       sp_ah_update_flag_missing
PURPOSE:    update the flag for eis and prod 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        02/09/2026           Francis              1. Create sp_ah_update_flag_missing



NOTES:

 ******************************************************************************/
BEGIN 


    BEGIN
        -- adw_prod_tgt.sp_adw_table_logs('temp_missing_batchno','sp_ah_add_missing_batchno',SYSDATE,'','DELETE');


        -- adw_prod_tgt.sp_adw_table_logs('temp_missing_batchno','sp_ah_add_missing_batchno',SYSDATE,'','INSERT');


        --STEP 1 
        BEGIN

            -- Update eis_flag to 'Y' if batchno exists in NLR_PORTFOLIO_PISC_EIS_DAILY
            UPDATE TEMP_MISSING_BATCHNO t
            SET t.eis_flag = 'Y',last_update_date = sysdate
            WHERE EXISTS (
                SELECT 1
                FROM NLR_PORTFOLIO_PISC_EIS_DAILY e
                WHERE e.batchno = t.batchno
                AND e.line_pref = t.line_pref
                        )
                AND t.eis_flag is null
            ;

            -- Update prod_flag to 'Y' if batchno exists in travel_transactions_daily
            UPDATE TEMP_MISSING_BATCHNO t
            SET t.prod_flag = 'Y',last_update_date = sysdate
            WHERE EXISTS (
                SELECT 1
                FROM travel_transactions_daily ttd
                WHERE ttd.batchno = t.batchno
                        )
                AND t.prod_flag is null
            ;

            -- Update prod_flag to 'Y' if batchno exists in ah_transactions_daily
            UPDATE TEMP_MISSING_BATCHNO t
            SET t.prod_flag = 'Y', last_update_date = sysdate
            WHERE EXISTS (
                SELECT 1
                FROM ah_transactions_daily ahd
                WHERE ahd.batchno = t.batchno
                        )
                AND t.prod_flag is null
            ;

            COMMIT;



        END;

    END;    


END sp_ah_update_flag_missing;        