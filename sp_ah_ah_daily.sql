
create
or replace PROCEDURE SP_AH_AH_DAILY --(p_date IN DATE)
AS 

p_date DATE := TRUNC(SYSDATE) - 1;

/******************************************************************************

NAME:       SP_AH_TRAVEL_DAILY
PURPOSE:    ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        09/29/2025            Francis          1. Create SP_AH_AH_DAILY

NOTES:

 ******************************************************************************/

 BEGIN
    
    
    
---------------------------------------------------4-------------------------------------------------------- 
    
    DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;
    COMMIT;

    BEGIN
        SP_AH_AHDETAILS_DAILY(p_date);
    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_AHDETAILS_DAILY', err_msg,'AH Sales Dashboard AH DATA');
            COMMIT;
        END;    
    END; 

---------------------------------------------------4-------------------------------------------------------- 

    ---run EIS 
    BEGIN
        SP_AH_LOAD_EIS(p_date);
    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_LOAD_EIS', err_msg,'AH Sales Dashboard AH DATA EIS');
            COMMIT;
        END;    
    END;

---------------------------------------------------4-------------------------------------------------------- 
        BEGIN
        SP_AH_LOAD_AHTRANSACTIONS(p_date);
    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_LOAD_AHTRANSACTIONS', err_msg,'AH Sales Dashboard');
            COMMIT;
        END;    
    END;


    COMMIT;

    EXCEPTION
    WHEN OTHERS THEN
    DECLARE
            err_msg VARCHAR2(4000);
    BEGIN        
        ROLLBACK; -- rollback everything if the master procedure itself fails unexpectedly
        INSERT INTO process_error_log (procedure_name, error_message, remarks)
        VALUES ('SP_AH_AH_DAILY', err_msg, 'Main procedure failed');
        COMMIT; -- commit error log
    END;

  END SP_AH_AH_DAILY;