create
or replace PROCEDURE SP_AH_TRAVEL_DAILY --(p_date IN DATE)
AS 

p_date DATE := TRUNC(SYSDATE) - 1;

/******************************************************************************

NAME:       SP_AH_TRAVEL_DAILY
PURPOSE:    travel & ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/03/2025            Francis          1. Create SP_AH_TRAVEL_DAILY
2.0        09/12/2025            Francis          1. Each data insert should be followed by the execution of the EIS procedure.

NOTES:

 ******************************************************************************/

 BEGIN

---------------------------------------------------1--------------------------------------------------------

    -- Step 1: Clear existing rows but keep table structure intact
    DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;
    COMMIT;
    
    -- Step 2: Insert Ctrip data into TEMP_NLR_PORTFOLIO_PISC_DAILY (TRAVEL CTRIP)
    BEGIN
        SP_AH_TRAVEL_CTRIP_DAILY(p_date);
    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_TRAVEL_CTRIP_DAILY', err_msg,'AH Sales Dashboard CTRIP');
            COMMIT;
        END;    
    END; 

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
            VALUES ('SP_AH_LOAD_EIS', err_msg,'AH Sales Dashboard CTRIP EIS');
            COMMIT;
        END;    
    END;

---------------------------------------------------1.1--------------------------------------------------------


    -- Step 3: Insert Other Travel data into TEMP_NLR_PORTFOLIO_PISC_DAILY (TRAVEL OTHERS)

    DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;
    COMMIT;

    BEGIN
        SP_AH_TRAVEL_OTHERS_DAILY(p_date);
    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_TRAVEL_OTHERS_DAILY', err_msg,'AH Sales Dashboard OTHERS');
            COMMIT;
        END;    
    END; 


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
            VALUES ('SP_AH_LOAD_EIS', err_msg,'AH Sales Dashboard OTHERS EIS');
            COMMIT;
        END;    
    END;

    ---------------------------------------------------2--------------------------------------------------------

    -- Step 4: Insert AH data into TEMP_NLR_PORTFOLIO_PISC_DAILY (AH)
    --insert AH data to TEMP_NLR_PORTFOLIO_PISC_DAILY(AH)


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

   


    ---------------------------------------------------3--------------------------------------------------------

    -- Step 6: INSERT TRAVEL_TRANSACTIONS_DAILY
    BEGIN
        SP_AH_LOAD_TRAVELTRANSACTIONS(p_date);
    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_LOAD_TRAVELTRANSACTIONS', err_msg,'AH Sales Dashboard');
            COMMIT;
        END;    
    END;


    ---------------------------------------------------4--------------------------------------------------------

    -- Step 7: INSERT AH_TRANSACTIONS_DAILY
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
        VALUES ('SP_AH_TRAVEL_DAILY', err_msg, 'Main procedure failed');
        COMMIT; -- commit error log
    END;

  END SP_AH_TRAVEL_DAILY;
 