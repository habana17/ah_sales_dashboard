CREATE OR REPLACE PROCEDURE sp_ah_travel_daily_hist
AS 

TYPE t_days_back IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
v_days_back t_days_back;
p_date DATE;

/******************************************************************************

NAME:       SP_AH_TRAVEL_DAILY_HIST
PURPOSE:    travel & ah transactions history processing for multiple dates

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        10/10/2025            Francis          1. Create sp_ah_travel_daily_hist
1.1        10/10/2025            Francis          2. Added loop for multiple dates
2.0        10/13/2025            Francis          2. added -7 refresh
NOTES:

 ******************************************************************************/

BEGIN

    -- Define the days back to process
    v_days_back(1) := 90;
    v_days_back(2) := 60;
    v_days_back(3) := 30;
    v_days_back(4) := 7; --added by francis 10132025

    -- Loop through each date
    FOR i IN 1..v_days_back.COUNT LOOP
    
        p_date := TRUNC(SYSDATE) - v_days_back(i);
        

---------------------------------------------------ARCHIVE & DELETE--------------------------------------------------------

        -- Insert data into history table
        INSERT INTO NLR_PORTFOLIO_PISC_EIS_DAILY_HIST
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM NLR_PORTFOLIO_PISC_EIS_DAILY d
        WHERE line_pref = 'GA'
        AND TRUNC(trandate) = p_date;

        -- Delete data from source table
        DELETE FROM NLR_PORTFOLIO_PISC_EIS_DAILY
        WHERE line_pref = 'GA'
        AND TRUNC(trandate) = p_date;  

        -- Insert travel data into history table
        INSERT INTO TRAVEL_TRANSACTIONS_DAILY_HIST
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM TRAVEL_TRANSACTIONS_DAILY d
        WHERE TRUNC(trandate) = p_date;

        -- Delete travel data from source table
        DELETE FROM TRAVEL_TRANSACTIONS_DAILY
        WHERE TRUNC(trandate) = p_date;

---------------------------------------------------PROCESS DATA--------------------------------------------------------

        -- Clear temp table
        DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;
        
        -- Process Ctrip data
        BEGIN
            SP_AH_TRAVEL_CTRIP_DAILY(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_TRAVEL_CTRIP_DAILY', err_msg, 'AH Sales Dashboard CTRIP - ' || v_days_back(i) || ' days');
                COMMIT;
            END;    
        END; 

        -- Run EIS
        BEGIN
            SP_AH_LOAD_EIS(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_LOAD_EIS', err_msg, 'AH Sales Dashboard CTRIP EIS - ' || v_days_back(i) || ' days');
                COMMIT;
            END;    
        END;

        -- Clear temp table
        DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;

        -- Process Other Travel data
        BEGIN
            SP_AH_TRAVEL_OTHERS_DAILY(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_TRAVEL_OTHERS_DAILY', err_msg, 'AH Sales Dashboard OTHERS - ' || v_days_back(i) || ' days');
                COMMIT;
            END;    
        END; 

        -- Run EIS for Others
        BEGIN
            SP_AH_LOAD_EIS(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_LOAD_EIS', err_msg, 'AH Sales Dashboard OTHERS EIS - ' || v_days_back(i) || ' days');
                COMMIT;
            END;    
        END;

        -- Load Travel Transactions
        BEGIN
            SP_AH_LOAD_TRAVELTRANSACTIONS(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_LOAD_TRAVELTRANSACTIONS', err_msg, 'AH Sales Dashboard - ' || v_days_back(i) || ' days');
                COMMIT;
            END;    
        END;

    END LOOP;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
    DECLARE
        err_msg VARCHAR2(4000);
    BEGIN        
        ROLLBACK;
        err_msg := SQLERRM;
        INSERT INTO process_error_log (procedure_name, error_message, remarks)
        VALUES ('SP_AH_TRAVEL_DAILY_HIST', err_msg, 'Main procedure failed');
        COMMIT;
    END;

END sp_ah_travel_daily_hist;
/