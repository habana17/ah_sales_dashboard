CREATE OR REPLACE PROCEDURE sp_ah_ah_daily_hist
AS 

TYPE t_days_back IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
v_days_back t_days_back;
p_date DATE;

/******************************************************************************

NAME:       sp_ah_ah_daily_hist
PURPOSE:    ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        10/10/2025            Francis          1. Create sp_ah_ah_daily_hist
2.0        10/13/2025            Francis          2. added -7 refresh
3.0        12/12/2025            Francis          2. added 7 days of refresh per week  

NOTES:

 ******************************************************************************/

BEGIN
    
    -- Define the days back to process
    v_days_back(1) := 7; -- updated by francis 12122025
    v_days_back(2) := 6;
    v_days_back(3) := 5;
    v_days_back(4) := 4; 
    v_days_back(5) := 3;
    v_days_back(6) := 2;
    v_days_back(7) := 1;

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
        WHERE line_pref = 'AC'
        AND TRUNC(trandate) = p_date;

        -- Delete data from source table
        DELETE FROM NLR_PORTFOLIO_PISC_EIS_DAILY
        WHERE line_pref = 'AC'
        AND TRUNC(trandate) = p_date;  

        -- Insert AH data into history table
        INSERT INTO AH_TRANSACTIONS_DAILY_HIST
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM AH_TRANSACTIONS_DAILY d
        WHERE TRUNC(trandate) = p_date;

        -- Delete AH data from source table
        DELETE FROM AH_TRANSACTIONS_DAILY
        WHERE TRUNC(trandate) = p_date;

---------------------------------------------------PROCESS DATA-------------------------------------------------------- 
    
        DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;

        BEGIN
            SP_AH_AHDETAILS_DAILY(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_AHDETAILS_DAILY', err_msg, 'AH Sales Dashboard AH DATA - ' || v_days_back(i) || ' days');
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
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_LOAD_EIS', err_msg, 'AH Sales Dashboard AH DATA EIS - ' || v_days_back(i) || ' days');
                COMMIT;
            END;    
        END;

        BEGIN
            SP_AH_LOAD_AHTRANSACTIONS(p_date);
        EXCEPTION
            WHEN OTHERS THEN
            DECLARE
                err_msg VARCHAR2(4000);
            BEGIN
                err_msg := SQLERRM;
                INSERT INTO process_error_log (procedure_name, error_message, remarks)
                VALUES ('SP_AH_LOAD_AHTRANSACTIONS', err_msg, 'AH Sales Dashboard - ' || v_days_back(i) || ' days');
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
        VALUES ('SP_AH_AH_DAILY_HIST', err_msg, 'Main procedure failed');
        COMMIT;
    END;

END sp_ah_ah_daily_hist;
/