CREATE
OR REPLACE PROCEDURE sp_ah_sales_daily_hist AS 

TYPE t_days_back IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
v_days_back t_days_back;
p_date DATE;

/******************************************************************************

NAME:       sp_ah_sales_daily_hist
PURPOSE:    archiving data and refresh

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        10/17/2025       Francis          1. Create sp_ah_sales_daily_hist


NOTES:

 ******************************************************************************/   

  -- Local procedure for autonomous error logging
    PROCEDURE log_error(p_procedure_name VARCHAR2, p_err_msg VARCHAR2) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO process_error_log (procedure_name, error_message, remarks)
        VALUES (p_procedure_name, p_err_msg, 'AH Sales Dashboard Archiving');
        COMMIT;
    END log_error;
    

BEGIN

    -- Define the days back to process
    v_days_back(1) := 90;
    v_days_back(2) := 60;
    v_days_back(3) := 30;
    v_days_back(4) := 7; 

    -- Loop through each date
    FOR i IN 1..v_days_back.COUNT LOOP
    
        v_date := TRUNC(SYSDATE) - v_days_back(i);

---------------------------------------------------1--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO medicash_or_nonpolicy_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM medicash_or_nonpolicy_daily d
        WHERE 1=1
        AND TRUNC(ordate) = p_date;

        -- Delete data from source table
        DELETE FROM medicash_or_nonpolicy_daily
        WHERE 1=1
        AND TRUNC(ordate) = p_date;  

    BEGIN
        SP_AH_MEDICASH_OR_NONPOLICY_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_MEDICASH_OR_NONPOLICY_DAILY', SQLERRM);        
    END;
---------------------------------------------------2--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO medicash_or_ret_policy_det_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM medicash_or_ret_policy_det_daily d
        WHERE 1=1
        AND TRUNC(ordate) = p_date;

        -- Delete data from source table
        DELETE FROM medicash_or_ret_policy_det_daily
        WHERE 1=1
        AND TRUNC(ordate) = p_date;  


    BEGIN
        SP_AH_MEDICASH_OR_RETAIL_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_MEDICASH_OR_RETAIL_DAILY', SQLERRM); 
    END;
---------------------------------------------------3-------------------------------------------------------- 

    -- Insert data into history table
        INSERT INTO medicash_or_d2c_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM medicash_or_d2c_daily d
        WHERE 1=1
        AND TRUNC(ordate) = p_date;

        -- Delete data from source table
        DELETE FROM medicash_or_d2c_daily
        WHERE 1=1
        AND TRUNC(ordate) = p_date;  

    BEGIN
        SP_AH_MEDICASH_OR_D2C_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_MEDICASH_OR_D2C_DAILY', SQLERRM); 
    END;
---------------------------------------------------4-------------------------------------------------------- 

    -- Insert data into history table
        INSERT INTO ooopsie_cardsales_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM ooopsie_cardsales_daily d
        WHERE 1=1
        AND TRUNC(paymentdate) = p_date;

        -- Delete data from source table
        DELETE FROM ooopsie_cardsales_daily
        WHERE 1=1
        AND TRUNC(paymentdate) = p_date; 

    BEGIN
        SP_AH_OOOPSIE_CARDSALES_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_OOOPSIE_CARDSALES_DAILY', SQLERRM);  
    END;
---------------------------------------------------5--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO ooops_sale_extract_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM ooops_sale_extract_daily d
        WHERE 1=1
        AND TRUNC(paymentdate) = p_date;

        -- Delete data from source table
        DELETE FROM ooops_sale_extract_daily
        WHERE 1=1
        AND TRUNC(paymentdate) = p_date; 

    BEGIN
        SP_AH_OOOPS_SALE_EXTRACT_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_OOOPS_SALE_EXTRACT_DAILY', SQLERRM);  
    END;

---------------------------------------------------6--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO elife_polstat_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM elife_polstat_daily d
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

        -- Delete data from source table
        DELETE FROM elife_polstat_daily
        WHERE 1=1
        AND TRUNC(timestmp) = p_date; 
 
    BEGIN
        SP_AH_ELIFE_POLSTAT_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_ELIFE_POLSTAT_DAILY', SQLERRM);   
    END;        

---------------------------------------------------7--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO nlr_polstat_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM nlr_polstat_daily d
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

        -- Delete data from source table
        DELETE FROM nlr_polstat_daily
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

    BEGIN
        SP_AH_NLR_POLSTAT_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_NLR_POLSTAT_DAILY', SQLERRM);
    END;       
---------------------------------------------------8--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO nlr_paidprem_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM nlr_paidprem_daily d
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

        -- Delete data from source table
        DELETE FROM nlr_paidprem_daily
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

    BEGIN
        SP_AH_NLR_PAIDPREM_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_NLR_PAIDPREM_DAILY', SQLERRM);
    END; 
---------------------------------------------------9--------------------------------------------------------

    -- Insert data into history table
        INSERT INTO bcc_paidprem_daily_hist
        SELECT 
            d.*,
            SYSDATE as archive_date
        FROM bcc_paidprem_daily d
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

        -- Delete data from source table
        DELETE FROM bcc_paidprem_daily
        WHERE 1=1
        AND TRUNC(timestmp) = p_date;

    BEGIN
        SP_AH_BCC_PAIDPREM_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_BCC_PAIDPREM_DAILY', SQLERRM);
    END; 
---------------------------------------------------10--------------------------------------------------------
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
        VALUES ('sp_ah_sales_daily_hist', err_msg, 'Main procedure failed');
        COMMIT;
    END;    

END sp_ah_sales_daily_hist;