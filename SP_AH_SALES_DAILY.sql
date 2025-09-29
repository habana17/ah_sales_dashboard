CREATE
OR REPLACE PROCEDURE SP_AH_SALES_DAILY AS 

v_date DATE := TRUNC(SYSDATE) - 1;

/******************************************************************************

NAME:       SP_AH_SALES_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        06/11/2025       Francis          1. Create SP_AH_SALES_DAILY


NOTES:

 ******************************************************************************/   

  -- Local procedure for autonomous error logging
    PROCEDURE log_error(p_procedure_name VARCHAR2, p_err_msg VARCHAR2) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO process_error_log (procedure_name, error_message, remarks)
        VALUES (p_procedure_name, p_err_msg, 'AH Sales Dashboard');
        COMMIT;
    END log_error;
    

BEGIN
---------------------------------------------------1--------------------------------------------------------
    BEGIN
        SP_AH_MEDICASH_OR_NONPOLICY_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_MEDICASH_OR_NONPOLICY_DAILY', SQLERRM);        
    END;
---------------------------------------------------2--------------------------------------------------------
    BEGIN
        SP_AH_MEDICASH_OR_RETAIL_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_MEDICASH_OR_RETAIL_DAILY', SQLERRM); 
    END;
---------------------------------------------------3--------------------------------------------------------  
    BEGIN
        SP_AH_MEDICASH_OR_D2C_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_MEDICASH_OR_D2C_DAILY', SQLERRM); 
    END;
---------------------------------------------------4-------------------------------------------------------- 
    BEGIN
        SP_AH_OOOPSIE_CARDSALES_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_OOOPSIE_CARDSALES_DAILY', SQLERRM);  
    END;
---------------------------------------------------5--------------------------------------------------------
    BEGIN
        SP_AH_OOOPS_SALE_EXTRACT_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_OOOPS_SALE_EXTRACT_DAILY', SQLERRM);  
    END;

---------------------------------------------------6--------------------------------------------------------
 
    BEGIN
        SP_AH_ELIFE_POLSTAT_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_ELIFE_POLSTAT_DAILY', SQLERRM);   
    END;        

---------------------------------------------------7--------------------------------------------------------
    BEGIN
        SP_AH_NLR_POLSTAT_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_NLR_POLSTAT_DAILY', SQLERRM);
    END;       
---------------------------------------------------8--------------------------------------------------------
    BEGIN
        SP_AH_NLR_PAIDPREM_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_NLR_PAIDPREM_DAILY', SQLERRM);
    END; 
---------------------------------------------------9--------------------------------------------------------
    BEGIN
        SP_AH_BCC_PAIDPREM_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_AH_BCC_PAIDPREM_DAILY', SQLERRM);
    END; 
---------------------------------------------------10--------------------------------------------------------
    BEGIN
        SP_MPII_SALES_DAILY(v_date);
    EXCEPTION
            WHEN OTHERS THEN
            log_error('SP_MPII_SALES_DAILY', SQLERRM); 
    END;    

---------------------------------------------------11--------------------------------------------------------
    --transfer to another schedule at 6 am
    -- BEGIN
    --     SP_AH_TRAVEL_DAILY(v_date);
    -- EXCEPTION
    --         WHEN OTHERS THEN
    --     DECLARE
    --         err_msg VARCHAR2(4000);
    --     BEGIN
    --         err_msg := SQLERRM;
    --         INSERT INTO process_error_log (procedure_name, error_message,remarks)
    --         VALUES ('SP_AH_TRAVEL_DAILY', err_msg,'AH Sales Dashboard');
    --         COMMIT;
    --     END;    
    -- END;  

    -- null;

    COMMIT;

END SP_AH_SALES_DAILY;