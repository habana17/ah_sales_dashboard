DECLARE 
    CURSOR x IS 
        SELECT calendar_date 
        FROM adw_prod_tgt.adw_ref_calendar 
        WHERE 1 = 1 
        AND calendar_date BETWEEN '11/01/2025' AND '11/01/2025' 
--        AND calendar_date in  
--        ( 
--        ) 
        ORDER BY 1 
        ; 

--04012024 update policy holder later 

BEGIN 


    FOR rec IN x 

    LOOP 

--              adw_prod_tgt.SP_AH_SALES_DAILY; 
--            adw_prod_tgt.SP_AH_TRAVEL_DAILY; 


-----------------------------------------AH-----------------------------------
         delete
         from NLR_PORTFOLIO_PISC_EIS_DAILY
         where line_pref = 'AC'
         and trunc(trandate) = rec.calendar_date;
       
         delete
         from ah_transactions_daily
         where 1=1 
         and trunc(trandate) =  rec.calendar_date;
         commit;
        adw_prod_tgt.SP_AH_AHDETAILS_DAILY(rec.calendar_date); 
        adw_prod_tgt.SP_AH_LOAD_EIS(rec.calendar_date); 
        adw_prod_tgt.SP_AH_LOAD_AHTRANSACTIONS(rec.calendar_date); 

 ------------------------------------------TRavel---------------------- 
--         delete
--         from NLR_PORTFOLIO_PISC_EIS_DAILY
--         where line_pref = 'GA'
--         and trunc(trandate) = rec.calendar_date;
--       
--         delete
--         from travel_transactions_daily
--         where 1=1 
--         and trunc(trandate) =  rec.calendar_date;
--  
--         DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY; 
--         COMMIT; 
--        adw_prod_tgt.SP_AH_TRAVEL_CTRIP_DAILY(rec.calendar_date);
--        adw_prod_tgt.SP_AH_LOAD_EIS(rec.calendar_date); 
--        DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY; 
--         COMMIT; 
--        adw_prod_tgt.SP_AH_TRAVEL_OTHERS_DAILY(rec.calendar_date); 
--        adw_prod_tgt.SP_AH_LOAD_EIS(rec.calendar_date); 
--        adw_prod_tgt.SP_AH_LOAD_TRAVELTRANSACTIONS(rec.calendar_date); 

  -----------------------------------------------------------misc------------------------------------
--         adw_prod_tgt.SP_AH_MEDICASH_OR_NONPOLICY_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_MEDICASH_OR_RETAIL_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_MEDICASH_OR_D2C_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_OOOPSIE_CARDSALES_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_OOOPS_SALE_EXTRACT_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_ELIFE_POLSTAT_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_NLR_POLSTAT_DAILY(rec.calendar_date); 
--         adw_prod_tgt.SP_AH_NLR_PAIDPREM_DAILY(rec.calendar_date); 
--           adw_prod_tgt.SP_AH_BCC_PAIDPREM_DAILY(rec.calendar_date); 

--        adw_prod_tgt.SP_MPII_SALES_DAILY(rec.calendar_date); 

    END LOOP; 

END; 

/ 