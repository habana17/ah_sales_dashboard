CREATE OR REPLACE PROCEDURE sp_load_nlr_billing_hist AS
BEGIN
    -- Step 1: Truncate and load today's data into staging
    EXECUTE IMMEDIATE 'TRUNCATE TABLE nlr_billing_stg';
    
    INSERT INTO nlr_billing_stg
    SELECT 
        BILLSEQNO, BILLSTAT, CANCELDATE, REMARKS, LIFECNT, PAYMODE,
        ISSUEDATE, BILLEFFDTE, DUEDATE, NETPREM, PREMTAX, LGT,
        DOCSTAMPS, ADMINFEE, OTHERCHARGES, COMMAMT, COMMWTAX,
        SFEEAMT, SFEETAX, TOTAMTDUE, USERID, TIMESTMP, POLNO,
        POLYER, OSBALANCE, COMMWTAXAMT, SFEEWTAXAMT, SUBSIDIARY,
        BATCHNO, REINSTAG, TOTSI, DISCOUNTED_AMT, DISCOUNT_VALUE,
        DISCOUNT_RATE, PROMOCODE, FOREX, VATAMT, VATRATE, FULLBILL,
        APPL_NO, COMMRATE, SFEERATE, OLD_GROSSPREM, NEW_GROSSPREM,
        OLDBILL_NO, DFIXTAG, REFNO,
        -- Generate hash for change detection - Simple approach without DBMS_CRYPTO
        ORA_HASH(
            NVL(TO_CHAR(BILLSTAT),'') || '|' ||
            NVL(TO_CHAR(CANCELDATE,'YYYY-MM-DD'),'') || '|' ||
            NVL(TO_CHAR(REMARKS),'') || '|' ||
            NVL(TO_CHAR(NETPREM),'') || '|' ||
            NVL(TO_CHAR(PREMTAX),'') || '|' ||
            NVL(TO_CHAR(LGT),'') || '|' ||
            NVL(TO_CHAR(DOCSTAMPS),'') || '|' ||
            NVL(TO_CHAR(ADMINFEE),'') || '|' ||
            NVL(TO_CHAR(OTHERCHARGES),'') || '|' ||
            NVL(TO_CHAR(COMMAMT),'') || '|' ||
            NVL(TO_CHAR(COMMWTAX),'') || '|' ||
            NVL(TO_CHAR(SFEEAMT),'') || '|' ||
            NVL(TO_CHAR(SFEETAX),'') || '|' ||
            NVL(TO_CHAR(TOTAMTDUE),'') || '|' ||
            NVL(TO_CHAR(TIMESTMP,'YYYY-MM-DD'),'') || '|' ||
            NVL(TO_CHAR(OSBALANCE),'') || '|' ||
            NVL(TO_CHAR(COMMWTAXAMT),'') || '|' ||
            NVL(TO_CHAR(SFEEWTAXAMT),'') || '|' ||
            NVL(TO_CHAR(TOTSI),'') || '|' ||
            NVL(TO_CHAR(DISCOUNTED_AMT),'') || '|' ||
            NVL(TO_CHAR(DISCOUNT_VALUE),'') || '|' ||
            NVL(TO_CHAR(DISCOUNT_RATE),'') || '|' ||
            NVL(TO_CHAR(PROMOCODE),'') || '|' ||
            NVL(TO_CHAR(FOREX),'') || '|' ||
            NVL(TO_CHAR(VATAMT),'') || '|' ||
            NVL(TO_CHAR(VATRATE),'') || '|' ||
            NVL(TO_CHAR(COMMRATE),'') || '|' ||
            NVL(TO_CHAR(SFEERATE),'') || '|' ||
            NVL(TO_CHAR(OLD_GROSSPREM),'') || '|' ||
            NVL(TO_CHAR(NEW_GROSSPREM),'') || '|' ||
            NVL(TO_CHAR(DFIXTAG),'') || '|' ||
            NVL(POLNO,'') || '|' ||
            NVL(TO_CHAR(BILLSTAT),'')
            -- Add other critical fields
        ) as record_hash,
        TRUNC(SYSDATE) as load_date
    FROM nlr_billing_mst_v2;
    
    COMMIT;
    
    -- Step 2: Mark DELETED records
    UPDATE nlr_billing_hist h
    SET 
        effective_to_date = TRUNC(SYSDATE) - 1,
        is_current = 'N',
        record_status = 'DELETED',
        dw_update_timestamp = SYSTIMESTAMP
    WHERE h.is_current = 'Y'
      AND NOT EXISTS (
          SELECT 1 FROM nlr_billing_stg s 
          WHERE s.BILLSEQNO = h.BILLSEQNO
      );
    
    COMMIT;
    
    -- Step 3: Mark MODIFIED records
    UPDATE nlr_billing_hist h
    SET 
        effective_to_date = TRUNC(SYSDATE) - 1,
        is_current = 'N',
        record_status = 'MODIFIED',
        dw_update_timestamp = SYSTIMESTAMP
    WHERE h.is_current = 'Y'
      AND EXISTS (
          SELECT 1 FROM nlr_billing_stg s 
          WHERE s.BILLSEQNO = h.BILLSEQNO
            AND s.record_hash != h.record_hash
      );
    
    COMMIT;
    
    -- Step 4: Insert NEW and MODIFIED records
    INSERT INTO nlr_billing_hist (
        BILLSEQNO, BILLSTAT, CANCELDATE, REMARKS, LIFECNT, PAYMODE,
        ISSUEDATE, BILLEFFDTE, DUEDATE, NETPREM, PREMTAX, LGT,
        DOCSTAMPS, ADMINFEE, OTHERCHARGES, COMMAMT, COMMWTAX,
        SFEEAMT, SFEETAX, TOTAMTDUE, USERID, TIMESTMP, POLNO,
        POLYER, OSBALANCE, COMMWTAXAMT, SFEEWTAXAMT, SUBSIDIARY,
        BATCHNO, REINSTAG, TOTSI, DISCOUNTED_AMT, DISCOUNT_VALUE,
        DISCOUNT_RATE, PROMOCODE, FOREX, VATAMT, VATRATE, FULLBILL,
        APPL_NO, COMMRATE, SFEERATE, OLD_GROSSPREM, NEW_GROSSPREM,
        OLDBILL_NO, DFIXTAG, REFNO,
        effective_from_date, effective_to_date, is_current, 
        record_status, record_hash, dw_insert_timestamp
    )
    SELECT 
        s.BILLSEQNO, s.BILLSTAT, s.CANCELDATE, s.REMARKS, s.LIFECNT, s.PAYMODE,
        s.ISSUEDATE, s.BILLEFFDTE, s.DUEDATE, s.NETPREM, s.PREMTAX, s.LGT,
        s.DOCSTAMPS, s.ADMINFEE, s.OTHERCHARGES, s.COMMAMT, s.COMMWTAX,
        s.SFEEAMT, s.SFEETAX, s.TOTAMTDUE, s.USERID, s.TIMESTMP, s.POLNO,
        s.POLYER, s.OSBALANCE, s.COMMWTAXAMT, s.SFEEWTAXAMT, s.SUBSIDIARY,
        s.BATCHNO, s.REINSTAG, s.TOTSI, s.DISCOUNTED_AMT, s.DISCOUNT_VALUE,
        s.DISCOUNT_RATE, s.PROMOCODE, s.FOREX, s.VATAMT, s.VATRATE, s.FULLBILL,
        s.APPL_NO, s.COMMRATE, s.SFEERATE, s.OLD_GROSSPREM, s.NEW_GROSSPREM,
        s.OLDBILL_NO, s.DFIXTAG, s.REFNO,
        TRUNC(SYSDATE) as effective_from_date,
        TO_DATE('9999-12-31','YYYY-MM-DD') as effective_to_date,
        'Y' as is_current,
        'ACTIVE' as record_status,
        s.record_hash,
        SYSTIMESTAMP as dw_insert_timestamp
    FROM nlr_billing_stg s
    LEFT JOIN nlr_billing_hist h 
        ON s.BILLSEQNO = h.BILLSEQNO 
        AND h.is_current = 'Y'
    WHERE h.BILLSEQNO IS NULL  -- New records
       OR s.record_hash != h.record_hash;  -- Modified records
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END sp_load_nlr_billing_hist;
/