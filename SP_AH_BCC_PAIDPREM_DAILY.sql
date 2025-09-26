CREATE
OR REPLACE PROCEDURE SP_AH_BCC_PAIDPREM_DAILY  (p_date IN DATE)  AS
/******************************************************************************

NAME:       SP_AH_BCC_PAIDPREM_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/08/2025       Francis              1. Create SP_AH_BCC_PAIDPREM_DAILY


NOTES:

 ******************************************************************************/
BEGIN 


BEGIN


--Delete all policy numbers (POLNO) that were added yesterday to allow re-insertion of all policy numbers in the next step.
adw_prod_tgt.sp_adw_table_logs('BCC_PAIDPREM_DAILY','SP_AH_BCC_PAIDPREM_DAILY',SYSDATE,'','DELETE');
DELETE FROM BCC_PAIDPREM_DAILY
WHERE
    polno IN (
WITH BCC_PAIDPREM_DATA AS 
                                  (
          SELECT a.polno
          FROM grb_policy_mst_v2 a,
               grb_poldate_mst  b,
               xbc_dtcfund_mst  c
         WHERE     (   prodcode IN ('DHI', 'TBU')
                    OR (    prodcode LIKE 'BC%'
                        AND prodcode NOT IN ('BCI', 'BCJ', 'BCM')))
               AND fundtrntype = 6483
               AND a.polno = b.polno
               AND a.polno = c.polno
               AND enddate IS NULL
               AND a.statcode IN (551,
                                  552,
                                  553,
                                  2619)
            --    AND trunc (b.effdate) = trunc(sysdate - 1)   --incremental
               AND trunc (b.timestmp) = p_date             
                                  )
               SELECT        
               polno
               FROM 
               BCC_PAIDPREM_DATA
               WHERE 1=1
               
    );


adw_prod_tgt.sp_adw_table_logs('BCC_PAIDPREM_DAILY','SP_AH_BCC_PAIDPREM_DAILY',SYSDATE,'','INSERT');
INSERT INTO
    BCC_PAIDPREM_DAILY (
        PRODCODE_GRP,
        POLNO,
        PRODCODE,
        EFFDATE,
        EFFMONTH,
        FUNDTRNDATE,
        DUEDATE,
        PMONTH,
        SALES_AMT,
        PREMIUM_AMT
    )
    WITH BCC_PRAIDPREM_DATA AS 
                                  (
               SELECT 
               a.polno,
               prodcode,
               effdate,
               TO_CHAR (effdate, 'MON-YY') effmonth,
               fundtrndate,
               '' AS duedate,
               DENSE_RANK () OVER (PARTITION BY a.polno ORDER BY c.fundmstseqno)  pmonth,
               c.amount as sales_amt,
               c.baseamount as premium_amt
          FROM grb_policy_mst_v2   a,
               grb_poldate_mst  b,
               xbc_dtcfund_mst  c
         WHERE     (   prodcode IN ('DHI', 'TBU')
                    OR (    prodcode LIKE 'BC%'
                        AND prodcode NOT IN ('BCI', 'BCJ', 'BCM')))
               AND fundtrntype = 6483
               AND a.polno = b.polno
               AND a.polno = c.polno
               AND enddate IS NULL
               AND a.statcode IN (551,
                                  552,
                                  553,
                                  2619)
            --   AND trunc (b.effdate) = trunc(sysdate - 1) --incremental 
              AND trunc (b.timestmp) = p_date               
                                  )
                                  SELECT 
                                  DECODE (prodcode,
               'BCA', 'BCA, BCB',
               'BCB', 'BCA, BCB',
               'BCC', 'BCC, BCD',
               'BCD', 'BCC, BCD',
               'BCK', 'BCK, BCL',
               'BCL', 'BCK, BCL',
               prodcode)    prodcode_grp,
               polno,
               prodcode,
               effdate,
               effmonth,
               fundtrndate,
               duedate,
               pmonth,
               sales_amt,
               premium_amt
               from 
               BCC_PRAIDPREM_DATA;

                COMMIT;
                adw_prod_tgt.sp_adw_table_logs('BCC_PAIDPREM_DAILY','SP_AH_BCC_PAIDPREM_DAILY',SYSDATE,SYSDATE,'UPDATE');

    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_PAIDPREM_POLSTAT_DAILY', err_msg,'AH Sales Dashboard BCC_PAIDPREM_DAILY');
            COMMIT;
        END;  


END;    


END SP_AH_BCC_PAIDPREM_DAILY;