CREATE
OR REPLACE PROCEDURE SP_AH_PAIDPREM_POLSTAT_DAILY AS
/******************************************************************************

NAME:       SP_AH_PAIDPREM_POLSTAT_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/08/2025       Francis              1. Create SP_AH_PAIDPREM_POLSTAT_DAILY


NOTES:

 ******************************************************************************/
BEGIN 

    BEGIN
        INSERT INTO
            NLR_POLSTAT_DAILY (
                PRODCODE_GRP,
                POLSTAT_GRP,
                POLNO,
                PRODCODE,
                EFFDATE,
                EFFMONTH,
                POLSTAT
                                )
SELECT
    DECODE (
        prodcode,
        'BCA',
        'BCA, BCB',
        'BCB',
        'BCA, BCB',
        'BCC',
        'BCC, BCD',
        'BCD',
        'BCC, BCD',
        'BCK',
        'BCK, BCL',
        'BCL',
        'BCK, BCL',
        prodcode
    ) prodcode_grp,
    DECODE (
        statcode,
        551,
        'IF In-Force',
        552,
        'CA Cancelled',
        'Lapsed'
    ) polstat_grp,
    a.polno,
    a.prodcode,
    b.effdate,
    TO_CHAR (b.effdate, 'MON-YY') effmonth,
    adw_prod_tgt.fn_grp_getrefdesc (a.statcode) polstat
FROM
    adw_prod_tgt.nlr_policy_mst_v2 a,
    adw_prod_tgt.nlr_poldate_mst_v2 b
WHERE
    a.prodcode IN ('ISR', 'HOS', 'HCH', 'PCP', 'HCN')
    AND a.polno = b.polno
    AND b.enddate IS NULL
    AND a.statcode IN (551, 552, 553, 2619)
    and trunc (effdate) = trunc (sysdate -1); --incremental

COMMIT;

    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_PAIDPREM_POLSTAT_DAILY', err_msg,'AH Sales Dashboard NLR_POLSTAT_DAILY');
            COMMIT;
        END;    

END;


BEGIN
INSERT INTO
    ELIFE_POLSTAT_DAILY (
        PRODCODE_GRP,
        POLSTAT_GRP,
        POLNO,
        PRODCODE,
        EFFDATE,
        EFFMONTH,
        POLSTAT
    )
SELECT
    DECODE (
        prodcode,
        'BCA',
        'BCA, BCB',
        'BCB',
        'BCA, BCB',
        'BCC',
        'BCC, BCD',
        'BCD',
        'BCC, BCD',
        'BCK',
        'BCK, BCL',
        'BCL',
        'BCK, BCL',
        prodcode
    ) prodcode_grp,
    DECODE (
        statcode,
        551,
        'IF In-Force',
        552,
        'CA Cancelled',
        'Lapsed'
    ) polstat_grp,
    polno,
    prodcode,
    effdate,
    effmonth,
    polstat
FROM
    (
        SELECT
            a.polno,
            prodcode,
            effdate,
            TO_CHAR (b.effdate, 'MON-YY') effmonth,
            adw_prod_tgt.fn_grp_getrefdesc (statcode) polstat,
            statcode
        FROM
            adw_prod_tgt.grb_policy_mst_v2 a,
            adw_prod_tgt.grb_poldate_mst b
        WHERE
            (
                prodcode IN ('DHI', 'TBU')
                OR (
                    prodcode LIKE 'BC%'
                    AND prodcode NOT IN ('BCI', 'BCJ', 'BCM')
                )
            )
            AND a.polno = b.polno
            AND enddate IS NULL
            AND a.statcode IN (551, 552, 553, 2619)
    ) z
WHERE
    trunc (z.effdate) = trunc (sysdate - 1); --incremental

COMMIT;


    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_PAIDPREM_POLSTAT_DAILY', err_msg,'AH Sales Dashboard ELIFE_POLSTAT_DAILY');
            COMMIT;
        END;    

END;

BEGIN
--Delete all policy numbers (POLNO) that were added yesterday to allow re-insertion of all policy numbers in the next step.
DELETE FROM NLR_PAIDPREM_DAILY
WHERE
    polno IN (
        WITH
            nlr_paidprem_data AS (
                SELECT DISTINCT
                    a.polno
                FROM
                    nlr_policy_mst_v2 a
                    JOIN nlr_poldate_mst_v2 b ON a.polno = b.polno
                    JOIN nlr_billing_mst_v2 c ON a.polno = c.polno
                WHERE
                    a.prodcode IN ('ISR', 'HOS', 'HCH', 'PCP', 'HCN')
                    AND b.enddate IS NULL
                    AND a.statcode IN (551, 552, 553, 2619)
                    AND c.osbalance = 0
                    AND c.billstat = 932
                    AND c.netprem > 0
                    AND trunc (b.effdate) = trunc (sysdate - 1) ---incremental
            )
        SELECT
            polno
        FROM
            nlr_paidprem_data
    );

INSERT INTO
    NLR_PAIDPREM_DAILY (
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
WITH
    nlr_paidprem_data AS (
        SELECT DISTINCT
            a.polno,
            a.prodcode,
            b.effdate,
            TO_CHAR (b.effdate, 'MON-YY') AS effmonth,
            DENSE_RANK() OVER (
                PARTITION BY
                    a.polno
                ORDER BY
                    c.billseqno
            ) AS pmonth,
            '' as fundtrndate,
            c.duedate,
            c.TOTAMTDUE AS sales_amt,
            c.netprem AS premium_amt
        FROM
            nlr_policy_mst_v2 a
            JOIN nlr_poldate_mst_v2 b ON a.polno = b.polno
            JOIN nlr_billing_mst_v2 c ON a.polno = c.polno
        WHERE
            a.prodcode IN ('ISR', 'HOS', 'HCH', 'PCP', 'HCN')
            AND b.enddate IS NULL
            AND a.statcode IN (551, 552, 553, 2619)
            AND c.osbalance = 0
            AND c.billstat = 932
            AND c.netprem > 0
            AND trunc (b.effdate) = trunc (sysdate - 1) --incremental
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
FROM
    nlr_paidprem_data;

COMMIT;

    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_PAIDPREM_POLSTAT_DAILY', err_msg,'AH Sales Dashboard NLR_PAIDPREM_DAILY');
            COMMIT;
        END;  

END;



BEGIN


    --Delete all policy numbers (POLNO) that were added yesterday to allow re-insertion of all policy numbers in the next step.
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
               AND trunc (b.effdate) = trunc(sysdate - 1)   --incremental                
                                  )
               SELECT        
               polno
               FROM 
               BCC_PAIDPREM_DATA
               WHERE 1=1
               
    );



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
              AND trunc (b.effdate) = trunc(sysdate - 1) --incremental                  
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


END SP_AH_PAIDPREM_POLSTAT_DAILY;