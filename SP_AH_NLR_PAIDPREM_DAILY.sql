CREATE
OR REPLACE PROCEDURE SP_AH_NLR_PAIDPREM_DAILY (p_date IN DATE) AS
/******************************************************************************

NAME:       SP_AH_NLR_PAIDPREM_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/08/2025       Francis              1. Create SP_AH_NLR_PAIDPREM_DAILY


NOTES:

 ******************************************************************************/
BEGIN 



BEGIN
--Delete all policy numbers (POLNO) that were added yesterday to allow re-insertion of all policy numbers in the next step.
adw_prod_tgt.sp_adw_table_logs('NLR_PAIDPREM_DAILY','SP_AH_NLR_PAIDPREM_DAILY',SYSDATE,'','DELETE');
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
                    -- AND trunc (b.effdate) = trunc (sysdate - 1) ---incremental
                    AND trunc (b.timestmp) = p_date
            )
        SELECT
            polno
        FROM
            nlr_paidprem_data
    );

adw_prod_tgt.sp_adw_table_logs('NLR_PAIDPREM_DAILY','SP_AH_NLR_PAIDPREM_DAILY',SYSDATE,'','INSERT');
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
            -- AND trunc (b.timestmp) = trunc(sysdate - 1) --incremental
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
FROM
    nlr_paidprem_data;
   COMMIT;
adw_prod_tgt.sp_adw_table_logs('NLR_PAIDPREM_DAILY','SP_AH_NLR_PAIDPREM_DAILY',SYSDATE,SYSDATE,'UPDATE');

 

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



END SP_AH_NLR_PAIDPREM_DAILY;