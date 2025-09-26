CREATE
OR REPLACE PROCEDURE SP_AH_NLR_POLSTAT_DAILY (p_date IN DATE) AS
/******************************************************************************

NAME:       SP_AH_NLR_POLSTAT_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/08/2025       Francis              1. Create SP_AH_NLR_POLSTAT_DAILY


NOTES:

 ******************************************************************************/
BEGIN 

BEGIN

    adw_prod_tgt.sp_adw_table_logs('NLR_POLSTAT_DAILY','SP_AH_NLR_POLSTAT_DAILY',SYSDATE,'','INSERT');
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
    -- and trunc (timestmp) = trunc (sysdate -1) --incremental
    and trunc (b.timestmp) = p_date
    ;
COMMIT;
adw_prod_tgt.sp_adw_table_logs('NLR_POLSTAT_DAILY','SP_AH_NLR_POLSTAT_DAILY',SYSDATE,SYSDATE,'UPDATE');

    EXCEPTION
            WHEN OTHERS THEN
        DECLARE
            err_msg VARCHAR2(4000);
        BEGIN
            err_msg := SQLERRM;
            INSERT INTO process_error_log (procedure_name, error_message,remarks)
            VALUES ('SP_AH_NLR_POLSTAT_DAILY', err_msg,'AH Sales Dashboard NLR_POLSTAT_DAILY');
            COMMIT;
        END;    

END;





END SP_AH_NLR_POLSTAT_DAILY;