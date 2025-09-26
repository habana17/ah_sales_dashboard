CREATE
OR REPLACE PROCEDURE SP_AH_ELIFE_POLSTAT_DAILY (p_date IN DATE)  AS
/******************************************************************************

NAME:       SP_AH_ELIFE_POLSTAT_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/08/2025       Francis              1. Create SP_AH_ELIFE_POLSTAT_DAILY


NOTES:

 ******************************************************************************/
BEGIN 

    

BEGIN

adw_prod_tgt.sp_adw_table_logs('ELIFE_POLSTAT_DAILY','SP_AH_ELIFE_POLSTAT_DAILY',SYSDATE,'','INSERT');
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
            statcode,
            b.timestmp
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
WHERE 1=1 
    --AND trunc (z.effdate) = trunc (sysdate - 1) --incremental
    AND trunc (z.timestmp) = p_date
    ; 

COMMIT;
adw_prod_tgt.sp_adw_table_logs('ELIFE_POLSTAT_DAILY','SP_AH_ELIFE_POLSTAT_DAILY',SYSDATE,SYSDATE,'UPDATE');


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




END SP_AH_ELIFE_POLSTAT_DAILY;