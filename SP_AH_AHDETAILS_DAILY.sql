create
or replace PROCEDURE SP_AH_AHDETAILS_DAILY (p_date IN DATE)
AS 

/******************************************************************************

NAME:       SP_AH_AHDETAILS_DAILY
PURPOSE:    travel & ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/03/2025            Francis          1. SP_AH_AHDETAILS_DAILY
2.0        08/13/2025            Francis          1. added NLR_EOM_D2C_PRODUCTS in map_description
3.0        08/20/2025            Francis          1. added PHB in prodcode. PHB is obsolete in 2025, 
                                                     but we needed this prodcode to get the 2024 data 
        

NOTES:

 ******************************************************************************/
BEGIN


--create temp table for non d2c a&h policies
--EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_EOM_HC_INPOLNO_DAILY'); --truncate
adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_HC_INPOLNO_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
DELETE FROM TEMP_EOM_HC_INPOLNO_DAILY;

adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_HC_INPOLNO_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
INSERT INTO adw_prod_tgt.TEMP_EOM_HC_INPOLNO_DAILY  --G1
(
POLNO,PRODCODE,YR,CURRENCY,STATCODE,
OREFFDATE,FOREX,OLDPOLNO,USERID,TIMESTMP,
PREMIUMTYPE,POLICYTYPE,ISSUE_SOURCE,EMPRATETAG,GROUP_CLASS,
POL_SOURCE,REFERAL_CODE,BRANCHCODE,SEGMENTCODE,LONGPOLNO,
CAMPAIGNCODE,POLNO_LIFE,PROPERTYPOLNO
)
SELECT 
a.POLNO,a.PRODCODE,a.YR,a.CURRENCY,a.STATCODE,
a.OREFFDATE,a.FOREX,a.OLDPOLNO,a.USERID,a.TIMESTMP,
a.PREMIUMTYPE,a.POLICYTYPE,a.ISSUE_SOURCE,a.EMPRATETAG,a.GROUP_CLASS,
a.POL_SOURCE,a.REFERAL_CODE,a.BRANCHCODE,a.SEGMENTCODE,a.LONGPOLNO,
a.CAMPAIGNCODE,a.POLNO_LIFE,a.PROPERTYPOLNO
FROM adw_prod_tgt.nlr_policy_mst_v2 a
WHERE 1=1 
        AND a.prodcode IN (SELECT map_value FROM adw_prod_tgt.nlr_data_mapping WHERE map_description IN ('NLR_EOM_OTHER_AH_PRODUCTS')
                                           UNION
                                           SELECT 'PHB' FROM dual
                                          )   
       --AND TRUNC(a.timestmp) = TRUNC(sysdate - 1) --incremental
       --AND a.timestmp >= TRUNC(SYSDATE - 1) AND a.timestmp <  TRUNC(SYSDATE) --optimize incremental added by francis 08192025
       --AND a.timestmp >= DATE '2024-05-01'  -- fixed date
       --AND a.timestmp <  DATE '2024-06-01'  -- get the data between this date
       AND TRUNC(a.timestmp) = p_date --loop test
       AND a.statcode = 551
       ;

adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_HC_INPOLNO_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');
    --COMMIT;


--create temp table for d2c policies
--i added the non d2c on this temp table as well
--EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_ISRHOS_DAILY'); --truncate
adw_prod_tgt.sp_adw_table_logs('TEMP_ISRHOS_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
DELETE FROM TEMP_ISRHOS_DAILY;

adw_prod_tgt.sp_adw_table_logs('TEMP_ISRHOS_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
INSERT INTO adw_prod_tgt.TEMP_ISRHOS_DAILY  --G2
(
ORNO,ORDATE,ORORNO,BILLBILLSEQNO,BILLISSUEDATE,
BILLBILLEFFDATE,BILLDUEDATE,OLD_GROSSPREM,NEW_GROSSPREM,OLDBILL_NO,
DFIXTAG,REFNO,BILLSEQNO,BILLSTAT,CANCELDATE,
REMARKS,LIFECNT,PAYMODE,ISSUEDATE,BILLEFFDTE,
DUEDATE,NETPREM,PREMTAX,LGT,DOCSTAMPS,
ADMINFEE,OTHERCHARGES,COMMAMT,COMMWTAX,SFEEAMT,
SFEETAX,TOTAMTDUE,USERID,TIMESTMP,POLNO,
POLYER,OSBALANCE,COMMWTAXAMT,SFEEWTAXAMT,SUBSIDIARY,
BATCHNO,REINSTAG,TOTSI,DISCOUNTED_AMT,DISCOUNT_VALUE,
DISCOUNT_RATE,PROMOCODE,FOREX,VATAMT,VATRATE,
FULLBILL,APPL_NO,COMMRATE,SFEERATE
)

WITH 
d2c_products AS (
    SELECT map_value AS prodcode
    FROM adw_prod_tgt.nlr_data_mapping 
    WHERE map_description = 'NLR_EOM_D2C_PRODUCTS'
),
d2c_excluded_batches AS (
    SELECT map_value AS batchno
    FROM adw_prod_tgt.nlr_data_mapping 
    WHERE map_description = 'NLR_EOM_D2C_EXCLUDE'
),
other_ah_products AS (
    SELECT map_value AS prodcode
    FROM adw_prod_tgt.nlr_data_mapping 
    WHERE map_description = 'NLR_EOM_OTHER_AH_PRODUCTS'
    UNION
    SELECT 'PHB' FROM dual
)
SELECT DISTINCT
    c.orno,
    c.ordate,
    c.orno AS ororno,
    a.billseqno AS billbillseqno,
    a.issuedate AS billissuedate,
    a.billeffdte AS billbilleffdate,
    a.duedate AS billduedate,
    a.old_grossprem,
    a.new_grossprem,
    a.oldbill_no,
    a.dfixtag,
    a.refno,
    a.billseqno,
    a.billstat,
    a.canceldate,
    a.remarks,
    a.lifecnt,
    a.paymode,
    a.issuedate,
    a.billeffdte,
    a.duedate,
    a.netprem,
    a.premtax,
    a.lgt,
    a.docstamps,
    a.adminfee,
    a.othercharges,
    a.commamt,
    a.commwtax,
    a.sfeeamt,
    a.sfeetax,
    a.totamtdue,
    a.userid,
    a.timestmp,
    a.polno,
    a.polyer,
    a.osbalance,
    a.commwtaxamt,
    a.sfeewtaxamt,
    a.subsidiary,
    a.batchno,
    a.reinstag,
    a.totsi,
    a.discounted_amt,
    a.discount_value,
    a.discount_rate,
    a.promocode,
    a.forex,
    a.vatamt,
    a.vatrate,
    a.fullbill,
    a.appl_no,
    a.commrate,
    a.sfeerate
FROM adw_prod_tgt.nlr_billing_mst_v2 a
INNER JOIN adw_prod_tgt.nlr_billing_paydtl b ON a.billseqno = b.billseqno
INNER JOIN adw_prod_tgt.nlr_or_history c ON b.reconref = TO_CHAR(c.orno)
INNER JOIN adw_prod_tgt.nlr_policy_mst_v2 p ON a.polno = p.polno
LEFT JOIN d2c_products d2c ON p.prodcode = d2c.prodcode
LEFT JOIN other_ah_products ah ON p.prodcode = ah.prodcode
LEFT JOIN d2c_excluded_batches excl ON TO_CHAR(a.batchno) = excl.batchno
WHERE 1=1
    AND a.osbalance = 0
    AND a.totamtdue > 0
    AND a.billeffdte < SYSDATE
    AND c.canceltag = 'N'
    AND b.ostag = 'Y'
    AND TRUNC(c.ordate) = p_date --loop test  
    AND p.statcode <> 2653
    AND (d2c.prodcode IS NOT NULL OR ah.prodcode IS NOT NULL)
    -- Only exclude batches for D2C products
    AND (d2c.prodcode IS NULL OR excl.batchno IS NULL);

adw_prod_tgt.sp_adw_table_logs('TEMP_ISRHOS_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');    



/****************OLD SCRIPT*************
--  SELECT DISTINCT
--     c.orno,c.ordate,c.orno AS ororno,a.billseqno AS billbillseqno,a.issuedate AS billissuedate,
--     a.billeffdte AS billbilleffdate,a.duedate AS billduedate,a.old_grossprem,a.new_grossprem,a.oldbill_no,
--     a.dfixtag,a.refno,a.billseqno,a.billstat,a.canceldate,
--     a.remarks,a.lifecnt,a.paymode,a.issuedate,a.billeffdte,
--     a.duedate,a.netprem,a.premtax,a.lgt,a.docstamps,
--     a.adminfee,a.othercharges,a.commamt,a.commwtax,a.sfeeamt,
--     a.sfeetax,a.totamtdue,a.userid,a.timestmp,a.polno,
--     a.polyer,a.osbalance,a.commwtaxamt,a.sfeewtaxamt,a.subsidiary,
--     a.batchno,a.reinstag,a.totsi,a.discounted_amt,a.discount_value,
--     a.discount_rate,a.promocode,a.forex,a.vatamt,a.vatrate,
--     a.fullbill,a.appl_no,a.commrate,a.sfeerate
--       FROM adw_prod_tgt.nlr_billing_mst_v2     a,
--            adw_prod_tgt.nlr_billing_paydtl  b,
--            adw_prod_tgt.nlr_or_history      c
--      WHERE     osbalance = 0
--            AND totamtdue > 0
--            AND polno IN
--                    (SELECT polno
--                       FROM adw_prod_tgt.nlr_policy_mst_v2
--                      WHERE     prodcode IN
--                                    (SELECT map_value
--                                       FROM adw_prod_tgt.nlr_data_mapping
--                                      WHERE map_description =
--                                            'NLR_EOM_D2C_PRODUCTS')
--                            AND statcode <> 2653)
--            --AND a.batchno NOT IN (SELECT batchno FROM adw_prod_tgt.nlr_portfolio_pisc) --remove since some of the batchno cannot be read 
--            AND trunc(billeffdte) < trunc(sysdate)
--            AND a.billseqno = b.billseqno
--            AND b.reconref = TO_CHAR (c.orno)
--            AND canceltag = 'N'
--            AND ostag = 'Y'
--            --AND a.batchno = 2056285
--            AND TO_CHAR (a.batchno) NOT IN
--                    (SELECT map_value
--                       FROM adw_prod_tgt.nlr_data_mapping
--                      WHERE map_description = 'NLR_EOM_D2C_EXCLUDE')
--               AND TRUNC(c.ordate) = p_date --loop test  

--               UNION ALL 

--     SELECT DISTINCT
--     c.orno,c.ordate,c.orno AS ororno,a.billseqno AS billbillseqno,a.issuedate AS billissuedate,
--     a.billeffdte AS billbilleffdate,a.duedate AS billduedate,a.old_grossprem,a.new_grossprem,a.oldbill_no,
--     a.dfixtag,a.refno,a.billseqno,a.billstat,a.canceldate,
--     a.remarks,a.lifecnt,a.paymode,a.issuedate,a.billeffdte,
--     a.duedate,a.netprem,a.premtax,a.lgt,a.docstamps,
--     a.adminfee,a.othercharges,a.commamt,a.commwtax,a.sfeeamt,
--     a.sfeetax,a.totamtdue,a.userid,a.timestmp,a.polno,
--     a.polyer,a.osbalance,a.commwtaxamt,a.sfeewtaxamt,a.subsidiary,
--     a.batchno,a.reinstag,a.totsi,a.discounted_amt,a.discount_value,
--     a.discount_rate,a.promocode,a.forex,a.vatamt,a.vatrate,
--     a.fullbill,a.appl_no,a.commrate,a.sfeerate
--       FROM adw_prod_tgt.nlr_billing_mst_v2     a,
--            adw_prod_tgt.nlr_billing_paydtl  b,
--            adw_prod_tgt.nlr_or_history      c
--      WHERE     a.osbalance = 0
--            AND a.totamtdue > 0
--            AND a.polno IN
--                    (SELECT polno
--                       FROM adw_prod_tgt.nlr_policy_mst_v2
--                      WHERE     prodcode IN
--                                    (SELECT map_value
--                                       FROM adw_prod_tgt.nlr_data_mapping
--                                      WHERE map_description =
--                                            'NLR_EOM_OTHER_AH_PRODUCTS'
--                                        UNION
--                                        SELECT 'PHB' FROM dual    
--                                            )
--                            AND statcode <> 2653)

--            AND trunc(a.billeffdte) < trunc(sysdate)
--            AND a.billseqno = b.billseqno
--            AND b.reconref = TO_CHAR (c.orno)
--            AND c.canceltag = 'N'
--            AND b.ostag = 'Y'
--            AND TRUNC(c.ordate) = p_date --loop test  
--             ;
****************OLD SCRIPT*************/
              

    --COMMIT;

    --EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_EOM_HC_DAILY'); --truncate
    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_HC_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
    DELETE FROM TEMP_EOM_HC_DAILY;  ---G1

    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_HC_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
    INSERT INTO adw_prod_tgt.TEMP_EOM_HC_DAILY 
    (
        POLNO,
        BATCHNO

    )
    --     SELECT distinct a.polno, b.batchno
    --   FROM adw_prod_tgt.TEMP_EOM_HC_INPOLNO_DAILY  a, adw_prod_tgt.nlr_premium_summary_v2 b
    --  WHERE     a.polno = b.polno
    --        AND b.batchno IN (SELECT batchno
    --                            FROM adw_prod_tgt.nlr_policy_tran_v2
    --                           WHERE trantype = 10009112); -- old script 

    WITH getpolno_1 AS (
    SELECT DISTINCT b.polno, b.batchno
    FROM adw_prod_tgt.TEMP_EOM_HC_INPOLNO_DAILY a
    -- JOIN adw_prod_tgt.nlr_premium_summary_v2 b
    -- JOIN adw_prod_tgt.NLR_PREMIUM_SUMMARY_CURR b --change nlr_premium_summary_v2 to NLR_PREMIUM_SUMMARY_CURR
    JOIN adw_prod_tgt.nlr_billing_mst_v2 b  --replcae NLR_PREMIUM_SUMMARY_CURR to nlr_billing_mst_v2 for complete details 
      ON a.polno = b.polno
    WHERE b.batchno IN (
            SELECT batchno
            FROM adw_prod_tgt.nlr_policy_tran_v2
            WHERE trantype = 10009112
    )
                        ),
    getpolno_2 AS (
    SELECT c.polno, c.batchno
    FROM adw_prod_tgt.nlr_billing_mst_v2 c
    WHERE EXISTS (
        SELECT 1
        FROM getpolno_1 g
        WHERE g.batchno = c.batchno
    )
      -- AND c.issuedate >= DATE '2024-05-01' -- fixed date
      -- AND c.issuedate <  DATE '2024-06-01' -- fixed date
         AND TRUNC(c.issuedate) = p_date --loop test
                  )
      SELECT polno, batchno
      FROM getpolno_2;

      adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_HC_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');


    -- --COMMIT;

    -- EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_EOM_ISRHOS_DAILY'); --truncate
    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_ISRHOS_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
    DELETE FROM TEMP_EOM_ISRHOS_DAILY;

    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_ISRHOS_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
    INSERT INTO adw_prod_tgt.TEMP_EOM_ISRHOS_DAILY --G2
    (
      POLNO,
      BATCHNO  
    ) 
    SELECT distinct a.polno ,a.batchno from adw_prod_tgt.TEMP_ISRHOS_DAILY a;
    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_ISRHOS_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');
   --COMMIT;


    -- EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_EOM_AHR_DAILY'); --truncate
    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_AHR_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
    DELETE FROM TEMP_EOM_AHR_DAILY;

    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_AHR_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
    INSERT INTO adw_prod_tgt.TEMP_EOM_AHR_DAILY --g2 & g3
    (
        POLNO,
        BATCHNO
    )
    SELECT distinct * FROM adw_prod_tgt.TEMP_EOM_HC_DAILY
    UNION
    SELECT distinct * FROM adw_prod_tgt.TEMP_EOM_ISRHOS_DAILY;
    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_AHR_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');
    -- --COMMIT;


    -- EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_EOM_BILL_DAILY'); --truncate
    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_BILL_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
    DELETE FROM TEMP_EOM_BILL_DAILY;

    adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_BILL_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
    INSERT INTO adw_prod_tgt.TEMP_EOM_BILL_DAILY
    (
        BILLSEQNO,REFNO,CANCELDATE,REMARKS,LIFECNT,
        PAYMODE,ISSUEDATE,BILLEFFDTE,DUEDATE,NETPREM,
        PREMTAX,LGT,DOCSTAMPS,ADMINFEE,OTHERCHARGES,
        COMMAMT,COMMWTAX,SFEEAMT,SFEETAX,TOTAMTDUE,
        USERID,TIMESTMP,POLNO,POLYER,OSBALANCE,
        COMMWTAXAMT,SFEEWTAXAMT,SUBSIDIARY,BATCHNO,REINSTAG,
        TOTSI,DISCOUNTED_AMT,DISCOUNT_VALUE,DISCOUNT_RATE,PROMOCODE,
        FOREX,VATAMT,VATRATE,FULLBILL,APPL_NO,
        COMMRATE,SFEERATE,OLD_GROSSPREM,NEW_GROSSPREM,OLDBILL_NO,
        DFIXTAG,BILLSTAT
    )
        SELECT
        BILLSEQNO,REFNO,CANCELDATE,REMARKS,LIFECNT,
        PAYMODE,ISSUEDATE,BILLEFFDTE,DUEDATE,NETPREM,
        PREMTAX,LGT,DOCSTAMPS,ADMINFEE,OTHERCHARGES,
        COMMAMT,COMMWTAX,SFEEAMT,SFEETAX,TOTAMTDUE,
        USERID,TIMESTMP,POLNO,POLYER,OSBALANCE,
        COMMWTAXAMT,SFEEWTAXAMT,SUBSIDIARY,BATCHNO,REINSTAG,
        TOTSI,DISCOUNTED_AMT,DISCOUNT_VALUE,DISCOUNT_RATE,PROMOCODE,
        FOREX,VATAMT,VATRATE,FULLBILL,APPL_NO,
        COMMRATE,SFEERATE,OLD_GROSSPREM,NEW_GROSSPREM,OLDBILL_NO,
        DFIXTAG,BILLSTAT
        FROM  nlr_billing_mst_v2
        WHERE 
        batchno in (select batchno from TEMP_EOM_AHR_DAILY)
        ;

        adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_BILL_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');


      -- EXECUTE IMMEDIATE ('TRUNCATE TABLE TEMP_EOM_AHR_CANCEL_DAILY'); --truncate
      adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_AHR_CANCEL_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
      DELETE FROM TEMP_EOM_AHR_CANCEL_DAILY;

      adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_AHR_CANCEL_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
      INSERT INTO adw_prod_tgt.TEMP_EOM_AHR_CANCEL_DAILY
      (
        BATCHNO,INCEPT_DT,PRODLINE,POLYEAR,SERV_AGENTCODE,
        SERV_AGENTNAME,SEGMENT_CODE,CARDCLIENT,CLNTID,POLICYHOLDER,
        POLTRNEFFDATE,EFFDATE,EXPDATE,TRANDATE,BRANCHCODE,
        PRODTYPE,POLSTAT,POLMODE,ACCOUNTOFFICER,DIST,
        DIST_VALUE,CURRENCY,LOB,SUBLINE,STATCODE,
        POLICYTYPE,PRODCATEGORY,ORG_TYPE,SEGCODE_SEQ,COMPCODE,
        MAINLINE,AGTNO,TRANTYPE,ISSUE_SOURCE,NETPREMTOT,
        DOCSAMT,LGTAMT,PTAXAMT,GPREMTOT,COMMAMT,
        COMMWTAXAMT,COMWTAX,SFEEAMT,SFEETAX,SFEEWTAXAMT,
        TRANDATEPARM,OTHER_CHARGES,VATAMT,TOTSI,EFFDATE2,
        PROPOSAL_NO,POLSOURCE,ACCT_OFFICER,USERID,POLNUM,
        REPORTNAME,ACCTGDST,ACCTGTSI,ACCTGOTHCHRG,EDNT_TYPE,
        POLNO
      )
      WITH canceltemp as (SELECT 
         b.batchno, 
         b.polno, SUBSTR (f.t1, 1, 1) AS prodline, 
         adw_prod_tgt.fn_nlr_polyr_bytran_batchno (b.polno, b.batchno) polyear,
         gg.agtno as  serv_agentcode,
         (adw_prod_tgt.parsename_temp (cc.namestr, 'LFM', 'FML')) AS serv_agentname,
         da.refdesc AS segment_code, 
         ' ' AS cardclient, 
        bx.clntid AS clntid,
        (adw_prod_tgt.parsename_temp(ba.namestr, 'LFM', 'FML')) AS policyholder, 
        b.effdate AS poltrneffdate,
        ins.effdate  AS effdate,   -- 12
        ins.expdate  AS expdate,   -- 13
        MAX (b.trandate) AS trandate, 
        d.branchcode AS branchcode, 
        d.prodcode AS prodtype,
        gstat.refdesc AS polstat, gmode.refdesc AS polmode, 
        adw_prod_tgt.parsename_temp(bb.namestr, 'LFM', 'FML') AS accountofficer,  
        CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE (g.dist / 100) END AS dist,
        CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE g.dist END AS dist_value, 
        cref.refdesc AS currency, 
        f.LOB, f.subline, d.statcode, d.policytype, f.prodcategory, f.orgtype AS org_type, 
        d.segmentcode AS segcode_seq, 
        SUBSTR (fa.refdesc, 1, 1) AS compcode,
        f.mainline, 
        CASE WHEN g.pertype = 562 THEN x.max_agtno ELSE NULL END agtno,
        b.trantype, 
        d.issue_source, 
        z.netprem as netpremtot ,-- z.netpremtot, 
        z.docstamps as docsamt,-- z.docsamt, 
        z.lgt as lgtamt,-- z.lgtamt, 
        z.premtax as ptaxamt,-- z.ptaxamt, 
        z.totamtdue as gpremtot,-- z.gpremtot, 
        z.commamt, 
        z.commwtaxamt, 
        z.commwtax as comwtax,-- z.comwtax, 
        z.sfeeamt, 
        z.sfeetax, 
        z.sfeewtaxamt as sfeewtaxamt,-- z.sfeewtaxamt,
        z.issuedate as trandateparm,-- z.trandate AS trandateparm, 
        z.othercharges as other_charges,-- z.other_charges, 
        0 vatamt, 
        z.totsi, 
        b.effdate AS effdate2, 
        b.proposal_no,
        dc.refdesc as polsource,  
        ' ' acct_officer, 
        b.userid, 
        b.polno AS polnum, 
        ' ' AS reportname, 
        0 acctgdst, 
        0 acctgtsi, 
        0 acctgothchrg,
        CASE WHEN b.trantype <> 10009112 THEN CASE WHEN z.netprem > 0 THEN 'A' WHEN z.netprem < 0 THEN 'R' ELSE 'N' END ELSE NULL END ednt_type,
        xx.effdate AS incept_dt 
  FROM 
      adw_prod_tgt.nlr_policy_tran_v2 b
      LEFT JOIN (Select distinct cnt.namestr,npt.polno from nlr_polrole_trn_v2 npt,  cnb_namelst_trn_v2 cnt where npt.nameid = cnt.nameid and npt.pertype = 556 and npt.enddate is null) ba
      ON ba.polno = b.polno --policyholder
      LEFT JOIN (Select distinct cntt.namestr,nptt.polno from nlr_polrole_trn_v2 nptt,  cnb_namelst_trn_v2 cntt where nptt.nameid = cntt.nameid and nptt.pertype = 862 and nptt.enddate is null) bb
      ON bb.polno = b.polno --account officer
      LEFT JOIN NLR_POLROLE_TRN_V2 ax ON b.polno = ax.polno AND ax.pertype = 556 AND ax.enddate IS NULL--clntid -1 
      LEFT JOIN cnb_namelst_trn_v2 bx ON ax.nameid = bx.nameid  --clntid - 2
      LEFT JOIN (
        SELECT 
           z.batchno,
           z.polno,
           MAX(x.effdate)     AS effdate,
           MAX(x.expirydate)  AS expdate
        FROM   adw_prod_tgt.nlr_insured_mst_v2 x
           JOIN adw_prod_tgt.nlr_insured_trn_v2 z
             ON z.inseqno = x.inseqno
            AND z.polno   = x.polno
        GROUP BY z.batchno, z.polno 
                ) ins
      ON ins.batchno = b.batchno
      AND ins.polno   = b.polno
      LEFT JOIN adw_prod_tgt.nlr_poldate_mst_v2 xx ON xx.polno = b.polno
      JOIN adw_prod_tgt.nlr_policy_mst_v2 d ON b.polno = d.polno
      LEFT JOIN adw_prod_tgt.gct_geninfo_ref dc 
      ON dc.refseqno = d.pol_source --polsource
      LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 da ON d.segmentcode = da.refseqno --segment code
      LEFT JOIN adw_prod_tgt.gct_geninfo_ref gstat ON gstat.refseqno = d.statcode   
      LEFT JOIN adw_prod_tgt.cxx_geninfo_ref cref ON cref.refseqno = d.currency
      LEFT JOIN adw_prod_tgt.nlr_polrole_trn_v2 g ON d.polno = g.polno AND g.pertype = 562 AND g.enddate IS NULL
      LEFT JOIN adw_prod_tgt.cnb_namelst_trn_v2 cc ON g.nameid = cc.nameid --serv_agentname
      LEFT JOIN adw_prod_tgt.xag_profile_v2 gg ON g.nameid = gg.nameid --serv_agentcode
      LEFT JOIN (SELECT nameid, MAX(agtno) AS max_agtno FROM adw_prod_tgt.xag_profile_v2 GROUP BY nameid) x ON x.nameid = g.nameid
      JOIN adw_prod_tgt.grb_product_mst_iris f ON d.prodcode = f.prodcode
      LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 fa ON f.co_code = fa.refseqno --compcode
      JOIN adw_prod_tgt.nlr_polbill_ref_v2 h ON h.polno = b.polno AND h.enddate IS NULL
      LEFT JOIN adw_prod_tgt.gct_geninfo_ref gmode ON gmode.refseqno = h.billmode  
      -- JOIN adw_prod_tgt.nlr_premium_summary_v2 z ON z.batchno = b.batchno AND z.polno = b.polno
      -- JOIN adw_prod_tgt.NLR_PREMIUM_SUMMARY_CURR z ON z.batchno = b.batchno AND z.polno = b.polno --change nlr_premium_summary_v2 to NLR_PREMIUM_SUMMARY_CURR
      JOIN adw_prod_tgt.nlr_billing_mst_v2 z ON z.batchno = b.batchno AND z.polno = b.polno
 WHERE f.enddate IS NULL 
       AND d.statcode <> 529
       AND d.statcode <> 2653 
       AND b.trantype = 10009081
       AND d.prodcode in ('ISR', 'HCH', 'HOS', 'PCP', 'HCN', 'HCC', 'HCA', 'HCE','HCB','HCF', 'HCG', 'HCI', 'HCK', 'PET', 'HCP', 'PDC')
      GROUP BY 
      b.batchno, b.proposal_no, b.polno, f.t1, g.nameid, 
      d.prodcode, 
      d.statcode, 
      h.billmode, 
      g.dist, 
      f.subline, 
      f.LOB,
      d.statcode, 
      d.policytype, 
      f.prodcategory, 
      d.segmentcode, 
      d.branchcode, 
      f.co_code, 
      d.currency, 
      f.orgtype,
      f.mainline,
      g.nameid, g.pertype, 
      b.trantype, 
      d.issue_source,              
      z.netprem, -- z.netpremtot, 
      z.docstamps, -- z.docsamt, 
      z.lgt,--  z.lgtamt, 
      z.premtax,-- z.ptaxamt, 
      z.totamtdue,-- z.gpremtot, 
      z.commamt, 
      z.commwtaxamt, 
      z.commwtax,-- z.comwtax, 
      z.sfeeamt, 
      z.sfeetax, 
      z.sfeewtaxamt,-- z.sfeewtaxamt,
      z.issuedate,-- z.trandate AS trandateparm, 
      z.othercharges, -- z.other_charges 
      z.vatamt, 
      z.totsi,  
      b.effdate,
      d.pol_source, b.userid,xx.effdate,x.max_agtno,cref.refdesc,gstat.refdesc,gmode.refdesc,ins.effdate,ins.expdate,gg.agtno,
      cc.namestr,da.refdesc,bx.clntid,ba.namestr,bb.namestr,fa.refdesc,dc.refdesc             
      )

select  
        BATCHNO,INCEPT_DT,PRODLINE,POLYEAR,SERV_AGENTCODE,
        SERV_AGENTNAME,SEGMENT_CODE,CARDCLIENT,CLNTID,POLICYHOLDER,
        POLTRNEFFDATE,EFFDATE,EXPDATE,TRANDATE,BRANCHCODE,
        PRODTYPE,POLSTAT,POLMODE,ACCOUNTOFFICER,DIST,
        DIST_VALUE,CURRENCY,LOB,SUBLINE,STATCODE,
        POLICYTYPE,PRODCATEGORY,ORG_TYPE,SEGCODE_SEQ,COMPCODE,
        MAINLINE,AGTNO,TRANTYPE,ISSUE_SOURCE,NETPREMTOT,
        DOCSAMT,LGTAMT,PTAXAMT,GPREMTOT,COMMAMT,
        COMMWTAXAMT,COMWTAX,SFEEAMT,SFEETAX,SFEEWTAXAMT,
        TRANDATEPARM,OTHER_CHARGES,VATAMT,TOTSI,EFFDATE2,
        PROPOSAL_NO,POLSOURCE,ACCT_OFFICER,USERID,POLNUM,
        REPORTNAME,ACCTGDST,ACCTGTSI,ACCTGOTHCHRG,EDNT_TYPE,
        POLNO
from canceltemp 
where 1=1 
--and polnum ='AH-IR-HO-22-0013148-00-D' --test
--AND trunc(trandate) = trunc(sysdate - 1) --incremental
--AND trandate >= TRUNC(SYSDATE - 1) AND trandate <  TRUNC(SYSDATE) --optimize incremental added by francis 08192025
--AND trandate >= DATE '2024-05-01' AND trandate <  DATE '2024-06-01' --fixed date
AND TRUNC(trandate) = p_date --loop test
;

adw_prod_tgt.sp_adw_table_logs('TEMP_EOM_AHR_CANCEL_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');


--COMMIT;

-- --retain just the valid transactions only. for d2c policies, though the policies were cancelled, no need to book reversal of entries. 
-- DELETE  
-- from TEMP_EOM_AHR_CANCEL_DAILY a where polno not in (select polno from adw_prod_tgt.d2c_valid_cancellation) and prodtype not in ('HCC', 'HCA', 'HCE','HCB','HCF', 'HCG', 'HCI', 'HCK', 'PET', 'HCP', 'PDC');
-- --COMMIT;


    

    adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_AH_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
    DELETE FROM TEMP_NLR_PORTFOLIO_PISC_AH_DAILY;

    adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_AH_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
    INSERT INTO adw_prod_tgt.TEMP_NLR_PORTFOLIO_PISC_AH_DAILY --insert the first data   e 
    (
    batchno,polno,prodline,polyear,serv_agentcode,
    serv_agentname,segment_code,cardclient,clntid,
    policyholder,poltrneffdate,effdate,expdate,trandate,branchcode,
    prodtype,polstat,polmode,accountofficer,dist,
    dist_value,currency,lob,subline,statcode,
    policytype,prodcategory,org_type,segcode_seq,compcode,
    mainline,agtno,trantype,issue_source,netpremtot,
    docsamt,lgtamt,ptaxamt,gpremtot,commamt,
    commwtaxamt,comwtax,sfeeamt,sfeetax,sfeewtaxamt,
    trandateparm,other_charges,vatamt,totsi,effdate2,
    proposal_no,polsource,acct_officer,userid,polnum,
    reportname,acctgdst,acctgtsi,acctgothchrg,ednt_type,incept_dt
    )
 SELECT             b.batchno, b.polno, SUBSTR (f.t1, 1, 1) AS prodline,
                        adw_prod_tgt.fn_nlr_polyr_bytran_batchno (b.polno, b.batchno) polyear,
                        gg.agtno AS serv_agentcode,
                        (adw_prod_tgt.parsename_temp (cc.namestr, 'LFM', 'FML')) AS serv_agentname,
                        da.refdesc AS segment_code,
                        ' ' AS cardclient,
                        bx.clntid AS clntid,
                        (adw_prod_tgt.parsename_temp(ba.namestr, 'LFM', 'FML')) AS policyholder,
                        b.effdate AS poltrneffdate,
                        DECODE(d.prodcode, 'PHB', pd.effdate, tb.billeffdte) AS effdate,
                        pd.expdate AS expdate,
                        -- DECODE(
                        --         d.prodcode,
                        --         'HCA', b.effdate, 'HCC', b.effdate, 'HCE', b.effdate, 'HCB', b.effdate,
                        --         'HCF', b.effdate, 'HCG', b.effdate, 'HCI', b.effdate, 'PHB', b.effdate,
                        --         'HCK', b.effdate, 'PET', b.effdate, 'HCP', b.effdate, 'HCN', b.effdate,
                        --         'PDC', b.effdate, ti.ordate) AS trandate,
                        NVL(ti.ordate,tb.issuedate) AS trandate,
                        d.branchcode, d.prodcode AS prodtype, gstat.refdesc AS polstat, gmode.refdesc AS polmode,  
                        adw_prod_tgt.parsename_temp(bb.namestr, 'LFM', 'FML') AS accountofficer,  
                        CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE (g.dist / 100) END AS dist,
                        CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE g.dist END AS dist_value, 
                        cref.refdesc AS currency, f.LOB, f.subline, d.statcode, d.policytype, f.prodcategory, 
                        f.orgtype AS org_type, d.segmentcode AS segcode_seq, 
                        SUBSTR (fa.refdesc, 1, 1) AS compcode, 
                        f.mainline, 
                        CASE WHEN g.pertype = 562 THEN x.max_agtno ELSE NULL END AS agtno,
                        b.trantype, d.issue_source, 
                        z.netprem,-- z.netpremtot, 
                        z.docstamps,-- z.docsamt, 
                        z.lgt,-- z.lgtamt, 
                        z.premtax,-- z.ptaxamt, 
                        z.totamtdue,-- z.gpremtot, 
                        z.commamt, 
                        z.commwtaxamt, 
                        z.commwtax,-- z.comwtax, 
                        z.sfeeamt, 
                        z.sfeetax, 
                        z.sfeewtaxamt,-- z.sfeewtaxamt,
                        z.issuedate as trandateparm,-- z.trandate AS trandateparm, 
                        z.othercharges,-- z.other_charges, 
                        0 vatamt, 
                        z.totsi, 
                        b.effdate AS effdate2, 
                        b.proposal_no, 
                        dc.refdesc as polsource, 
                        ' ' acct_officer, 
                        b.userid, b.polno AS polnum, ' ' AS reportname, 0 acctgdst, 0 acctgtsi, 0 acctgothchrg,
                        -- CASE WHEN b.trantype <> 10009112 THEN CASE WHEN z.netpremtot > 0 THEN 'A' WHEN z.netpremtot < 0 THEN 'R' ELSE 'N' END ELSE NULL END ednt_type,
                        CASE WHEN b.trantype <> 10009112 THEN CASE WHEN z.netprem > 0 THEN 'A' WHEN z.netprem < 0 THEN 'R' ELSE 'N' END ELSE NULL END ednt_type,
                        xx.effdate AS incept_dt  
                FROM adw_prod_tgt.nlr_policy_tran_v2 b
                   LEFT JOIN adw_prod_tgt.nlr_poldate_mst_v2 xx ON xx.polno = b.polno
                   LEFT JOIN NLR_POLROLE_TRN_V2 ax ON b.polno = ax.polno AND ax.pertype = 556 AND ax.enddate IS NULL--clntid -1 
                   LEFT JOIN cnb_namelst_trn_v2 bx ON ax.nameid = bx.nameid  --clntid - 2
                   LEFT JOIN (Select distinct cnt.namestr,npt.polno from nlr_polrole_trn_v2 npt,  cnb_namelst_trn_v2 cnt where npt.nameid = cnt.nameid and npt.pertype = 556 and npt.enddate is null) ba
                   ON ba.polno = b.polno --policyholder
                   LEFT JOIN (Select distinct cntt.namestr,nptt.polno from nlr_polrole_trn_v2 nptt,  cnb_namelst_trn_v2 cntt where nptt.nameid = cntt.nameid and nptt.pertype = 862 and nptt.enddate is null) bb
                   ON bb.polno = b.polno --account officer
                   LEFT JOIN adw_prod_tgt.nlr_poldate_mst_v2 pd ON pd.polno = b.polno AND pd.enddate IS NULL
                   LEFT JOIN adw_prod_tgt.TEMP_EOM_BILL_DAILY tb ON tb.batchno = b.batchno     
                   LEFT JOIN adw_prod_tgt.TEMP_ISRHOS_DAILY ti ON ti.batchno = b.batchno     
                   JOIN adw_prod_tgt.nlr_policy_mst_v2 d ON b.polno = d.polno
                   LEFT JOIN adw_prod_tgt.gct_geninfo_ref dc 
                   ON dc.refseqno = d.pol_source --polsource
                   LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 da ON d.segmentcode = da.refseqno --segment_code
                   LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 db ON d.segmentcode = db.refseqno
                   LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 cref ON cref.refseqno = d.currency
                   LEFT JOIN adw_prod_tgt.gct_geninfo_ref gstat ON gstat.refseqno = d.statcode     
                   JOIN adw_prod_tgt.grb_product_mst_iris f ON d.prodcode = f.prodcode
                   LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 fa ON f.co_code = fa.refseqno --compcode
                   LEFT JOIN adw_prod_tgt.nlr_polrole_trn_v2 g ON d.polno = g.polno AND g.pertype = 562 AND g.enddate IS NULL
                   LEFT JOIN adw_prod_tgt.xag_profile_v2 gg ON g.nameid = gg.nameid --serv_agentcode
                   LEFT JOIN adw_prod_tgt.cnb_namelst_trn_v2 cc ON g.nameid = cc.nameid --serv_agentname
                   LEFT JOIN (SELECT nameid, MAX(agtno) AS max_agtno FROM adw_prod_tgt.xag_profile_v2 GROUP BY nameid) x ON x.nameid = g.nameid
                   JOIN adw_prod_tgt.nlr_polbill_ref_v2 h ON h.polno = b.polno AND h.enddate IS NULL
                   LEFT JOIN adw_prod_tgt.gct_geninfo_ref gmode ON gmode.refseqno = h.billmode        
                  --  JOIN adw_prod_tgt.nlr_premium_summary_v2 z ON z.batchno = b.batchno AND z.polno = b.polno
                  --  JOIN adw_prod_tgt.NLR_PREMIUM_SUMMARY_CURR z ON z.batchno = b.batchno AND z.polno = b.polno --change nlr_premium_summary_v2 to NLR_PREMIUM_SUMMARY_CURR
                   JOIN adw_prod_tgt.nlr_billing_mst_v2 z ON z.batchno = b.batchno AND z.polno = b.polno
                WHERE f.enddate IS NULL 
                AND d.statcode <> 529 
                AND d.statcode <> 2653 
                AND b.trantype = 10009112
                AND d.prodcode IN (SELECT map_value FROM adw_prod_tgt.nlr_data_mapping WHERE map_description IN ('NLR_EOM_D2C_PRODUCTS','NLR_EOM_OTHER_AH_PRODUCTS')
                                           UNION
                                           SELECT 'PHB' FROM dual
                                          )
                        AND b.batchno IN (SELECT batchno FROM adw_prod_tgt.TEMP_EOM_AHR_DAILY)
                        --AND b.polno = 'AH-IR-HO-20-0004413-00-D'
                GROUP BY b.batchno, b.proposal_no, b.polno, f.t1, g.nameid, d.prodcode, d.statcode, h.billmode, g.dist, f.subline, 
                f.LOB, d.statcode, d.policytype, f.prodcategory, d.segmentcode, d.branchcode, f.co_code, d.currency, f.orgtype, f.mainline, 
                ' ', g.nameid, g.pertype, b.trantype, d.issue_source, 
                z.netprem,-- z.netpremtot, 
                z.docstamps,-- z.docsamt, 
                z.lgt,-- z.lgtamt, 
                z.premtax,-- z.ptaxamt, 
                z.totamtdue,-- z.gpremtot, 
                z.commamt, 
                z.commwtaxamt, 
                z.commwtax,-- z.comwtax, 
                z.sfeeamt, 
                z.sfeetax, 
                z.sfeewtaxamt,-- z.sfeewtaxamt,
                z.issuedate,-- z.trandate AS trandateparm, 
                z.othercharges,-- z.other_charges 
                z.vatamt, 
                z.totsi, 
                b.effdate, d.pol_source, b.userid, xx.effdate, x.max_agtno, cref.refdesc, gstat.refdesc, gmode.refdesc, ti.ordate, pd.expdate, pd.effdate, tb.billeffdte,
                gg.agtno,cc.namestr,ba.namestr,da.refdesc,bb.namestr,fa.refdesc,dc.refdesc,bx.clntid,tb.issuedate
                ;
                




   INSERT INTO TEMP_NLR_PORTFOLIO_PISC_AH_DAILY --insert the second data  
    (
    batchno,polno,prodline,polyear,serv_agentcode,
    serv_agentname,segment_code,cardclient,clntid,
    policyholder,poltrneffdate,effdate,expdate,trandate,branchcode,
    prodtype,polstat,polmode,accountofficer,dist,
    dist_value,currency,lob,subline,statcode,
    policytype,prodcategory,org_type,segcode_seq,compcode,
    mainline,agtno,trantype,issue_source,netpremtot,
    docsamt,lgtamt,ptaxamt,gpremtot,commamt,
    commwtaxamt,comwtax,sfeeamt,sfeetax,sfeewtaxamt,
    trandateparm,other_charges,vatamt,totsi,effdate2,
    proposal_no,polsource,acct_officer,userid,polnum,
    reportname,acctgdst,acctgtsi,acctgothchrg,ednt_type,
    incept_dt

    )
      SELECT   b.batchno,  --1
         b.polno, --2
         SUBSTR (f.t1, 1, 1) AS prodline,  --3 
         adw_prod_tgt.fn_nlr_polyr_bytran_batchno (b.polno, b.batchno) polyear, --4 
         gg.agtno AS serv_agentcode,
         (adw_prod_tgt.parsename_temp (cc.namestr, 'LFM', 'FML')) AS serv_agentname,
         da.refdesc AS segment_code,
         ' ' AS cardclient,  --8
         bx.clntid AS clntid,
         (adw_prod_tgt.parsename_temp(ba.namestr, 'LFM', 'FML')) AS policyholder,
         b.effdate AS poltrneffdate, --11
         ins.effdate  AS effdate,   -- 12
         ins.expdate  AS expdate,   -- 13
         MAX (b.trandate) AS trandate, --14
         d.branchcode AS branchcode, 
         d.prodcode AS prodtype,
         gstat.refdesc AS polstat, gmode.refdesc AS polmode,  
         adw_prod_tgt.parsename_temp(bb.namestr, 'LFM', 'FML') AS accountofficer,   
         CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE (g.dist / 100) END AS dist,
         CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE g.dist END AS dist_value, 
         cref.refdesc AS currency,
         f.LOB, 
         f.subline, 
         d.statcode, 
         d.policytype, 
         f.prodcategory, 
         f.orgtype AS org_type, 
         d.segmentcode AS segcode_seq, 
         SUBSTR (fa.refdesc, 1, 1) AS compcode, 
        f.mainline, CASE WHEN g.pertype = 562 THEN x.max_agtno ELSE NULL END agtno,
        b.trantype, d.issue_source, 
        --z.netpremtot,
        yy.netprem, --add basic premium
        --z.docsamt,
        yy.docstamps, --add dst
        --z.lgtamt, 
        yy.lgt, --add lgt
        --z.ptaxamt, 
        yy.premtax,--add premtax
        yy.totamtdue as gpremtot,
        --z.commamt,
        yy.commamt, --add
        --z.commwtaxamt, 
        yy.commwtaxamt,
--        z.comwtax,
        yy.commwtax,-- add
--        z.sfeeamt,
        yy.sfeeamt, --add 
        --z.sfeetax,
        yy.sfeetax,--add
        --z.sfeewtaxamt,
        yy.sfeewtaxamt,--add
        -- z.trandate AS trandateparm, 
        yy.issuedate AS trandateparm, 
       -- z.other_charges,
        yy.othercharges, --add
        0 vatamt, 
        --z.totsi, 
        yy.totsi, --add
        b.effdate AS effdate2, b.proposal_no,
        dc.refdesc as polsource,  
        ' ' acct_officer, b.userid, b.polno AS polnum, ' ' AS reportname, 0 acctgdst, 0 acctgtsi, 0 acctgothchrg,
        CASE WHEN b.trantype <> 10009112 THEN CASE WHEN yy.netprem > 0 THEN 'A' WHEN yy.netprem < 0 THEN 'R' ELSE 'N' END ELSE NULL END ednt_type,
        xx.effdate AS incept_dt
  FROM adw_prod_tgt.nlr_policy_tran_v2 b
       LEFT JOIN (Select distinct cnt.namestr,npt.polno from nlr_polrole_trn_v2 npt,  cnb_namelst_trn_v2 cnt where npt.nameid = cnt.nameid and npt.pertype = 556 and npt.enddate is null) ba
       ON ba.polno = b.polno --policyholder
       LEFT JOIN (Select distinct cntt.namestr,nptt.polno from nlr_polrole_trn_v2 nptt,  cnb_namelst_trn_v2 cntt where nptt.nameid = cntt.nameid and nptt.pertype = 862 and nptt.enddate is null) bb
       ON bb.polno = b.polno --account officer
       LEFT JOIN adw_prod_tgt.nlr_poldate_mst_v2 xx ON xx.polno = b.polno
       LEFT JOIN NLR_POLROLE_TRN_V2 ax ON b.polno = ax.polno AND ax.pertype = 556 AND ax.enddate IS NULL--clntid -1 
       LEFT JOIN cnb_namelst_trn_v2 bx ON ax.nameid = bx.nameid  --clntid - 2
       LEFT JOIN (
    SELECT 
           z.batchno,
           z.polno,
           MAX(x.effdate)     AS effdate,
           MAX(x.expirydate)  AS expdate
    FROM   adw_prod_tgt.nlr_insured_mst_v2 x
           JOIN adw_prod_tgt.nlr_insured_trn_v2 z
             ON z.inseqno = x.inseqno
            AND z.polno   = x.polno
    GROUP BY z.batchno, z.polno
) ins
      ON ins.batchno = b.batchno
      AND ins.polno   = b.polno 
       JOIN adw_prod_tgt.nlr_policy_mst_v2 d ON b.polno = d.polno
       LEFT JOIN adw_prod_tgt.gct_geninfo_ref dc 
       ON dc.refseqno = d.pol_source --polsource
       LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 da ON d.segmentcode = da.refseqno --segment_code
       LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 cref ON cref.refseqno = d.currency
       LEFT JOIN adw_prod_tgt.gct_geninfo_ref gstat ON gstat.refseqno = d.statcode   
       JOIN adw_prod_tgt.grb_product_mst_iris f ON d.prodcode = f.prodcode
       LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 fa ON f.co_code = fa.refseqno --compcode
       LEFT JOIN adw_prod_tgt.nlr_polrole_trn_v2 g ON d.polno = g.polno AND g.pertype = 562 AND g.enddate IS NULL
       LEFT JOIN adw_prod_tgt.xag_profile_v2 gg ON g.nameid = gg.nameid --serv_agentcode
       LEFT JOIN adw_prod_tgt.cnb_namelst_trn_v2 cc ON g.nameid = cc.nameid --serv_agentname
       LEFT JOIN (SELECT nameid, MAX(agtno) AS max_agtno FROM adw_prod_tgt.xag_profile_v2 GROUP BY nameid) x ON x.nameid = g.nameid
       JOIN adw_prod_tgt.nlr_polbill_ref_v2 h ON h.polno = b.polno AND h.enddate IS NULL
       LEFT JOIN adw_prod_tgt.gct_geninfo_ref gmode ON gmode.refseqno = h.billmode     
      --  JOIN adw_prod_tgt.nlr_premium_summary_v2 z ON z.batchno = b.batchno AND z.polno = b.polno --
      --  JOIN adw_prod_tgt.NLR_PREMIUM_SUMMARY_CURR z ON z.batchno = b.batchno AND z.polno = b.polno --change nlr_premium_summary_v2 to NLR_PREMIUM_SUMMARY_CURR
       JOIN adw_prod_tgt.nlr_billing_mst_v2 yy ON yy.batchno = b.batchno AND yy.polno = b.polno
 WHERE f.enddate IS NULL 
 AND d.statcode <> 529
 AND d.statcode <> 2653
 AND b.trantype = 10009081   
 AND d.prodcode IN (SELECT map_value
                                      FROM adw_prod_tgt.nlr_data_mapping
                                     WHERE map_description in ('NLR_EOM_D2C_PRODUCTS','NLR_EOM_OTHER_AH_PRODUCTS')
                     UNION
                     SELECT 'PHB' FROM dual
                   )
and b.polno in (select polno from adw_prod_tgt.TEMP_EOM_AHR_CANCEL_DAILY)
--AND b.polno = 'AH-BP-HO-24-0000226-00-D'
GROUP  BY b.batchno, b.proposal_no, b.polno, f.t1, g.nameid, d.prodcode, d.statcode, h.billmode, g.dist, f.subline, f.LOB,
                   d.statcode, d.policytype, f.prodcategory, d.segmentcode, d.branchcode, f.co_code, d.currency, f.orgtype, f.mainline,
                   g.nameid, g.pertype, b.trantype, d.issue_source, 
                  --  z.gpremtot, 
                  --  z.netpremtot, 
                   yy.netprem, --add basic premium
                   yy.docstamps, --add dst
                   yy.lgt, --add lgt
                   yy.premtax,--add premtax
                   yy.commwtaxamt, --add commwtaxamt
                   yy.sfeeamt, --add 
                   yy.sfeetax,--add
                   yy.sfeewtaxamt,--add
                   yy.totsi, --add
                   yy.othercharges, -- add
                   yy.commwtax,
                   yy.totamtdue, 
                  --  z.docsamt, 
                  --  z.lgtamt, 
                  --  z.ptaxamt, 
                  --  z.commamt,
                   yy.commamt,
                  --  z.commwtaxamt, 
                  --  z.comwtax, 
                  --  z.sfeeamt, 
                  --  z.sfeetax, 
                  --  z.sfeewtaxamt, 
                  --  z.trandate, 
                  --  z.other_charges, 
                  --  z.vatamt, 
                  --  z.totsi, 
                   b.effdate,
                   d.pol_source, b.userid,xx.effdate,cref.refdesc,gstat.refdesc,gmode.refdesc,ins.effdate,ins.expdate,x.max_agtno,
                   gg.agtno,cc.namestr,da.refdesc,bx.clntid,ba.namestr,bb.namestr,fa.refdesc,dc.refdesc
                   ;

  adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_AH_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');

  
  adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','DELETE');
  DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;

  adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,'','INSERT');
  INSERT INTO TEMP_NLR_PORTFOLIO_PISC_DAILY 
(
    batchno,polno,prodline,polyear,serv_agentcode,
    serv_agentname,segment_code,cardclient,clntid,
    policyholder,poltrneffdate,effdate,expdate,trandate,branchcode,
    prodtype,polstat,polmode,accountofficer,dist,
    dist_value,currency,lob,subline,statcode,
    policytype,prodcategory,org_type,segcode_seq,compcode,
    mainline,agtno,trantype,issue_source,netpremtot,
    docsamt,lgtamt,ptaxamt,gpremtot,commamt,
    commwtaxamt,comwtax,sfeeamt,sfeetax,sfeewtaxamt,
    trandateparm,other_charges,vatamt,totsi,effdate2,
    proposal_no,polsource,acct_officer,userid,polnum,
    reportname,acctgdst,acctgtsi,acctgothchrg,ednt_type,
    incept_dt

) 
  SELECT 
    batchno,polno,prodline,polyear,serv_agentcode,
    serv_agentname,segment_code,cardclient,clntid,
    policyholder,poltrneffdate,effdate,expdate,trandate,branchcode,
    prodtype,polstat,polmode,accountofficer,dist,
    dist_value,currency,lob,subline,statcode,
    policytype,prodcategory,org_type,segcode_seq,compcode,
    mainline,agtno,trantype,issue_source,netpremtot,
    docsamt,lgtamt,ptaxamt,gpremtot,commamt,
    commwtaxamt,comwtax,sfeeamt,sfeetax,sfeewtaxamt,
    trandateparm,other_charges,vatamt,totsi,effdate2,
    proposal_no,polsource,acct_officer,userid,polnum,
    reportname,acctgdst,acctgtsi,acctgothchrg,ednt_type,
    incept_dt
   FROM TEMP_NLR_PORTFOLIO_PISC_AH_DAILY;
   
  COMMIT;
  adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_AHDETAILS_DAILY',SYSDATE,SYSDATE,'UPDATE');


END SP_AH_AHDETAILS_DAILY;