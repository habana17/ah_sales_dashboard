create
or replace PROCEDURE SP_MPII_SALES_DAILY (p_date IN DATE)
AS 

/******************************************************************************

NAME:       SP_MPII_SALES_DAILY
PURPOSE:    travel & ah transactions for mpii

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/03/2025            Francis          1. SP_MPII_SALES_DAILY
2.0        09/16/2025            Francis          1. change the channel ,platform,location coe 
NOTES:

 ******************************************************************************/

BEGIN


-- Step 1: Insert data to TEMP_MPII_RAW_PORTFOLIO_DAILY
adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_RAW_PORTFOLIO_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','DELETE');
DELETE 
FROM 
adw_prod_tgt.TEMP_MPII_RAW_PORTFOLIO_DAILY;

adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_RAW_PORTFOLIO_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','INSERT');
INSERT INTO adw_prod_tgt.TEMP_MPII_RAW_PORTFOLIO_DAILY 
(
    POLTRANDATE,BATCHNO,POLNO,PRODLINE,POLYEAR,
    SERV_AGENTCODE,SERV_AGENTNAME,SEGMENT_CODE,
    CARDCLIENT,CLNTID,POLICYHOLDER,POLTRNEFFDATE,EFFDATE,
    EXPDATE,TRANDATE,BRANCHCODE,PRODTYPE,POLSTAT,
    POLMODE,ACCOUNTOFFICER,DIST,DIST_VALUE,CURRENCY,
    LOB,SUBLINE,STATCODE,POLICYTYPE,PRODCATEGORY,
    ORG_TYPE,SEGCODE_SEQ,COMPCODE,MAINLINE,AGTNO,
    TRANTYPE,ISSUE_SOURCE,NETPREMTOT,DOCSAMT,LGTAMT,
    PTAXAMT,GPREMTOT,COMMAMT,COMMWTAXAMT,COMWTAX,
    SFEEAMT,SFEETAX,SFEEWTAXAMT,TRANDATEPARM,
    OTHER_CHARGES,VATAMT,TOTSI,EFFDATE2,
    PROPOSAL_NO,POLSOURCE,ACCT_OFFICER,USERID,
    POLNUM,REPORTNAME,ACCTGDST,ACCTGTSI,ACCTGOTHCHRG,
    EDNT_TYPE,INCEPT_DT,EXTRACTSTART,EXTRACTEND
)
select 
    POLTRANDATE,BATCHNO,POLNO,PRODLINE,POLYEAR,
    SERV_AGENTCODE,SERV_AGENTNAME,SEGMENT_CODE,
    CARDCLIENT,CLNTID,POLICYHOLDER,POLTRNEFFDATE,EFFDATE,
    EXPDATE,TRANDATE,BRANCHCODE,PRODTYPE,POLSTAT,
    POLMODE,ACCOUNTOFFICER,DIST,DIST_VALUE,CURRENCY,
    LOB,SUBLINE,STATCODE,POLICYTYPE,PRODCATEGORY,
    ORG_TYPE,SEGCODE_SEQ,COMPCODE,MAINLINE,AGTNO,
    TRANTYPE,ISSUE_SOURCE,NETPREMTOT,DOCSAMT,LGTAMT,
    PTAXAMT,GPREMTOT,COMMAMT,COMMWTAXAMT,COMWTAX,
    SFEEAMT,SFEETAX,SFEEWTAXAMT,TRANDATEPARM,
    OTHER_CHARGES,VATAMT,TOTSI,EFFDATE2,
    PROPOSAL_NO,POLSOURCE,ACCT_OFFICER,USERID,
    POLNUM,REPORTNAME,ACCTGDST,ACCTGTSI,ACCTGOTHCHRG,
    EDNT_TYPE,INCEPT_DT,EXTRACTSTART,EXTRACTEND
 from                         
(    SELECT       
                  b.timestmp as poltrandate, 
                  b.batchno, 
                  b.polno, 
                  SUBSTR (f.t1, 1, 1) AS prodline,  
                  adw_prod_tgt.fn_nlr_polyr_bytran_batchno_mpprd (b.polno, b.batchno) polyear,                        
                  adw_prod_tgt.fngetagtnobynameid_mpprd (g.nameid) AS serv_agentcode, 
                  ( adw_prod_tgt.parsename_temp_mpprd (adw_prod_tgt.fngetnamestr_mpprd (g.nameid), 'LFM', 'FML')) AS serv_agentname,                        
                  adw_prod_tgt.fngetrefdesc_mpprd (d.segmentcode) AS segment_code, 
                  ' ' AS cardclient,  
                  adw_prod_tgt.fn_nlr_getclntidbynameid_ai_mpprd (b.polno, 556) AS clntid,                        
                 ( adw_prod_tgt.parsename_temp_mpprd( adw_prod_tgt.nlr_fngetnamestrbypertype_mpprd (b.polno, 556), 'LFM', 'FML')) AS policyholder, 
                 b.effdate AS poltrneffdate,                        
                 (SELECT MAX (x.effdate) FROM  adw_prod_tgt.mpprd_nlr_insured_mst x WHERE EXISTS ( SELECT 1 FROM  adw_prod_tgt.mpprd_nlr_insured_trn z WHERE z.batchno = b.batchno AND z.inseqno = x.inseqno AND z.polno = x.polno)) effdate,                        
                 (SELECT MAX (x.expirydate) FROM  adw_prod_tgt.mpprd_nlr_insured_mst x WHERE EXISTS (SELECT 1 FROM  adw_prod_tgt.mpprd_nlr_insured_trn z WHERE z.batchno = b.batchno AND z.inseqno = x.inseqno AND z.polno = x.polno)) AS expdate,                        
                 MAX (b.trandate) AS trandate,                         
                 d.branchcode AS branchcode, 
                 d.prodcode AS prodtype,                        
                 (SELECT refdesc FROM  adw_prod_tgt.mpprd_gct_geninfo_ref WHERE refseqno = d.statcode) polstat, 
                 (SELECT refdesc FROM  adw_prod_tgt.mpprd_gct_geninfo_ref WHERE refseqno = h.billmode) polmode,                        
                 adw_prod_tgt.parsename_temp_mpprd( adw_prod_tgt.nlr_fngetnamestrbypertype_mpprd (b.polno, 862), 'LFM', 'FML') AS accountofficer, 
                 CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE (g.dist / 100) END AS dist,                        
                 CASE WHEN g.dist IS NULL THEN 1 WHEN g.dist = 0 THEN 1 ELSE g.dist END AS dist_value, 
                 (SELECT refdesc FROM  mpprd_cxx_geninfo_ref WHERE refseqno = d.currency) AS currency,                         
                 f.LOB, 
                 f.subline, 
                 d.statcode, 
                 d.policytype, 
                 f.prodcategory, 
                 f.orgtype AS org_type, 
                 d.segmentcode AS segcode_seq, 
                 SUBSTR ( fngetrefdesc_mpprd (f.co_code), 1, 1) AS compcode,                        
                 case  when  f.mainline  = 'TR' THEN 'GA' ELSE f.mainline end as mainline, 
                 CASE WHEN g.pertype = 562 THEN (SELECT MAX (x.agtno) FROM adw_prod_tgt.mpprd_xag_profile x WHERE x.nameid = g.nameid) ELSE NULL END agtno,                        
                 b.trantype, 
                 d.issue_source, 
                 z.netpremtot, 
                 z.docsamt, 
                 z.lgtamt, 
                 z.ptaxamt, 
                 z.gpremtot, 
                 z.commamt, 
                 z.commwtaxamt, 
                 z.comwtax, 
                 z.sfeeamt, 
                 z.sfeetax, 
                 z.sfeewtaxamt,                        
                 z.trandate AS trandateparm, 
                 z.other_charges,  
                 0 vatamt, 
                 z.totsi, 
                 b.effdate AS effdate2, 
                 b.proposal_no,                        
                 adw_prod_tgt.fn_grp_getrefdesc_mpprd (d.pol_source) polsource, 
                 ' ' acct_officer, 
                 b.userid, 
                 b.polno AS polnum, 
                 ' ' AS reportname, 
                 0 acctgdst, 
                 0 acctgtsi, 
                 0 acctgothchrg,                        
                 CASE WHEN b.trantype <> 10009112 THEN CASE WHEN z.netpremtot > 0 THEN 'A' WHEN z.netpremtot < 0 THEN 'R' ELSE 'N' END ELSE NULL END ednt_type,                        
                 (SELECT xx.effdate FROM  adw_prod_tgt.mpprd_nlr_poldate_mst xx WHERE xx.polno = b.polno and enddate is null) AS incept_dt, 

                --old script 
                --  to_date('12/01/2024', 'MM/DD/YYYY') extractstart, 
                --  to_date('12/31/2024', 'MM/DD/YYYY') extractend
                --  new script 
                 sysdate extractstart,  --new script 
                 sysdate extractend     --new script                     
  FROM  
  adw_prod_tgt.mpprd_nlr_policy_tran b,  
  adw_prod_tgt.mpprd_nlr_policy_mst d,  
  adw_prod_tgt.mpprd_nlr_polrole_trn g,  
  adw_prod_tgt.mpprd_grb_product_mst f,  
  adw_prod_tgt.mpprd_nlr_polbill_ref h,                          
  adw_prod_tgt.mpprd_nlr_premium_summary z                        
 WHERE 
 f.enddate IS NULL 
 AND g.enddate IS NULL 
 AND d.statcode <> 529                         
 AND g.pertype = 562                         
 AND b.polno = d.polno 
 AND d.prodcode = f.prodcode                        
 AND d.polno = g.polno                          
 AND z.batchno = b.batchno 
 AND z.polno = b.polno 
 AND d.statcode <> 2653                        
 AND b.batchno not in (select  b.batchno from adw_prod_tgt.mpprd_nlr_insured_mst a,  adw_prod_tgt.mpprd_nlr_insured_trn  b                        
                          where a.statcode = 2565 
                            
                            --olf fixed dates script
                            --and a.effdate between to_date('12/01/2024', 'MM/DD/YYYY') AND to_date('12/31/2024', 'MM/DD/YYYY')   -- fixed dates

                            --optimized fixed dates script 
                            --and a.effdate >= DATE '2025-07-01' AND a.effdate < DATE '2025-08-01' --optimize fixed dates
                            AND trunc(a.effdate) = p_date --incremental
 
                            and a.inseqno = b.inseqno )                             
  GROUP BY b.batchno, b.proposal_no, b.polno, f.t1, g.nameid, d.prodcode, d.statcode,                        
  g.dist, f.subline, f.LOB,                        
                   d.statcode, d.policytype, f.prodcategory, d.segmentcode, d.branchcode, f.co_code, d.currency, f.orgtype, f.mainline,                        
                   g.nameid, g.pertype, b.trantype, d.issue_source, z.gpremtot, z.netpremtot, z.docsamt, z.lgtamt, z.ptaxamt, z.commamt,                        
                   z.commwtaxamt, z.comwtax, z.sfeeamt, z.sfeetax, z.sfeewtaxamt, z.trandate, z.other_charges, z.vatamt, z.totsi, b.effdate,                        
                   d.pol_source, b.userid   ,   h.billmode, b.timestmp                             
) 
  where 1=1 
  --and trunc(poltrandate) = p_date --incremental
  and trunc(trandate) = p_date 
  
--   1 = (

--   --old fixed dates script 
--   -- case when trantype in (10009081, 10009091) and trunc(poltrandate) between to_date('12/01/2024', 'MM/DD/YYYY') AND to_date('12/31/2024', 'MM/DD/YYYY') then 1 --fixed dates
--   -- when trantype not in (10009081, 10009091) and trunc(effdate) between to_date('12/01/2024', 'MM/DD/YYYY') AND to_date('12/31/2024', 'MM/DD/YYYY') then 1 --fixed dates
--   -- end

-- --optimize fixed date script   W
-- --   CASE 
-- --     WHEN trantype IN (10009081, 10009091,10009140) 
-- --          AND poltrandate >= DATE '2025-07-01' 
-- --          AND poltrandate < DATE '2025-08-01' 
-- --     THEN 1 -- optimized fixed dates

-- --     WHEN trantype NOT IN (10009081, 10009091) 
-- --          AND effdate >= DATE '2025-07-01' 
-- --          AND effdate < DATE '2025-08-01' 
-- --     THEN 1 -- optimize fixed dates 
-- -- END

-- --incremental
-- CASE 
--     WHEN trantype IN (10009081, 10009091,10009140) 
--          AND trunc(poltrandate) = p_date
--     THEN 1 -- incremental (yesterday’s transactions) optimize

--     WHEN trantype NOT IN (10009081, 10009091) 
--          AND trunc(effdate)  = p_date
--     THEN 1 -- incremental (yesterday’s effective dates) optimize 
-- END
  
--             )
;
adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_RAW_PORTFOLIO_DAILY','SP_MPII_SALES_DAILY',SYSDATE,SYSDATE,'UPDATE');


-- Step 2: Insert data to TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY
adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','DELETE');
DELETE 
FROM 
adw_prod_tgt.TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY;

adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','INSERT');
INSERT INTO adw_prod_tgt.TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY 
(
    POLNO, BATCHNO,INTMCD, INTERMEDIARY, ASSD_NO, ASSURED_NAME, CO_CD, LINE_PREF, SUBLINE_PREF, ISS_PREF,
    POL_YY, POL_SEQ_NO, REN_SEQ_NO, POLICY_TYPE,ENDT_ISS_PREF, ENDT_YY, ENDT_SEQ_NO, EDNT_TYPE, AFFECT_TAG,
    POLICY_STATUS, INCEPT_DT, EXPIRY_DT, ISSUE_DT, POLTRNEFFDATE, EFF_DT, ITEM_NO, PERIL_CD,
    PREM_AMT, TSI_AMT, CURR_CD, DOC_STAMPS, VAT_AMT, PREM_TAX, OCHR_FST, OTH_CHRGS,
    COMM_AMT, COMM_RATE, COMM_TAX,SOURCE_SYS, ACCTC_TAG, LOC_GOV_TAX, NET_RET_PREM, NET_RET_TSI, 
    TOT_FAC_PREM, TOT_FAC_TSI, XOL_PREM_AMT,ORG_TYPE, SEGMENT_CODE, POLTYPE,
    EFFDATE, CLNTID, AGTNO, TRANDATE, EFFDATE_PARM, SFEEAMT, SFEETAX, SFEEWTAXAMT,
    CREATED_BY, POLSOURCE, ACCT_OFFICER, USERID, POLNUM, REPORTNAME, ACCTGDST, 
    ACCTGTSI, ACCTGOTHCHRG,ISS_NAME, TRANTYPE, TERM, GPREMTOT, TRANDATEPARM
)

SELECT DISTINCT 
       a.polno,
       a.batchno,
       
       -- Intermediary Code
       CASE
           WHEN adw_prod_tgt.fn_nlr_getpisccode_mpprd(a.serv_agentcode) = ' '
             OR adw_prod_tgt.fn_nlr_getpisccode_mpprd(a.serv_agentcode) IS NULL
           THEN '1'
           ELSE adw_prod_tgt.fn_nlr_getpisccode_mpprd(a.serv_agentcode)
       END AS intmcd,
       
       -- Intermediary Name
       adw_prod_tgt.fngetagtnamebyagtno_mpprd(a.serv_agentcode) AS intermediary,
       
       0 AS assd_no,
       a.policyholder AS assured_name,
       a.compcode AS co_cd,
       
       -- Line Prefix
       CASE
           WHEN a.mainline = 'AH' THEN 'AC'
           WHEN a.mainline = 'TR' THEN 'GA'
           ELSE a.mainline
       END AS line_pref,
       
       a.subline AS subline_pref,
       
       -- Issue Prefix
       CASE
           WHEN a.mainline = 'TR' THEN 
                CASE
                    WHEN a.polsource = 'NLR' THEN TRIM(LEADING '-' FROM (
                        SUBSTR(polno, 6, INSTR(SUBSTR(polno, 7), '-', 1))
                    ))
                    ELSE a.issue_source
                END
           ELSE a.issue_source
       END AS iss_pref,
       
       TO_NUMBER(TO_CHAR(a.effdate, 'YYYY')) AS pol_yy,
       
       SUBSTR(
           REPLACE(
               SUBSTR(a.polno,
                      INSTR(a.polno, '-', 1, 4),
                      INSTR(a.polno, '-', 1, 5) - INSTR(a.polno, '-', 1, 4)),
               '-',
               ''
           ),
           4
       ) AS pol_seq_no,
       
       TO_NUMBER('0') AS ren_seq_no,
       'D' AS policy_type,
       
       -- Endorsement Issue Prefix
       CASE
           WHEN a.trantype IN (10009112, 10009090) THEN ''
           ELSE a.issue_source
       END AS endt_iss_pref,
       
       CASE
           WHEN a.trantype IN (10009112, 10009090) THEN ' '
           ELSE TO_CHAR(a.effdate, 'YYYY')
       END AS endt_yy,
       
       CASE
           WHEN a.trantype IN (10009112, 10009090) THEN ' '
           ELSE TO_CHAR(a.batchno)
       END AS endt_seq_no,
       
       a.ednt_type,
       ' ' AS affect_tag,
       
       -- Policy Status
       CASE
           WHEN a.trantype IN (10009112, 10009090) THEN '1' -- NB
           WHEN a.trantype IN (10010347) THEN '5'           -- Spoilage
           WHEN a.trantype IN (10006463, 10009081, 10009093, 10009714) THEN '4'
           ELSE '0'                                         -- Endorsement
       END AS policy_status,
       
       -- Inception Date
       DECODE(a.mainline, 'TR', a.effdate, incept_dt) AS incept_dt,
       
       TO_CHAR(a.expdate, 'DD-Mon-RRRR') AS expiry_dt,
       TO_CHAR(a.trandate, 'DD-Mon-RRRR') AS issue_dt,
       a.poltrneffdate,
       TO_CHAR(a.effdate, 'DD-Mon-RRRR') AS eff_dt,
       
       '1' AS item_no,
       
       -- Peril Code
       CASE
           WHEN a.mainline = 'TR' THEN 17
           ELSE NVL(adw_prod_tgt.fn_nlr_getperilcde_mpprd(a.polno, 505, 508), 0)
       END AS peril_cd,
         

       
       NVL(a.netpremtot, 0) AS prem_amt,
       a.totsi AS tsi_amt,
       a.currency AS curr_cd,
       a.docsamt AS doc_stamps,
       a.vatamt AS vat_amt,
       a.ptaxamt AS prem_tax,
       0 AS ochr_fst,
       a.other_charges AS oth_chrgs,
       
       NVL(a.commamt, a.sfeeamt) AS comm_amt,
       
       -- Commission Rate
       (SELECT CASE
                   WHEN z.commrate = 0 THEN z.sfeerate
                   ELSE NVL(z.commrate, z.sfeerate)
               END
          FROM adw_prod_tgt.mpprd_nlr_billing_mst z
         WHERE z.polno = a.polno
           AND a.batchno = z.batchno
           AND ROWNUM = 1) AS comm_rate,
      
       
       NVL(a.commwtaxamt, a.sfeewtaxamt) AS comm_tax,
       
       '3' AS source_sys,
       'N' AS acctc_tag,
       a.lgtamt AS loc_gov_tax,
       0 AS net_ret_prem,
       0 AS net_ret_tsi,
       0 AS tot_fac_prem,
       0 AS tot_fac_tsi,
       0 AS xol_prem_amt,
       a.org_type,
       a.segment_code,
       
       -- Policy Type
       CASE
           WHEN a.polyear <= 1 THEN 'NB'
           ELSE 'RN'
       END AS poltype,
       
       a.effdate,
       a.clntid,
       a.agtno,
       a.trandate,
       a.effdate AS effdate_parm,
       a.sfeeamt,
       a.sfeetax,
       a.sfeewtaxamt,
       
       adw_prod_tgt.fn_nlr_getcollsoausername_mpprd(a.proposal_no) AS created_by,
       a.polsource,
       ' ' AS acct_officer,
       a.userid,
       a.polnum,
       a.reportname,
       a.acctgdst,
       a.acctgtsi,
       a.acctgothchrg,
       
       -- Issue Name
       NVL(
           CASE
               WHEN a.mainline = 'TR' THEN 
                    CASE
                        WHEN polsource = 'NLR' THEN (
                            SELECT map_value
                              FROM adw_prod_tgt.mpprd_nlr_data_mapping x
                             WHERE map_description = 'ISSUE_SOURCE'
                               AND x.list_of_value = TRIM(
                                   LEADING '-' FROM SUBSTR(polno, 6, INSTR(SUBSTR(polno, 7), '-', 1))
                               )
                        )
                        ELSE (
                            SELECT x.iss_name
                              FROM adw_prod_tgt.mpprd_nlr_branch_ref x
                             WHERE x.iss_pref = a.issue_source
                        )
                    END
               ELSE (
                   SELECT x.iss_name
                     FROM adw_prod_tgt.mpprd_nlr_branch_ref x
                    WHERE x.iss_pref = a.issue_source
               )
           END,
           (SELECT x.iss_name
              FROM adw_prod_tgt.mpprd_nlr_branch_ref x
             WHERE x.iss_pref = a.issue_source)
       ) AS iss_name,
       
       -- Transaction Type
       CASE
           WHEN a.trantype IN (10009112, 10009090) THEN 'NEW'
           WHEN a.trantype IN (10006463, 10009081, 10009093, 10009714) THEN 'CANCELLATION'
           WHEN a.trantype = 10010347 THEN 'SPOILAGE'
           ELSE 'ENDT'
       END AS trantype,
       
       adw_prod_tgt.fn_nlr_getsoaterm_mpprd(a.effdate, a.expdate) AS term,
       
       a.gpremtot,
       a.trandateparm
FROM   TEMP_MPII_RAW_PORTFOLIO_DAILY a;
adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,SYSDATE,'UPDATE');


adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','DELETE');
DELETE 
FROM 
adw_prod_tgt.TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY;

-- Step 3: Insert data to TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY 
adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','INSERT');
INSERT INTO adw_prod_tgt.TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY
(
    POLNO, BATCHNO, INTMCD, INTERMEDIARY, ASSD_NO,
    ASSURED_NAME, CO_CD, LINE_PREF, SUBLINE_PREF, ISS_PREF, POL_YY, POL_SEQ_NO,
    REN_SEQ_NO, POLICY_TYPE, ENDT_ISS_PREF, ENDT_YY, ENDT_SEQ_NO, EDNT_TYPE,
    AFFECT_TAG, POLICY_STATUS, INCEPT_DT, EXPIRY_DT, ISSUE_DT, POLTRNEFFDATE,
    EFF_DT, ITEM_NO, PERIL_CD, PREM_AMT, TSI_AMT, CURR_CD, DOC_STAMPS, VAT_AMT,
    PREM_TAX, OCHR_FST, OTH_CHRGS, COMM_AMT, COMM_RATE, COMM_TAX, SOURCE_SYS,
    ACCTC_TAG, LOC_GOV_TAX, NET_RET_PREM, NET_RET_TSI, TOT_FAC_PREM,
    TOT_FAC_TSI, XOL_PREM_AMT, ORG_TYPE, SEGMENT_CODE, POLTYPE, EFFDATE, CLNTID,
    AGTNO, TRANDATE, EFFDATE_PARM, SFEEAMT, SFEETAX, SFEEWTAXAMT, CREATED_BY,
    POLSOURCE, ACCT_OFFICER, USERID, POLNUM, REPORTNAME, ACCTGDST, ACCTGTSI,
    ACCTGOTHCHRG, ISS_NAME, TRANTYPE, TERM, GPREMTOT, TRANDATEPARM,
    PLAN_TYPE, COVERAGE_TYPE, DURATION, INSURED_COUNT, PROMO_CODE, GROUP_POLNO
)
SELECT DISTINCT
       a.polno,a.batchno,a.intmcd,a.intermediary,a.assd_no,
       a.assured_name,a.co_cd,a.line_pref,a.subline_pref,a.iss_pref,
       a.pol_yy,a.pol_seq_no,a.ren_seq_no,a.policy_type,a.endt_iss_pref,
       a.endt_yy,a.endt_seq_no,a.ednt_type,a.affect_tag,a.policy_status,
       a.incept_dt,a.expiry_dt,a.issue_dt,a.poltrneffdate,a.eff_dt,
       a.item_no,a.peril_cd,a.prem_amt,a.tsi_amt,a.curr_cd,
       a.doc_stamps,a.vat_amt,a.prem_tax,a.ochr_fst,a.oth_chrgs,
       a.comm_amt,a.comm_rate,a.comm_tax,a.source_sys,a.acctc_tag,
       a.loc_gov_tax,a.net_ret_prem,a.net_ret_tsi,a.tot_fac_prem,a.tot_fac_tsi,
       a.xol_prem_amt,a.org_type,a.segment_code,a.poltype,a.effdate,
       a.clntid,a.agtno,a.trandate,a.effdate_parm,a.sfeeamt,
       a.sfeetax,a.sfeewtaxamt,a.created_by,a.polsource,a.acct_officer,
       a.userid,a.polnum,a.reportname,a.acctgdst,a.acctgtsi,
       a.acctgothchrg,a.iss_name,a.trantype,a.term,a.gpremtot,
       a.trandateparm,b.plan_description plan_type,b.ins_type coverage_type,
       e.term duration,
       (  SELECT COUNT (*)
            FROM adw_prod_tgt.mpprd_nlr_bill_sched_ins_dtl
           WHERE batchno = a.batchno
        GROUP BY batchno)
           insured_count,
       (SELECT DISTINCT x.discount_code
          FROM adw_prod_tgt.mpprd_tlight_master x, adw_prod_tgt.xag_agt_onboard_ref c
         WHERE x.polno = a.polno AND c.promo_code = x.discount_code)
           promo_code,
--       fngetrefdesc (channel),
--       location,
--       fngetrefdesc (platform),
       CASE WHEN c.group_class = 2308 THEN c.polno ELSE NULL END
           group_polno
  FROM TEMP_MPII_NLR_PORTFOLIO_PISC_EIS_DAILY  a,
       adw_prod_tgt.mpprd_nlr_polcov_trn                b,
       adw_prod_tgt.mpprd_nlr_policy_mst                c,
       adw_prod_tgt.mpprd_nlr_insured_trn               d,
       adw_prod_tgt.mpprd_nlr_insured_mst               e,
       adw_prod_tgt.mpprd_grb_lifemst                   f
 WHERE     a.polno = b.polno
       AND b.polno = c.polno
       AND e.polno = a.polno
       AND a.batchno = d.batchno
       AND d.inseqno = e.inseqno
       AND e.covseqno = b.covseqno
       AND e.lifeid = f.lifeid
    --    and a.trandate >= DATE '2025-07-01' AND a.trandate < DATE '2025-08-01' --incremental
       and trunc(a.trandate) = p_date
       ;
adw_prod_tgt.sp_adw_table_logs('TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,SYSDATE,'UPDATE');


-- Step 4: Insert data to MPII_IRISNLR_TRANSACTIONS_DAILY -- final insert 

adw_prod_tgt.sp_adw_table_logs('MPII_IRISNLR_TRANSACTIONS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,'','INSERT');
INSERT INTO MPII_IRISNLR_TRANSACTIONS_DAILY
(
    POLNO, BATCHNO, INTMCD, INTERMEDIARY, ASSD_NO,
    ASSURED_NAME, CO_CD, LINE_PREF, SUBLINE_PREF, ISS_PREF, POL_YY, POL_SEQ_NO,
    REN_SEQ_NO, POLICY_TYPE, ENDT_ISS_PREF, ENDT_YY, ENDT_SEQ_NO, EDNT_TYPE,
    AFFECT_TAG, POLICY_STATUS, INCEPT_DT, EXPIRY_DT, ISSUE_DT, POLTRNEFFDATE,
    EFF_DT, ITEM_NO, PERIL_CD, PREM_AMT, TSI_AMT, CURR_CD, DOC_STAMPS, VAT_AMT,
    PREM_TAX, OCHR_FST, OTH_CHRGS, COMM_AMT, COMM_RATE, COMM_TAX, SOURCE_SYS,
    ACCTC_TAG, LOC_GOV_TAX, NET_RET_PREM, NET_RET_TSI, TOT_FAC_PREM,
    TOT_FAC_TSI, XOL_PREM_AMT, ORG_TYPE, SEGMENT_CODE, POLTYPE, EFFDATE, CLNTID,
    AGTNO, TRANDATE, EFFDATE_PARM, SFEEAMT, SFEETAX, SFEEWTAXAMT, CREATED_BY,
    POLSOURCE, ACCT_OFFICER, USERID, POLNUM, REPORTNAME, ACCTGDST, ACCTGTSI,
    ACCTGOTHCHRG, ISS_NAME, TRANTYPE, TERM, GPREMTOT, TRANDATEPARM,
    PLAN_TYPE, COVERAGE_TYPE, DURATION, INSURED_COUNT, PROMO_CODE, GROUP_POLNO,
    DISTCHANNEL, CHANNEL, LOCATION, PLATFORM
)
WITH --updated by francisc 09162025
latest_agent_assignments AS (
    SELECT 
        fngetrefdesc(ranked.distchannel) AS distchannel_ref_desc, 
        ranked.agtno, 
        ranked.basebranch,
        ranked.position
    FROM (
        SELECT 
            x.*,
            ROW_NUMBER() OVER (
                PARTITION BY x.agtno 
                ORDER BY x.timestmp DESC
            ) AS rn
        FROM mpprd_xag_assign x
        WHERE x.enddate IS NULL
    ) ranked
    WHERE ranked.rn = 1
),

issue_source_ass AS (
    SELECT   
        polno, 
        pol_source,
        CASE
            WHEN pol_source = 2311 THEN
                CASE
                    WHEN LENGTH(
                             TRIM(LEADING '-' FROM (
                                 SUBSTR(polno, 6, INSTR(SUBSTR(polno, 7), '-', 1))
                             ))
                         ) = 4
                    THEN branchcode
                    ELSE issue_source
                END
            ELSE issue_source
        END AS issue_source
    FROM mpprd_nlr_policy_mst
)

SELECT DISTINCT
    a.POLNO, a.BATCHNO, a.INTMCD, a.INTERMEDIARY, a.ASSD_NO,
    a.ASSURED_NAME, '14' AS CO_CD , a.LINE_PREF, a.SUBLINE_PREF, a.ISS_PREF, a.POL_YY, a.POL_SEQ_NO,
    a.REN_SEQ_NO, a.POLICY_TYPE, a.ENDT_ISS_PREF, a.ENDT_YY, a.ENDT_SEQ_NO, a.EDNT_TYPE,
    a.AFFECT_TAG, a.POLICY_STATUS, a.INCEPT_DT, a.EXPIRY_DT, a.ISSUE_DT, a.POLTRNEFFDATE,
    a.EFF_DT, a.ITEM_NO, a.PERIL_CD, a.PREM_AMT, a.TSI_AMT, a.CURR_CD, a.DOC_STAMPS, a.VAT_AMT,
    a.PREM_TAX, a.OCHR_FST, a.OTH_CHRGS, a.COMM_AMT, a.COMM_RATE, a.COMM_TAX, a.SOURCE_SYS,
    a.ACCTC_TAG, a.LOC_GOV_TAX, a.NET_RET_PREM, a.NET_RET_TSI, a.TOT_FAC_PREM,
    a.TOT_FAC_TSI, a.XOL_PREM_AMT, a.ORG_TYPE, a.SEGMENT_CODE, a.POLTYPE, a.EFFDATE, a.CLNTID,
    a.AGTNO, a.TRANDATE, a.EFFDATE_PARM, a.SFEEAMT, a.SFEETAX, a.SFEEWTAXAMT, a.CREATED_BY,
    a.POLSOURCE, a.ACCT_OFFICER, a.USERID, a.POLNUM, a.REPORTNAME, a.ACCTGDST, a.ACCTGTSI,
    a.ACCTGOTHCHRG, a.ISS_NAME, a.TRANTYPE, a.TERM, a.GPREMTOT, a.TRANDATEPARM,
    a.PLAN_TYPE, a.COVERAGE_TYPE, a.DURATION, a.INSURED_COUNT, a.PROMO_CODE, a.GROUP_POLNO,
    
    --distchannel
    -- a.SEGMENT_CODE AS DISTCHANNEL, 
    c.distchannel_ref_desc AS DISTCHANNEL,

    --channel
    -- y.refdesc as CHANNEL,
    CASE
        WHEN d.nameid IN (
            3931949, 908463, 169951, 182836, 3931717, 3931837, 182836,
            3931511, 6179934, 3934879, 165493, 3931837, 3934843
        ) THEN fngetrefdesc_mpprd(10054505) -- FINANCIAL INSTITUTION
        WHEN c.distchannel_ref_desc = 'CU - Customer Engagement Center'
            THEN fngetrefdesc_mpprd(10054508)
        WHEN c.distchannel_ref_desc = 'PA - Professional Agency'
            THEN fngetrefdesc_mpprd(10054515)
        WHEN c.distchannel_ref_desc = 'MD - Middle Income Market'
            THEN fngetrefdesc_mpprd(10054516)
        WHEN c.distchannel_ref_desc = 'Travel Agency'
            THEN fngetrefdesc_mpprd(10054509)
        WHEN c.distchannel_ref_desc = 'HO - Affinity Broker'
            THEN fngetrefdesc_mpprd(10054506)
        WHEN c.distchannel_ref_desc = 'AP - PISC Broker'
            THEN fngetrefdesc_mpprd(10054506)
        WHEN c.distchannel_ref_desc IN (
            'GA - General Agency', ' HO - VisMin Direct',
            'HO - VisMin Agents', 'LIFE RETAIL', 'HO - PISC'
        ) THEN fngetrefdesc_mpprd(10054507)
        WHEN c.distchannel_ref_desc = 'MI - Microinsurance'
            THEN fngetrefdesc_mpprd(10054511)
        WHEN c.distchannel_ref_desc = 'MG - Migrant'
            THEN fngetrefdesc_mpprd(10054508)
        WHEN c.distchannel_ref_desc = 'Non Life Retail'
            THEN CASE
                WHEN c.position = 10005835 THEN '10054507'
                ELSE fngetrefdesc_mpprd(10054508)
            END
        WHEN g.pisc_code IS NOT NULL
            THEN fngetrefdesc_mpprd(10054507)
        ELSE fngetrefdesc_mpprd(10054508)
    END AS channel,

    --location
    -- b.LOCATION,
    CASE
        WHEN c.distchannel_ref_desc = 'Travel Agency' THEN
            CASE 
                WHEN c.basebranch = 'HO' THEN 'HEAD OFFICE'
                ELSE c.basebranch
            END
        ELSE
            CASE
                WHEN f.iss_name = 'INSURE SHOP'
                    THEN 'HEAD OFFICE '
                ELSE f.iss_name
            END
    END AS location,

    --platform
    -- z.refdesc as PLATFORM
    CASE
        WHEN e.pol_source = 2634 THEN fngetrefdesc_mpprd(10054537)   -- Insureshop
        WHEN e.pol_source = 2310 THEN fngetrefdesc_mpprd(10054539)   -- FLS
        WHEN e.pol_source IN (2846, 2835) THEN fngetrefdesc_mpprd(10054540) -- Microsite
        ELSE fngetrefdesc_mpprd(10054545)   -- NLR
    END AS platform


FROM TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY a
LEFT JOIN latest_agent_assignments c 
    ON c.agtno = a.agtno 
LEFT JOIN mpprd_nlr_polrole_trn d 
    ON d.polno = a.polno
   AND d.enddate IS NULL
   AND d.pertype = 562 
LEFT JOIN issue_source_ass e 
    ON e.polno = a.polno
LEFT JOIN mpprd_nlr_branch_ref f 
    ON e.issue_source = f.iss_pref
LEFT JOIN mpprd_xag_profile g 
    ON g.agtno = a.agtno 
   AND g.nameid = d.nameid
-- JOIN MPPRD_NLR_POLICY_PROD_TRN_V2 b
--      ON a.polno = b.polno
-- LEFT JOIN adw_prod_tgt.mpprd_cxx_geninfo_ref z
--             ON z.refseqno = b.platform
-- LEFT JOIN adw_prod_tgt.mpprd_cxx_geninfo_ref y
--             ON y.refseqno = b.channel             
JOIN mpprd_nlr_insured_trn h
     ON a.polno = h.polno
    AND e.polno = h.polno 
    AND a.batchno = h.batchno
    --AND b.inseqno = h.inseqno
    ;

/*******old 1 *******/
-- SELECT DISTINCT
--     a.POLNO, a.BATCHNO, a.INTMCD, a.INTERMEDIARY, a.ASSD_NO,
--     a.ASSURED_NAME, '14' AS CO_CD , a.LINE_PREF, a.SUBLINE_PREF, a.ISS_PREF, a.POL_YY, a.POL_SEQ_NO,
--     a.REN_SEQ_NO, a.POLICY_TYPE, a.ENDT_ISS_PREF, a.ENDT_YY, a.ENDT_SEQ_NO, a.EDNT_TYPE,
--     a.AFFECT_TAG, a.POLICY_STATUS, a.INCEPT_DT, a.EXPIRY_DT, a.ISSUE_DT, a.POLTRNEFFDATE,
--     a.EFF_DT, a.ITEM_NO, a.PERIL_CD, a.PREM_AMT, a.TSI_AMT, a.CURR_CD, a.DOC_STAMPS, a.VAT_AMT,
--     a.PREM_TAX, a.OCHR_FST, a.OTH_CHRGS, a.COMM_AMT, a.COMM_RATE, a.COMM_TAX, a.SOURCE_SYS,
--     a.ACCTC_TAG, a.LOC_GOV_TAX, a.NET_RET_PREM, a.NET_RET_TSI, a.TOT_FAC_PREM,
--     a.TOT_FAC_TSI, a.XOL_PREM_AMT, a.ORG_TYPE, a.SEGMENT_CODE, a.POLTYPE, a.EFFDATE, a.CLNTID,
--     a.AGTNO, a.TRANDATE, a.EFFDATE_PARM, a.SFEEAMT, a.SFEETAX, a.SFEEWTAXAMT, a.CREATED_BY,
--     a.POLSOURCE, a.ACCT_OFFICER, a.USERID, a.POLNUM, a.REPORTNAME, a.ACCTGDST, a.ACCTGTSI,
--     a.ACCTGOTHCHRG, a.ISS_NAME, a.TRANTYPE, a.TERM, a.GPREMTOT, a.TRANDATEPARM,
--     a.PLAN_TYPE, a.COVERAGE_TYPE, a.DURATION, a.INSURED_COUNT, a.PROMO_CODE, a.GROUP_POLNO,
--     a.SEGMENT_CODE AS DISTCHANNEL, y.refdesc as CHANNEL, b.LOCATION, z.refdesc as PLATFORM
-- FROM TEMP_MPII_IRISNLR_TRANSACTIONS_DAILY a
-- JOIN MPPRD_NLR_POLICY_PROD_TRN_V2 b
--      ON a.polno = b.polno
-- LEFT JOIN adw_prod_tgt.mpprd_cxx_geninfo_ref z
--             ON z.refseqno = b.platform
-- LEFT JOIN adw_prod_tgt.mpprd_cxx_geninfo_ref y
--             ON y.refseqno = b.channel             
-- JOIN mpprd_nlr_insured_trn c
--      ON b.polno = c.polno
--     AND a.batchno = c.batchno
--     AND b.inseqno = c.inseqno;
/*******old 1 *******/

COMMIT;

adw_prod_tgt.sp_adw_table_logs('MPII_IRISNLR_TRANSACTIONS_DAILY','SP_MPII_SALES_DAILY',SYSDATE,SYSDATE,'UPDATE');

END SP_MPII_SALES_DAILY;