create
or replace PROCEDURE SP_AH_TRAVEL_CTRIP_DAILY (p_date IN DATE)
AS 

/******************************************************************************

NAME:       SP_AH_TRAVEL_CTRIP_DAILY
PURPOSE:    travel & ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/03/2025            Francis          1. SP_AH_TRAVEL_CTRIP_DAILY
2.0        08/11/2025            Francis          1. Changed the incremental into trandate
                                                  2. Changed not in to not exists

NOTES:

 ******************************************************************************/
BEGIN

adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_CTRIP','SP_AH_TRAVEL_CTRIP_DAILY',SYSDATE,'','DELETE');
DELETE FROM TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_CTRIP;

adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_CTRIP','SP_AH_TRAVEL_CTRIP_DAILY',SYSDATE,'','INSERT');
INSERT INTO TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_CTRIP ( ---insert ctrip agency
                          batchno,polno,prodline,polyear,serv_agentcode,
                          serv_agentname,segment_code,cardclient,clntid,
                          policyholder,poltrneffdate,effdate,expdate,
                          trandate,branchcode,prodtype,polstat,polmode,
                          accountofficer,dist,dist_value,currency,lob,
                          subline,statcode,policytype,prodcategory,
                          org_type,segcode_seq,compcode,mainline,
                          agtno,trantype,issue_source,netpremtot,docsamt,  
                          lgtamt,ptaxamt,gpremtot,commamt,commwtaxamt,
                          comwtax,sfeeamt,sfeetax,sfeewtaxamt,trandateparm,
                          other_charges,vatamt,totsi,effdate2,proposal_no,
                          polsource,acct_officer,userid,polnum,reportname,
                          acctgdst,acctgtsi,acctgothchrg,ednt_type,incept_dt,inseqno

                          
)

-- WITH getbatch_1 as 
-- (
-- SELECT b.batchno
--                                      FROM adw_prod_tgt.nlr_insured_mst_v2 a, adw_prod_tgt.nlr_insured_trn_v2 b
--                                     WHERE     a.statcode = 2565
--                                           --AND (a.effdate BETWEEN '05/26/2025' AND '06/25/2025')
--                                           AND a.inseqno = b.inseqno
-- ),
-- nlr_portfolio_pisc_st_ctrip AS (
-- SELECT *
--            FROM (  SELECT b.batchno,
--                           b.polno,
--                           c.inseqno,
--                           SUBSTR (f.t1, 1, 1)
--                               AS prodline,
                              
--                           adw_prod_tgt.fn_nlr_polyr_bytran_batchno (b.polno, b.batchno)
--                               polyear,
                              
--                            (select max(aa.agtno) from xag_profile_v2 aa where aa.nameid = g.nameid) AS serv_agentcode,
                           
--                           (adw_prod_tgt.parsename_temp (fngetnamestr (g.nameid),
--                                                 'LFM',
--                                                 'FML'))
--                               AS serv_agentname,
                              
--                             d1.refdesc AS segment_code,
                            
--                           ' 'AS cardclient,
--                             (SELECT  distinct b.clntid 
--                             from cnb_namelst_trn_v2 b
--                             where nameid = (Select nameid from NLR_POLROLE_TRN_v2 a where a.polno=b.polno and a.pertype=556 and a.enddate is null)) AS clntid,
                                                          
--                           (adw_prod_tgt.parsename_temp (
--                                (Select Distinct Bb.Namestr
--                             From  adw_prod_tgt.Nlr_polrole_trn_v2 A, adw_prod_tgt.cnb_namelst_trn_v2 Bb
--                             Where  A.Nameid = Bb.Nameid And A.Polno =  b.polno 
--                             and A.Pertype = 556
--                             and A.Enddate Is Null),
--                                'LFM',
--                                'FML'))
--                               AS policyholder, --no data
                              
--                           b.effdate AS poltrneffdate,
--                           c.effdate effdate,
--                           c.expirydate AS expdate,
--                           MAX (b.trandate) AS trandate,
--                           d.branchcode
--                               AS branchcode,
--                           d.prodcode
--                               AS prodtype,
--                           (SELECT refdesc
--                              FROM adw_prod_tgt.gct_geninfo_ref
--                             WHERE refseqno = d.statcode)
--                               polstat,
--                           (SELECT refdesc
--                              FROM adw_prod_tgt.gct_geninfo_ref
--                             WHERE refseqno = h.billmode)
--                               polmode,
--                           adw_prod_tgt.parsename_temp (
--                                 (Select Distinct Bb.Namestr
--                             From  adw_prod_tgt.nlr_polrole_trn_v2 A, adw_prod_tgt.cnb_namelst_trn_v2 Bb
--                             Where  A.Nameid = Bb.Nameid And A.Polno =  b.polno 
--                             and A.Pertype = 862
--                             and A.Enddate Is Null),
--                               'LFM',
--                               'FML')
--                               AS accountofficer,
--                           CASE
--                               WHEN g.dist IS NULL THEN 1
--                               WHEN g.dist = 0 THEN 1
--                               ELSE (g.dist / 100)
--                           END
--                               AS dist,
--                           CASE
--                               WHEN g.dist IS NULL THEN 1
--                               WHEN g.dist = 0 THEN 1
--                               ELSE g.dist
--                           END
--                               AS dist_value,
--                           (SELECT refdesc
--                              FROM adw_prod_tgt.cxx_geninfo_ref_v2
--                             WHERE refseqno = d.currency)
--                               AS currency,
--                           f.lob,
--                           f.subline,
--                           d.statcode,
--                           d.policytype,
--                           f.prodcategory,
--                           f.orgtype
--                               AS org_type,
--                           d.segmentcode
--                               AS segcode_seq,
--                           SUBSTR (f1.refdesc, 1, 1)
--                               AS compcode,
--                           f.mainline,
--                           CASE
--                               WHEN g.pertype = 562
--                               THEN
--                                   (SELECT MAX (x.agtno)
--                                      FROM adw_prod_tgt.xag_profile_v2 x
--                                     WHERE x.nameid = g.nameid)
--                               ELSE
--                                   NULL
--                           END
--                               agtno,
--                           b.trantype,
--                           d.issue_source,
--                           z.netprem,
--                           z.dst,
--                           z.lgt,
--                           z.premtax,
--                           z.premium,
--                           0
--                               commamt,
--                           0
--                               commwtaxamt,
--                           0
--                               comwtax,
--                           z.sfeeamt,
--                           0
--                               sfeetax,
--                           z.sfeewtaxamt,
--                           a.trandate
--                               AS trandateparm,
--                           0
--                               other_charges,
--                           0
--                               vatamt,
--                           0
--                               totsi,
--                           b.effdate
--                               AS effdate2,
--                           b.proposal_no,
--                           adw_prod_tgt.fn_grp_getrefdesc (d.pol_source)
--                               polsource,
--                           ' '
--                               acct_officer,
--                           b.userid,
--                           b.polno
--                               AS polnum,
--                           ' '
--                               AS reportname,
--                           0
--                               acctgdst,
--                           0
--                               acctgtsi,
--                           0
--                               acctgothchrg,
--                           CASE
--                               WHEN b.trantype <> 10009112
--                               THEN
--                                   CASE
--                                       WHEN z.netprem > 0 THEN 'A'
--                                       WHEN z.netprem < 0 THEN 'R'
--                                       ELSE 'N'
--                                   END
--                               ELSE
--                                   NULL
--                           END
--                               ednt_type,
--                           (SELECT xx.effdate
--                              FROM adw_prod_tgt.nlr_poldate_mst_v2 xx
--                             WHERE xx.polno = b.polno)
--                               AS incept_dt
--                      FROM adw_prod_tgt.nlr_policy_tran_v2       b,---
--                           adw_prod_tgt.nlr_policy_mst_v2        d
--                           LEFT JOIN adw_prod_tgt.LU_REF_DIM d1 ---segmentcode
--                           ON d1.refseqno = d.segmentcode
--                           AND d1.source_name = 'ELIFE'
--                           AND d1.table_name = 'CXX_GENINFO_REF'  ,---
--                           adw_prod_tgt.nlr_polrole_trn_v2       g,---
--                           adw_prod_tgt.grb_product_mst_iris       f
--                           LEFT JOIN adw_prod_tgt.LU_REF_DIM f1 ---compcode
--                           ON f1.refseqno = f.co_code
--                           AND f1.source_name = 'ELIFE'
--                           AND f1.table_name = 'CXX_GENINFO_REF' ,---
--                           adw_prod_tgt.nlr_polbill_ref_v2       h,---
--                           adw_prod_tgt.nlr_bill_sched_ins_dtl_v2 z,
--                           adw_prod_tgt.nlr_insured_trn_v2       a,
--                           adw_prod_tgt.nlr_insured_mst_v2       c
--                     WHERE     a.batchno = b.batchno
--                           AND a.inseqno = c.inseqno
--                           AND z.inseqno = a.inseqno
--                           AND c.polno = d.polno
--                           AND f.enddate IS NULL
--                           AND g.enddate IS NULL
--                           AND d.prodcode IN ('ST','SC','DA','SU')
--                           AND g.nameid IN (7289388)
--                           AND d.statcode <> 529
--                           AND g.pertype = 562
--                           AND b.polno = d.polno
--                           AND d.prodcode = f.prodcode
--                           AND d.polno = g.polno
--                           AND h.polno = b.polno
--                           AND h.enddate IS NULL
--                           AND z.batchno = b.batchno
--                           AND z.polno = b.polno
--                           AND d.statcode <> 2653
--                           AND NOT EXISTS (select 1 from getbatch_1 gb WHERE gb.batchno = b.batchno) --added by francis 08112025 changed not in to not exists
--                           --AND trunc(b.trandate) = trunc(sysdate - 1) --incremental added 08112025
--                           --AND b.trandate >= TRUNC(SYSDATE - 1) AND b.trandate <  TRUNC(SYSDATE) --optimize incremental added by francis 08192025
--                           AND b.trandate >= DATE '2024-05-01' AND b.trandate <  DATE '2024-06-01'   --get the data between this date
--                  GROUP BY b.batchno,b.proposal_no,b.polno,f.t1,g.nameid,
--                           d.prodcode,d.statcode,h.billmode,g.dist,f.subline,
--                           f.lob,d.statcode,d.policytype,f.prodcategory,d.segmentcode,
--                           d1.refdesc, -- added segmentcode
--                           f1.refdesc, --added compcode
--                           d.branchcode,f.co_code,d.currency,f.orgtype,f.mainline,
--                           g.nameid,g.pertype,b.trantype,d.issue_source,z.netprem,
--                           z.dst,z.lgt,z.premtax,z.premium,z.sfeeamt,
--                           z.sfeewtaxamt,a.trandate,b.effdate,d.pol_source,b.userid,
--                           c.inseqno,c.effdate,c.expirydate)
--           WHERE 1=1 

--           )
          
--            SELECT DISTINCT batchno,polno,prodline,polyear,serv_agentcode,
--                            serv_agentname,segment_code,cardclient,clntid,policyholder,
--                            poltrneffdate,MAX (effdate) effdate,MAX (expdate) expdate,MAX (trandate) trandate,branchcode,
--                            prodtype,polstat,polmode,accountofficer,dist,
--                            dist_value,currency,lob,subline,statcode,
--                            policytype,prodcategory,org_type,segcode_seq,compcode,
--                            mainline,agtno,trantype,issue_source,SUM (netprem) netprem,
--                            SUM (dst) dst,SUM (lgt) lgt,SUM (premtax) premtax,SUM (premium) premium,SUM (commamt) commamt,
--                            SUM (commwtaxamt) commwtaxamt,comwtax,SUM (sfeeamt) sfeeamt,sfeetax,SUM (sfeewtaxamt) sfeewtaxamt,
--                            MAX (trandateparm) trandateparm,SUM (other_charges) other_charges,SUM (vatamt) vatamt,SUM (totsi) totsi,MAX (effdate2) effdate2,
--                            proposal_no,polsource,acct_officer,userid,polnum,
--                            reportname,acctgdst,acctgtsi,acctgothchrg,ednt_type,
--                            MAX (incept_dt) incept_dt
--             FROM nlr_portfolio_pisc_st_ctrip
--         GROUP BY batchno,polno,prodline,polyear,serv_agentcode,
--                  serv_agentname,segment_code,cardclient,clntid,policyholder,
--                  poltrneffdate,branchcode,prodtype,polstat,polmode,
--                  accountofficer,dist,dist_value,currency,lob,
--                  subline,statcode,policytype,prodcategory,org_type,
--                  segcode_seq,compcode,mainline,agtno,trantype,
--                  issue_source,
--                  comwtax,
--                  sfeetax,
--                  proposal_no,
--                  polsource,
--                  acct_officer,
--                  userid,
--                  polnum,
--                  reportname,
--                  acctgdst,
--                  acctgtsi,
--                  acctgothchrg,
--                  ednt_type;



WITH 

-- getbatch_1 AS (
--     SELECT DISTINCT b.batchno
--     FROM adw_prod_tgt.nlr_insured_mst_v2 a 
--     JOIN adw_prod_tgt.nlr_insured_trn_v2 b ON a.inseqno = b.inseqno
--     WHERE a.statcode = 2565 --spoiled
-- ),
--  agent lookup
agent_data AS (
    SELECT nameid, MAX(agtno) as max_agtno
    FROM adw_prod_tgt.xag_profile_v2 
    GROUP BY nameid
),
--  client and officer lookups
client_officer_data AS (
    SELECT 
        pr.polno,
        MAX(CASE WHEN pr.pertype = 556 THEN nl.clntid END) as clntid,
        MAX(CASE WHEN pr.pertype = 556 THEN 
            adw_prod_tgt.parsename_temp(nl.namestr, 'LFM', 'FML') END) as policyholder,
        MAX(CASE WHEN pr.pertype = 862 THEN 
            adw_prod_tgt.parsename_temp(nl.namestr, 'LFM', 'FML') END) as accountofficer
    FROM adw_prod_tgt.nlr_polrole_trn_v2 pr
    JOIN adw_prod_tgt.cnb_namelst_trn_v2 nl ON pr.nameid = nl.nameid
    WHERE pr.pertype IN (556, 862) 
      AND pr.enddate IS NULL
    GROUP BY pr.polno
),
-- Get single policy date per policy
policy_dates AS (
    SELECT polno, effdate,
           ROW_NUMBER() OVER (PARTITION BY polno ORDER BY effdate DESC) as rn
    FROM adw_prod_tgt.nlr_poldate_mst_v2
)
 
SELECT DISTINCT
    b.batchno,
    b.polno,
    -- c.inseqno,
    SUBSTR(f.t1, 1, 1) AS prodline,
    --adw_prod_tgt.fn_nlr_polyr_bytran_batchno(b.polno, b.batchno) AS polyear,
    null AS polyear,
    -- Agent information from CTE
    ag.max_agtno AS serv_agentcode,
--    adw_prod_tgt.parsename_temp(adw_prod_tgt.fngetnamestr(g.nameid), 'LFM', 'FML') AS serv_agentname,
    adw_prod_tgt.parsename_temp(cc.namestr, 'LFM', 'FML') AS serv_agentname,
    d1.refdesc AS segment_code,
    ' ' AS cardclient,
    -- Client data from CTE
    cod.clntid,
    cod.policyholder,
    b.effdate AS poltrneffdate,
    c.effdate AS effdate,
    c.expirydate AS expdate,
    b.trandate AS trandate,  -- Removed MAX() since we're using DISTINCT
    d.branchcode,
    d.prodcode AS prodtype,
    -- Reference lookups using JOINs
    ref_stat.refdesc AS polstat,
    ref_bill.refdesc AS polmode,
    cod.accountofficer,
    -- Distribution logic
    CASE
        WHEN g.dist IS NULL OR g.dist = 0 THEN 1
        ELSE g.dist / 100
    END AS dist,
    CASE
        WHEN g.dist IS NULL OR g.dist = 0 THEN 1
        ELSE g.dist
    END AS dist_value,
    ref_curr.refdesc AS currency,
    f.lob,
    f.subline,
    d.statcode,
    d.policytype,
    f.prodcategory,
    f.orgtype AS org_type,
    d.segmentcode AS segcode_seq,
    SUBSTR(f1.refdesc, 1, 1) AS compcode,
    f.mainline,
    -- Agent number logic
    CASE WHEN g.pertype = 562 THEN ag.max_agtno END AS agtno,
    b.trantype,
    d.issue_source,
    -- Financial fields (no aggregation needed with DISTINCT)
    z.netprem,
    z.dst,
    z.lgt,
    z.premtax as ptaxamt,
    z.premium as gpremtot,
    0 AS commamt,
    0 AS commwtaxamt,
    0 AS comwtax,
    z.sfeeamt,
    0 AS sfeetax,
    z.sfeewtaxamt,
    a.trandate AS trandateparm,
    0 AS other_charges,
    0 AS vatamt,
    0 AS totsi,
    b.effdate AS effdate2,
    b.proposal_no,
    dc.refdesc AS polsource,
    ' ' AS acct_officer,
    b.userid,
    b.polno AS polnum,
    ' ' AS reportname,
    0 AS acctgdst,
    0 AS acctgtsi,
    0 AS acctgothchrg,
    CASE
        WHEN b.trantype <> 10009112 THEN
            CASE
                WHEN z.netprem > 0 THEN 'A'
                WHEN z.netprem < 0 THEN 'R'
                ELSE 'N'
            END
        ELSE NULL
    END AS ednt_type,
    pd.effdate AS incept_dt,
    z.inseqno
 
FROM adw_prod_tgt.nlr_policy_tran_v2 b
 
-- Core joins using  JOIN syntax
JOIN adw_prod_tgt.nlr_policy_mst_v2 d ON b.polno = d.polno
JOIN adw_prod_tgt.nlr_insured_trn_v2 a ON a.batchno = b.batchno
JOIN adw_prod_tgt.nlr_insured_mst_v2 c ON a.inseqno = c.inseqno AND c.polno = d.polno
JOIN adw_prod_tgt.nlr_bill_sched_ins_dtl_v2 z ON z.inseqno = a.inseqno 
                                                AND z.batchno = b.batchno 
                                                AND z.polno = b.polno
JOIN adw_prod_tgt.grb_product_mst_iris f ON d.prodcode = f.prodcode AND f.enddate IS NULL
JOIN adw_prod_tgt.nlr_polbill_ref_v2 h ON h.polno = b.polno AND h.enddate IS NULL
 
-- Agent role join with filtering
JOIN adw_prod_tgt.nlr_polrole_trn_v2 g ON d.polno = g.polno 
                                        AND g.pertype = 562 
                                        AND g.enddate IS NULL
 
-- Lookup joins
LEFT JOIN adw_prod_tgt.gct_geninfo_ref dc ON dc.refseqno = d.pol_source --polsource
LEFT JOIN adw_prod_tgt.cnb_namelst_trn_v2 cc ON g.nameid = cc.nameid --serv_agentname
LEFT JOIN adw_prod_tgt.LU_REF_DIM d1 ON d1.refseqno = d.segmentcode
                                      AND d1.source_name = 'ELIFE'
                                      AND d1.table_name = 'CXX_GENINFO_REF'
LEFT JOIN adw_prod_tgt.LU_REF_DIM f1 ON f1.refseqno = f.co_code
                                      AND f1.source_name = 'ELIFE'
                                      AND f1.table_name = 'CXX_GENINFO_REF'
 
-- Reference data joins 
LEFT JOIN adw_prod_tgt.gct_geninfo_ref ref_stat ON ref_stat.refseqno = d.statcode
LEFT JOIN adw_prod_tgt.gct_geninfo_ref ref_bill ON ref_bill.refseqno = h.billmode
LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 ref_curr ON ref_curr.refseqno = d.currency
 
-- CTE joins for optimized lookups
LEFT JOIN agent_data ag ON ag.nameid = g.nameid
LEFT JOIN client_officer_data cod ON cod.polno = b.polno
LEFT JOIN policy_dates pd ON pd.polno = b.polno AND pd.rn = 1
 
WHERE 1=1
    -- AND a.batchno = 1739616
    -- AND b.polno = 'TR-SC-IS-24-0082011-00-D'
    -- AND TRUNC(c.effdate) >= DATE '2024-11-26' AND TRUNC(c.effdate) < DATE '2024-12-25'
    AND d.prodcode IN ('ST','SC','DA','SU')
    AND g.nameid IN (7289388)
    AND d.statcode NOT IN (529, 2653)
    --AND NOT EXISTS (SELECT 1 FROM getbatch_1 gb WHERE gb.batchno = b.batchno)
    AND TRUNC(b.trandate) = p_date  --incremental

    ;
adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_CTRIP','SP_AH_TRAVEL_CTRIP_DAILY',SYSDATE,SYSDATE,'UPDATE');
    --COMMIT;


--   DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;

adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_TRAVEL_CTRIP_DAILY',SYSDATE,'','INSERT');
  INSERT INTO adw_prod_tgt.TEMP_NLR_PORTFOLIO_PISC_DAILY 
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
    incept_dt,inseqno

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
    incept_dt,inseqno
   FROM TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_CTRIP;

  COMMIT;
  adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_TRAVEL_CTRIP_DAILY',SYSDATE,SYSDATE,'UPDATE');




END SP_AH_TRAVEL_CTRIP_DAILY;