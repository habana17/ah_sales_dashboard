create
or replace PROCEDURE SP_AH_TRAVEL_OTHERS_DAILY (p_date IN DATE)
AS  

/******************************************************************************

NAME:       SP_AH_TRAVEL_OTHERS_DAILY
PURPOSE:    travel & ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        07/03/2025            Francis          1. SP_AH_TRAVEL_OTHERS_DAILY
2.0        08/11/2025            Francis          1. Changed the incremental into trandate
                                                  2. Changed not in to not exists

NOTES:

 ******************************************************************************/
BEGIN

adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_OTHERS','SP_AH_TRAVEL_OTHERS_DAILY',SYSDATE,'','DELETE');
DELETE FROM TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_OTHERS;

adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_OTHERS','SP_AH_TRAVEL_OTHERS_DAILY',SYSDATE,'','INSERT');
INSERT INTO adw_prod_tgt.TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_OTHERS ( ---insert travel agencies 
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

-- WITH getbatch_1 AS 
-- (
-- SELECT b.batchno
--                                     FROM adw_prod_tgt.nlr_insured_mst_v2 a, adw_prod_tgt.nlr_insured_trn_v2 b
--                                     WHERE 1=1
--                                           AND a.statcode = 2565
--                                           AND a.inseqno = b.inseqno
-- ),
-- nlr_portfolio_pisc_st_ctrip AS (
-- SELECT *
--            FROM (  SELECT b.batchno,
--                           b.polno,
--                           SUBSTR (f.t1, 1, 1)
--                               AS prodline,
--                           adw_prod_tgt.fn_nlr_polyr_bytran_batchno (b.polno, b.batchno)
--                               polyear,
--                         (select max(aa.agtno) from xag_profile_v2 aa where aa.nameid = g.nameid) AS serv_agentcode,
--                          (adw_prod_tgt.parsename_temp (fngetnamestr (g.nameid),
--                                                 'LFM',
--                                                 'FML'))
--                               AS serv_agentname,
--                           d1.refdesc AS segment_code,
--                           ' ' AS cardclient,
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
--                               AS policyholder, 
--                           b.effdate
--                               AS poltrneffdate,
--                           (SELECT MAX (x.effdate)
--                              FROM adw_prod_tgt.nlr_insured_mst_v2 x
--                             WHERE EXISTS
--                                       (SELECT 1
--                                          FROM adw_prod_tgt.nlr_insured_trn_v2 z
--                                         WHERE     z.batchno = b.batchno
--                                               AND z.inseqno = x.inseqno
--                                               AND z.polno = x.polno))
--                               effdate,
--                           (SELECT MAX (x.expirydate)
--                              FROM adw_prod_tgt.nlr_insured_mst_v2 x
--                             WHERE EXISTS
--                                       (SELECT 1
--                                          FROM adw_prod_tgt.nlr_insured_trn_v2 z
--                                         WHERE     z.batchno = b.batchno
--                                               AND z.inseqno = x.inseqno
--                                               AND z.polno = x.polno))
--                               AS expdate,
--                           MAX (b.trandate)
--                               AS trandate,
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
--                             From  adw_prod_tgt.Nlr_polrole_trn_v2 A, adw_prod_tgt.cnb_namelst_trn_v2 Bb
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
--                           z.netpremtot,
--                           z.docsamt,
--                           z.lgtamt,
--                           z.ptaxamt,
--                           z.gpremtot,
--                           z.commamt,
--                           z.commwtaxamt,
--                           z.comwtax,
--                           z.sfeeamt,
--                           z.sfeetax,
--                           z.sfeewtaxamt,
--                           z.trandate
--                               AS trandateparm,
--                           z.other_charges,
--                           0
--                               vatamt,
--                           z.totsi,
--                           b.effdate
--                               AS effdate2,
--                           b.proposal_no,
--                          adw_prod_tgt.fn_grp_getrefdesc (d.pol_source)
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
--                                       WHEN z.netpremtot > 0 THEN 'A'
--                                       WHEN z.netpremtot < 0 THEN 'R'
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
--                      FROM adw_prod_tgt.nlr_policy_tran_v2    b,
--                           adw_prod_tgt.nlr_policy_mst_v2        d
--                           LEFT JOIN adw_prod_tgt.LU_REF_DIM d1 ---segmentcode
--                           ON d1.refseqno = d.segmentcode
--                           AND d1.source_name = 'ELIFE'
--                           AND d1.table_name = 'CXX_GENINFO_REF'  ,--
--                           adw_prod_tgt.nlr_polrole_trn_v2    g,
--                           adw_prod_tgt.grb_product_mst_iris       f
--                           LEFT JOIN adw_prod_tgt.LU_REF_DIM f1 ---compcode
--                           ON f1.refseqno = f.co_code
--                           AND f1.source_name = 'ELIFE'
--                           AND f1.table_name = 'CXX_GENINFO_REF' ,--
--                           adw_prod_tgt.nlr_polbill_ref_v2    h,
--                           adw_prod_tgt.nlr_premium_summary_v2 z
--                     WHERE     f.enddate IS NULL
--                           AND g.enddate IS NULL
--                           AND d.prodcode IN ('ST','SC','DA','SU')
--                           AND g.nameid NOT IN (7289388)
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
--                           AND b.trandate >= DATE '2024-03-01' AND b.trandate <  DATE '2024-04-01' --get the data between this date
                                              
--                  GROUP BY b.batchno,
--                           b.proposal_no,
--                           b.polno,
--                           f.t1,
--                           g.nameid,
--                           d.prodcode,
--                           d.statcode,
--                           h.billmode,
--                           g.dist,
--                           f.subline,
--                           f.lob,
--                           d.statcode,
--                           d.policytype,
--                           f.prodcategory,
--                           d.segmentcode,
--                           d1.refdesc, -- added segmentcode
--                           f1.refdesc, --added compcode
--                           d.branchcode,
--                           f.co_code,
--                           d.currency,
--                           f.orgtype,
--                           f.mainline,
--                           g.nameid,
--                           g.pertype,
--                           b.trantype,
--                           d.issue_source,
--                           z.gpremtot,
--                           z.netpremtot,
--                           z.docsamt,
--                           z.lgtamt,
--                           z.ptaxamt,
--                           z.commamt,
--                           z.commwtaxamt,
--                           z.comwtax,
--                           z.sfeeamt,
--                           z.sfeetax,
--                           z.sfeewtaxamt,
--                           z.trandate,
--                           z.other_charges,
--                           z.vatamt,
--                           z.totsi,
--                           b.effdate,
--                           d.pol_source,
--                           b.userid)
--           WHERE 1=1  
--           )
--           select 
--                           batchno,polno,prodline,polyear,serv_agentcode,
--                           serv_agentname,segment_code,cardclient,clntid,
--                           policyholder,poltrneffdate,effdate,expdate,
--                           trandate,branchcode,prodtype,polstat,polmode,
--                           accountofficer,dist,dist_value,currency,lob,
--                           subline,statcode,policytype,prodcategory,
--                           org_type,segcode_seq,compcode,mainline,
--                           agtno,trantype,issue_source,netpremtot,docsamt,  
--                           lgtamt,ptaxamt,gpremtot,commamt,commwtaxamt,
--                           comwtax,sfeeamt,sfeetax,sfeewtaxamt,trandateparm,
--                           other_charges,vatamt,totsi,effdate2,proposal_no,
--                           polsource,acct_officer,userid,polnum,reportname,
--                           acctgdst,acctgtsi,acctgothchrg,ednt_type,incept_dt
--            from nlr_portfolio_pisc_st_ctrip
--             ;
            
-- Optimized version with single aggregation and performance improvements
WITH 
-- Pre-filter batch numbers to exclude
excluded_batches AS (
    SELECT DISTINCT b.batchno
    FROM adw_prod_tgt.nlr_insured_mst_v2 a 
    JOIN adw_prod_tgt.nlr_insured_trn_v2 b ON a.inseqno = b.inseqno
    WHERE a.statcode = 2565
),
-- Pre-aggregate agent data to avoid repeated subqueries
agent_lookup AS (
    SELECT nameid, MAX(agtno) as max_agtno
    FROM adw_prod_tgt.xag_profile_v2 
    GROUP BY nameid
),
-- Optimize client and officer lookups
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
-- Optimize insured data lookup for effdate/expdate
insured_data AS (
    SELECT 
        z.batchno,
        MAX(x.effdate) as max_effdate,
        MAX(x.expirydate) as max_expdate
    FROM adw_prod_tgt.nlr_insured_mst_v2 x
    JOIN adw_prod_tgt.nlr_insured_trn_v2 z ON z.inseqno = x.inseqno AND z.polno = x.polno
    GROUP BY z.batchno
)

-- Main query with single aggregation
SELECT 
    b.batchno,
    b.polno,
    SUBSTR(f.t1, 1, 1) AS prodline,
    adw_prod_tgt.fn_nlr_polyr_bytran_batchno(b.polno, b.batchno) as polyear,
    aag.max_agtno AS serv_agentcode,
    adw_prod_tgt.parsename_temp(cc.namestr, 'LFM', 'FML') AS serv_agentname,
    d1.refdesc AS segment_code,
    ' ' AS cardclient,
    cod.clntid,
    cod.policyholder,
    b.effdate AS poltrneffdate,
    
    -- Optimized effdate/expdate using pre-aggregated data
    id.max_effdate AS effdate,
    id.max_expdate AS expdate,
    MAX(b.trandate) AS trandate,
    
    d.branchcode,
    d.prodcode AS prodtype,
    
    -- Reference lookups using JOINs
    ref_stat.refdesc AS polstat,
    ref_bill.refdesc AS polmode,
    
    cod.accountofficer,
    
    -- Simplified distribution calculation
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
    
    -- Simplified agent number logic
    CASE WHEN g.pertype = 562 THEN aag.max_agtno END AS agtno,
    
    b.trantype,
    d.issue_source,
    
       -- Financial fields from premium summary
    z.netprem,--    z.netpremtot,
    z.dst,--    z.docsamt,
    z.lgt,--    z.lgtamt,
    z.premtax,--    z.ptaxamt,
    z.premium,--    z.gpremtot,
    0 as commamt,--    z.commamt,
    0 as commwtaxamt,--    z.commwtaxamt,
    0 as comwtax,--    z.comwtax,
    z.sfeeamt,--    z.sfeeamt,
    0 as sfeetax,--    z.sfeetax,
    z.sfeewtaxamt,--    z.sfeewtaxamt,
    nit.trandate as trandateparm, --    z.trandate AS trandateparm,
    0 as other_charges,--    z.other_charges,
    z.vatamt,--    0 AS vatamt,
    0 as totsi,--    z.totsi,
    b.effdate AS effdate2,
    
    b.proposal_no,
    ref_polsource.refdesc AS polsource,
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
    nim.inseqno

FROM adw_prod_tgt.nlr_policy_tran_v2 b

-- Core joins
JOIN adw_prod_tgt.nlr_policy_mst_v2 d ON b.polno = d.polno
JOIN adw_prod_tgt.nlr_polrole_trn_v2 g ON d.polno = g.polno 
                                       AND g.pertype = 562 
                                       AND g.enddate IS NULL
LEFT JOIN adw_prod_tgt.cnb_namelst_trn_v2 cc ON g.nameid = cc.nameid --serv_agentname                                       
JOIN adw_prod_tgt.grb_product_mst_iris f ON d.prodcode = f.prodcode AND f.enddate IS NULL
JOIN adw_prod_tgt.nlr_polbill_ref_v2 h ON h.polno = b.polno AND h.enddate IS NULL
JOIN adw_prod_tgt.nlr_insured_trn_v2 nit ON nit.batchno = b.batchno
JOIN adw_prod_tgt.nlr_insured_mst_v2 nim ON nit.inseqno = nim.inseqno AND nim.polno = b.polno
--JOIN adw_prod_tgt.nlr_premium_summary_v2 z ON z.batchno = b.batchno AND z.polno = b.polno
--JOIN adw_prod_tgt.NLR_PREMIUM_SUMMARY_CURR z ON z.batchno = b.batchno AND z.polno = b.polno
JOIN adw_prod_tgt.nlr_bill_sched_ins_dtl_v2 z ON z.inseqno = nit.inseqno 
                                                AND z.batchno = b.batchno 
                                                AND z.polno = b.polno

-- Optimized lookups
LEFT JOIN adw_prod_tgt.LU_REF_DIM d1 ON d1.refseqno = d.segmentcode
                                      AND d1.source_name = 'ELIFE'
                                      AND d1.table_name = 'CXX_GENINFO_REF'
LEFT JOIN adw_prod_tgt.LU_REF_DIM f1 ON f1.refseqno = f.co_code
                                      AND f1.source_name = 'ELIFE'
                                      AND f1.table_name = 'CXX_GENINFO_REF'
LEFT JOIN agent_lookup aag ON aag.nameid = g.nameid
LEFT JOIN adw_prod_tgt.nlr_poldate_mst_v2 pd ON pd.polno = b.polno

-- Reference data joins
LEFT JOIN adw_prod_tgt.gct_geninfo_ref ref_stat ON ref_stat.refseqno = d.statcode
LEFT JOIN adw_prod_tgt.gct_geninfo_ref ref_bill ON ref_bill.refseqno = h.billmode  
LEFT JOIN adw_prod_tgt.gct_geninfo_ref ref_polsource ON ref_polsource.refseqno = d.pol_source
LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 ref_curr ON ref_curr.refseqno = d.currency

-- Client and officer data
LEFT JOIN client_officer_data cod ON cod.polno = b.polno

-- Optimized insured data
LEFT JOIN insured_data id ON id.batchno = b.batchno


WHERE 1=1
    AND d.prodcode IN ('ST','SC','DA','SU')
    AND g.nameid NOT IN (7289388)
    AND d.statcode NOT IN (529, 2653)
    AND NOT EXISTS (SELECT 1 FROM excluded_batches eb WHERE eb.batchno = b.batchno)
    AND TRUNC(b.trandate) = p_date --incremental
    -- AND b.polno = 'TR-ST-HO-24-0048407-00-D'
    --AND b.trandate >= DATE '2024-05-01' 
    --AND b.trandate < DATE '2024-06-01'


GROUP BY 
        b.batchno, b.proposal_no, b.polno, f.t1, g.nameid,
    d.prodcode, d.statcode, h.billmode, g.dist, f.subline,
    f.lob, d.policytype, f.prodcategory, d.segmentcode,
    d1.refdesc, f1.refdesc, d.branchcode, f.co_code, 
    d.currency, f.orgtype, f.mainline, g.pertype, 
    b.trantype, d.issue_source, b.effdate, d.pol_source, 
    b.userid, cod.clntid, cod.policyholder, cod.accountofficer,
    aag.max_agtno, ref_stat.refdesc, ref_bill.refdesc, 
    ref_curr.refdesc, ref_polsource.refdesc,   
    z.netprem,--    z.netpremtot,
    z.dst,--    z.docsamt,
    z.lgt,--    z.lgtamt,
    z.premtax,--    z.ptaxamt,
    z.premium,--    z.gpremtot,
    z.sfeeamt,--    z.sfeeamt,
    z.sfeewtaxamt,--    z.sfeewtaxamt,
    nit.trandate,
    z.vatamt,--    0 AS vatamt,
--    z.netpremtot,z.docsamt, z.lgtamt, z.ptaxamt, z.gpremtot, z.commamt,
--    z.commwtaxamt, z.comwtax, z.sfeeamt, z.sfeetax, 
--    z.sfeewtaxamt, z.trandate, z.other_charges, z.totsi,
    pd.effdate, id.max_effdate, id.max_expdate,cc.namestr,
    nim.inseqno
    ;

    adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_OTHERS','SP_AH_TRAVEL_OTHERS_DAILY',SYSDATE,SYSDATE,'UPDATE');


            -- COMMIT;


--   DELETE FROM TEMP_NLR_PORTFOLIO_PISC_DAILY;
adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_TRAVEL_OTHERS_DAILY',SYSDATE,'','INSERT');
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
   FROM adw_prod_tgt.TEMP_NLR_PORTFOLIO_PISC_ST_DAILY_OTHERS;

  COMMIT;
  adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_DAILY','SP_AH_TRAVEL_OTHERS_DAILY',SYSDATE,SYSDATE,'UPDATE');            

END SP_AH_TRAVEL_OTHERS_DAILY;