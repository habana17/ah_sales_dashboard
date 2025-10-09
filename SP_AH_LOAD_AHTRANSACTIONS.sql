create
or replace PROCEDURE SP_AH_LOAD_AHTRANSACTIONS (p_date IN DATE)
AS 
/******************************************************************************

NAME:       SP_AH_LOAD_AHTRANSACTIONS
PURPOSE:    travel & ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        08/07/2025            Francis          1. Create SP_AH_LOAD_AHTRANSACTIONS
2.0        10/09/2025            Francis          1. added update transactions to 0 for cancelled d2c transactions 

NOTES:

 ******************************************************************************/

 BEGIN



adw_prod_tgt.sp_adw_table_logs('AH_TRANSACTIONS_DAILY','SP_AH_LOAD_AHTRANSACTIONS',SYSDATE,'','INSERT');
INSERT INTO AH_TRANSACTIONS_DAILY
(
         polno,issue_dt,endtno,distribution_channel,intermediarycode,intermediary,
         effectivedate,expirydate,totalsi,commissionrate,commamt,commtax,sfeeamt,sfeewtax,
         commvat,grosspremin_peso,dst,other_charges,lgt,premtax,basicpremium,
         remarks,issuesource,issue_source_name,plantype,travelagency,segment,
         policyholder,trandate,effdate_parm,created_by,batchno,polsource,
         acct_officer,reportname,userid,line_pref,subline_pref,branchcode,promocode,
         channel,location,platform,extract_date,co_cd,trantype

)
WITH nlr_portfolio_branch_date AS (
    SELECT MAX(yy.startdate)max_start, yy.agtno
    FROM adw_prod_tgt.agent_dim yy
    WHERE yy.enddate IS NULL
    AND yy.source_name= 'ELIFE'
    AND yy.table_name = 'XAG_ASSIGN'
    GROUP BY yy.agtno),

    nlr_portfolio_branch AS (
    SELECT upper(z.description) description, x.agtno
    FROM adw_prod_tgt.agent_dim x, --xag_profile    x,
         adw_prod_tgt.agent_dim y, --xag_assign     y,
         adw_prod_tgt.lu_branch_dim z,--cxx_branch_ref z
         nlr_portfolio_branch_date zz
    WHERE x.agtno = y.agtno
    AND y.branchcode = z.branchcode
    AND y.enddate IS NULL
    AND x.source_name = 'ELIFE'
    AND x.table_name = 'XAG_PROFILE'
    AND y.source_name = 'ELIFE'
    AND y.table_name = 'XAG_ASSIGN'
    AND z.source_name = 'ELIFE'
    AND z.table_name = 'CXX_BRANCH_REF'
    AND y.startdate = zz.max_start
    AND y.agtno = zz.agtno)

    SELECT DISTINCT z.polno,z.issue_dt,
            CASE WHEN z.trantype IN ('10009112','10009090','NEW') THEN ' '
                                    ELSE TO_CHAR(z.batchno)
                            END AS endtno,
           UPPER (z.segment_code) distribution_channel,
           z.intmcd intermediarycode,z.intermediary, z.effdate effectivedate,
           z.expiry_dt expirydate,
           --z.tot_fac_tsi totalsi,
           z.tsi_amt totalsi,
           z.comm_rate commissionrate,z.comm_amt commamt,
           z.comm_tax commtax,z.sfeeamt sfeeamt,z.sfeetax sfeewtax,
           z.comm_tax commvat,
           ( nvl(z.prem_amt, 0) + ( nvl(z.doc_stamps, 0) + nvl(z.loc_gov_tax, 0) + nvl(z.prem_tax, 0) + nvl(z.oth_chrgs, 0) ) ) grosspremin_peso,
            z.doc_stamps dst,z.oth_chrgs other_charges,
            z.loc_gov_tax lgt,z.prem_tax premtax, NVL(z.prem_amt,0)basicpremium,
           'BATCHNO =  ' || z.batchno AS remarks,
           z.iss_pref issuesource,
           z.ISS_NAME issue_source_name,
           e.prodcode plantype,
           z.intermediary travelagency,
           z.SEGMENT_CODE segment,
           z.ASSURED_NAME policyholder,
           z.trandate,effdate_parm,created_by,
           z.batchno,z.polsource,z.acct_officer,z.reportname,z.userid,
           z.line_pref,z.subline_pref,
           f.description branchcode,
           CASE WHEN z.subline_pref IN ( 'ST', 'SC', 'DA', 'SU' ) THEN g.discount_code
                WHEN z.subline_pref IN ( 'HB' ) THEN h.psa
                ELSE NULL
            END AS promocode,
            i.refdesc channel,
            z.location, 
            case when j.refdesc = 'Insureshop'
                            then 'INSURE SHOP'
                            else j.refdesc
                            end platform,
             sysdate extract_date,z.co_cd,z.trantype
    FROM adw_prod_tgt.NLR_PORTFOLIO_PISC_EIS_DAILY z
    LEFT JOIN nlr_portfolio_branch f
    ON f.agtno = z.agtno    
    LEFT JOIN (SELECT DISTINCT x.discount_code, x.polno
               FROM adw_prod_tgt.tlight_master      x,
                    adw_prod_tgt.xag_agt_onboard_ref c
               WHERE c.promo_code = x.discount_code) g
    ON g.polno = z.polno
    LEFT JOIN (SELECT decode(b.nameid,266209,NULL,'PSA' || to_char(agtno)) psa, b.polno
               FROM adw_prod_tgt.agent_dim a, --xag_profile          a,
                    adw_prod_tgt.nlr_polrole_trn_v2 b--POLICY_ROLE_DIM b
               WHERE a.nameid = b.nameid
               AND pertype = 561
               AND a.source_name = 'ELIFE'
               AND a.table_name = 'XAG_PROFILE') h
    ON h.polno = z.polno
    LEFT JOIN adw_prod_tgt.lu_ref_dim i
    ON i.refseqno = z.channel
    AND i.source_name = 'ELIFE'
    AND i.table_name = 'CXX_GENINFO_REF'
    LEFT JOIN adw_prod_tgt.lu_ref_dim j
    ON j.refseqno = z.platform
    AND j.source_name = 'ELIFE'
    AND j.table_name = 'CXX_GENINFO_REF'
    LEFT JOIN (SELECT DISTINCT ee.*
                FROM adw_prod_tgt.nlr_policy_mst_V2 ee)e
    ON e.polno = z.polno
    WHERE 1=1 
    AND line_pref = 'AC'
    --AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') = trunc(sysdate ) - 1 -- incremental
    --AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') >= TRUNC(SYSDATE - 1) AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') <  TRUNC(SYSDATE) --optimize incremental added by francis 08192025
    --AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') >= DATE '2024-05-01' AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') <  DATE '2024-06-01' --get the data between this date
    AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') = p_date 
    order by z.issue_dt
    ;

    UPDATE AH_TRANSACTIONS_DAILY --added by francisc 10092025
    SET 
    totalsi = 0,
    commissionrate = 0,
    commamt = 0,
    commtax = 0,
    sfeeamt = 0,
    sfeewtax = 0,
    commvat = 0,
    grosspremin_peso = 0,
    dst = 0,
    other_charges = 0,
    lgt = 0,
    premtax = 0,
    basicpremium = 0
    WHERE trantype = 'CANCELLATION'
    AND channel = 'D2C'
    AND polno NOT IN (
        SELECT polno
        FROM adw_prod_tgt.d2c_valid_cancellation
                      )
    AND plantype NOT IN ('HCC', 'HCA', 'HCE', 'HCB', 'HCF', 'HCG', 'HCI', 'HCK', 'PET', 'HCP', 'PDC')
    AND TO_DATE(issue_dt, 'DD-Mon-YYYY') = p_date
     ;

    COMMIT;
    adw_prod_tgt.sp_adw_table_logs('AH_TRANSACTIONS_DAILY','SP_AH_LOAD_AHTRANSACTIONS',SYSDATE,SYSDATE,'UPDATE');
 
END SP_AH_LOAD_AHTRANSACTIONS;
 