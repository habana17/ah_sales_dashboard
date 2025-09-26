create
or replace PROCEDURE SP_AH_LOAD_TRAVELTRANSACTIONS (p_date IN DATE)
AS 
/******************************************************************************

NAME:       SP_AH_LOAD_TRAVELTRANSACTIONS
PURPOSE:    travel & ah transactions 

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        08/07/2025            Francis          1. Create SP_AH_LOAD_TRAVELTRANSACTIONS

NOTES:

 ******************************************************************************/

BEGIN


adw_prod_tgt.sp_adw_table_logs('TRAVEL_TRANSACTIONS_DAILY','SP_AH_LOAD_TRAVELTRANSACTIONS',SYSDATE,'','INSERT');
INSERT INTO TRAVEL_TRANSACTIONS_DAILY
(
                    polno,batchno,
                    intmcd,intermediary,assd_no,assured_name,
                    co_cd,line_pref,subline_pref,iss_pref,
                    pol_yy,pol_seq_no,ren_seq_no,policy_type,
                    endt_iss_pref,endt_yy,endt_seq_no,ednt_type,
                    affect_tag,policy_status,incept_dt,expiry_dt,
                    issue_dt,poltrneffdate,eff_dt,item_no,peril_cd,
                    prem_amt,tsi_amt,curr_cd,doc_stamps,vat_amt,
                    prem_tax,ochr_fst,oth_chrgs,comm_amt,comm_rate,
                    comm_tax,source_sys,acctc_tag,loc_gov_tax,net_ret_prem,
                    net_ret_tsi,tot_fac_prem,tot_fac_tsi,xol_prem_amt,
                    org_type,segment_code,poltype,effdate,clntid,agtno,
                    trandate,effdate_parm,sfeeamt,sfeetax,sfeewtaxamt,
                    created_by,polsource,acct_officer,userid,polnum,
                    reportname,acctgdst,acctgtsi,acctgothchrg,iss_name,
                    trantype,term,gpremtot,trandateparm,plan_type,
                    coverage_type,duration,insured_count,promo_code,
                    channel,location,platform,group_polno,extract_date,
                    distchannel,inseqno

)
WITH tbl_sched AS (
    SELECT COUNT (*) ctr, nn.batchno
    FROM adw_prod_tgt.nlr_bill_sched_ins_dtl_v2 nn
    GROUP BY nn.batchno)

    SELECT DISTINCT polno,batchno,intm_no intmcd,
                   intm_name intermediary,assd_no,assd_name assured_name,
                   co_cd,line_name line_pref, subline_name subline_pref,
                   iss_pref,pol_yy,pol_seq_no,ren_seq_no,pol_type policy_type,
                   endt_iss_pref,endt_yy,endt_seq_no,ednt_type,affect_tag,
                   policy_status, acct_ent_dt incept_dt,expiry_dt,issue_dt,poltrneffdate,
                   eff_dt,item_no,peril_cd,s_prem_amt prem_amt,tsi_amt,curr_cd,doc_stamps,
                   vat_amt,prem_tax,ochr_fst,other_charges oth_chrgs,comm_amt,comm_rate,
                   comm_tax,source_sys,acctc_tag,loc_gov_tax,net_ret_prem,
                   net_ret_tsi,tot_fac_prem,tot_fac_tsi,xol_prem_amt,org_type,
                   segment_code,poltype,effdate,clntid,agtno,trandate,effdate_parm,
                   sfeeamt,sfeetax,sfeewtaxamt,created_by,polsource,acct_officer,
                   userid,polnum,reportname,acctgdst,acctgtsi,acctgothchrg,iss_name,
                   tran_type trantype,termofpolicy term,gpremtot,trandateparm,plan_type,coverage_type,
                   termofpolicy duration, insured_count,promo_code,
                   channel,location,platform,
                   group_polno,sysdate extract_date, distchannel_desc distchannel, inseqno
    FROM (
            SELECT DISTINCT x.polno, x.batchno, x.acct_ent_dt,x.co_cd,x.org_type,x.line_name,x.iss_name,
                            x.subline_name,x.tran_type,x.pol_yy,x.pol_seq_no,x.ren_seq_no,
                            x.pol_type,x.endt_iss_pref,x.endt_yy,x.endt_seq_no,x.bill_yy,
                            x.bill_seq_no,x.endt_type,x.assd_no,x.assd_name,x.property_1,
                            x.property_2,x.eff_dt,x.intm_no,x.intm_name,x.peril_cd,x.peril_sname,
                            x.peril_type,x.s_tsi_amt,x.s_prem_amt,x.doc_stamps,x.fst,x.other_charges,
                            x.prem_tax,x.loc_gov_tax,x.vat_amt,x.record_type,x.curr_cd,x.io_tag,x.termofpolicy,
                            x.plan_description, y.refdesc channel,x.location,
                            case when z.refdesc = 'Insureshop'
                            then 'INSURE SHOP'
                            else z.refdesc
                            end platform,
                            x.group_polno,x.iss_pref, x.affect_tag,
                            x.ednt_type, x.poltrneffdate, x.issue_dt, x.expiry_dt, x.policy_status,
                            x.comm_rate, x.comm_amt, x.ochr_fst, x.tsi_amt, x.item_no, x.xol_prem_amt,
                            x.tot_fac_prem, x.tot_fac_tsi, x.comm_tax,x.source_sys,x.acctc_tag,x.net_ret_prem,
                            x.net_ret_tsi,x.segment_code,x.poltype,x.effdate,x.clntid,x.agtno,x.trandate,
                            x.effdate_parm,x.sfeeamt,x.sfeetax,x.sfeewtaxamt,x.created_by,x.polsource,
                            x.acct_officer,x.userid,x.polnum,x.reportname,x.acctgdst,x.acctgtsi,x.acctgothchrg,
                            x.gpremtot,x.trandateparm, m.plan_description plan_type,m.ins_type coverage_type,
                            o.discount_code promo_code, x.distchannel_desc,
                            n.ctr insured_count , x.inseqno
            FROM (SELECT incept_dt acct_ent_dt,
                        co_cd,
                        org_type,
                        line_pref line_name,
                        iss_name,
                        subline_pref subline_name,
                        z.trantype tran_type,
                        pol_yy,
                        pol_seq_no,
                        ren_seq_no,
                        policy_type pol_type,
                        endt_iss_pref ,
                        endt_yy,
                        endt_seq_no,
                        NULL bill_yy,
                        NULL bill_seq_no,
                        NULL endt_type,
                        assd_no,
                        assured_name assd_name,
                        NULL property_1,
                        NULL property_2,
                        eff_dt eff_dt,
                        intmcd intm_no,
                        --intermediary intm_name,
                        (SELECT max(b.namestr) FROM XAG_PROFILE_v2 A, CNB_NAMELST_TRN_V2 B 
                            WHERE a.nameid = b.nameid
                            AND a.agtno = Z.agtno) AS intm_name,
                        peril_cd peril_cd,
                        peril_cd peril_sname,
                        'B' peril_type,
                        CASE WHEN doc_stamps <= 0 THEN 0
                            ELSE tsi_amt
                        END AS s_tsi_amt,
                        prem_amt s_prem_amt,
                        doc_stamps,
                        NULL fst,
                        oth_chrgs other_charges,
                        prem_tax,
                        loc_gov_tax,
                        vat_amt,
                        NULL record_type,
                        curr_cd,
                        org_type io_tag,
                        z.term termofpolicy,
                        z.batchno,
                        z.expiry_dt,
                        b.plan_description,
                        z.channel,
                        z.location,
                        z.platform,
                        CASE WHEN e.group_class = 2308 THEN z.polno
                            ELSE NULL
                        END group_polno, z.polno, z.iss_pref,
                        z.affect_tag, z.ednt_type,z.poltrneffdate, z.issue_dt, z.policy_status,
                        z.comm_rate, z.comm_amt, z.ochr_fst, z.tsi_amt, z.item_no, z.xol_prem_amt,
                        z.tot_fac_prem, z.tot_fac_tsi, z.comm_tax,z.source_sys,z.acctc_tag,z.net_ret_prem,
                        z.net_ret_tsi, z.segment_code,z.poltype,z.effdate,z.clntid,z.agtno,z.trandate,
                        z.effdate_parm,z.sfeeamt,z.sfeetax,z.sfeewtaxamt,z.created_by,z.polsource,z.acct_officer,
                        z.userid,z.polnum,z.reportname,z.acctgdst,z.acctgtsi,z.acctgothchrg,
                        z.gpremtot,z.trandateparm, d.covseqno, z.distchannel_desc,--,z.plan_type--,z.coverage_type
                        z.inseqno
                FROM adw_prod_tgt.NLR_PORTFOLIO_PISC_EIS_DAILY z,
                    (SELECT DISTINCT bb.*
                     FROM adw_prod_tgt.nlr_polcov_trn_v2 bb) b,
                    (SELECT DISTINCT cc.*
                     FROM adw_prod_tgt.nlr_insured_trn_v2 cc) c,
                    (SELECT DISTINCT dd.*
                     FROM adw_prod_tgt.nlr_insured_mst_v2 dd)  d,
                    (SELECT DISTINCT ee.*
                     FROM adw_prod_tgt.nlr_policy_mst_v2 ee)    e
                WHERE z.polno = b.polno
                AND b.polno = c.polno
                AND z.batchno = c.batchno
                AND d.covseqno = b.covseqno
                AND c.inseqno = d.inseqno
                AND e.polno = z.polno
                AND z.line_pref = 'GA'
                --AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') = trunc(sysdate) - 1 --incremental
                --AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') >= TRUNC(SYSDATE - 1) AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') <  TRUNC(SYSDATE) --optimize incremental added by francis 08192025
                -- AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') >= DATE '2024-03-01' AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') <  DATE '2024-04-01'--get the data between this date
                AND TO_DATE(z.issue_dt, 'DD-Mon-YYYY') = p_date 
                ) x
            LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 y
            ON y.refseqno = x.channel
            LEFT JOIN adw_prod_tgt.cxx_geninfo_ref_v2 z
            ON z.refseqno = x.platform 
            LEFT JOIN (SELECT DISTINCT ins_type,plan_description, polno, covseqno
                        FROM adw_prod_tgt.nlr_polcov_trn_v2
                        WHERE enddate IS NULL)m
            ON x.polno = m.polno
            AND x.covseqno = m.covseqno
            LEFT JOIN tbl_sched n
            ON n.batchno = x.batchno
            LEFT JOIN (SELECT DISTINCT oo.discount_code, oo.polno
                       FROM adw_prod_tgt.tlight_master oo, 
                            adw_prod_tgt.xag_agt_onboard_ref ooo
                       WHERE ooo.promo_code = oo.discount_code)o
            ON o.polno = x.polno );


            COMMIT;
            adw_prod_tgt.sp_adw_table_logs('TRAVEL_TRANSACTIONS_DAILY','SP_AH_LOAD_TRAVELTRANSACTIONS',SYSDATE,SYSDATE,'UPDATE');

END SP_AH_LOAD_TRAVELTRANSACTIONS;
 