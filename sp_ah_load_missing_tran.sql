CREATE
OR REPLACE PROCEDURE sp_ah_load_missing_tran
AS
/******************************************************************************

NAME:       sp_ah_load_missing_tran
PURPOSE:    

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        02/10/2026           Francis              1. Create sp_ah_load_missing_tran



NOTES:

 ******************************************************************************/


BEGIN 

        BEGIN --INSERT MISSING TRANSACTIONS TO AH_TRANSACTIONS_DAILY 


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
                            WITH 
                            nlr_portfolio_branch_date AS (
                                SELECT 
                                    MAX(yy.startdate) AS max_start, 
                                    yy.agtno
                                FROM adw_prod_tgt.agent_dim yy
                                WHERE yy.enddate IS NULL
                                AND yy.source_name = 'ELIFE'
                                AND yy.table_name = 'XAG_ASSIGN'
                                GROUP BY yy.agtno
                            ),
                            nlr_portfolio_branch AS (
                                SELECT 
                                    UPPER(z.description) AS description, 
                                    x.agtno
                                FROM adw_prod_tgt.agent_dim x
                                INNER JOIN adw_prod_tgt.agent_dim y ON x.agtno = y.agtno
                                INNER JOIN adw_prod_tgt.lu_branch_dim z ON y.branchcode = z.branchcode
                                INNER JOIN nlr_portfolio_branch_date zz ON y.agtno = zz.agtno AND y.startdate = zz.max_start
                                WHERE y.enddate IS NULL
                                AND x.source_name = 'ELIFE'
                                AND x.table_name = 'XAG_PROFILE'
                                AND y.source_name = 'ELIFE'
                                AND y.table_name = 'XAG_ASSIGN'
                                AND z.source_name = 'ELIFE'
                                AND z.table_name = 'CXX_BRANCH_REF'
                            )
                            SELECT DISTINCT 
                                z.polno,
                                z.issue_dt,
                                CASE 
                                    WHEN z.trantype IN ('10009112', '10009090', 'NEW') THEN ' '
                                    ELSE TO_CHAR(z.batchno)
                                END AS endtno,
                                UPPER(z.segment_code) AS distribution_channel,
                                z.intmcd AS intermediarycode,
                                z.intermediary, 
                                z.effdate AS effectivedate,
                                z.expiry_dt AS expirydate,
                                z.tsi_amt AS totalsi,
                                z.comm_rate AS commissionrate,
                                z.comm_amt AS commamt,
                                z.comm_tax AS commtax,
                                z.sfeeamt AS sfeeamt,
                                z.sfeetax AS sfeewtax,
                                z.comm_tax AS commvat,
                                (NVL(z.prem_amt, 0) + (NVL(z.doc_stamps, 0) + NVL(z.loc_gov_tax, 0) + NVL(z.prem_tax, 0) + NVL(z.oth_chrgs, 0))) AS grosspremin_peso,
                                z.doc_stamps AS dst,
                                z.oth_chrgs AS other_charges,
                                z.loc_gov_tax AS lgt,
                                z.prem_tax AS premtax, 
                                NVL(z.prem_amt, 0) AS basicpremium,
                                'BATCHNO =  ' || z.batchno AS remarks,
                                z.iss_pref AS issuesource,
                                z.ISS_NAME AS issue_source_name,
                                e.prodcode AS plantype,
                                z.intermediary AS travelagency,
                                z.SEGMENT_CODE AS segment,
                                z.ASSURED_NAME AS policyholder,
                                z.trandate,
                                z.effdate_parm,
                                z.created_by,
                                z.batchno,
                                z.polsource,
                                z.acct_officer,
                                z.reportname,
                                z.userid,
                                z.line_pref,
                                z.subline_pref,
                                f.description AS branchcode,
                                CASE 
                                    WHEN z.subline_pref IN ('ST', 'SC', 'DA', 'SU') THEN g.discount_code
                                    WHEN z.subline_pref IN ('HB') THEN h.psa
                                    ELSE NULL
                                END AS promocode,
                                i.refdesc AS channel,
                                z.location, 
                                CASE 
                                    WHEN j.refdesc = 'Insureshop' THEN 'INSURE SHOP'
                                    ELSE j.refdesc
                                END AS platform,
                                SYSDATE AS extract_date,
                                z.co_cd,
                                z.trantype
                            FROM adw_prod_tgt.NLR_PORTFOLIO_PISC_EIS_DAILY z
                            LEFT JOIN nlr_portfolio_branch f ON f.agtno = z.agtno    
                            LEFT JOIN (
                                SELECT DISTINCT x.discount_code, x.polno
                                FROM adw_prod_tgt.tlight_master x
                                INNER JOIN adw_prod_tgt.xag_agt_onboard_ref c ON c.promo_code = x.discount_code
                            ) g ON g.polno = z.polno
                            LEFT JOIN (
                                SELECT 
                                    DECODE(b.nameid, 266209, NULL, 'PSA' || TO_CHAR(agtno)) AS psa, 
                                    b.polno
                                FROM adw_prod_tgt.agent_dim a
                                INNER JOIN adw_prod_tgt.nlr_polrole_trn_v2 b ON a.nameid = b.nameid
                                WHERE pertype = 561
                                AND b.enddate IS NULL 
                                AND a.source_name = 'ELIFE'
                                AND a.table_name = 'XAG_PROFILE'
                            ) h ON h.polno = z.polno
                            LEFT JOIN adw_prod_tgt.lu_ref_dim i 
                                ON i.refseqno = z.channel
                                AND i.source_name = 'ELIFE'
                                AND i.table_name = 'CXX_GENINFO_REF'
                            LEFT JOIN adw_prod_tgt.lu_ref_dim j 
                                ON j.refseqno = z.platform
                                AND j.source_name = 'ELIFE'
                                AND j.table_name = 'CXX_GENINFO_REF'
                            LEFT JOIN (
                                SELECT DISTINCT ee.*
                                FROM adw_prod_tgt.nlr_policy_mst_V2 ee
                            ) e ON e.polno = z.polno
                            WHERE 1=1 
                            AND line_pref = 'AC'
                            AND z.BATCHNO IN (
                                SELECT batchno 
                                FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS
                                where line_pref = 'AC' 
                            );


                                UPDATE AH_TRANSACTIONS_DAILY 
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
                                AND polno NOT IN    (
                                    SELECT polno
                                    FROM adw_prod_tgt.d2c_valid_cancellation
                                                    )
                                AND plantype NOT IN ('HCC', 'HCA', 'HCE', 'HCB', 'HCF', 'HCG', 'HCI', 'HCK', 'PET', 'HCP', 'PDC')
                                AND BATCHNO IN      (
                                SELECT batchno 
                                FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
                                where line_pref = 'AC'  
                                                    );

                                COMMIT;
        
        END;

        BEGIN --INSERT MISSING TRANSACTIONS TO TRAVEL_TRANSACTIONS_DAILY

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
                                                        AND c.inseqno = z.inseqno   --added by francis 2/10/2026 for reading 2 plan type 
                                                        AND e.polno = z.polno
                                                        AND z.line_pref = 'GA'
                                                        AND z.BATCHNO IN (
                                                                        SELECT batchno 
                                                                        FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS
                                                                        where line_pref = 'GA' 
                                                                        ) 
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


                                                    ---intermediary cleanup for null data---
                                                    UPDATE travel_transactions_daily ttd
                                                    SET intermediary = (
                                                        SELECT MAX(b.namestr)
                                                        FROM XAG_PROFILE_v2 x
                                                        INNER JOIN cnb_namelst_trn_v2 b ON x.nameid = b.nameid
                                                        WHERE x.agtno = ttd.agtno
                                                        GROUP BY x.agtno
                                                                        )
                                                    WHERE intermediary IS NULL
                                                    AND BATCHNO IN (
                                                                        SELECT batchno 
                                                                        FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS
                                                                        where line_pref = 'GA' 
                                                                        );

                                                    ---assured_name cleanup for null data---
                                                    UPDATE travel_transactions_daily ttd
                                                    SET assured_name = (
                                                        SELECT MAX(CASE WHEN pr.pertype = 556 THEN 
                                                                    adw_prod_tgt.parsename_temp(nl.namestr, 'LFM', 'FML') END)
                                                        FROM adw_prod_tgt.nlr_polrole_trn_v2 pr
                                                        JOIN adw_prod_tgt.cnb_namelst_trn_v2 nl ON pr.nameid = nl.nameid
                                                        WHERE pr.pertype IN (556, 862) 
                                                        AND pr.enddate IS NULL
                                                        AND pr.polno = ttd.polno
                                                        GROUP BY pr.polno
                                                                        )
                                                        WHERE assured_name IS NULL;    

                                                        --channel clean up for travel if missed on EIS -- added by francis 10292025
                                                        MERGE INTO travel_transactions_daily ttd
                                                        USING (
                                                            SELECT DISTINCT 
                                                                a.polno, 
                                                                a.inseqno,
                                                                CASE 
                                                                    WHEN c.nametype IN (44, 61115) THEN 'Enterprise Direct'
                                                                    ELSE 'Individual Direct'
                                                                END AS new_channel
                                                            FROM travel_transactions_daily a,
                                                                nlr_insured_mst_v2 nim,
                                                                nlr_polrole_trn_v2 b,
                                                                cnb_namelst_trn_v2 c
                                                            WHERE a.inseqno = nim.inseqno
                                                            AND nim.polno = b.polno
                                                            AND b.pertype = 556
                                                            AND b.nameid = c.nameid
                                                            AND a.channel IS NULL
                                                            ) src
                                                            ON (ttd.inseqno = src.inseqno AND ttd.polno = src.polno)
                                                            WHEN MATCHED THEN
                                                            UPDATE SET ttd.channel = src.new_channel
                                                            WHERE BATCHNO IN (
                                                                        SELECT batchno 
                                                                        FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS
                                                                        where line_pref = 'GA' 
                                                                        );


                                                        ---update gpremtot for new_grossprem to align in front end
                                                        UPDATE TRAVEL_TRANSACTIONS_DAILY ttd
                                                        SET gpremtot = (
                                                            SELECT nbs.new_grossprem
                                                            FROM nlr_bill_sched_ins_dtl_v2 nbs
                                                            WHERE nbs.batchno = ttd.batchno
                                                                AND nbs.inseqno = ttd.inseqno
                                                                AND nbs.new_grossprem IS NOT NULL
                                                                AND nbs.new_grossprem > 0
                                                                AND nbs.premium <> nbs.new_grossprem
                                                                AND nbs.polno LIKE 'TR%'
                                                                AND nbs.premium > 0
                                                                AND nbs.batchno NOT IN (
                                                                    SELECT DISTINCT batchno 
                                                                    FROM travel_transactions_daily 
                                                                    WHERE group_polno IS NOT NULL
                                                                )
                                                            AND ROWNUM = 1
                                                        )
                                                        WHERE EXISTS (
                                                            SELECT 1
                                                            FROM nlr_bill_sched_ins_dtl_v2 nbs
                                                            WHERE nbs.batchno = ttd.batchno
                                                                AND nbs.inseqno = ttd.inseqno
                                                                AND nbs.new_grossprem IS NOT NULL
                                                                AND nbs.new_grossprem > 0
                                                                AND nbs.premium <> nbs.new_grossprem
                                                                AND nbs.polno LIKE 'TR%'
                                                                AND nbs.premium > 0
                                                                AND nbs.batchno NOT IN (
                                                                    SELECT DISTINCT batchno 
                                                                    FROM travel_transactions_daily 
                                                                    WHERE group_polno IS NOT NULL
                                                                )
                                                                AND ttd.gpremtot <> nbs.new_grossprem  -- Only update if values are different
                                                        )
                                                        AND BATCHNO IN (
                                                                        SELECT batchno 
                                                                        FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS
                                                                        where line_pref = 'GA' 
                                                                        );

                                                        COMMIT;                



        END;



END sp_ah_load_missing_tran;        