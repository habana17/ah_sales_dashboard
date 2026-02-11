CREATE
OR REPLACE PROCEDURE sp_ah_load_eis_missing
AS
/******************************************************************************

NAME:       sp_ah_load_eis_missing
PURPOSE:    

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        02/10/2026           Francis              1. Create sp_ah_load_eis_missing



NOTES:

 ******************************************************************************/
BEGIN 


    BEGIN

        BEGIN -- INSERT DATA TO TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS

                -- adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS','sp_ah_load_eis_missing',SYSDATE,'','DELETE');
                DELETE FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS;
                COMMIT;

                -- adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS','sp_ah_load_eis_missing',SYSDATE,'','INSERT');
                INSERT INTO TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS (
                            POLNO, BATCHNO, INTMCD, INTERMEDIARY,
                            ASSD_NO, ASSURED_NAME, CO_CD, LINE_PREF, SUBLINE_PREF, ISS_PREF,
                            POL_YY, POL_SEQ_NO, REN_SEQ_NO, POLICY_TYPE, ENDT_ISS_PREF, ENDT_YY,
                            ENDT_SEQ_NO, EDNT_TYPE, AFFECT_TAG, POLICY_STATUS, INCEPT_DT, EXPIRY_DT,
                            ISSUE_DT, POLTRNEFFDATE, EFF_DT, ITEM_NO, PERIL_CD, PREM_AMT, TSI_AMT,
                            CURR_CD, DOC_STAMPS, VAT_AMT, PREM_TAX, OCHR_FST, OTH_CHRGS, COMM_AMT,
                            COMM_RATE, COMM_TAX, SOURCE_SYS, ACCTC_TAG, LOC_GOV_TAX, NET_RET_PREM,
                            NET_RET_TSI, TOT_FAC_PREM, TOT_FAC_TSI, XOL_PREM_AMT, ORG_TYPE,
                            SEGMENT_CODE, POLTYPE, EFFDATE, CLNTID, AGTNO, TRANDATE, EFFDATE_PARM,
                            SFEEAMT, SFEETAX, SFEEWTAXAMT, CREATED_BY, POLSOURCE, ACCT_OFFICER,
                            USERID, POLNUM, REPORTNAME, ACCTGDST, ACCTGTSI, ACCTGOTHCHRG, ISS_NAME,
                            TRANTYPE, TERM, GPREMTOT, TRANDATEPARM, INSEQNO, PRODTYPE
                        )
                        SELECT DISTINCT 
                            a.polno, 
                            a.batchno, 
                            CASE WHEN ab.pisc_code = ' ' OR ab.pisc_code IS NULL THEN '1' ELSE ab.pisc_code END AS intmcd,
                            ac.max_namestr AS intermediary,
                            0 AS assd_no, 
                            a.policyholder AS assured_name,
                            a.compcode AS co_cd,
                            CASE WHEN a.mainline = 'AH' THEN 'AC' WHEN a.mainline = 'TR' THEN 'GA' ELSE a.mainline END AS line_pref,
                            a.subline AS subline_pref,
                            CASE 
                                WHEN a.mainline = 'TR' THEN 
                                    CASE 
                                        WHEN a.polsource = 'NLR' THEN 
                                            TRIM(LEADING '-' FROM SUBSTR(a.polno, 6, INSTR(SUBSTR(a.polno, 7), '-', 1)))
                                        ELSE a.issue_source
                                    END
                                ELSE a.issue_source
                            END AS iss_pref,
                            TO_NUMBER(TO_CHAR(a.effdate, 'YYYY')) AS pol_yy,
                            SUBSTR(REPLACE(SUBSTR(a.polno, INSTR(a.polno, '-', 1, 4), INSTR(a.polno, '-', 1, 5) - INSTR(a.polno, '-', 1, 4)), '-', ''), 3) AS pol_seq_no,
                            TO_NUMBER('0') AS ren_seq_no, 
                            'D' AS policy_type,
                            CASE WHEN a.trantype IN (10009112, 10009090) THEN '' ELSE a.issue_source END AS endt_iss_pref,
                            CASE WHEN a.trantype IN (10009112, 10009090) THEN ' ' ELSE TO_CHAR(a.effdate, 'YYYY') END AS endt_yy,
                            CASE WHEN a.trantype IN (10009112, 10009090) THEN ' ' ELSE TO_CHAR(a.batchno) END AS endt_seq_no,
                            a.ednt_type AS ednt_type, 
                            ' ' AS affect_tag,
                            CASE 
                                WHEN a.trantype IN (10009112, 10009090) THEN '1' -- NB
                                WHEN a.trantype IN (10010347) THEN '5' -- spoilage
                                WHEN a.trantype IN (10006463, 10009081, 10009093, 10009714) THEN '4' -- Cancellation/endorsement
                                ELSE '0' -- endorsement
                            END AS policy_status,
                            DECODE(a.mainline, 'TR', a.effdate, a.incept_dt) AS incept_dt,
                            TO_CHAR(a.expdate, 'DD-Mon-RRRR') AS expiry_dt,
                            TO_CHAR(a.trandate, 'DD-Mon-RRRR') AS issue_dt,
                            a.poltrneffdate,
                            TO_CHAR(a.effdate, 'DD-Mon-RRRR') AS eff_dt,
                            '1' AS item_no,
                            CASE WHEN a.mainline = 'TR' THEN 17 ELSE NVL(TO_NUMBER(peril_data.perilcde), 0) END AS peril_cd,
                            NVL(a.netpremtot, 0) AS prem_amt, 
                            a.totsi AS tsi_amt,
                            a.currency AS curr_cd, 
                            a.docsamt AS doc_stamps,
                            a.vatamt AS vat_amt, 
                            a.ptaxamt AS prem_tax, 
                            0 AS ochr_fst,
                            a.other_charges AS oth_chrgs,
                            NVL(a.commamt, a.sfeeamt) AS comm_amt,
                            (SELECT CASE WHEN z.commrate = 0 THEN z.sfeerate ELSE NVL(z.commrate, z.sfeerate) END AS commrate
                            FROM adw_prod_tgt.nlr_billing_mst_v2 z
                            WHERE z.polno = a.polno AND a.batchno = z.batchno AND ROWNUM = 1) AS comm_rate,
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
                            CASE WHEN a.polyear <= 1 THEN 'NB' ELSE 'RN' END AS poltype, 
                            a.effdate, 
                            a.clntid, 
                            a.agtno, 
                            a.trandate,
                            a.effdate AS effdate_parm, 
                            a.sfeeamt, 
                            a.sfeetax,
                            a.sfeewtaxamt,
                            adw_prod_tgt.fn_nlr_getcollsoausername(a.proposal_no) AS created_by,
                            a.polsource AS polsource, 
                            ' ' AS acct_officer, 
                            a.userid,
                            a.polnum, 
                            a.reportname, 
                            a.acctgdst, 
                            a.acctgtsi,
                            a.acctgothchrg,
                            NVL(
                                CASE 
                                    WHEN a.mainline = 'TR' THEN 
                                        CASE 
                                            WHEN a.polsource = 'NLR' THEN 
                                                (SELECT x.map_value
                                                FROM adw_prod_tgt.nlr_data_mapping x
                                                WHERE x.map_description = 'ISSUE_SOURCE'
                                                AND x.list_of_value = TRIM(LEADING '-' FROM SUBSTR(a.polno, 6, INSTR(SUBSTR(a.polno, 7), '-', 1))))
                                            ELSE ins.iss_name
                                        END
                                    ELSE ins.iss_name
                                END,
                                ins.iss_name
                            ) AS iss_name,
                            CASE 
                                WHEN a.trantype IN (10009112, 10009090) THEN 'NEW'
                                WHEN a.trantype IN (10006463, 10009081, 10009093, 10009714) THEN 'CANCELLATION'
                                WHEN a.trantype = 10010347 THEN 'SPOILAGE'
                                ELSE 'ENDT'
                            END AS trantype,
                            adw_prod_tgt.fn_nlr_getsoaterm(a.effdate, a.expdate) AS term,
                            a.gpremtot, 
                            a.trandateparm, 
                            a.inseqno, 
                            a.prodtype
                        FROM adw_prod_tgt.TEMP_NLR_PORTFOLIO_PISC_DAILY_MIS a
                        LEFT JOIN adw_prod_tgt.nlr_branch_ref_v2 ins 
                            ON ins.iss_pref = a.issue_source
                        LEFT JOIN (
                            SELECT DISTINCT z.perilcde, x.polno
                            FROM grb_benefit_mst z
                            INNER JOIN nlr_pol_benefit_v2 x ON z.bencode = x.bencode
                            WHERE x.enddate IS NULL
                            AND z.enddate IS NULL
                            AND x.prodclass = 505 -- BASE
                            AND x.covgrp = 508
                        ) peril_data ON peril_data.polno = a.polno
                        LEFT JOIN xag_profile_v2 ab 
                            ON ab.agtno = a.serv_agentcode
                        LEFT JOIN (
                            SELECT MAX(b.namestr) AS max_namestr, x.agtno 
                            FROM XAG_PROFILE_v2 x
                            INNER JOIN cnb_namelst_trn_v2 b ON x.nameid = b.nameid
                            GROUP BY x.agtno
                        ) ac ON ac.agtno = a.serv_agentcode;

                        COMMIT; 
                        -- adw_prod_tgt.sp_adw_table_logs('TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS','sp_ah_load_eis_missing',SYSDATE,SYSDATE,'UPDATE');
                                          
        END;

        BEGIN -- INSERT DATA TO NLR_PORTFOLIO_PISC_EIS_DAILY


                        --INSERT DATA to EIS
                        -- adw_prod_tgt.sp_adw_table_logs('NLR_PORTFOLIO_PISC_EIS_DAILY','sp_ah_load_eis_missing',SYSDATE,'','INSERT');
                        INSERT INTO NLR_PORTFOLIO_PISC_EIS_DAILY (
                            POLNO, BATCHNO, INTMCD, INTERMEDIARY,
                            ASSD_NO, ASSURED_NAME, CO_CD, LINE_PREF, SUBLINE_PREF, ISS_PREF,
                            POL_YY, POL_SEQ_NO, REN_SEQ_NO, POLICY_TYPE, ENDT_ISS_PREF, ENDT_YY,
                            ENDT_SEQ_NO, EDNT_TYPE, AFFECT_TAG, POLICY_STATUS, INCEPT_DT, EXPIRY_DT,
                            ISSUE_DT, POLTRNEFFDATE, EFF_DT, ITEM_NO, PERIL_CD, PREM_AMT, TSI_AMT,
                            CURR_CD, DOC_STAMPS, VAT_AMT, PREM_TAX, OCHR_FST, OTH_CHRGS, COMM_AMT,
                            COMM_RATE, COMM_TAX, SOURCE_SYS, ACCTC_TAG, LOC_GOV_TAX, NET_RET_PREM,
                            NET_RET_TSI, TOT_FAC_PREM, TOT_FAC_TSI, XOL_PREM_AMT, ORG_TYPE,
                            SEGMENT_CODE, POLTYPE, EFFDATE, CLNTID, AGTNO, TRANDATE, EFFDATE_PARM,
                            SFEEAMT, SFEETAX, SFEEWTAXAMT, CREATED_BY, POLSOURCE, ACCT_OFFICER,
                            USERID, POLNUM, REPORTNAME, ACCTGDST, ACCTGTSI, ACCTGOTHCHRG, ISS_NAME,
                            TRANTYPE, TERM, GPREMTOT, TRANDATEPARM, CHANNEL, LOCATION, PLATFORM, DISTCHANNEL_DESC,
                            INSEQNO, PRODTYPE
                        )
                        WITH 
                        latest_agent_assignments AS (
                            SELECT 
                                fngetrefdesc(ranked.distchannel) AS distchannel_ref_desc, 
                                ranked.agtno, 
                                ranked.basebranch,
                                ranked.position
                            FROM (
                                SELECT 
                                    x.*,
                                    ROW_NUMBER() OVER (PARTITION BY x.agtno ORDER BY x.timestmp DESC) AS rn
                                FROM xag_assign_v2 x
                                WHERE x.enddate IS NULL
                            ) ranked
                            WHERE ranked.rn = 1
                        ),
                        issue_source_ass AS (
                            SELECT   
                                polno, 
                                pol_source,
                                polno_life,
                                CASE
                                    WHEN pol_source = 2311 THEN
                                        CASE
                                            WHEN LENGTH(TRIM(LEADING '-' FROM SUBSTR(polno, 6, INSTR(SUBSTR(polno, 7), '-', 1)))) = 4
                                            THEN branchcode
                                            ELSE issue_source
                                        END
                                    ELSE issue_source
                                END AS issue_source
                            FROM nlr_policy_mst_v2
                        ),
                        get_ah_loc AS (
                            SELECT a.map_value, b.polno
                            FROM NLR_DATA_MAPPING a, nlr_policy_mst_v2 b
                            WHERE map_description = 'AH_PROUCT_LOCATION'
                            AND list_of_value = b.prodcode
                        ),
                        get_ah_chan AS (
                            SELECT a.map_value, b.polno
                            FROM NLR_DATA_MAPPING a, nlr_policy_mst_v2 b
                            WHERE map_description = 'AH_PROUCT_CHANNEL'
                            AND list_of_value = b.prodcode
                        ),
                        get_tr_channel AS (
                            SELECT DISTINCT agtno, vchannel
                            FROM (
                                SELECT 
                                    a.agtno,
                                    COALESCE(
                                        ndm_st.map_value, -- ST_CHANNEL mapping (highest priority)
                                        CASE a.agtno WHEN 1 THEN '10054508' END, -- Individual Direct
                                        CASE a.distchannel 
                                            WHEN 5569 THEN '10054515' -- Life AB
                                            WHEN 10054468 THEN '10054516' -- Life Mid-Income  
                                            WHEN 10005357 THEN '10054511' -- Microinsurance Accounts
                                            WHEN 10010305 THEN '10054509' -- Enterprise Direct
                                            WHEN 10006985 THEN '10054508' -- Individual Direct
                                            WHEN 10054082 THEN '10054507' -- Broad Agents
                                        END,
                                        CASE WHEN y.pisc_code IS NULL THEN '10054508' END -- Individual Direct
                                    ) AS vchannel
                                FROM xag_assign_v2 a
                                JOIN xag_profile_V2 y ON a.agtno = y.agtno
                                LEFT JOIN nlr_data_mapping ndm_st 
                                    ON ndm_st.map_description = 'ST_CHANNEL' 
                                    AND ndm_st.list_of_value = TO_CHAR(a.agtno)
                                WHERE a.enddate IS NULL
                            ) channel_logic
                        ),
                        get_ah_channel AS (
                            SELECT map_value, list_of_value
                            FROM nlr_data_mapping
                            WHERE map_description = 'AH_DIST_CHANNEL' 
                        ),
                        get_tr_niis_channel AS (
                            SELECT DISTINCT m.list_of_value, a.intm_no
                            FROM nlr_data_mapping m
                            JOIN NIIS_AGENT_INFO_TMP a ON m.map_value = a.channel_cd
                            WHERE m.map_description = 'PRODUCT_CHANNEL_CD'
                            AND a.co_cd = 1
                        )
                        SELECT DISTINCT
                            a.POLNO, a.BATCHNO, a.INTMCD, a.INTERMEDIARY,
                            a.ASSD_NO, a.ASSURED_NAME, a.CO_CD, a.LINE_PREF, a.SUBLINE_PREF, a.ISS_PREF,
                            a.POL_YY, a.POL_SEQ_NO, a.REN_SEQ_NO, a.POLICY_TYPE, a.ENDT_ISS_PREF, a.ENDT_YY,
                            a.ENDT_SEQ_NO, a.EDNT_TYPE, a.AFFECT_TAG, a.POLICY_STATUS, a.INCEPT_DT, a.EXPIRY_DT,
                            a.ISSUE_DT, a.POLTRNEFFDATE, a.EFF_DT, a.ITEM_NO, a.PERIL_CD, a.PREM_AMT, a.TSI_AMT,
                            a.CURR_CD, a.DOC_STAMPS, a.VAT_AMT, a.PREM_TAX, a.OCHR_FST, a.OTH_CHRGS, a.COMM_AMT,
                            a.COMM_RATE, a.COMM_TAX, a.SOURCE_SYS, a.ACCTC_TAG, a.LOC_GOV_TAX, a.NET_RET_PREM,
                            a.NET_RET_TSI, a.TOT_FAC_PREM, a.TOT_FAC_TSI, a.XOL_PREM_AMT, a.ORG_TYPE,
                            a.SEGMENT_CODE, a.POLTYPE, a.EFFDATE, a.CLNTID, a.AGTNO, a.TRANDATE, a.EFFDATE_PARM,
                            a.SFEEAMT, a.SFEETAX, a.SFEEWTAXAMT, a.CREATED_BY, a.POLSOURCE, a.ACCT_OFFICER,
                            a.USERID, a.polnum, a.REPORTNAME, a.ACCTGDST, a.acctgtsi, a.ACCTGOTHCHRG, a.ISS_NAME,
                            a.TRANTYPE, a.TERM, a.GPREMTOT, a.TRANDATEPARM,
                            -- Channel
                            CASE
                                WHEN a.line_pref = 'GA' THEN NVL(j.vchannel, l.list_of_value)
                                WHEN a.line_pref = 'AC' THEN COALESCE(m.map_value, k.map_value, j.vchannel, l.list_of_value)
                                ELSE NULL
                            END AS Channel,
                            -- Location
                            CASE
                                WHEN a.line_pref = 'GA' THEN 
                                    CASE
                                        WHEN c.distchannel_ref_desc = 'Travel Agency' THEN
                                            CASE 
                                                WHEN c.basebranch = 'HO' THEN 'HEAD OFFICE'
                                                ELSE c.basebranch
                                            END
                                        ELSE
                                            CASE
                                                WHEN f.iss_name = 'INSURE SHOP' THEN 'HEAD OFFICE '
                                                ELSE f.iss_name
                                            END
                                    END 
                                WHEN a.line_pref = 'AC' THEN 
                                    NVL(
                                        CASE 
                                            WHEN a.prodtype IN ('HCP') THEN h.map_value
                                            ELSE NULL 
                                        END,
                                        CASE 
                                            WHEN a.prodtype IN ('HCB', 'PDC') THEN DECODE(a.prodtype, 'HCK', 'HEAD OFFICE', f.iss_name)
                                            WHEN i.distchannel IN (10054468, 5569) THEN 'HEAD OFFICE'
                                            WHEN a.prodtype IN ('HCP') THEN h.map_value
                                            ELSE h.map_value 
                                        END
                                    )  
                            END AS location,
                            -- Platform
                            CASE
                                WHEN e.pol_source = 2634 THEN 10054537   -- Insureshop
                                WHEN e.pol_source = 2310 THEN 10054539   -- FLS
                                WHEN e.pol_source IN (2846, 2835) THEN 10054540 -- Microsite
                                ELSE 10054545   -- NLR
                            END AS platform,
                            -- Dist Channel Desc
                            c.distchannel_ref_desc AS distchannel_desc,
                            a.inseqno,
                            a.prodtype
                        FROM adw_prod_tgt.TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS a
                        LEFT JOIN latest_agent_assignments c ON c.agtno = a.agtno 
                        LEFT JOIN nlr_polrole_trn_v2 d ON d.polno = a.polno AND d.enddate IS NULL AND d.pertype = 562
                        LEFT JOIN issue_source_ass e ON e.polno = a.polno
                        LEFT JOIN nlr_branch_ref_v2 f ON e.issue_source = f.iss_pref
                        LEFT JOIN xag_profile_v2 g ON g.agtno = a.agtno AND g.nameid = d.nameid
                        LEFT JOIN get_ah_loc h ON h.polno = a.polno
                        LEFT JOIN xag_assign_v2 i ON i.agtno = g.agtno AND i.enddate IS NULL       
                        LEFT JOIN get_tr_channel j ON j.agtno = a.agtno
                        LEFT JOIN get_ah_channel k ON k.list_of_value = i.distchannel
                        LEFT JOIN get_tr_niis_channel l ON l.intm_no = g.pisc_code
                        LEFT JOIN get_ah_chan m ON m.polno = a.polno;

                                -- adw_prod_tgt.sp_adw_table_logs('NLR_PORTFOLIO_PISC_EIS_DAILY','sp_ah_load_eis_missing',SYSDATE,SYSDATE,'UPDATE');
        END; 

        BEGIN --UPDATE LOCATIONS OF TRAVEL DATA IN EIS 
               
                    MERGE INTO NLR_PORTFOLIO_PISC_EIS_DAILY a
                    USING (
                        SELECT DISTINCT 
                            a.polno, 
                            a.inseqno, 
                            UPPER(d.iss_name) AS new_loc
                        FROM NLR_PORTFOLIO_PISC_EIS_DAILY a,
                            xag_assign_v2 c,
                            (SELECT iss_pref, iss_name FROM nlr_branch_ref_v2
                            UNION ALL
                            SELECT branchcode, description FROM cxx_branch_ref_v2) d
                        WHERE 1=1
                        AND a.batchno IN (
                            SELECT batchno 
                            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
                            WHERE line_pref = 'GA'    
                        )
                        AND a.line_pref = 'GA'
                        AND a.agtno = c.agtno
                        AND c.branchcode = d.iss_pref
                        AND c.enddate IS NULL
                        AND a.agtno IN (
                            10352, 12651, 21543, 21550, 21586, 21605, 21606, 21609, 21618, 21619,
                            21620, 21622, 21623, 21624, 21627, 21628, 21629, 21630, 21631, 21632,
                            21633, 21634, 21635, 21638, 21647, 21653, 21654, 21656, 21657, 21658,
                            21661, 21662, 21665, 21667, 21670, 21675, 21679, 21689, 21691, 21696,
                            21700, 21703, 21705, 21715, 21717, 21718, 21757, 21768, 21779, 21781,
                            21796, 21801, 21802, 21803, 21805, 21807, 21808, 21851, 21861, 21866,
                            21869, 21881, 21888, 21892, 21895, 21898, 21902, 21917, 21924, 21930,
                            21932, 21934, 21935, 21947, 21948, 26723, 26780, 27161, 27320, 27327,
                            27348, 27362, 27368, 27640, 28392, 30490, 30496, 31324, 31341, 31559,
                            31629, 31689, 31719, 31779, 32226, 32289, 32322, 32363, 32372, 32466,
                            32477, 32596, 32602, 32619, 32902, 33030, 33138, 33150, 33230, 33306,
                            33311, 33419, 33551
                        )
                    ) b
                    ON (a.inseqno = b.inseqno AND a.polno = b.polno)
                    WHEN MATCHED THEN
                        UPDATE SET a.location = b.new_loc;

                    COMMIT;
        
        END;

        BEGIN --LOCATION CLEAN UP EIS. MAKING IT MORE READABLE 

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = TRIM(location)
            WHERE 1=1
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'HEAD OFFICE'
            WHERE location = 'ALA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'BACOLOD'
            WHERE location = 'BAC'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CABANATUAN'
            WHERE location = 'CAB'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CAGAYAN'
            WHERE location = 'CAG'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CEBU'
            WHERE location = 'CEB'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'HEAD OFFICE'
            WHERE TRIM (location) = 'HEAD OFFICE'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'HEAD OFFICE'
            WHERE TRIM (location) = 'HO'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'MIGRANT-ORTIGAS'
            WHERE TRIM (location) = 'OFO'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'MIGRANT WORKERS'
            WHERE     (polno LIKE '%MWHO%' OR polno LIKE '%MWIS%')
            AND location = 'HEAD OFFICE'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'SAN FERNANDO'
            WHERE TRIM (location) = 'SAN'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'BACOLOD'
            WHERE location = 'PBA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CABANATUAN'
            WHERE location = 'PCA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CAGAYAN'
            WHERE location = 'PCD'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CEBU'
            WHERE location = 'PCE'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'DAGUPAN'
            WHERE location = 'PDG'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'DAVAO'
            WHERE location = 'PDV'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'GENERAL SANTOS'
            WHERE location = 'PGS'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'GENERAL SANTOS'
            WHERE location = 'GEN'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;  

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'ILOILO'
            WHERE location = 'PII'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'LAOAG SERVICE OFFICE'
            WHERE location = 'PLA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'LIPA'
            WHERE location = 'PLI'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'NAGA'
            WHERE location = 'PNA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'PSA - DIRECT'
            WHERE location = 'PSA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'SAN FERNANDO'
            WHERE location = 'PSF'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'SUBIC'
            WHERE location = 'PSU'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'TACLOBAN'
            WHERE location = 'PTA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'TACLOBAN'
            WHERE location = 'TAC'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ; 

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'ZAMBOANGA'
            WHERE location = 'PZA'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;


            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'BACOLOD'
            WHERE polno LIKE '%ISBC%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CEBU'
            WHERE polno LIKE '%ISCB%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CAGAYAN'
            WHERE polno LIKE '%ISCG%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CABANATUAN'
            WHERE polno LIKE '%ISCN%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'DAVAO'
            WHERE polno LIKE '%ISDV%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'ILIGAN'
            WHERE polno LIKE '%ISIG%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'ILOILO'
            WHERE polno LIKE '%ISIL%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'LIPA'
            WHERE polno LIKE '%ISLP%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'MANILA OFFICE'
            WHERE polno LIKE '%ISMN%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'NAGA'
            WHERE polno LIKE '%ISNA%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'SAN FERNANDO'
            WHERE polno LIKE '%ISSN%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'TACLOBAN'
            WHERE polno LIKE '%ISTC%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'DAGUPAN'
            WHERE polno LIKE '%ISDP%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'BACOLOD'
            WHERE location = 'PISC BACOLOD'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'DAVAO'
            WHERE location LIKE '%PISC DAVAO%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'TACLOBAN'
            WHERE location LIKE '%PISC TACLOBAN%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'CEBU'
            WHERE location LIKE '%PISC-CEBU%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET location = 'ILOILO'
            WHERE location LIKE '%PISC-ILOILO%'
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;

            COMMIT;


        END;

        BEGIN --PLATFORM CLEANUP FOR EIS
            UPDATE NLR_PORTFOLIO_PISC_EIS_DAILY
            SET platform = 10054537
            WHERE 1=1 
            AND LENGTH(REGEXP_SUBSTR(polno, '[^-]+', 1, 3)) = 4 
            AND REGEXP_SUBSTR(polno, '[^-]+', 1, 3) LIKE 'IS%' 
            AND BATCHNO IN 
            (
            SELECT batchno 
            FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
            )
            ;
        END;

        BEGIN --CHANNEL CLEANUP FOR EIS
                    MERGE INTO NLR_PORTFOLIO_PISC_EIS_DAILY ttd --added by francis 10292025
                            USING (
                                SELECT DISTINCT 
                                    a.polno, 
                                    a.inseqno,
                                    CASE 
                                        WHEN c.nametype IN (44, 61115) THEN 10054509  -- ED (Enterprise Direct)
                                        ELSE 10054508  -- ID (Individual Direct)
                                    END AS new_channel
                                FROM NLR_PORTFOLIO_PISC_EIS_DAILY a,
                                    nlr_insured_mst_v2 nim,
                                    nlr_polrole_trn_v2 b,
                                    cnb_namelst_trn_v2 c
                                WHERE 1=1
                                AND a.BATCHNO IN 
                                (
                                SELECT batchno 
                                FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
                                )
                                AND a.inseqno = nim.inseqno
                                AND nim.polno = b.polno
                                AND b.pertype = 556
                                AND b.nameid = c.nameid
                                AND a.channel IS NULL
                                ) src
                                ON (ttd.inseqno = src.inseqno AND ttd.polno = src.polno)
                                WHEN MATCHED THEN
                                UPDATE SET ttd.channel = src.new_channel
                                WHERE BATCHNO IN 
                                                (
                                                SELECT batchno 
                                                FROM TEMP_NLR_PORTFOLIO_PISC_EIS_DAILY_MIS 
                                                )
                                ;
                    COMMIT;
        END;    
    END;    


END sp_ah_load_eis_missing;        