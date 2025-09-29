CREATE
OR REPLACE PROCEDURE SP_AH_MEDICASH_OR_D2C_DAILY (p_date IN DATE) 
AS 

/******************************************************************************

NAME:       SP_AH_MEDICASH_OR_D2C_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        06/11/2025       Francis          1. Create SP_AH_MEDICASH_OR_D2C_DAILY
2.0        07/01/2025       Francis          1. added tranno,cardnumber ,variant and consignee

NOTES:

 ******************************************************************************/   
BEGIN
        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_D2C_DAILY','SP_AH_MEDICASH_OR_D2C_DAILY',SYSDATE,'','DELETE');
        DELETE adw_prod_tgt.medicash_or_d2c_daily
        WHERE trunc(ordate) >= trunc(sysdate); --updated by francis 06112025

        -- COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_D2C_DAILY','SP_AH_MEDICASH_OR_D2C_DAILY',SYSDATE,'','INSERT');
        INSERT INTO adw_prod_tgt.medicash_or_d2c_daily (ordate,transubtype,orname,ordisplay,ornum,oramt,collsource,
                                     nonpolremark,igp_agent,or_status,paytran_timestmp, product_channel,
                                     location, platform, tranno,cardnumber,variant,consignee,
                                     polno  --added by francis 07302025
                                     )
     --    SELECT DISTINCT b.ordate,
     --           adw_prod_tgt.fngetrefdesc(a.transubtype)transubtype,
     --           b.namestr orname,
     --           b.ordisplay,
     --           TO_CHAR(b.orno) ornum,
     --           b.oramt,
     --           adw_prod_tgt.fngetrefdesc(c.collsource) collsource,
     --           b.nonpolremark,
     --           adw_prod_tgt.fn_get_agent_medicash_igp(b.ordisplay,b.ordate) igp_agent,
     --           adw_prod_tgt.fngetrefdesc(d.status) or_status,
     --           trunc(a.timestmp) paytran_timestmp,
     --           'D2C' product_channel,
     --           'PLI Makati Head Office' location,
     --           'D2C' platform,
     --           b.tranno  
     --    FROM adw_prod_tgt.xbc_paytran_dtl a,
     --         adw_prod_tgt.xbc_or_history b,
     --         adw_prod_tgt.xbc_paymst c,
     --         adw_prod_tgt.xbc_ormst d
     --    WHERE EXISTS (SELECT 1
     --                FROM adw_prod_tgt.dtoc_medicash_interim y
     --                WHERE rn_orno = b.orno)
     --    AND a.tranno = b.tranno
     --    AND a.tranno = c.tranno
     --    AND a.tranno = d.tranno
     --    AND trunc(b.ordate) = trunc(sysdate -1); --updated by francis 06112025
     WITH polmaster as (
     SELECT a.regrefno, a.polno, a.plancode,
                           b.refdesc product_channel,
                           a.location, 
                           c.refdesc platform,
                           dd.planmstdesc
                    FROM (
                        SELECT xx.regrefno, xx.polno, xx.plancode,ww.product_channel, ww.platform,           
                               ww.location
                        FROM adw_prod_tgt.POLICY_COVERAGE_DIM xx     --rnb_polcov_trn xx,
                        LEFT JOIN adw_prod_tgt.rnb_policymst_v2 ww
                        ON xx.polno = ww.polno,  
                            adw_prod_tgt.rct_planmst yy, 
                            adw_prod_tgt.POLICY_MST_DIM zz  --rnb_policymst zz
                        WHERE xx.plancode = yy.plancode 
                        AND yy.typecode = 367
                        AND xx.enddate IS NULL
                        AND xx.polno = zz.polno
                        AND xx.source_name = 'BD'
                        AND xx.table_name = 'RNB_POLCOV_TRN'
                        ) a
                    LEFT JOIN adw_prod_tgt.lu_ref_dim b
                    ON b.refseqno = NVL(a.product_channel,0)
                    AND b.source_name = 'ELIFE'
                    AND b.table_name = 'CXX_GENINFO_REF'
                    LEFT JOIN adw_prod_tgt.lu_ref_dim c
                    ON c.refseqno = NVL(a.platform,0)
                    AND c.source_name = 'ELIFE'
                    AND c.table_name = 'CXX_GENINFO_REF'
                    LEFT JOIN adw_prod_tgt.RCT_PLANMST dd
                    ON a.plancode = dd.plancode
),

 d2ccoverage AS (SELECT DISTINCT b.ordate,
               adw_prod_tgt.fngetrefdesc(a.transubtype)transubtype,
               b.namestr orname,
               b.ordisplay,
               TO_CHAR(b.orno) ornum,
               b.oramt,
               adw_prod_tgt.fngetrefdesc(c.collsource) collsource,
               b.nonpolremark,
               adw_prod_tgt.fn_get_agent_medicash_igp(b.ordisplay,b.ordate) igp_agent,
               adw_prod_tgt.fngetrefdesc(d.status) or_status,
               trunc(a.timestmp) paytran_timestmp,
               'D2C' product_channel,
               'PLI Makati Head Office' location,
               'D2C' platform ,
               b.tranno,
               f.regrefno as cardnumber,
               f.plancode,
               CASE WHEN nonpolremark like '%MEDICASH DENGUE%'
               THEN 'Medicash Dengue (D2C)'
               else 
               adw_prod_tgt.fngetrefdesc(g.planmstdesc) END as variant,
               f.polno
        FROM adw_prod_tgt.xbc_paytran_dtl a,
             adw_prod_tgt.xbc_or_history b
        LEFT JOIN  dtoc_medicash_interim e
        ON e.rn_orno = b.orno
        LEFT JOIN polmaster f
        ON f.polno = e.rn_polno
        LEFT JOIN adw_prod_tgt.RCT_PLANMST g
        ON g.plancode = f.plancode,
             adw_prod_tgt.xbc_paymst c,
             adw_prod_tgt.xbc_ormst d
        WHERE EXISTS (SELECT 1
                    FROM adw_prod_tgt.dtoc_medicash_interim y
                    WHERE rn_orno = b.orno)
        AND a.tranno = b.tranno
        AND a.tranno = c.tranno
        AND a.tranno = d.tranno
        )
        select DISTINCT
        a.ordate,
        a.transubtype,
        a.orname,
        a.ordisplay,
        a.ornum,
        a.oramt,
        a.collsource,
        a.nonpolremark,
        a.igp_agent,
        a.or_status,
        a.paytran_timestmp,
        a.product_channel,
        a.location,
        a.platform,
        a.tranno,
        a.cardnumber,
        a.variant,
        adw_prod_tgt.parsename_temp(adw_prod_tgt.fngetnamestr(c.nameid),'LFM','FML') consignee,
        a.polno
        from d2ccoverage a
        LEFT JOIN adw_prod_tgt.rct_consign_mst b 
        ON b.controlno_value = a.cardnumber
        LEFT JOIN adw_prod_tgt.agent_dim c
        ON   b.agtno = c.agtno
        AND c.source_name = 'ELIFE'
        AND c.table_name = 'XAG_PROFILE'
        WHERE 1=1
        --AND trunc(a.ordate) = trunc(sysdate -1)
        AND trunc(a.ordate) = p_date
        ; --incremental --updated 07012025
        
        COMMIT;
        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_D2C_DAILY','SP_AH_MEDICASH_OR_D2C_DAILY',SYSDATE,SYSDATE,'UPDATE');




    END SP_AH_MEDICASH_OR_D2C_DAILY;