CREATE
OR REPLACE PROCEDURE SP_AH_MEDICASH_OR_RETAIL_DAILY (p_date IN DATE)
AS 

/******************************************************************************

NAME:       SP_AH_MEDICASH_OR_RETAIL_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        06/11/2025       Francis          1. Create SP_AH_MEDICASH_OR_RETAIL_DAILY


NOTES:

 ******************************************************************************/   

BEGIN

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_RET_POLICY_DET_DAILY','SP_AH_MEDICASH_OR_RETAIL_DAILY',SYSDATE,'','DELETE');

        --EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.medicash_or_ret_policy_det';

        DELETE adw_prod_tgt.medicash_or_ret_policy_det_daily
        WHERE trunc(ordate) >= trunc(sysdate); --updated by francis 06112025

        COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_RET_POLICY_DET_DAILY','SP_AH_MEDICASH_OR_RETAIL_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.medicash_or_ret_policy_det_daily (ordate,transubtype,orname,ordisplay,ornum,trantype,
                                                            collsource,nonpolremark,cardnumber,or_status,
                                                            discountcode,cardprem,consignee, product_channel,
                                                            location, platform,
                                                            oramt,
                                                            igp_agent,
                                                            variant,
                                                            tranno,
                                                            polno --added by francis 07302025
                                                            )
        SELECT DISTINCT b.ordate,
               adw_prod_tgt.fngetrefdesc(a.transubtype) transubtype,
               b.namestr orname,
               b.ordisplay,
               TO_CHAR(b.orno) ornum,
               adw_prod_tgt.fngetrefdesc(c.trantype) trantype,
               adw_prod_tgt.fngetrefdesc(c.collsource) collsource,
               b.nonpolremark,
               e.refno cardnumber,
               adw_prod_tgt.fngetrefdesc(d.status) or_status,
               --e.refno cardno,
               e.retcode discountcode,
               e.cardprem,
               e.consigneename as consignee,
               e.product_channel,
               e.location,
               e.platform,
               b.oramt as oramt,
               e.agtno as igpagent,
               e.variant,
               b.tranno,
               e.polno             
        FROM adw_prod_tgt.xbc_paytran_dtl a,
             adw_prod_tgt.xbc_or_history b,
             adw_prod_tgt.xbc_paymst c,
             adw_prod_tgt.xbc_ormst d,
            (SELECT tranno,
                    a.refno,
                    decode(substr(d.plancode,1,5),'NSMDE',430,'NSML5',0,(NVL(a.cardamt,0)-NVL(a.DSCNTAMT,0))) cardprem,
                    a.retcode,
                    adw_prod_tgt.parsename_temp(adw_prod_tgt.fngetnamestr(c.nameid),'LFM','FML') consigneename,
                    d.polno, d.product_channel, d.location, d.platform,  adw_prod_tgt.fngetrefdesc(d.planmstdesc) as variant, c.agtno
             FROM adw_prod_tgt.xbc_consign_mst a,
                  adw_prod_tgt.rct_consign_mst b,
                  adw_prod_tgt.agent_dim c,      --xag_profile
                  (SELECT a.regrefno, a.polno, a.plancode,
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
                        adw_prod_tgt.rct_planmst yy
                        WHERE xx.plancode = yy.plancode 
                        AND yy.typecode = 367
                        AND xx.enddate IS NULL
                        AND xx.source_name = 'BD'
                        AND xx.table_name = 'RNB_POLCOV_TRN') a
                    LEFT JOIN adw_prod_tgt.lu_ref_dim b
              --       ON b.refseqno = NVL(a.product_channel,0)
                    ON b.refseqno = a.product_channel
                    AND b.source_name = 'ELIFE'
                    AND b.table_name = 'CXX_GENINFO_REF'
                    LEFT JOIN adw_prod_tgt.lu_ref_dim c
              --       ON c.refseqno = NVL(a.platform,0)
                    ON c.refseqno = a.platform
                    AND c.source_name = 'ELIFE'
                    AND c.table_name = 'CXX_GENINFO_REF'
                    LEFT JOIN adw_prod_tgt.RCT_PLANMST dd
                    ON a.plancode = dd.plancode
                    ) d 
             WHERE a.refno = b.controlno_value(+)
             AND b.agtno = c.agtno(+)
             AND a.refno = d.regrefno(+) 
             AND c.source_name(+) = 'ELIFE'
             AND c.table_name(+) = 'XAG_PROFILE') e
        WHERE a.tranno = b.tranno
        AND a.tranno = c.tranno
        AND a.tranno = d.tranno(+) 
        AND a.tranno = e.tranno
        --AND trunc(b.ordate) = trunc(sysdate -1)
        AND trunc(b.ordate) = p_date
        ; --incremental  --updated by francis 06112025 incremental
 

        COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_RET_POLICY_DET_DAILY','SP_AH_MEDICASH_OR_RETAIL_DAILY',SYSDATE,SYSDATE,'UPDATE');


        
        END SP_AH_MEDICASH_OR_RETAIL_DAILY;