CREATE
OR REPLACE PROCEDURE SP_AH_MEDICASH_OR_NONPOLICY_DAILY (p_date IN DATE)
AS 

/******************************************************************************

NAME:       SP_AH_MEDICASH_OR_NONPOLICY_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        06/11/2025       Francis          1. Create SP_AH_MEDICASH_OR_NONPOLICY_DAILY
1.1        06/20/2025       Francis          1. ADDED LOCATION 


NOTES:

 ******************************************************************************/   
 BEGIN
        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_NONPOLICY_TEMP_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','DELETE');  
        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.MEDICASH_OR_NONPOLICY_TEMP_DAILY';

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_NONPOLICY_TEMP_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','INSERT');    
        INSERT INTO adw_prod_tgt.MEDICASH_OR_NONPOLICY_TEMP_DAILY (addr,advamt,canceltag,cashier,
                                                              collproofref,collref,ctr,cur,cwttag,
                                                              fao,insured_name,lifeid,
                                                              md,namestr,nonpolremark,oramt,ordate,
                                                              ordisplay,orlength,orno,poldate,
                                                              policyrelation,printag,recepient_id,
                                                              remarks,timestmp,tin,tranno,trans,
                                                              trantype, agtno, distchannel, promocode,product_name)       
          SELECT DISTINCT b.addr,b.advamt,b.canceltag,b.cashier,
              b.collproofref,b.collref,b.ctr,b.cur,b.cwttag,
              b.fao,b.insured_name,b.lifeid,
              b.md,b.namestr,b.nonpolremark,b.oramt,b.ordate,
              b.ordisplay,b.orlength,b.orno,b.poldate,
              b.policyrelation,b.printag,b.recepient_id,
              b.remarks,b.timestmp,b.tin,b.tranno,b.trans,
              b.trantype, f.agtno, g.distchannel, e.discount_code,h.name as product_name  --updated by francis 06202025
        FROM adw_prod_tgt.xbc_or_history b
        LEFT JOIN adw_prod_tgt.insure_shop_buyer_info e
        ON e.or_no = b.ordisplay
        AND e.discount_code IS NOT NULL
        LEFT JOIN (SELECT a.agtno, a.promo_code, a.active_flag, a.program_type, a.onlineshop_flag ,
                         a.startdate, a.enddate
                    FROM adw_prod_tgt.xag_agt_onboard_ref a) f
        ON ((f.program_type = '10012176' AND f.active_flag = 'Y')
        OR (f.program_type IS NULL AND f.onlineshop_flag = 'Y'))
        AND b.ordate BETWEEN f.startdate AND nvl(f.enddate,trunc(SYSDATE) ) 
        AND e.discount_code = f.promo_code
        LEFT JOIN (SELECT a.distchannel, a.agtno
                    FROM adw_prod_tgt.agent_dim a
                   WHERE a.enddate IS NULL
                     AND a.source_name = 'ELIFE'
                     AND a.table_name = 'XAG_ASSIGN'
                     AND a.startdate = (SELECt MAX(b.startdate)
                                        FROM adw_prod_tgt.agent_dim b
                                        WHERE b.enddate IS NULL
                                         AND b.agtno = a.agtno
                                         AND b.source_name = 'ELIFE'
                                         AND b.table_name = 'XAG_ASSIGN'))g
        ON g.agtno = TO_NUMBER(f.agtno)
        LEFT JOIN (SELECT 
                    a.name,
                    b.or_no
                    FROM 
                    adw_prod_tgt.insure_shop_variant a
                    LEFT JOIN 
                    adw_prod_tgt.insure_shop_buyer_info b 
                    ON 
                    b.product = a.shortcode) h
         ON h.or_no = b.ordisplay
         WHERE 1=1 
         AND trunc(b.ordate) = p_date
    ;

     --    COMMIT;
        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_NONPOLICY_TEMP_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,SYSDATE,'UPDATE');  


        --block for getting the location --ADDED BY FRANCIS 06202025


        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_AGTNO_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.MEDICASH_LOCATION_AGTNO_DAILY';

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_AGTNO_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.MEDICASH_LOCATION_AGTNO_DAILY (agtno, nameid)
        SELECT agtno, nameid
        FROM adw_prod_tgt.xag_profile_v2 a
        WHERE EXISTS (SELECT (1)
                     FROM adw_prod_tgt.medicash_or_nonpolicy_temp_daily x
                     WHERE x.agtno = a.agtno
                     );

     --    COMMIT; 

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_AGTNO_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_TEMP_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.MEDICASH_LOCATION_TEMP_DAILY';

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_TEMP_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.MEDICASH_LOCATION_TEMP_DAILY (tcode,agtno)
        SELECT adw_prod_tgt.fnget_tcodesutil_t9 (a.nameid) tcode, a.agtno
        FROM adw_prod_tgt.MEDICASH_LOCATION_AGTNO_DAILY a;

     --    COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_TEMP_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.MEDICASH_LOCATION_DAILY';

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.MEDICASH_LOCATION_DAILY (location, agtno)
        SELECT distinct x.location, z.agtno
        FROM adw_prod_tgt.xac_tcode_ref x,
             adw_prod_tgt.lu_ref_dim y,
             adw_prod_tgt.MEDICASH_LOCATION_TEMP_DAILY z
        WHERE y.refseqno = x.tcode
        AND y.source_name = 'ELIFE'
        AND y.table_name = 'CXX_GENINFO_REF'
        AND y.grkey = 'cxx_geninfo_tcodesT9'
        AND x.prohibited_tcode = z.tcode;

     --    COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_LOCATION_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,SYSDATE,'UPDATE');

------

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_NONPOLICY_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','DELETE');

        DELETE adw_prod_tgt.medicash_or_nonpolicy_daily
        WHERE 1=1 
        AND  trunc(ordate) >= p_date; --updated by francis 09292025 
        --COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_NONPOLICY_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.medicash_or_nonpolicy_daily (ordate,transubtype,orname,ordisplay,ornum,oramt,collsource,
                                           nonpolremark,igp_agent,or_status,paytran_timestmp,promocode, prodchannel
                                           ,location,tranno,variant --added by francis 06202025
                                           
                                           )
        SELECT DISTINCT a.ordate, b.refdesc transubtype, a.orname, a.ordisplay, a.ornum, a.oramt, 
                c.refdesc collsource, a.nonpolremark, e.agtno, d.refdesc or_status ,
                a.paytran_timestmp, e.promocode,
                CASE WHEN e.distchannel in (5572,10005357) THEN adw_prod_tgt.fngetrefdesc(10054506)
                      WHEN e.distchannel in (5571,10002218,10002789,10006276,10006985,10007904) THEN adw_prod_tgt.fngetrefdesc(10054508)
                      WHEN e.distchannel in 10005357 THEN adw_prod_tgt.fngetrefdesc(10054511)
                      WHEN e.distchannel in 5569 THEN adw_prod_tgt.fngetrefdesc(10054515)
                      WHEN e.distchannel in 10054468 THEN adw_prod_tgt.fngetrefdesc(10054516) 
                      ELSE 'Individual Direct' END prodchannel
                      ,f.location,a.tranno,e.product_name as variant --added by francis 06202025
                      
        FROM (
        SELECT         b.ordate,
                       a.transubtype,
                       b.namestr orname,
                       b.ordisplay,
                       TO_CHAR(b.orno) ornum,
                       b.oramt,
                       c.collsource,
                       b.nonpolremark,
                       d.status,
                       trunc(a.timestmp) paytran_timestmp,
                       a.tranno --added by francis 06202025
                FROM adw_prod_tgt.xbc_paytran_dtl a,
                     adw_prod_tgt.xbc_or_history b,
                     adw_prod_tgt.xbc_paymst c,
                     adw_prod_tgt.xbc_ormst d
                WHERE a.transubtype IN (SELECT refseqno
                                      FROM adw_prod_tgt.rct_orsubclass_medicash)
                AND a.tranno = b.tranno
                AND a.tranno = c.tranno
                AND a.tranno = d.tranno) a
        LEFT JOIN adw_prod_tgt.LU_REF_DIM b
        ON b.refseqno = a.transubtype
        AND b.source_name = 'ELIFE'
        AND b.table_name = 'CXX_GENINFO_REF'
        LEFT JOIN adw_prod_tgt.LU_REF_DIM c
        ON c.refseqno = a.collsource
        AND c.source_name = 'ELIFE'
        AND c.table_name = 'CXX_GENINFO_REF'
        LEFT JOIN adw_prod_tgt.LU_REF_DIM d
        ON d.refseqno = a.status
        AND d.source_name = 'ELIFE'
        AND d.table_name = 'CXX_GENINFO_REF'
        LEFT JOIN adw_prod_tgt.medicash_or_nonpolicy_temp_daily e
        ON e.orno = a.ornum
        LEFT JOIN adw_prod_tgt.MEDICASH_LOCATION_DAILY f --added by francis 06202025
        ON e.agtno = f.agtno
        WHERE 1=1
        AND trunc(a.ordate) = p_date
        ; --incremental --updated by francis 06112025


        COMMIT;

        adw_prod_tgt.sp_adw_table_logs('MEDICASH_OR_NONPOLICY_DAILY','SP_AH_MEDICASH_OR_NONPOLICY_DAILY',SYSDATE,SYSDATE,'UPDATE');


        END SP_AH_MEDICASH_OR_NONPOLICY_DAILY;