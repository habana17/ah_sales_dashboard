CREATE
OR REPLACE PROCEDURE SP_AH_OOOPS_SALE_EXTRACT_DAILY  (p_date IN DATE)  AS 

/******************************************************************************

NAME:       SP_AH_OOOPS_SALE_EXTRACT_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        06/11/2025       Francis          1. Create SP_AH_OOOPS_SALE_EXTRACT_DAILY


NOTES:

 ******************************************************************************/   

BEGIN

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TEMP_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','DELETE'); 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.OOOPS_SALE_EXTRACT_TEMP_DAILY';

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TEMP_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','INSERT');
        INSERT INTO adw_prod_tgt.OOOPS_SALE_EXTRACT_TEMP_DAILY (paymentdate,paysource,ordisplay,orno,
                                                        billseqno,payor,planno,variant,
                                                        premium_percard,qty,payment,netprem,
                                                        cardstart,cardend,remarks,consignee,
                                                        segmentcode,branchcode,prodcode,
                                                        cardsaleno,cardpayment,agtno,tranno)
        (SELECT DISTINCT b.ordate AS paymentdate, 'OR' AS paysource,
                         b.ordisplay AS ordisplay, a.orno AS orno,
                         a.billseqno,
                         adw_prod_tgt.parsename_temp (b.namestr, 'LFM', 'FML') AS payor,
                         c.planno AS planno, d.packdesc AS variant,
                         c.premium AS premium_percard,
                         (a.cardend - a.cardstart) + 1 AS qty,
                         b.oramt AS payment, d.netprem AS netprem,
                         a.cardstart, a.cardend, b.nonpolremark AS remarks,
                         c.consignee, c.segmentcode, c.branch AS branchcode,
                         d.prodcode,
                         d.cardsaleno,
                         CASE WHEN a.discountval IS NULL THEN a.cardamt
                            ELSE (a.cardamt - a.discountval)
                         END AS cardpayment,
                         a.agtno,
                         b.tranno
                    FROM adw_prod_tgt.grb_card_paydtl a,
                         adw_prod_tgt.xbc_or_history b,
                         (SELECT f.cardstart, f.cardend, f.cardsaleno,
                                 f.planno, f.premium,
                                 h.vnamestr AS consignee,
                                 i.vrefdesc AS segmentcode,
                                 f.branchcode AS branch
                          FROM adw_prod_tgt.ahr_billing_mst g, 
                               adw_prod_tgt.ahr_cardsales f
                          LEFT JOIN (SELECT DISTINCT NAMESTR vnamestr, NAMEID
                                     FROM adw_prod_tgt.CNB_NAMELST_TRN_v2 a) h
                          ON h.nameid = f.byrnameid
                          LEFT JOIN (SELECT refdesc vrefdesc, refseqno
                                       FROM adw_prod_tgt.lu_ref_dim
                                       WHERE source_name = 'ELIFE'
                                       AND table_name = 'CXX_GENINFO_REF') i
                          ON i.refseqno =  f.segmentcode
                          WHERE f.cardsaleno = g.cardsaleno) c,
                         (SELECT f.cardstart, f.cardend, g.prodcode,
                                 f.cardsaleno, g.packdesc, g.netprem
                          FROM adw_prod_tgt.ahr_cardsales f, adw_prod_tgt.ahr_products g
                          WHERE f.prodseqno = g.prodseqno
                          AND f.planno = g.planno) d
                    WHERE a.orno = b.orno
                    AND a.billseqno LIKE '9%'
                    AND b.canceltag = 'N'
                    AND a.cardstart = c.cardstart
                    AND a.cardstart = d.cardstart
                    AND b.timestmp = (select max(m.timestmp) from adw_prod_tgt.xbc_or_history m where m.ordisplay = b.ordisplay)
                    AND trunc(b.ordate) = p_date --incremental
                    );

       --  COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TEMP_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TCODE_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','DELETE'); 
        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.OOOPS_SALE_EXTRACT_TCODE_DAILY';

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TCODE_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','INSERT');
        INSERT INTO adw_prod_tgt.OOOPS_SALE_EXTRACT_TCODE_DAILY (nameid, tcode)
        SELECT a.nameid, NVL(NVL(tcode,vT9),'ZZZZZZ') tcode
        FROM (SELECT DISTINCT b.tcode, a.nameid
              FROM adw_prod_tgt.cxx_employee_mst a, 
                  adw_prod_tgt.cxx_branch_ref_v2 b--CXX_BRANCH_REF b
              WHERE a.branchcode = b.branchcode) a,
             (SELECT DISTINCT NVL(C.TCODE,'ZZZZZZ') vT9, b.nameid 
                    FROM adw_prod_tgt.XAG_ASSIGN_v2 a, --XAG_ASSIGN A, 
                       adw_prod_tgt.XAG_PROFILE_v2 b, --XAG_PROFILE B, 
                       adw_prod_tgt.CXX_BRANCH_REF_v2 c --CXX_BRANCH_REF C
                    WHERE a.AGTNO = b.AGTNO
                    AND a.ENDDATE IS NULL
                    AND a.BRANCHCODE = c.BRANCHCODE
                    AND a.assignseqno = (SELECT MAX(aa.assignseqno)
                                       FROM adw_prod_tgt.XAG_ASSIGN_v2 aa
                                       WHERE a.agtno = aa.agtno)) b
        WHERE a.nameid = b.nameid;

       --  COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TCODE_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TCODE2_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','DELETE'); 

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.OOOPS_SALE_EXTRACT_TCODE2_DAILY';

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TCODE2_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.OOOPS_SALE_EXTRACT_TCODE2_DAILY
        SELECT DISTINCT NVL(C.TCODE,'ZZZZZZ') vT9, b.nameid 
        FROM adw_prod_tgt.XAG_ASSIGN_v2 a, --XAG_ASSIGN A, 
           adw_prod_tgt.XAG_PROFILE_v2 b, --XAG_PROFILE B, 
           adw_prod_tgt.CXX_BRANCH_REF_v2 c --CXX_BRANCH_REF C
        WHERE a.AGTNO = b.AGTNO
        AND a.ENDDATE IS NULL
        AND a.BRANCHCODE = c.BRANCHCODE
        AND a.assignseqno = (SELECT MAX(aa.assignseqno)
                           FROM adw_prod_tgt.XAG_ASSIGN_v2 aa
                           WHERE a.agtno = aa.agtno);

       --  COMMIT;       

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_TCODE2_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_LOCATION_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','DELETE'); 

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.OOOPS_SALE_EXTRACT_LOCATION_DAILY';

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_LOCATION_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.OOOPS_SALE_EXTRACT_LOCATION_DAILY (location,vgrkey,prohibited_tcode)
        SELECT DISTINCT a.location, b.grkey vgrkey, a.prohibited_tcode
        FROM adw_prod_tgt.xac_tcode_ref a
        LEFT JOIN adw_prod_tgt.lu_ref_dim b
        ON b.refseqno = a.tcode
        AND b.source_name = 'ELIFE'
        AND b.table_name = 'CXX_GENINFO_REF'
        WHERE b.grkey = 'cxx_geninfo_tcodesT9';

       --  COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_LOCATION_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','DELETE'); 

        DELETE adw_prod_tgt.OOOPS_SALE_EXTRACT_DAILY
        WHERE 1=1
        AND trunc(paymentdate) >= trunc(sysdate); --updated by francis 06112025

       --  COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,'','INSERT');
        INSERT INTO adw_prod_tgt.OOOPS_SALE_EXTRACT_DAILY (paymentdate,paysource,ordisplay,orno,billseqno,
                                                    payor,planno,variant,premium_percard,qty,
                                                    payment,cardstart,cardend,remarks,
                                                    consignee,segmentcode,branchcode,prodcode,
                                                    expdate,cardsaleno,channel,location,platform,agtno,tranno)
        SELECT a.paymentdate,a.paysource,a.ordisplay,a.orno,a.billseqno,a.payor,
               a.planno,a.variant,a.premium_percard,a.qty,a.cardpayment,--a.payment,netprem,
               a.cardstart,a.cardend,a.remarks,a.consignee,a.segmentcode,
               a.branchcode,a.prodcode,i.expdate,a.cardsaleno, 
               h.refdesc channel, 
               CASE WHEN NVL(d.location,dd.location) = 'Not Applicable-Not Applicable' THEN j.location
                    ELSE NVL(d.location,dd.location)
               END, e.vPlatform,a.agtno,a.tranno
        FROM adw_prod_tgt.OOOPS_SALE_EXTRACT_TEMP_DAILY a
        LEFT JOIN (SELECT DISTINCT intermediary, cardstart
                   FROM adw_prod_tgt.ahr_cardsales) b
        ON b.cardstart = a.cardstart
        LEFT JOIN adw_prod_tgt.OOOPS_SALE_EXTRACT_TCODE_DAILY c
        ON b.intermediary = c.nameid  
        LEFT JOIN adw_prod_tgt.OOOPS_SALE_EXTRACT_TCODE2_DAILY cc
        ON b.intermediary = cc.nameid  
        LEFT JOIN adw_prod_tgt.OOOPS_SALE_EXTRACT_LOCATION_DAILY d
        ON c.tcode = d.prohibited_tcode
        LEFT JOIN adw_prod_tgt.OOOPS_SALE_EXTRACT_LOCATION_DAILY dd
        ON cc.vt9 = dd.prohibited_tcode
        LEFT JOIN (SELECT DISTINCT a.cardno,
                           DECODE (b.refdesc, 'IS - Insureshop', 'Insureshop', 'Microsites') vPlatform
                    FROM (SELECT x.segmentcode,--DECODE (adw_prod_tgt.fngetrefdesc(x.segmentcode), 'IS - Insureshop', 'Insureshop', 'Microsites') vPlatform,
                                  y.cardno  
                            FROM adw_prod_tgt.grb_pol_loadex x, 
                                adw_prod_tgt.grb_insured_mst y
                            WHERE x.enddate IS NULL 
                            AND x.polno = y.polno) a
                    LEFT JOIN adw_prod_tgt.lu_ref_dim b
                    ON b.refseqno = a.segmentcode
                    AND b.source_name = 'ELIFE'
                    AND b.table_name = 'CXX_GENINFO_REF'
                    WHERE a.cardno IS NOT NULL) e
        ON e.cardno = a.cardstart   
        LEFT jOIN (SELECT DISTINCT DECODE(d.agtno, 1, 10054508, e.distchannel) vDistChannel,
                           a.cardstart,d.agtno
                    FROM adw_prod_tgt.ahr_cardsales      a,
                           adw_prod_tgt.xag_profile_v2  d,
                           adw_prod_tgt.xag_assign_v2   e
                    WHERE a.intermediary = d.nameid
                    AND d.agtno = e.agtno
                    AND e.enddate IS NULL)f
        ON f.cardstart = a.cardstart              
        LEFT JOIN adw_prod_tgt.grb_data_mapping g
        ON g.list_of_value = f.vDistChannel
        AND g.map_description = 'AH_DIST_CHANNEL'
        LEFT JOIN adw_prod_tgt.lu_ref_dim h
        ON h.refseqno = g.map_value
        AND h.source_name = 'ELIFE'
        AND h.table_name = 'CXX_GENINFO_REF'
        LEFT JOIN (SELECT MAX (g.expdate) expdate, f.cardstart
                          FROM adw_prod_tgt.ahr_cardsales f, adw_prod_tgt.ahr_aoc_pin g
                          WHERE f.cardsaleno = g.cardsaleno
                          GROUP BY f.cardstart) i
        ON a.cardstart = i.cardstart 
        LEFT JOIN (SELECT distinct d.location, a.cardstart
                   FROM adw_prod_tgt.ahr_cardsales    a,
                        adw_prod_tgt.ahr_billing_mst  b,
                        adw_prod_tgt.grb_insured_mst  c,
                        adw_prod_tgt.cxx_branch_ref_v2   d
                   WHERE a.cardsaleno = b.cardsaleno
                   AND c.cardno = a.cardstart
                   AND a.branchcode = d.branchcode)j
        ON j.cardstart = a.cardstart
        WHERE a.prodcode = 'AOC'
        AND variant != 'Ooopsie Plan 750 with COVID'
        --AND trunc(a.paymentdate) = trunc(sysdate - 1) --updated by francis 06112025 incremental
        AND trunc(a.paymentdate) = p_date
        ;

         COMMIT;

       adw_prod_tgt.sp_adw_table_logs('OOOPS_SALE_EXTRACT_DAILY','SP_AH_OOOPS_SALE_EXTRACT_DAILY',SYSDATE,SYSDATE,'UPDATE');


END SP_AH_OOOPS_SALE_EXTRACT_DAILY;