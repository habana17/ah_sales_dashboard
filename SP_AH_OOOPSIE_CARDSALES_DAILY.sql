CREATE
OR REPLACE PROCEDURE SP_AH_OOOPSIE_CARDSALES_DAILY (p_date IN DATE) 
AS 

/******************************************************************************

NAME:       SP_AH_OOOPSIE_CARDSALES_DAILY
PURPOSE:    Insert data for extraction

REVISIONS:
Ver          Date                  Author             Description
---------  ----------          ---------------  ------------------------------------
1.0        06/11/2025       Francis          1. Create SP_AH_OOOPSIE_CARDSALES_DAILY


NOTES:

 ******************************************************************************/   

BEGIN


        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_TEMP1_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.OOOPSIE_CARDSALES_TEMP1_DAILY';

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_TEMP1_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.OOOPSIE_CARDSALES_TEMP1_DAILY(paymentdate,paysource,ordisplay,orno,billseqno,
                                                        payor,planno,variant,premium_percard,qty,payment,
                                                        cardstart,cardend,remarks,consignee,segmentcode,
                                                        branchcode,prodcode,promocode,agtno,tranno)
        SELECT DISTINCT b.ordate AS paymentdate,
            'OR' AS paysource,
            b.ordisplay AS ordisplay,
            a.orno AS orno,
            a.billseqno,
            adw_prod_tgt.parsename_temp(b.namestr, 'LFM', 'FML') AS payor,
            5 AS planno,
            'Ooopsie Plan 750' variant,
            a.cardamt AS premium_percard,
            a.cardqty qty,
            b.oramt AS payment,
            a.cardstart,
            a.cardend,
            b.nonpolremark AS remarks,
            'INSURESHOP' AS consignee,
            adw_prod_tgt.fngetrefdesc('10007676') AS segmentcode,
            'HO' AS branchcode,
            (SELECT DISTINCT g.prodcode
             FROM adw_prod_tgt.ahr_cardsales f,
                  adw_prod_tgt.ahr_products  g
             WHERE f.prodseqno = g.prodseqno
             AND f.planno = g.planno
             AND a.cardstart BETWEEN f.cardstart AND f.cardend) AS prodcode,
            CASE WHEN a.agtno IS NOT NULL THEN 'PSA' || a.agtno
                ELSE NULL
            END promocode,
            a.agtno,
            b.tranno
       FROM adw_prod_tgt.grb_card_paydtl a,
            adw_prod_tgt.xbc_or_history  b
       WHERE a.orno = b.orno
       AND a.billseqno LIKE '9%'
       AND b.nonpolremark = 'Ooopsie! Card Sales'
       AND b.canceltag = 'N'
       AND b.timestmp = (select max(m.timestmp) from adw_prod_tgt.xbc_or_history m where m.ordisplay = b.ordisplay)
       AND trunc(b.ordate) = p_date --incremental
       ;

    --    COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_TEMP1_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_TEMP2_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.OOOPSIE_CARDSALES_TEMP2_DAILY';

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_TEMP2_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','INSERT');
        INSERT INTO adw_prod_tgt.OOOPSIE_CARDSALES_TEMP2_DAILY (paymentdate,paysource,ordisplay,orno,billseqno,
                                                          payor,planno,variant,premium_percard,qty,payment,
                                                          cardstart,cardend,remarks,consignee,segmentcode,
                                                          branchcode,prodcode,promocode,agtno,tranno)
        SELECT DISTINCT  b.ordate AS paymentdate,
                'OR' AS paysource,
                b.ordisplay AS ordisplay,
                a.orno AS orno,
                a.billseqno,
                adw_prod_tgt.parsename_temp(b.namestr, 'LFM', 'FML') AS payor,
                5  AS planno,
                'Ooopsie Plan 750' variant,
                a.cardamt AS premium_percard,
                a.cardqty qty,
                b.oramt AS payment,
                a.cardstart,
                a.cardend,
                b.nonpolremark AS remarks,
                'INSURESHOP' AS consignee,
                adw_prod_tgt.fngetrefdesc('10007676') AS segmentcode,
                'HO' AS branchcode,
                (SELECT DISTINCT g.prodcode
                 FROM adw_prod_tgt.ahr_cardsales f,
                      adw_prod_tgt.ahr_products  g
                 WHERE f.prodseqno = g.prodseqno
                 AND f.planno = g.planno) AS prodcode,
                CASE WHEN a.agtno IS NOT NULL THEN
                        'PSA' || a.agtno
                    ELSE NULL
                END  promocode,
            a.agtno,
            b.tranno
        FROM adw_prod_tgt.grb_card_paydtl a,
            adw_prod_tgt.xbc_or_history  b
        WHERE a.orno = b.orno
        AND a.billseqno LIKE '9%'
        AND b.canceltag = 'N'
        AND b.nonpolremark = 'Ooopsie! Card Sales'
        AND b.timestmp = (select max(m.timestmp) from adw_prod_tgt.xbc_or_history m where m.ordisplay = b.ordisplay)
        AND trunc(b.ordate) = p_date --incremental
        ;

        -- COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_TEMP2_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_AGTNO_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.AOCAGT_LOCATION_AGTNO_DAILY';

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_AGTNO_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.AOCAGT_LOCATION_AGTNO_DAILY (agtno, nameid)
        SELECT agtno, nameid
        FROM adw_prod_tgt.xag_profile_v2 a
        WHERE EXISTS (SELECT (1)
                     FROM adw_prod_tgt.OOOPSIE_CARDSALES_TEMP1_DAILY x
                     WHERE x.agtno = a.agtno
                     UNION 
                     SELECT (1)
                     FROM adw_prod_tgt.OOOPSIE_CARDSALES_TEMP2_DAILY xx
                     WHERE xx.agtno = a.agtno);

        -- COMMIT; 

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_AGTNO_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_TEMP_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.AOCAGT_LOCATION_TEMP_DAILY';

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_TEMP_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.AOCAGT_LOCATION_TEMP_DAILY (tcode,agtno)
        SELECT adw_prod_tgt.fnget_tcodesutil_t9 (a.nameid) tcode, a.agtno
        FROM adw_prod_tgt.AOCAGT_LOCATION_AGTNO_DAILY a;

        -- COMMIT;

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_TEMP_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','DELETE');

        EXECUTE IMMEDIATE 'TRUNCATE TABLE adw_prod_tgt.AOCAGT_LOCATION_DAILY';

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.AOCAGT_LOCATION_DAILY (location, agtno)
        SELECT distinct x.location, z.agtno
        FROM adw_prod_tgt.xac_tcode_ref x,
             adw_prod_tgt.lu_ref_dim y,
             adw_prod_tgt.AOCAGT_LOCATION_TEMP_DAILY z
        WHERE y.refseqno = x.tcode
        AND y.source_name = 'ELIFE'
        AND y.table_name = 'CXX_GENINFO_REF'
        AND y.grkey = 'cxx_geninfo_tcodesT9'
        AND x.prohibited_tcode = z.tcode;

        -- COMMIT;

        adw_prod_tgt.sp_adw_table_logs('AOCAGT_LOCATION_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,SYSDATE,'UPDATE');

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','DELETE'); 

       DELETE adw_prod_tgt.OOOPSIE_CARDSALES_DAILY
       WHERE trunc(paymentdate) >= trunc(sysdate)
       ;

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,'','INSERT');

        INSERT INTO adw_prod_tgt.OOOPSIE_CARDSALES_DAILY (paymentdate,paysource,ordisplay,orno,billseqno,
                                                    payor,planno,variant,premium_percard,qty,payment,
                                                    cardstart,cardend,remarks,consignee,segmentcode,
                                                    branchcode,prodcode,promocode,product_channel,
                                                    platform, location,agtno,tranno)
        SELECT DISTINCT paymentdate,paysource,ordisplay,orno,billseqno,
                payor,planno,variant,premium_percard,qty,payment,
                cardstart,cardend,remarks,consignee,segmentcode,
                branchcode,prodcode,promocode,
                adw_prod_tgt.fngetrefdesc (adw_prod_tgt.fn_grb_ah_channel(TO_NUMBER(adw_prod_tgt.fn_grb_ahagt_dischan(TRUNC(a.agtno))))) channel,
               'Insureshop' platform,
               b.location,a.agtno,a.tranno
        FROM adw_prod_tgt.OOOPSIE_CARDSALES_TEMP1_DAILY a
        LEFT JOIN adw_prod_tgt.AOCAGT_LOCATION_DAILY b
        ON a.agtno = b.agtno
        WHERE a.prodcode = 'AOC'
        --AND trunc(a.paymentdate) = trunc(sysdate-1) -- incremental
        AND trunc(a.paymentdate) = p_date
        UNION
        SELECT DISTINCT paymentdate,paysource,ordisplay,orno,billseqno,
                payor,planno,variant,premium_percard,qty,payment,
                cardstart,cardend,remarks,consignee,segmentcode,
                branchcode,prodcode,promocode,
                adw_prod_tgt.fngetrefdesc (adw_prod_tgt.fn_grb_ah_channel(adw_prod_tgt.fn_grb_ahagt_dischan(a.agtno))) channel,
                'Insureshop' platform,
                b.location,a.agtno,a.tranno
        FROM adw_prod_tgt.OOOPSIE_CARDSALES_TEMP2_DAILY a
        LEFT JOIN adw_prod_tgt.AOCAGT_LOCATION_DAILY b
        ON a.agtno = b.agtno
        WHERE 1=1
        -- AND trunc(a.paymentdate) = trunc(sysdate-1) -- incremental
        AND trunc(a.paymentdate) = p_date
        ;
        COMMIT;

        adw_prod_tgt.sp_adw_table_logs('OOOPSIE_CARDSALES_DAILY','SP_AH_OOOPSIE_CARDSALES_DAILY',SYSDATE,SYSDATE,'UPDATE');


END SP_AH_OOOPSIE_CARDSALES_DAILY;