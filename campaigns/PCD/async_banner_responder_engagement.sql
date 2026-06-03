-- async_banner_responder_engagement.sql
-- Engine: Starburst (Trino). Federated: Teradata cohort/conversion tables + edl0_im GA4.
-- Purpose: Of converters, how many engaged with the async creative?
--   converters          = distinct clnt_no who converted within 60-day window
--   engaged_converters  = subset with any GA4 view or click in window
--   engaged_converters_clicked = subset with >= 1 click_p in window
-- No vintage_day breakdown, no rates. Counts only.
-- Sibling files:
--   async_banner_vintage_engagement.sql  — engagement curves (all cohort members)
--   async_banner_vintage_success.sql     — success curves (all cohort members)


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD                                                              ║
-- ║ Cohort + conversion: dl_mr_prod.cards_pcd_ongoing_decis_resp               ║
-- ║ Engagement: edl0_im GA4 ecommerce reduced                                  ║
-- ║ Segments: OVERALL (ALL/ALL) + PRODUCT (product_at_decision) + CHANNEL      ║
-- ║           (fulfillment_channel from cards_pcd_ongoing_decis_resp)          ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH

pcd_cohort AS (
    SELECT
        clnt_no,
        response_start,
        response_start                   AS wave_dt,
        product_at_decision,
        responder_anyproduct,
        COALESCE(CAST(fulfillment_channel AS VARCHAR), '(null)') AS fulfillment_channel,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
        END AS test_control_flag,
        CASE
            WHEN strategy_seg_cd IN (
                'MSC8YUS3','MAO28CJ5','MAO2EDB1',
                'MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7'
            ) THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start >= DATE '2026-04-01'
),

-- NOTE: engagement file filters PCD by it_item_name (not it_item_id). The four
-- it_item_name values used there are:
--   'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP'
--   'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV'
--   'PB_CC_ALL_26_02_RBC_PCD_PPCN'
--   'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
-- it_item_id equivalents for PCD are not confirmed in this repo. Filtering by
-- it_item_name here to stay consistent with the engagement sibling.
-- FLAGGED: swap to it_item_id once confirmed (feedback_ga4_correct_fields).
pcd_ga4_raw AS (
    SELECT
        event_date,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        CASE
            WHEN lower(event_name) = 'view_promotion' THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'n_Non intéressé','n_Not interested','n_Not now','n_Pas maintenant',
                'Not now','Pas maintenant','n_close','Close'
            ) THEN 'click_n'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'p_Chat to learn more','p_Chat with us','p_Clavarder avec nous',
                'p_Clavardez pour en savoir plus','Chat to learn more',
                'Clavardez pour en savoir plus','VSA_OFFER_SF'
            ) THEN 'click_p'
        END AS lead_class
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

-- Join GA4 events to converters only, filtered to window
pcd_converter_events AS (
    SELECT
        c.wave_dt,
        c.product_at_decision,
        c.fulfillment_channel,
        c.test_control_flag,
        c.cohort_arm,
        c.clnt_no,
        c.responder_anyproduct,
        MAX(CASE WHEN g.lead_class IN ('view','click_p','click_n') THEN 1 ELSE 0 END) AS had_engagement,
        MAX(CASE WHEN g.lead_class = 'click_p'                     THEN 1 ELSE 0 END) AS had_click_p
    FROM pcd_cohort c
    LEFT JOIN pcd_ga4_raw g
        ON  g.clnt_no    = c.clnt_no
        AND g.event_date BETWEEN c.response_start AND date_add('day', 60, c.response_start)
    WHERE c.responder_anyproduct = 1
      AND c.test_control_flag IS NOT NULL
    GROUP BY
        c.wave_dt, c.product_at_decision, c.fulfillment_channel,
        c.test_control_flag, c.cohort_arm, c.clnt_no, c.responder_anyproduct
),

pcd_overall AS (
    SELECT
        wave_dt,
        CAST('ALL'     AS VARCHAR) AS segment,
        CAST('OVERALL' AS VARCHAR) AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM pcd_converter_events
    GROUP BY wave_dt, test_control_flag, cohort_arm
),

pcd_product AS (
    SELECT
        wave_dt,
        CAST('PRODUCT' AS VARCHAR)  AS segment,
        product_at_decision         AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM pcd_converter_events
    GROUP BY wave_dt, product_at_decision, test_control_flag, cohort_arm
),

pcd_channel AS (
    SELECT
        wave_dt,
        CAST('CHANNEL' AS VARCHAR)  AS segment,
        fulfillment_channel         AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM pcd_converter_events
    GROUP BY wave_dt, fulfillment_channel, test_control_flag, cohort_arm
),

pcd_final AS (
    SELECT CAST('PCD' AS VARCHAR) AS campaign, wave_dt AS cohort, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM pcd_overall
    UNION ALL
    SELECT CAST('PCD' AS VARCHAR), wave_dt, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM pcd_product
    UNION ALL
    SELECT CAST('PCD' AS VARCHAR), wave_dt, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM pcd_channel
)

SELECT campaign, cohort, segment, segment_level, test_control_flag, cohort_arm,
       converters, engaged_converters, engaged_converters_clicked
FROM pcd_final
ORDER BY campaign, cohort, segment, segment_level, test_control_flag, cohort_arm
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU                                                              ║
-- ║ Cohort + conversion: dl_mr_prod.nbo_pba_upgrade                            ║
-- ║ Engagement: edl0_im GA4 ecommerce reduced, it_item_id = 'i_300102'         ║
-- ║ Segments: OVERALL (ALL/ALL) + CHANNEL (fulfilmnt_chnl from nbo_pba_upgrade)║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH

ctu_cohort AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_strt_dt                  AS wave_dt,
        success,
        COALESCE(CAST(fulfilmnt_chnl AS VARCHAR), '(null)') AS fulfilmnt_chnl,
        CAST('ALL' AS VARCHAR)           AS test_control_flag,
        CASE WHEN chnl_mb = 1 THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm
    FROM dl_mr_prod.nbo_pba_upgrade
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

ctu_ga4_raw AS (
    SELECT
        event_date,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        CASE
            WHEN lower(event_name) = 'view_promotion' THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'n_Non intéressé','n_Not interested','n_Not now','n_Pas maintenant',
                'Not now','Pas maintenant','n_close','Close'
            ) THEN 'click_n'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'p_Chat to learn more','p_Chat with us','p_Clavarder avec nous',
                'p_Clavardez pour en savoir plus','Chat to learn more',
                'Clavardez pour en savoir plus','VSA_OFFER_SF'
            ) THEN 'click_p'
        END AS lead_class
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

ctu_converter_events AS (
    SELECT
        c.wave_dt,
        c.fulfilmnt_chnl,
        c.test_control_flag,
        c.cohort_arm,
        c.clnt_no,
        MAX(CASE WHEN g.lead_class IN ('view','click_p','click_n') THEN 1 ELSE 0 END) AS had_engagement,
        MAX(CASE WHEN g.lead_class = 'click_p'                     THEN 1 ELSE 0 END) AS had_click_p
    FROM ctu_cohort c
    LEFT JOIN ctu_ga4_raw g
        ON  g.clnt_no    = c.clnt_no
        AND g.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    WHERE c.success = 1
    GROUP BY
        c.wave_dt, c.fulfilmnt_chnl, c.test_control_flag, c.cohort_arm, c.clnt_no
),

ctu_overall AS (
    SELECT
        wave_dt,
        CAST('ALL'     AS VARCHAR) AS segment,
        CAST('OVERALL' AS VARCHAR) AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM ctu_converter_events
    GROUP BY wave_dt, test_control_flag, cohort_arm
),

ctu_channel AS (
    SELECT
        wave_dt,
        CAST('CHANNEL' AS VARCHAR) AS segment,
        fulfilmnt_chnl             AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM ctu_converter_events
    GROUP BY wave_dt, fulfilmnt_chnl, test_control_flag, cohort_arm
),

ctu_final AS (
    SELECT CAST('CTU' AS VARCHAR) AS campaign, wave_dt AS cohort, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM ctu_overall
    UNION ALL
    SELECT CAST('CTU' AS VARCHAR), wave_dt, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM ctu_channel
)

SELECT campaign, cohort, segment, segment_level, test_control_flag, cohort_arm,
       converters, engaged_converters, engaged_converters_clicked
FROM ctu_final
ORDER BY campaign, cohort, segment, segment_level, test_control_flag, cohort_arm
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P                                                              ║
-- ║ Cohort: DG6V01.TACTIC_EVNT_IP_AR_HIST (TG4=TEST / TG7=CONTROL)            ║
-- ║ Conversion: CR_APP chain, fixed 60-day window (treatmt_strt_dt + 60)       ║
-- ║ Engagement: edl0_im GA4 ecommerce reduced, it_item_id = 'i_298045'         ║
-- ║ Segments: OVERALL + REPORT_GROUP. CHANNEL skipped — channel-of-response    ║
-- ║   field not confirmed on OVRL_CR_APP / CR_APP_PROD. Add when HELP TABLE    ║
-- ║   confirms the correct column name.                                        ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH

o2p_cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_strt_dt                  AS wave_dt,
        TRIM(rpt_grp_cd)                 AS rpt_grp_cd,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag,
        CASE
            WHEN TRIM(rpt_grp_cd) IN (
                'PO2PNL01','PO2PNL03','PO2PNL07',
                'PO2POT01','PO2POT03','PO2POT07',
                'PO2PPR01','PO2PPR03','PO2PPR07'
            ) THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

o2p_cohort AS (
    SELECT DISTINCT clnt_no, treatmt_strt_dt, wave_dt, rpt_grp_cd, test_control_flag, cohort_arm
    FROM o2p_cohort_raw
),

o2p_applications AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt, d.appl_for_prod_typ
    FROM DDWV01.CR_APP_CLNT_RELTN        AS a
    JOIN DDWV01.OVRL_CR_APP              AS b
        ON  b.cr_app_id  = a.cr_app_id
        AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN   AS c
        ON  c.cr_app_id          = a.cr_app_id
        AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id         = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD              AS d
        ON  d.cr_app_id          = c.cr_app_id
        AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id         = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN ('32','37','45','47','51','56','62')
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),

-- First approved app per client within fixed 60-day window
o2p_first_apps AS (
    SELECT
        c.wave_dt,
        c.rpt_grp_cd,
        c.test_control_flag,
        c.cohort_arm,
        c.clnt_no,
        c.treatmt_strt_dt,
        MIN(a.app_dt) AS first_app_dt
    FROM o2p_cohort c
    INNER JOIN o2p_applications a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt  BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY c.wave_dt, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm, c.clnt_no, c.treatmt_strt_dt
),

o2p_ga4_raw AS (
    SELECT
        event_date,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        CASE
            WHEN lower(event_name) = 'view_promotion' THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'n_Non intéressé','n_Not interested','n_Not now','n_Pas maintenant',
                'Not now','Pas maintenant','n_close','Close'
            ) THEN 'click_n'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'p_Chat to learn more','p_Chat with us','p_Clavarder avec nous',
                'p_Clavardez pour en savoir plus','Chat to learn more',
                'Clavardez pour en savoir plus','VSA_OFFER_SF'
            ) THEN 'click_p'
        END AS lead_class
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

o2p_converter_events AS (
    SELECT
        f.wave_dt,
        f.rpt_grp_cd,
        f.test_control_flag,
        f.cohort_arm,
        f.clnt_no,
        MAX(CASE WHEN g.lead_class IN ('view','click_p','click_n') THEN 1 ELSE 0 END) AS had_engagement,
        MAX(CASE WHEN g.lead_class = 'click_p'                     THEN 1 ELSE 0 END) AS had_click_p
    FROM o2p_first_apps f
    LEFT JOIN o2p_ga4_raw g
        ON  g.clnt_no    = f.clnt_no
        AND g.event_date BETWEEN f.treatmt_strt_dt AND date_add('day', 60, f.treatmt_strt_dt)
    GROUP BY f.wave_dt, f.rpt_grp_cd, f.test_control_flag, f.cohort_arm, f.clnt_no
),

o2p_overall AS (
    SELECT
        wave_dt,
        CAST('ALL'     AS VARCHAR) AS segment,
        CAST('OVERALL' AS VARCHAR) AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM o2p_converter_events
    GROUP BY wave_dt, test_control_flag, cohort_arm
),

o2p_report_group AS (
    SELECT
        wave_dt,
        CAST('REPORT_GROUP' AS VARCHAR) AS segment,
        rpt_grp_cd                      AS segment_level,
        test_control_flag,
        cohort_arm,
        COUNT(DISTINCT clnt_no)                             AS converters,
        COUNT(DISTINCT CASE WHEN had_engagement = 1 THEN clnt_no END) AS engaged_converters,
        COUNT(DISTINCT CASE WHEN had_click_p    = 1 THEN clnt_no END) AS engaged_converters_clicked
    FROM o2p_converter_events
    GROUP BY wave_dt, rpt_grp_cd, test_control_flag, cohort_arm
),

o2p_final AS (
    SELECT CAST('O2P' AS VARCHAR) AS campaign, wave_dt AS cohort, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM o2p_overall
    UNION ALL
    SELECT CAST('O2P' AS VARCHAR), wave_dt, segment, segment_level,
           test_control_flag, cohort_arm, converters, engaged_converters, engaged_converters_clicked
    FROM o2p_report_group
)

SELECT campaign, cohort, segment, segment_level, test_control_flag, cohort_arm,
       converters, engaged_converters, engaged_converters_clicked
FROM o2p_final
ORDER BY campaign, cohort, segment, segment_level, test_control_flag, cohort_arm
;
