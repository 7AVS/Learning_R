-- async_banner_summary_engagement.sql
-- Engine: Starburst (Trino). Federated: reads DG6V01.TACTIC_EVNT_IP_AR_HIST via
--   Teradata federation and edl0_im GA4 tables natively in Trino.
-- Purpose: Engagement totals (no vintage_day breakdown) for PCD, CTU, O2P.
--   No success / responder columns. No cumulative window functions — just totals.
-- Sibling files:
--   async_banner_vintage_engagement.sql — Trino, engagement curves by vintage_day
--   async_banner_summary_success.sql    — Teradata native, success totals


WITH

-- ═══════════════════════════════════════════════════════════════════════════════
-- BLOCK 1 — PCD
-- ═══════════════════════════════════════════════════════════════════════════════

pcd_cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_strt_dt AS wave_dt,
        element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 4) AS product_mnemonic,
        CASE
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%C' THEN 'CONTROL'
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%T' THEN 'TEST'
        END AS test_control_flag,
        CASE
            WHEN element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 3)
                IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
            THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD','2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND trim(coalesce(tst_grp_cd, '')) LIKE '%T'
),

pcd_cohort AS (
    SELECT DISTINCT
        clnt_no, treatmt_strt_dt,
        wave_dt, product_mnemonic, test_control_flag, cohort_arm
    FROM pcd_cohort_raw
),

pcd_population AS (
    SELECT wave_dt, product_mnemonic, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM pcd_cohort
    GROUP BY 1,2,3,4
),

pcd_engagement_events AS (
    SELECT
        event_date, event_name,
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
        END AS lead_class,
        COALESCE(TRY_CAST(up_srf_id2_value AS BIGINT), TRY_CAST(ep_srf_id2 AS BIGINT)) AS clnt_no
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

pcd_engagement_total AS (
    SELECT
        c.wave_dt, c.product_mnemonic, c.test_control_flag, c.cohort_arm,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'view_promotion'      THEN c.clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN e.lead_class IN ('click_p','click_n')        THEN c.clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_p'                     THEN c.clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_n'                     THEN c.clnt_no END) AS leads_n
    FROM pcd_cohort c
    INNER JOIN pcd_engagement_events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY 1,2,3,4
),

pcd_base AS (
    SELECT
        p.wave_dt, p.product_mnemonic, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(e.view_users,  0) AS view_users,
        COALESCE(e.click_users, 0) AS click_users,
        COALESCE(e.leads_p,     0) AS leads_p,
        COALESCE(e.leads_n,     0) AS leads_n
    FROM pcd_population p
    LEFT JOIN pcd_engagement_total e
        ON  e.wave_dt           = p.wave_dt
        AND e.product_mnemonic  = p.product_mnemonic
        AND e.test_control_flag = p.test_control_flag
        AND e.cohort_arm        = p.cohort_arm
),

pcd_final AS (
    SELECT
        CAST('PCD' AS VARCHAR) AS campaign,
        wave_dt                        AS cohort,
        CAST('ALL'     AS VARCHAR)     AS segment,
        CAST('OVERALL' AS VARCHAR)     AS segment_level,
        test_control_flag, cohort_arm,
        SUM(total_population) AS total_population,
        SUM(view_users)       AS view_users,
        SUM(click_users)      AS click_users,
        SUM(leads_p)          AS leads_p,
        SUM(leads_n)          AS leads_n
    FROM pcd_base
    GROUP BY wave_dt, test_control_flag, cohort_arm

    UNION ALL

    SELECT
        CAST('PCD' AS VARCHAR) AS campaign,
        wave_dt          AS cohort,
        'PRODUCT'        AS segment,
        product_mnemonic AS segment_level,
        test_control_flag, cohort_arm,
        total_population,
        view_users, click_users, leads_p, leads_n
    FROM pcd_base
),


-- ═══════════════════════════════════════════════════════════════════════════════
-- BLOCK 2 — CTU
-- ═══════════════════════════════════════════════════════════════════════════════

ctu_cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_strt_dt AS wave_dt,
        CASE WHEN substring(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'
             THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

ctu_cohort AS (
    SELECT DISTINCT clnt_no, treatmt_strt_dt, wave_dt, cohort_arm
    FROM ctu_cohort_raw
),

ctu_population AS (
    SELECT wave_dt, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
    FROM ctu_cohort
    GROUP BY 1,2
),

ctu_engagement_events AS (
    SELECT
        event_date, event_name,
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
        END AS lead_class,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

ctu_engagement_total AS (
    SELECT
        c.wave_dt, c.cohort_arm,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'view_promotion'      THEN c.clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN e.lead_class IN ('click_p','click_n')        THEN c.clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_p'                     THEN c.clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_n'                     THEN c.clnt_no END) AS leads_n
    FROM ctu_cohort c
    INNER JOIN ctu_engagement_events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY 1,2
),

ctu_final AS (
    SELECT
        CAST('CTU'     AS VARCHAR) AS campaign,
        p.wave_dt                  AS cohort,
        CAST('ALL'     AS VARCHAR) AS segment,
        CAST('OVERALL' AS VARCHAR) AS segment_level,
        CAST('ALL'     AS VARCHAR) AS test_control_flag,
        p.cohort_arm,
        p.total_population,
        COALESCE(e.view_users,  0) AS view_users,
        COALESCE(e.click_users, 0) AS click_users,
        COALESCE(e.leads_p,     0) AS leads_p,
        COALESCE(e.leads_n,     0) AS leads_n
    FROM ctu_population p
    LEFT JOIN ctu_engagement_total e
        ON  e.wave_dt    = p.wave_dt
        AND e.cohort_arm = p.cohort_arm
),


-- ═══════════════════════════════════════════════════════════════════════════════
-- BLOCK 3 — O2P
-- ═══════════════════════════════════════════════════════════════════════════════

o2p_cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_strt_dt AS wave_dt,
        TRIM(rpt_grp_cd) AS rpt_grp_cd,
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
    WHERE tactic_id IN ('2026099O2P','2026126O2P','2026132O2P')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) = 'TG4'
),

o2p_cohort AS (
    SELECT DISTINCT
        clnt_no, treatmt_strt_dt,
        wave_dt, rpt_grp_cd, test_control_flag, cohort_arm
    FROM o2p_cohort_raw
),

o2p_population AS (
    SELECT wave_dt, rpt_grp_cd, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM o2p_cohort
    GROUP BY 1,2,3,4
),

o2p_engagement_events AS (
    SELECT
        event_date, event_name,
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
        END AS lead_class,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

o2p_engagement_total AS (
    SELECT
        c.wave_dt, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'view_promotion'      THEN c.clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN e.lead_class IN ('click_p','click_n')        THEN c.clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_p'                     THEN c.clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_n'                     THEN c.clnt_no END) AS leads_n
    FROM o2p_cohort c
    INNER JOIN o2p_engagement_events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY 1,2,3,4
),

o2p_base AS (
    SELECT
        p.wave_dt, p.rpt_grp_cd, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(e.view_users,  0) AS view_users,
        COALESCE(e.click_users, 0) AS click_users,
        COALESCE(e.leads_p,     0) AS leads_p,
        COALESCE(e.leads_n,     0) AS leads_n
    FROM o2p_population p
    LEFT JOIN o2p_engagement_total e
        ON  e.wave_dt           = p.wave_dt
        AND e.rpt_grp_cd        = p.rpt_grp_cd
        AND e.test_control_flag = p.test_control_flag
        AND e.cohort_arm        = p.cohort_arm
),

o2p_final AS (
    SELECT
        CAST('O2P' AS VARCHAR) AS campaign,
        wave_dt                        AS cohort,
        CAST('ALL'     AS VARCHAR)     AS segment,
        CAST('OVERALL' AS VARCHAR)     AS segment_level,
        test_control_flag, cohort_arm,
        SUM(total_population) AS total_population,
        SUM(view_users)       AS view_users,
        SUM(click_users)      AS click_users,
        SUM(leads_p)          AS leads_p,
        SUM(leads_n)          AS leads_n
    FROM o2p_base
    GROUP BY wave_dt, test_control_flag, cohort_arm

    UNION ALL

    SELECT
        CAST('O2P' AS VARCHAR) AS campaign,
        wave_dt        AS cohort,
        'REPORT_GROUP' AS segment,
        rpt_grp_cd     AS segment_level,
        test_control_flag, cohort_arm,
        total_population,
        view_users, click_users, leads_p, leads_n
    FROM o2p_base
)

SELECT * FROM pcd_final
UNION ALL
SELECT * FROM ctu_final
UNION ALL
SELECT * FROM o2p_final
ORDER BY campaign, cohort, segment, segment_level, test_control_flag, cohort_arm
;
