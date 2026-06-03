-- async_banner_success_by_engagement.sql
-- Engine: Starburst (Trino). Federated: Teradata cohort/conversion tables + edl0_im GA4.
-- Purpose: split the FULL cohort by banner engagement, then measure conversion in each split.
--   engaged      = client had any GA4 view/click on the banner within their 60-day window
--   success      = client converted within their 60-day window (per-campaign conversion logic)
--   The number of interest is NOT_ENGAGED converters: clients who converted WITHOUT ever
--   touching the banner (the baseline that happens regardless of the creative).
-- Output: counts only (cohort_size, converters). Response rate = converters / cohort_size.
-- Grain: one row per client (flags MAX'd up), counted once per (test_control_flag, engaged).
-- CAVEAT: engaged is self-selected, NOT randomized -> this is correlational, not lift.
--   Control barely sees the banner, so the engaged/not split is a within-TEST diagnostic.
-- Blocks are independent (separate WITH ... SELECT ;) so you can run ONE campaign at a time
--   (Teradata-through-Trino is slow; skip the heavy CR_APP chain unless you need O2P).


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD   (success = responder_anyproduct; banner = it_item_name x4)  ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
pcd_cohort AS (
    SELECT
        clnt_no,
        response_start,
        responder_anyproduct,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
        END AS test_control_flag
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start >= DATE '2026-04-01'
),
pcd_ga4 AS (
    SELECT event_date, TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
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
pcd_client AS (
    SELECT
        c.clnt_no,
        c.test_control_flag,
        MAX(c.responder_anyproduct)                              AS success,
        MAX(CASE WHEN g.clnt_no IS NOT NULL THEN 1 ELSE 0 END)   AS engaged
    FROM pcd_cohort c
    LEFT JOIN pcd_ga4 g
        ON  g.clnt_no    = c.clnt_no
        AND g.event_date BETWEEN c.response_start AND date_add('day', 60, c.response_start)
    WHERE c.test_control_flag IS NOT NULL
    GROUP BY c.clnt_no, c.test_control_flag
)
SELECT
    CAST('PCD' AS VARCHAR) AS campaign,
    test_control_flag,
    CASE WHEN engaged = 1 THEN 'ENGAGED' ELSE 'NOT_ENGAGED' END AS engaged,
    COUNT(*)      AS cohort_size,
    SUM(success)  AS converters
FROM pcd_client
GROUP BY test_control_flag, engaged
ORDER BY test_control_flag, engaged
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU   (success = success column; banner = it_item_id 'i_300102')  ║
-- ║   No test/control arm coded for CTU -> test_control_flag = 'ALL'.           ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
ctu_cohort AS (
    SELECT clnt_no, treatmt_strt_dt, success
    FROM dl_mr_prod.nbo_pba_upgrade
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),
ctu_ga4 AS (
    SELECT event_date, TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
ctu_client AS (
    SELECT
        c.clnt_no,
        MAX(c.success)                                           AS success,
        MAX(CASE WHEN g.clnt_no IS NOT NULL THEN 1 ELSE 0 END)   AS engaged
    FROM ctu_cohort c
    LEFT JOIN ctu_ga4 g
        ON  g.clnt_no    = c.clnt_no
        AND g.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY c.clnt_no
)
SELECT
    CAST('CTU' AS VARCHAR) AS campaign,
    CAST('ALL' AS VARCHAR) AS test_control_flag,
    CASE WHEN engaged = 1 THEN 'ENGAGED' ELSE 'NOT_ENGAGED' END AS engaged,
    COUNT(*)      AS cohort_size,
    SUM(success)  AS converters
FROM ctu_client
GROUP BY engaged
ORDER BY engaged
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P   (success = CR_APP chain; banner = it_item_id 'i_298045')    ║
-- ║   GA4 + CR_APP aggregated SEPARATELY vs cohort to avoid a clicks x apps     ║
-- ║   cartesian per client.                                                     ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
o2p_cohort AS (
    SELECT DISTINCT
        clnt_no,
        treatmt_strt_dt,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),
o2p_apps AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN        AS a
    JOIN DDWV01.OVRL_CR_APP              AS b
        ON  b.cr_app_id  = a.cr_app_id AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN   AS c
        ON  c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no AND c.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD              AS d
        ON  d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no AND d.sys_src_id = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN ('32','37','45','47','51','56','62')
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),
o2p_ga4 AS (
    SELECT event_date, TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
o2p_succ AS (
    SELECT
        c.clnt_no, c.test_control_flag,
        MAX(CASE WHEN a.clnt_no IS NOT NULL THEN 1 ELSE 0 END) AS success
    FROM o2p_cohort c
    LEFT JOIN o2p_apps a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt  BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY c.clnt_no, c.test_control_flag
),
o2p_eng AS (
    SELECT
        c.clnt_no, c.test_control_flag,
        MAX(CASE WHEN g.clnt_no IS NOT NULL THEN 1 ELSE 0 END) AS engaged
    FROM o2p_cohort c
    LEFT JOIN o2p_ga4 g
        ON  g.clnt_no    = c.clnt_no
        AND g.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY c.clnt_no, c.test_control_flag
),
o2p_client AS (
    SELECT s.clnt_no, s.test_control_flag, s.success, e.engaged
    FROM o2p_succ s
    JOIN o2p_eng e ON e.clnt_no = s.clnt_no AND e.test_control_flag = s.test_control_flag
)
SELECT
    CAST('O2P' AS VARCHAR) AS campaign,
    test_control_flag,
    CASE WHEN engaged = 1 THEN 'ENGAGED' ELSE 'NOT_ENGAGED' END AS engaged,
    COUNT(*)      AS cohort_size,
    SUM(success)  AS converters
FROM o2p_client
GROUP BY test_control_flag, engaged
ORDER BY test_control_flag, engaged
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 4 — O2P_NONASYNC_MB  (same O2P cohort/CR_APP; banner = OLB creatives) ║
-- ║   engagement = lower(it_creative_id) IN ('od_olb','od_xolb').               ║
-- ║   FLAGGED: confirm it_creative_id column name + casing (see Block 3 note).  ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
o2pna_cohort AS (
    SELECT DISTINCT
        clnt_no,
        treatmt_strt_dt,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),
o2pna_apps AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN        AS a
    JOIN DDWV01.OVRL_CR_APP              AS b
        ON  b.cr_app_id  = a.cr_app_id AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN   AS c
        ON  c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no AND c.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD              AS d
        ON  d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no AND d.sys_src_id = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN ('32','37','45','47','51','56','62')
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),
o2pna_ga4 AS (
    SELECT event_date, TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_creative_id) IN ('od_olb','od_xolb')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
o2pna_succ AS (
    SELECT
        c.clnt_no, c.test_control_flag,
        MAX(CASE WHEN a.clnt_no IS NOT NULL THEN 1 ELSE 0 END) AS success
    FROM o2pna_cohort c
    LEFT JOIN o2pna_apps a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt  BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY c.clnt_no, c.test_control_flag
),
o2pna_eng AS (
    SELECT
        c.clnt_no, c.test_control_flag,
        MAX(CASE WHEN g.clnt_no IS NOT NULL THEN 1 ELSE 0 END) AS engaged
    FROM o2pna_cohort c
    LEFT JOIN o2pna_ga4 g
        ON  g.clnt_no    = c.clnt_no
        AND g.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY c.clnt_no, c.test_control_flag
),
o2pna_client AS (
    SELECT s.clnt_no, s.test_control_flag, s.success, e.engaged
    FROM o2pna_succ s
    JOIN o2pna_eng e ON e.clnt_no = s.clnt_no AND e.test_control_flag = s.test_control_flag
)
SELECT
    CAST('O2P_NONASYNC_MB' AS VARCHAR) AS campaign,
    test_control_flag,
    CASE WHEN engaged = 1 THEN 'ENGAGED' ELSE 'NOT_ENGAGED' END AS engaged,
    COUNT(*)      AS cohort_size,
    SUM(success)  AS converters
FROM o2pna_client
GROUP BY test_control_flag, engaged
ORDER BY test_control_flag, engaged
;
