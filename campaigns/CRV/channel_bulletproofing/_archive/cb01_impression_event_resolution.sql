-- cb01_impression_event_resolution.sql
--
-- PURPOSE: Resolve view_item vs view_promotion for PCL/CRV banners in GA4, then re-test
--   "equal PCL reach by arm" on whichever event actually fires. Q20 used view_item but its
--   own header flags this as uncertain vs view_promotion. This query settles it empirically.
-- UNIVERSE: Q20 universe exactly — PCL MB cohort treatmt_strt_dt Feb–Apr 2026,
--   offer_end_date >= DATE '2026-02-01' open-ended for CRV (kept for direct comparability
--   with Q20's logged population numbers: overlap_action 1,064,491 / control 55,155 / no_overlap 437,380).
-- COMPLEMENTS Q19: Q19 tests the same channel using it_item_name (creative strings, 0-row
--   PCL result documented); this query uses it_promotion_id (numeric IDs from Q20) and tests
--   both event types side by side. Not redundant — Q19 never ran and uses a different join key.
-- Trino/Starburst syntax. Counts only — no rate columns.

-- ============================================================
-- STATEMENT 1 — which impression event fires for PCL banners?
-- ============================================================
-- For the PCL it_promotion_id list, Feb–Apr 2026, show volume by event_name and month.
-- Tells us whether view_item or view_promotion (or both) actually populate for PCL banners.

SELECT
    year,
    month,
    event_name,
    COUNT(*)                                                           AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))               AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name IN ('view_promotion', 'view_item', 'select_promotion')
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
  )
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;

-- ============================================================
-- STATEMENT 2 — which impression event fires for CRV banners?
-- ============================================================
-- Same structure, CRV promotion IDs. Companion to Stmt 1.

SELECT
    year,
    month,
    event_name,
    COUNT(*)                                                           AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))               AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name IN ('view_promotion', 'view_item', 'select_promotion')
  AND it_promotion_id IN (
        '87348','87342','87343','87344'   -- CRV (note: Q20 header flags '87348' vs Excel '87340' — confirm)
  )
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;

-- ============================================================
-- STATEMENT 3 — PCL reach by arm: view_item vs view_promotion side by side
-- ============================================================
-- Q20-style universe CTEs reused verbatim (same windows, same overlap logic, same ID lists).
-- Per overlap_status: population + viewers under each event definition + overlap/union.
-- Run AFTER Stmts 1–2 to know which event to trust; this stmt shows whether the choice changes
-- the reach-parity finding (Action vs Control impressions equal?).

WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2026-02-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2026-02-01'
      AND action_control = 'Control'
),
pcl_universe AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli,
           date_trunc('month', treatmt_strt_dt) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30'
      AND channel LIKE '%MB%'
),
overlap_action_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_action c
      ON c.acct_no = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
),
overlap_control_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_control c
      ON c.acct_no = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
),
pcl_flagged AS (
    SELECT
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt,
        p.responder_cli, p.pcl_month,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt AND oa.treatmt_end_dt = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt AND oc.treatmt_end_dt = p.treatmt_end_dt
),
ga4_pcl_events AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT)                           AS clnt_no,
        event_date,
        CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END   AS vi_flag,
        CASE WHEN event_name = 'view_promotion'   THEN 1 ELSE 0 END   AS vp_flag
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_item', 'view_promotion')
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
      )
),
dep_eng AS (
    SELECT
        f.clnt_no, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        f.responder_cli, f.pcl_month, f.action_flag, f.control_flag,
        COALESCE(MAX(g.vi_flag), 0) AS saw_view_item,
        COALESCE(MAX(g.vp_flag), 0) AS saw_view_promotion
    FROM pcl_flagged f
    LEFT JOIN ga4_pcl_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
             f.responder_cli, f.pcl_month, f.action_flag, f.control_flag
),
client_roll AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END          AS overlap_status,
        MAX(saw_view_item)       AS vi,
        MAX(saw_view_promotion)  AS vp
    FROM dep_eng
    GROUP BY clnt_no
)
SELECT
    overlap_status,
    COUNT(*)                                                           AS population,
    SUM(vi)                                                            AS viewers_view_item,
    SUM(vp)                                                            AS viewers_view_promotion,
    SUM(CASE WHEN vi = 1 OR  vp = 1 THEN 1 ELSE 0 END)                AS viewers_either,
    SUM(CASE WHEN vi = 1 AND vp = 1 THEN 1 ELSE 0 END)                AS viewers_both_events
FROM client_roll
GROUP BY 1
ORDER BY 1
;
