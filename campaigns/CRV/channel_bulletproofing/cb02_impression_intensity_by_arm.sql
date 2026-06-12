-- cb02_impression_intensity_by_arm.sql
--
-- CONDITIONAL — run only if CB01 confirms the event AND Q24 Stmt 2 shows asymmetry.
--   (If CB01 shows view_item is the event, set IMPRESSION_EVENT = 'view_item' below.)
-- PURPOSE: rule out CRV cutting PCL impression FREQUENCY, not just any-reach.
--   Q20 re-tests reach parity (any impression ≥ 1); this query asks whether the distribution
--   of impression-days is also balanced. If Action clients see fewer PCL impression-days
--   than Control, slot competition (not attention) may still be in play at the intensive margin.
-- UNIVERSE: Q20 exactly — PCL MB Feb–Apr 2026, offer_end_date >= DATE '2026-02-01' open-ended.
-- Trino/Starburst syntax. Counts only — no rate columns.

-- ============================================================
-- PARAMETER — set per CB01 result
-- ============================================================
-- Default: 'view_promotion'. Change to 'view_item' if CB01 shows view_item is the live event.
-- Single edit point; referenced in the ga4_pcl_imp CTE WHERE clause below.

WITH
-- IMPRESSION_EVENT parameter: 'view_promotion' or 'view_item' — set per CB01 result
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
-- PCL impression events — EDIT event_name value here to match CB01 result (default: 'view_promotion')
ga4_pcl_imp AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT)   AS clnt_no,
        event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name = 'view_promotion'   -- PARAMETER: set to 'view_item' if CB01 shows that event
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
      )
),
-- PCL click events (select_promotion regardless of impression event)
ga4_pcl_clk AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT)   AS clnt_no,
        event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name = 'select_promotion'
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
      )
),
-- per-deployment: distinct impression-days and any-click in window
dep_eng AS (
    SELECT
        f.clnt_no, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        f.responder_cli, f.pcl_month, f.action_flag, f.control_flag,
        COUNT(DISTINCT gi.event_date)          AS pcl_impression_days,
        COALESCE(MAX(CASE WHEN gc.event_date IS NOT NULL THEN 1 ELSE 0 END), 0) AS pcl_clicked
    FROM pcl_flagged f
    LEFT JOIN ga4_pcl_imp gi
      ON gi.clnt_no    = f.clnt_no
     AND gi.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    LEFT JOIN ga4_pcl_clk gc
      ON gc.clnt_no    = f.clnt_no
     AND gc.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
             f.responder_cli, f.pcl_month, f.action_flag, f.control_flag
),
-- roll to client; use MAX impression-days across deployments (single-month cohort: most clients = 1 dep)
client_roll AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END          AS overlap_status,
        MAX(pcl_impression_days)   AS imp_days,
        MAX(pcl_clicked)           AS clicked,
        MAX(responder_cli)         AS converted
    FROM dep_eng
    GROUP BY clnt_no
)
SELECT
    overlap_status,
    CASE WHEN imp_days = 0          THEN '0'
         WHEN imp_days = 1          THEN '1'
         WHEN imp_days = 2          THEN '2'
         WHEN imp_days = 3          THEN '3'
         WHEN imp_days = 4          THEN '4'
         ELSE                            '5+' END                      AS imp_day_bucket,
    COUNT(*)                                                           AS clients,
    SUM(clicked)                                                       AS click_users,
    SUM(converted)                                                     AS converters
FROM client_roll
GROUP BY 1, 2
ORDER BY 1, 2
;
