-- 20_coexposure_crosstab_by_overlap.sql
--
-- GRAIN = CLIENT (clnt_no), one row per client. Built from the PCL deployment cohort that STARTED
-- Feb-Apr 2026, then rolled up to the client:
--   * overlap_status: action if ANY of the client's deployments overlapped a CRV Action offer,
--     else control if any overlapped Control, else no_overlap (precedence action > control > none).
--   * converted: the client converted on ANY of their deployments (responder_cli).
--   * engaged: the client VIEWED / CLICKED a banner during ANY of their deployment windows
--     (engagement scored per-deployment-window first, then MAX-rolled to the client — so it stays
--     anchored to real deployment windows, no calendar box, no double-count of the client).
--   * pcl_month: the client's FIRST deployment month (so each client sits in one month, no spill).
--
-- OUTPUT (wide): one row per (pcl_month x overlap_status). population = unique CLIENTS (denominator),
-- then the VIEWS breakdown (view_both/view_crv_only/view_pcl_only/view_neither) and the CLICKS
-- breakdown (click_*), each summing to population, plus converters. % = any column / population.
-- Compare overlap_action vs overlap_control (= H1 contrast).
--
-- Banner key = it_promotion_id (Excel Id). Table = _reduced. Join up_srf_id2_value = CLNT_NO
-- (cast GA4 side only). Counts only.
-- FLAGS [RESOLVED]: (1) CRV id '87348' was a typo — correct id is '87340' (confirmed 2026-06-12).
-- (2) impression event = 'view_item' vs 'view_promotion' — RESOLVED: view_item is a co-fired twin
--     artifact (fires 1:1 with view_promotion, carries no creative/location fields); view_item
--     discarded, view_promotion is the correct impression event. (3) CENSORING: Apr/late-Mar
--     deployments' 90-day windows run past GA4's ~2026-06 data; Feb is complete.
-- GA4 events per s2_code_selection.md (channel_bulletproofing, FINAL 2026-06-12):
--   impression = view_promotion (view_item = co-fired twin artifact, discarded);
--   ID allowlist updated 2026-06-12 (new ids inactive in Feb-Apr -- zero effect this window).
-- Android coverage RESTORED via numeric-id cast (prior runs = iOS-only; rerun needed)
-- 2026-06-12: promotion-id matching switched to numeric cast (Android stores ids as '87342.0' float strings; string IN-lists excluded Android)

WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2026-02-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2026-02-01' AND action_control = 'Control'
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli, p.pcl_month,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt AND oa.treatmt_end_dt = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt AND oc.treatmt_end_dt = p.treatmt_end_dt
),
ga4_events AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (87340,87342,87343,87344)
              AND event_name = 'select_promotion'  THEN 1 ELSE 0 END AS crv_click_e,
        CASE WHEN TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (87340,87342,87343,87344)
              AND event_name = 'view_promotion'    THEN 1 ELSE 0 END AS crv_view_e,
        CASE WHEN TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (156764,156788,162326,167715,167716,167717,289661,289662,289664,289665,289666,289698)
              AND event_name = 'select_promotion'  THEN 1 ELSE 0 END AS pcl_click_e,
        CASE WHEN TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (156764,156788,162326,167715,167716,167717,289661,289662,289664,289665,289666,289698)
              AND event_name = 'view_promotion'    THEN 1 ELSE 0 END AS pcl_view_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-02-01'
      AND TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (87340,87342,87343,87344,
                              156764,156788,162326,167715,167716,167717,289661,289662,289664,289665,289666,289698)
),
-- engagement per deployment window first (keeps it anchored to real windows)
dep_eng AS (
    SELECT
        f.clnt_no, f.pcl_month, f.action_flag, f.control_flag, f.responder_cli,
        f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        COALESCE(MAX(g.crv_click_e), 0) AS crv_click,
        COALESCE(MAX(g.crv_view_e),  0) AS crv_view,
        COALESCE(MAX(g.pcl_click_e), 0) AS pcl_click,
        COALESCE(MAX(g.pcl_view_e),  0) AS pcl_view
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.pcl_month, f.action_flag, f.control_flag, f.responder_cli,
             f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt
),
-- roll deployments up to ONE row per client
client_roll AS (
    SELECT
        clnt_no,
        MIN(pcl_month) AS pcl_month,                         -- client's first deployment month
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END AS overlap_status,
        MAX(responder_cli) AS responded,
        MAX(crv_click)     AS crv_click,
        MAX(crv_view)      AS crv_view,
        MAX(pcl_click)     AS pcl_click,
        MAX(pcl_view)      AS pcl_view
    FROM dep_eng
    GROUP BY clnt_no
)
SELECT
    pcl_month,
    overlap_status,
    COUNT(*) AS population,    -- unique clients in this month+group  <- denominator; % = column/population
    -- VIEWS breakdown (mutually exclusive, sums to population)
    SUM(CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 1 ELSE 0 END) AS view_both,
    SUM(CASE WHEN crv_view = 1 AND pcl_view = 0 THEN 1 ELSE 0 END) AS view_crv_only,
    SUM(CASE WHEN crv_view = 0 AND pcl_view = 1 THEN 1 ELSE 0 END) AS view_pcl_only,
    SUM(CASE WHEN crv_view = 0 AND pcl_view = 0 THEN 1 ELSE 0 END) AS view_neither,
    -- CLICKS breakdown (mutually exclusive, sums to population)
    SUM(CASE WHEN crv_click = 1 AND pcl_click = 1 THEN 1 ELSE 0 END) AS click_both,
    SUM(CASE WHEN crv_click = 1 AND pcl_click = 0 THEN 1 ELSE 0 END) AS click_crv_only,
    SUM(CASE WHEN crv_click = 0 AND pcl_click = 1 THEN 1 ELSE 0 END) AS click_pcl_only,
    SUM(CASE WHEN crv_click = 0 AND pcl_click = 0 THEN 1 ELSE 0 END) AS click_neither,
    SUM(responded) AS converters
FROM client_roll
GROUP BY 1, 2
ORDER BY 1, 2
;
