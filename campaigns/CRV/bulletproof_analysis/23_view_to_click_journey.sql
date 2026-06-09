-- 23_view_to_click_journey.sql
--
-- The VIEW -> CLICK journey. Grain = CLIENT (same roll-up as Q20). For each group and each VIEWING
-- pattern, what did they CLICK? This is where the click-diversion shows: among clients who saw BOTH
-- banners, does the click go to CRV instead of PCL?
--
-- OUTPUT (wide): one row per (overlap_status x view_category). clients_in_view_group = clients with
-- that viewing pattern (the denominator), then the CLICK breakdown (click_both / click_crv_only /
-- click_pcl_only / click_neither), which sums to clients_in_view_group. % = click_col / that total.
-- A click requires a view, so e.g. a 'CRV only' viewer can only click CRV or nothing — built-in check.
-- The key row: overlap_action / view_category='Both' -> of co-exposed clients, where the click went.
--
-- CAUSAL NOTE: this conditions on VIEW (a post-treatment outcome), so it is DESCRIPTIVE, not the
-- clean randomised contrast. Use it to understand the mechanism, not to size the effect.
--
-- Cohort = PCL deployments started Feb-Apr 2026; engagement in each deployment window, rolled to the
-- client. Banner key = it_promotion_id. Table = _reduced. Counts only.
-- FLAGS: '87348' (Excel showed 87340); impression event = 'view_item'; Apr right-censored.

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
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli,
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
        CASE WHEN it_promotion_id IN ('87348','87342','87343','87344')
              AND event_name = 'select_promotion' THEN 1 ELSE 0 END AS crv_click_e,
        CASE WHEN it_promotion_id IN ('87348','87342','87343','87344')
              AND event_name = 'view_item'        THEN 1 ELSE 0 END AS crv_view_e,
        CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
              AND event_name = 'select_promotion' THEN 1 ELSE 0 END AS pcl_click_e,
        CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
              AND event_name = 'view_item'        THEN 1 ELSE 0 END AS pcl_view_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-02-01'
      AND it_promotion_id IN ('87348','87342','87343','87344',
                              '156764','156788','162326','289661','289662','289664','289665','289666')
),
dep_eng AS (
    SELECT
        f.clnt_no, f.action_flag, f.control_flag, f.responder_cli,
        f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        COALESCE(MAX(g.crv_click_e), 0) AS crv_click,
        COALESCE(MAX(g.crv_view_e),  0) AS crv_view,
        COALESCE(MAX(g.pcl_click_e), 0) AS pcl_click,
        COALESCE(MAX(g.pcl_view_e),  0) AS pcl_view
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.action_flag, f.control_flag, f.responder_cli,
             f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt
),
client_roll AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END AS overlap_status,
        MAX(crv_click) AS crv_click,
        MAX(crv_view)  AS crv_view,
        MAX(pcl_click) AS pcl_click,
        MAX(pcl_view)  AS pcl_view
    FROM dep_eng
    GROUP BY clnt_no
)
SELECT
    overlap_status,
    CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 'Both'
         WHEN crv_view = 1 AND pcl_view = 0 THEN 'CRV only'
         WHEN crv_view = 0 AND pcl_view = 1 THEN 'PCL only'
         ELSE 'Neither' END AS view_category,
    COUNT(*) AS clients_in_view_group,   -- denominator: clients with this viewing pattern
    -- what those viewers CLICKED (sums to clients_in_view_group)
    SUM(CASE WHEN crv_click = 1 AND pcl_click = 1 THEN 1 ELSE 0 END) AS click_both,
    SUM(CASE WHEN crv_click = 1 AND pcl_click = 0 THEN 1 ELSE 0 END) AS click_crv_only,
    SUM(CASE WHEN crv_click = 0 AND pcl_click = 1 THEN 1 ELSE 0 END) AS click_pcl_only,
    SUM(CASE WHEN crv_click = 0 AND pcl_click = 0 THEN 1 ELSE 0 END) AS click_neither
FROM client_roll
GROUP BY 1, 2
ORDER BY 1, 2
;
