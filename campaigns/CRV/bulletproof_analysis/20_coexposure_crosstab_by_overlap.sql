-- 20_coexposure_crosstab_by_overlap.sql
--
-- ONE deployment cohort: PCL deployments that STARTED Feb-Apr 2026 (treatmt_strt_dt), each running
-- its ~90-day life. The three groups (overlap_action / overlap_control / no_overlap) are ALL this
-- SAME cohort, split only by whether a CRV offer was CONCURRENT with the PCL deployment window.
--
-- OUTPUT shaped like pic 20260609_115156 — LONG/STACKED: rows are (pcl_month, metric, overlap_status,
-- category) where metric = VIEW or CLICK and category = Both / CRV only / PCL only / Neither (mutually
-- exclusive). Pivot in Excel: category down the rows, overlap_status across, one table per metric ->
-- the VIEWS table and the CLICKS table. The 4 categories in a group sum to that group's population,
-- so % = counts / (group total). Compare overlap_action vs overlap_control (= H1 contrast).
--
-- Engagement counted AFTER each deployment, inside its OWN window (event_date BETWEEN
-- treatmt_strt_dt AND treatmt_end_dt). Grain = PCL deployment. Banner key = it_promotion_id (Excel
-- Id). Table = _reduced. Join up_srf_id2_value = CLNT_NO (cast GA4 side only). Counts only.
--
-- FLAGS: (1) CRV id list uses '87348' — Excel showed '87340'; confirm. (2) impression event =
-- 'view_item' (your query) vs 'view_promotion' (reference). (3) CENSORING: GA4 runs to ~2026-06, so
-- Apr (and late-Mar) deployments' 90-day windows are RIGHT-CENSORED; Feb is complete.

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
        CASE WHEN oa.acct_no IS NOT NULL THEN 'overlap_action'
             WHEN oc.acct_no IS NOT NULL THEN 'overlap_control'
             ELSE                             'no_overlap' END AS overlap_status
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
-- one row per PCL deployment; engagement counted only inside its own window
dep_eng AS (
    SELECT
        f.pcl_month, f.overlap_status, f.responder_cli, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        COALESCE(MAX(g.crv_click_e), 0) AS crv_click,
        COALESCE(MAX(g.crv_view_e),  0) AS crv_view,
        COALESCE(MAX(g.pcl_click_e), 0) AS pcl_click,
        COALESCE(MAX(g.pcl_view_e),  0) AS pcl_view
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.pcl_month, f.overlap_status, f.responder_cli, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt
)
-- LONG / STACKED output (shape of pic 115156): a VIEWS block and a CLICKS block, each with the
-- four categories per group. Pivot: rows = category, columns = overlap_status, one table per metric.
-- % within a group = counts / (sum of the group's 4 category counts) = counts / population.
SELECT
    pcl_month,
    'VIEW'         AS metric,
    overlap_status,
    CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 'Both'
         WHEN crv_view = 1 AND pcl_view = 0 THEN 'CRV only'
         WHEN crv_view = 0 AND pcl_view = 1 THEN 'PCL only'
         ELSE 'Neither' END AS category,
    COUNT(*)           AS counts,
    SUM(responder_cli) AS converters
FROM dep_eng
GROUP BY 1, 3, 4
UNION ALL
SELECT
    pcl_month,
    'CLICK'        AS metric,
    overlap_status,
    CASE WHEN crv_click = 1 AND pcl_click = 1 THEN 'Both'
         WHEN crv_click = 1 AND pcl_click = 0 THEN 'CRV only'
         WHEN crv_click = 0 AND pcl_click = 1 THEN 'PCL only'
         ELSE 'Neither' END AS category,
    COUNT(*)           AS counts,
    SUM(responder_cli) AS converters
FROM dep_eng
GROUP BY 1, 3, 4
ORDER BY pcl_month, metric, overlap_status, category
;
