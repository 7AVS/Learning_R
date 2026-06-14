-- 21_coexposure_crosstab_by_month.sql
--
-- Q20's co-exposure cross-tab, OPENED UP BY MONTH, back to GA4 history start (Feb 2025).
--
-- THE "IT GOES OVER MONTHS" FIX: a client's PCL + CRV span multiple months, so bucketing by
-- client+month double-counts. So the unit here is the PCL DEPLOYMENT (acct_no + treatmt_strt_dt),
-- bucketed by the month it fired (pcl_month) — exactly Q18/Q19. A client with 2 deployments = 2
-- rows in 2 months, each scored inside ITS OWN window. Nothing overlaps itself.
--
-- Per deployment: did the client VIEW / CLICK the CRV banners and the PCL banners WITHIN that
-- deployment's window (binary), then cross-tab Both / CRV only / PCL only / Neither, split by arm.
-- Population = every PCL deployment in the month (LEFT JOIN GA4 => non-engagers = Neither), so the
-- counts sum to the full deployment population per month per arm.
--
-- Table = ..._reduced (Feb-2025+ history; the full table retains only ~2 weeks).
-- Join up_srf_id2_value = CLNT_NO (cast GA4 side only). Starburst/Trino. Counts only.
-- Conventions per s2_code_selection.md (channel_bulletproofing, FINAL 2026-06-12): identity key = it_item_id ('i_'+offer id, format-stable all platforms, supersedes it_promotion_id which is float-formatted on Android); impression = view_promotion (view_item = discarded co-fired twin artifact); ID allowlist updated (87340 not 87348; +4 PCL ids). Android volume now included.

WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
pcl_universe AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt,
           date_trunc('month', treatmt_strt_dt) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01'
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.pcl_month,
        CASE WHEN oa.acct_no IS NOT NULL              THEN 'overlap_action'
             WHEN oc.acct_no IS NOT NULL              THEN 'overlap_control'
             ELSE                                          'no_overlap' END AS overlap_status
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
        CASE WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344')
              AND event_name = 'select_promotion'  THEN 1 ELSE 0 END AS crv_click_e,
        CASE WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344')
              AND event_name = 'view_promotion'    THEN 1 ELSE 0 END AS crv_view_e,
        CASE WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
              AND event_name = 'select_promotion'  THEN 1 ELSE 0 END AS pcl_click_e,
        CASE WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
              AND event_name = 'view_promotion'    THEN 1 ELSE 0 END AS pcl_view_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2025-02-01'
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                         'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
),
-- one row per PCL deployment, engagement counted only inside that deployment's window
dep_eng AS (
    SELECT
        f.pcl_month, f.overlap_status, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        COALESCE(MAX(g.crv_click_e), 0) AS crv_click,
        COALESCE(MAX(g.crv_view_e),  0) AS crv_view,
        COALESCE(MAX(g.pcl_click_e), 0) AS pcl_click,
        COALESCE(MAX(g.pcl_view_e),  0) AS pcl_view
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.pcl_month, f.overlap_status, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt
)
SELECT
    pcl_month,
    overlap_status,
    CASE WHEN crv_click = 1 AND pcl_click = 1 THEN 'Both'
         WHEN crv_click = 1 AND pcl_click = 0 THEN 'CRV only'
         WHEN crv_click = 0 AND pcl_click = 1 THEN 'PCL only'
         ELSE 'Neither' END AS click_category,
    CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 'Both'
         WHEN crv_view = 1 AND pcl_view = 0 THEN 'CRV only'
         WHEN crv_view = 0 AND pcl_view = 1 THEN 'PCL only'
         ELSE 'Neither' END AS view_category,
    COUNT(*) AS counts
FROM dep_eng
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
;
