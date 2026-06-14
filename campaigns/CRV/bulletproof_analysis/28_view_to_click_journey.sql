-- 28_view_to_click_journey.sql
-- Purpose: VIEW → CLICK JOURNEY — of clients who saw X banner combination, what did they click?
-- Reference: s2_code_selection.md (channel_bulletproofing, FINAL 2026-06-12) governs all codes.
-- Grain: client (clnt_no), one row per (overlap_status × view_category).
-- Click columns (click_both/crv_only/pcl_only/neither) partition the view group: they sum to clients.

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
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
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
        CASE WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344')
              AND event_name = 'view_promotion'   THEN 1 ELSE 0 END AS crv_view_e,
        CASE WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344')
              AND event_name = 'select_promotion' THEN 1 ELSE 0 END AS crv_click_e,
        CASE WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
                                 'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
              AND event_name = 'view_promotion'   THEN 1 ELSE 0 END AS pcl_view_e,
        CASE WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
                                 'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
              AND event_name = 'select_promotion' THEN 1 ELSE 0 END AS pcl_click_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-02-01'
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                         'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
                         'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
),
-- engagement scored per deployment window first (anchors to real windows, no calendar box)
dep_eng AS (
    SELECT
        f.clnt_no, f.action_flag, f.control_flag,
        f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        COALESCE(MAX(g.crv_view_e),  0) AS crv_view,
        COALESCE(MAX(g.crv_click_e), 0) AS crv_click,
        COALESCE(MAX(g.pcl_view_e),  0) AS pcl_view,
        COALESCE(MAX(g.pcl_click_e), 0) AS pcl_click
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.action_flag, f.control_flag,
             f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt
),
-- roll deployments up to one row per client (action > control > no_overlap precedence)
client_roll AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END AS overlap_status,
        MAX(crv_view)  AS crv_view,
        MAX(crv_click) AS crv_click,
        MAX(pcl_view)  AS pcl_view,
        MAX(pcl_click) AS pcl_click
    FROM dep_eng
    GROUP BY clnt_no
)
SELECT
    overlap_status,
    CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 'Both'
         WHEN crv_view = 1 AND pcl_view = 0 THEN 'CRV only'
         WHEN crv_view = 0 AND pcl_view = 1 THEN 'PCL only'
         ELSE                                     'Neither' END AS view_category,
    COUNT(*)                                                                          AS clients,
    SUM(CASE WHEN crv_click = 1 AND pcl_click = 1 THEN 1 ELSE 0 END)                AS click_both,
    SUM(CASE WHEN crv_click = 1 AND pcl_click = 0 THEN 1 ELSE 0 END)                AS click_crv_only,
    SUM(CASE WHEN crv_click = 0 AND pcl_click = 1 THEN 1 ELSE 0 END)                AS click_pcl_only,
    SUM(CASE WHEN crv_click = 0 AND pcl_click = 0 THEN 1 ELSE 0 END)                AS click_neither
FROM client_roll
GROUP BY 1, 2
ORDER BY 1, 2
;
