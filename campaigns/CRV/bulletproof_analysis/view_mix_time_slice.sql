-- view_mix_time_slice.sql
-- Purpose: TIME-SLICED VIEW MIX — detect WHEN iOS mobile priority-list serving failure started.
-- For the CRV action arm, compute view-bucket mix (Both / PCL only / CRV only) per
-- deployment week × platform. A step-up in iOS Both% at a given cohort_week = timestamp of priority break.
--
-- NOTE: cohort window is PCL treatmt_strt_dt 2025-12-01 to 2026-02-28.
-- To see a step-change the window must span BEFORE and AFTER the suspected break.
-- If the break predates Dec 2025, widen the treatmt_strt_dt range in pcl_universe and ga4_events.

WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01' AND action_control = 'Control'
),
pcl_universe AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt BETWEEN DATE '2025-12-01' AND DATE '2026-02-28'
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
    -- mobile-only split: IOS/ANDROID; platform propagates downstream for OS-level grouping
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        platform,
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
    WHERE event_date >= DATE '2025-12-01'
      AND ((year = '2025' AND month IN ('12')) OR (year = '2026' AND month IN ('01','02','03','04','05')))
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                         'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
                         'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
      AND platform IN ('IOS','ANDROID')
),
-- engagement scored per deployment window (dep_eng grain: clnt_no × treatmt_strt_dt × treatmt_end_dt × platform)
dep_eng AS (
    SELECT
        f.clnt_no, f.action_flag, f.control_flag,
        f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        g.platform,
        COALESCE(MAX(g.crv_view_e),  0) AS crv_view,
        COALESCE(MAX(g.crv_click_e), 0) AS crv_click,
        COALESCE(MAX(g.pcl_view_e),  0) AS pcl_view,
        COALESCE(MAX(g.pcl_click_e), 0) AS pcl_click
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.action_flag, f.control_flag,
             f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
             g.platform
)
-- Time-sliced view mix: action arm only, non-NULL platform (impressions with OS attribution).
-- Stays at dep_eng grain so each deployment maps to its own cohort_week.
-- 'Neither' = deployment had a GA4 row (platform not NULL) but no view event for either banner.
SELECT
    date_trunc('week', treatmt_strt_dt)                                     AS cohort_week,
    platform,
    SUM(CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 1 ELSE 0 END)         AS viewed_both,
    SUM(CASE WHEN crv_view = 0 AND pcl_view = 1 THEN 1 ELSE 0 END)         AS viewed_pcl_only,
    SUM(CASE WHEN crv_view = 1 AND pcl_view = 0 THEN 1 ELSE 0 END)         AS viewed_crv_only,
    SUM(CASE WHEN crv_view = 1 AND pcl_view = 1 THEN 1
             WHEN crv_view = 0 AND pcl_view = 1 THEN 1
             WHEN crv_view = 1 AND pcl_view = 0 THEN 1
             ELSE 0 END)                                                     AS total_viewers
FROM dep_eng
WHERE action_flag = 1
  AND platform IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
;
