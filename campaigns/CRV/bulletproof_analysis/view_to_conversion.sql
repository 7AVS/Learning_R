-- view_to_conversion.sql
-- Purpose: PCL CONVERSION × MOBILE ENGAGEMENT cross-tab — for each lead, join conversion flag
--          to mobile-banner engagement state to trace how converters reached the offer.
-- Grain: dep_eng lead grain (clnt_no × treatmt_strt_dt × treatmt_end_dt × platform_label).
--        'platform_label' = COALESCE(platform, 'no_impression'): leads with zero captured
--        mobile GA4 events get 'no_impression' (not IOS/ANDROID).
-- Output: counts only — no rate columns. GROUP BY 4 dimensions; user computes rates.
--
-- CAVEATS (read before interpreting):
--   1. MOBILE ONLY. Engagement observed = GA4 mobile banner events (view_promotion /
--      select_promotion on PCL item IDs). Non-engagement ≠ no marketing touch — PCL also
--      runs email + branch/RM channels, which are not in GA4. 'Converted with no mobile
--      engagement' is a LOWER BOUND on non-mobile attribution, not organic conversion.
--   2. GA4 COVERAGE. Partition window Dec 2025–May 2026. PCL deployments that started
--      Dec 2025–Feb 2026 are covered; converters outside this window appear as no_pcl_view
--      for data-coverage reasons, not actual non-exposure.
--   3. DESCRIPTIVE / CONDITIONS ON OUTCOME. This analysis conditions on conversion
--      (responder_cli = 1) to look back at engagement. It is NOT causal — do not interpret
--      engagement → conversion as treatment effect. The CRV arm split (overlap_status) is
--      structural randomization, but this table does not exploit it causally.
--   4. PRE-APPROVED OFFER. PCL is a pre-approved limit increase; responder_cli ≈ accepted
--      the offer. No underwriting / denial stage between click and conversion. Fulfilment
--      (actual limit change applied) is a later stage tracked via dt_cl_change — add when needed.

WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01'
      AND action_control = 'Control'
),
pcl_universe AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli
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
        p.responder_cli,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt AND oa.treatmt_end_dt = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt AND oc.treatmt_end_dt = p.treatmt_end_dt
),
ga4_events AS (
    -- mobile-only: IOS/ANDROID (platform NULL rows handled by LEFT JOIN in dep_eng)
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
dep_eng AS (
    -- lead grain: clnt_no × treatmt_strt_dt × treatmt_end_dt × platform (NULL = no mobile impression)
    SELECT
        f.clnt_no, f.action_flag, f.control_flag,
        f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        f.responder_cli,
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
             f.responder_cli, g.platform
)
SELECT
    CASE WHEN action_flag  = 1 THEN 'overlap_action'
         WHEN control_flag = 1 THEN 'overlap_control'
         ELSE                       'no_overlap' END                          AS overlap_status,
    COALESCE(platform, 'no_impression')                                       AS platform_label,
    CASE WHEN pcl_click = 1            THEN 'clicked_pcl'
         WHEN pcl_view  = 1            THEN 'viewed_pcl_noclick'
         ELSE                               'no_pcl_view' END                 AS pcl_engagement_state,
    responder_cli,
    COUNT(*)                                                                   AS leads
FROM dep_eng
GROUP BY
    CASE WHEN action_flag  = 1 THEN 'overlap_action'
         WHEN control_flag = 1 THEN 'overlap_control'
         ELSE                       'no_overlap' END,
    COALESCE(platform, 'no_impression'),
    CASE WHEN pcl_click = 1 THEN 'clicked_pcl'
         WHEN pcl_view  = 1 THEN 'viewed_pcl_noclick'
         ELSE                    'no_pcl_view' END,
    responder_cli
ORDER BY overlap_status, platform_label, pcl_engagement_state, responder_cli
;
