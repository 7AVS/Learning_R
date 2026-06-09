-- 20_coexposure_crosstab_by_overlap.sql
--
-- Andre's co-exposure cross-tab (pic 20260609_111759) + the requested OVERLAP STATUS column,
-- with the denominator expanded to the ENTIRE PCL population for the window.
--
-- His query: per GA4 user, did they VIEW / CLICK the CRV banners and the PCL banners (matched on
-- it_promotion_id = the digital-team Excel Id), then cross-tab Both / CRV only / PCL only / Neither.
-- ADDED: overlap_status (overlap_action / overlap_control / no_overlap) from curated, and the
-- population is now ALL PCL clients deployed in the window. LEFT JOIN GA4 => non-engagers = Neither,
-- so counts sum to the full population per arm, not just banner-touchers.
--
-- Banner ids = exact it_promotion_id from the Excel, verbatim from Andre's query.
-- ONE change to his core: GA4 user key user_id -> up_srf_id2_value, because the join to curated is
-- on up_srf_id2_value = CLNT_NO (the documented key). Cast GA4 side only (federation ROUND gotcha).
-- Starburst/Trino. Counts only.
--
-- FLAGS — please confirm:
--   1. CRV id list uses '87348'; the Excel (pic 185634) showed '87340' for CC-Instalments-INT_OTF.
--      Kept your 87348 verbatim — confirm it's intentional, not a typo for 87340.
--   2. FROM uses the FULL table tsz_00198_data_ga4_ecommerce. Earlier probe: the full table retains
--      only ~2 weeks (from ~2026-05-27), so this Mar-May window may come back NEAR-EMPTY. If it does,
--      switch FROM to ..._ga4_ecommerce_reduced (it has it_promotion_id + Feb-2025 history).
--   3. Impression event = 'view_item' (your query). We used 'view_promotion' before for impressions.
--      If view_category is all 'Neither'/'PCL only' oddly, view_item may be the wrong event here.

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
pli AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt BETWEEN DATE '2026-03-01' AND DATE '2026-05-30'
      AND channel LIKE '%MB%'
),
pli_flagged AS (
    SELECT p.clnt_no,
           MAX(CASE WHEN a.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_action,
           MAX(CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_control
    FROM pli p
    LEFT JOIN crv_action  a ON a.acct_no = p.acct_no
                           AND a.offer_start_date <= p.treatmt_end_dt AND a.offer_end_date >= p.treatmt_strt_dt
    LEFT JOIN crv_control c ON c.acct_no = p.acct_no
                           AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.clnt_no
),
-- the ENTIRE PCL population for the window, tagged by arm
roster AS (
    SELECT clnt_no,
           CASE WHEN any_action  = 1 THEN 'overlap_action'
                WHEN any_control = 1 THEN 'overlap_control'
                ELSE                      'no_overlap' END AS overlap_status
    FROM pli_flagged
),
-- Andre's engagement subquery, keyed on up_srf_id2_value (= CLNT_NO) instead of user_id
ga4_eng AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        MAX(CASE WHEN it_promotion_id IN ('87348','87342','87343','87344')
                  AND event_name = 'select_promotion' THEN 1 ELSE 0 END) AS crv_click,
        MAX(CASE WHEN it_promotion_id IN ('87348','87342','87343','87344')
                  AND event_name = 'view_item'        THEN 1 ELSE 0 END) AS crv_view,
        MAX(CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
                  AND event_name = 'select_promotion' THEN 1 ELSE 0 END) AS pcl_click,
        MAX(CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
                  AND event_name = 'view_item'        THEN 1 ELSE 0 END) AS pcl_view
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE it_promotion_id IN ('87348','87342','87343','87344',
                              '156764','156788','162326','289661','289662','289664','289665','289666')
      AND event_date BETWEEN DATE '2026-03-01' AND DATE '2026-05-30'
    GROUP BY TRY_CAST(up_srf_id2_value AS BIGINT)
)
SELECT
    r.overlap_status,
    CASE WHEN COALESCE(g.crv_click,0) = 1 AND COALESCE(g.pcl_click,0) = 1 THEN 'Both'
         WHEN COALESCE(g.crv_click,0) = 1 AND COALESCE(g.pcl_click,0) = 0 THEN 'CRV only'
         WHEN COALESCE(g.crv_click,0) = 0 AND COALESCE(g.pcl_click,0) = 1 THEN 'PCL only'
         ELSE 'Neither' END AS click_category,
    CASE WHEN COALESCE(g.crv_view,0) = 1 AND COALESCE(g.pcl_view,0) = 1 THEN 'Both'
         WHEN COALESCE(g.crv_view,0) = 1 AND COALESCE(g.pcl_view,0) = 0 THEN 'CRV only'
         WHEN COALESCE(g.crv_view,0) = 0 AND COALESCE(g.pcl_view,0) = 1 THEN 'PCL only'
         ELSE 'Neither' END AS view_category,
    COUNT(*) AS counts
FROM roster r
LEFT JOIN ga4_eng g ON g.clnt_no = r.clnt_no
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
