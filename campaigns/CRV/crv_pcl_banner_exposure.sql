-- =============================================================================
-- CRV x PCL banner EXPOSURE — Stage 1: PLI impressions per client, by CRV arm.
-- =============================================================================
-- The impression-level twin of Q18's click gap, but from the GA4 SOURCE (not the
-- curated clicked_mb), and randomised (CRV Action vs Control). Question: does CRV's
-- banner crowd PLI off the mobile surface? If overlap_action gets FEWER PLI impressions
-- per client than overlap_control, CRV is eating PLI's exposure.
--
-- ARCHITECTURE: GA4 `_reduced` (Feb-2025+, the historical table) = the engagement;
--   curated PLI/CRV tables = the overlap roster + CRV arm. Joined on
--   CLNT_NO (curated) = up_srf_id2_value (GA4). One Starburst/Trino query (federates to
--   dl_mr_prod, same pattern as the async trackers). NO QUALIFY, counts only.
--
-- GROUPS (same as Q18, collapsed to client grain): overlap_action / overlap_control /
--   no_overlap. Clean contrast = action vs control. Compute impressions-per-client in Excel.
--
-- EDIT before final run: replace the banner LIKE blocks with the locked it_item_id / name
--   IN-lists from the clean Q1 (crv_pcl_banner_lifecycle_check.sql). Confirm `platform`
--   scoping (mobile = IOS/ANDROID) against the lifecycle's per-banner platform split.
-- =============================================================================

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
-- PLI mobile leads in the GA4 window (Feb-2025+), with clnt_no for the GA4 join
pli AS (
    SELECT CAST(clnt_no AS BIGINT) AS clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
),
-- collapse PLI leads to CLIENT grain; flag if ANY lead overlapped CRV-Action / -Control
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
roster AS (
    SELECT clnt_no,
           CASE WHEN any_action  = 1 THEN 'overlap_action'
                WHEN any_control = 1 THEN 'overlap_control'
                ELSE                      'no_overlap' END AS grp
    FROM pli_flagged
),
-- GA4 banner impressions/clicks per client (Feb-2025+, mobile), split PLI vs CRV banner
ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
           SUM(CASE WHEN lower(event_name) = 'view_promotion'
                     AND lower(it_item_name) NOT LIKE '%cc-instalments%' THEN 1 ELSE 0 END) AS pli_impr,
           SUM(CASE WHEN lower(event_name) = 'select_promotion'
                     AND lower(it_item_name) NOT LIKE '%cc-instalments%' THEN 1 ELSE 0 END) AS pli_clk,
           SUM(CASE WHEN lower(event_name) = 'view_promotion'
                     AND lower(it_item_name) LIKE '%cc-instalments%' THEN 1 ELSE 0 END) AS crv_impr,
           SUM(CASE WHEN lower(event_name) = 'select_promotion'
                     AND lower(it_item_name) LIKE '%cc-instalments%' THEN 1 ELSE 0 END) AS crv_clk
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year IN ('2025', '2026')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
      AND platform IN ('IOS', 'ANDROID')                      -- mobile (EDIT if surface differs)
      AND (                                                    -- restrict scan to our banners
            lower(it_item_name) LIKE '%cc-instalments%'        -- CRV
         OR (                                                   -- PLI (VCL) card limit increase
                ( lower(it_item_name) LIKE '%vcl%'
               OR lower(it_item_name) LIKE '%pcl%'
               OR lower(it_item_name) LIKE '%limit%increase%' )
            AND lower(it_item_name) NOT LIKE '%ln_rcl%'         -- drop loan/line-of-credit
            AND lower(it_item_name) NOT LIKE '%dgt_ln%'
            AND lower(it_item_name) NOT LIKE '%cheq%'           -- drop chequing
            AND lower(it_item_name) NOT LIKE '%pcd_ccpij%'      -- drop PCD draft
         )
          )
    GROUP BY TRY_CAST(up_srf_id2_value AS BIGINT)
)
SELECT
    r.grp,
    COUNT(DISTINCT r.clnt_no)                   AS n_clients,             -- roster size
    COUNT(DISTINCT g.clnt_no)                   AS n_clients_with_banner, -- matched in GA4
    SUM(COALESCE(g.pli_impr, 0))                AS pli_impressions,
    SUM(COALESCE(g.pli_clk,  0))                AS pli_clicks,
    SUM(COALESCE(g.crv_impr, 0))                AS crv_impressions,
    SUM(COALESCE(g.crv_clk,  0))                AS crv_clicks
FROM roster r
LEFT JOIN ga4 g ON g.clnt_no = r.clnt_no
GROUP BY r.grp
ORDER BY r.grp
;
