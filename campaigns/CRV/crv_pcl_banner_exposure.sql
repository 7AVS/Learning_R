-- =============================================================================
-- CRV x PCL banner EXPOSURE — (1) banner-selection STRESS TEST, (2) MONTHLY by arm.
-- =============================================================================
-- Goal: does CRV's banner crowd PLI off the mobile surface? Impression-level twin of
-- Q18's click gap, from the GA4 SOURCE, randomised (CRV Action vs Control).
-- GA4 `_reduced` (Feb-2025+) = engagement; curated PLI/CRV = overlap roster + arm.
-- Join CLNT_NO (curated) = up_srf_id2_value (GA4). Starburst/Trino. Counts only.
--
-- RUN #1 FIRST — validate WHICH banners the filter calls CRV vs PLI before trusting #2.
-- EDIT before final: swap the banner LIKE blocks for the locked it_item_id IN-list once
-- #1 confirms the set; confirm platform scoping (mobile = IOS/ANDROID).
-- =============================================================================

-- ── #1 STRESS TEST: which it_item_names get tagged CRV / PLI / DROP / OTHER? ──────────
-- Eyeball: are the CRV rows really installments, the PLI rows really CC limit-increase,
-- the loan/cheq correctly dropped, and is anything important sitting in OTHER (= missed)?
SELECT
    CASE
        WHEN lower(it_item_name) LIKE '%cc-instalments%'                                THEN 'CRV'
        WHEN lower(it_item_name) LIKE '%ln_rcl%' OR lower(it_item_name) LIKE '%dgt_ln%' THEN 'DROP_loan'
        WHEN lower(it_item_name) LIKE '%cheq%'                                          THEN 'DROP_cheq'
        WHEN lower(it_item_name) LIKE '%pcd_ccpij%'                                     THEN 'DROP_pcd'
        WHEN ( lower(it_item_name) LIKE '%vcl%' OR lower(it_item_name) LIKE '%pcl%'
            OR lower(it_item_name) LIKE '%limit%increase%' )                            THEN 'PLI'
        ELSE 'OTHER'
    END                 AS family,
    lower(it_item_name) AS item_name,
    MIN(event_date)     AS first_seen,
    MAX(event_date)     AS last_seen,
    COUNT(*)            AS n_impressions
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2025', '2026')
  AND lower(event_name) = 'view_promotion'
  AND platform IN ('IOS', 'ANDROID')
  AND ( lower(it_item_name) LIKE '%vcl%'   OR lower(it_item_name) LIKE '%pcl%'
     OR lower(it_item_name) LIKE '%limit%increase%' OR lower(it_item_name) LIKE '%cc-instalments%'
     OR lower(it_item_name) LIKE '%ln_rcl%' OR lower(it_item_name) LIKE '%dgt_ln%'
     OR lower(it_item_name) LIKE '%cheq%' )
GROUP BY 1, 2
ORDER BY family, n_impressions DESC
;


-- ── #2 MONTHLY exposure: PLI/CRV impressions & clicks per CRV arm x GA4 event-month ──
-- The trend you wanted — how each month reacts. Read: PLI impressions/client, Action vs
-- Control, month by month (impr / n_clients_with_banner in Excel). Action < Control = crowd-out.
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
-- PLI mobile leads in the GA4 window. DO NOT cast clnt_no here (a CAST on a Teradata col
-- gets pushed down as ROUND -> Teradata err 9981); cast only the GA4 side below.
pli AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
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
roster AS (
    SELECT clnt_no,
           CASE WHEN any_action  = 1 THEN 'overlap_action'
                WHEN any_control = 1 THEN 'overlap_control'
                ELSE                      'no_overlap' END AS grp
    FROM pli_flagged
),
ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, year, month,
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
      AND platform IN ('IOS', 'ANDROID')
      AND (
            lower(it_item_name) LIKE '%cc-instalments%'
         OR (
                ( lower(it_item_name) LIKE '%vcl%'
               OR lower(it_item_name) LIKE '%pcl%'
               OR lower(it_item_name) LIKE '%limit%increase%' )
            AND lower(it_item_name) NOT LIKE '%ln_rcl%'
            AND lower(it_item_name) NOT LIKE '%dgt_ln%'
            AND lower(it_item_name) NOT LIKE '%cheq%'
            AND lower(it_item_name) NOT LIKE '%pcd_ccpij%'
         )
          )
    GROUP BY 1, 2, 3
)
SELECT
    r.grp,
    g.year,
    g.month,
    COUNT(DISTINCT g.clnt_no)        AS n_clients_with_banner,
    SUM(g.pli_impr)                  AS pli_impressions,
    SUM(g.pli_clk)                   AS pli_clicks,
    SUM(g.crv_impr)                  AS crv_impressions,
    SUM(g.crv_clk)                   AS crv_clicks
FROM roster r
JOIN ga4 g ON g.clnt_no = r.clnt_no
GROUP BY r.grp, g.year, g.month
ORDER BY r.grp, g.year, g.month
;
