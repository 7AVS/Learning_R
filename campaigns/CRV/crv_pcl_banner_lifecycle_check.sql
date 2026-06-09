-- =============================================================================
-- CRV x PCL banner lifecycle — SANITY CHECK (GA4 / Starburst-Trino)
-- =============================================================================
-- WHY: before trusting ~20 months of banner engagement, find WHEN each banner
--   appears in GA4, how consistent it is, and whether the IDs/names get RECYCLED
--   (same id reused for a different banner) — codes change generation by generation.
--
-- BANNER IDENTITY = it_item_name (the deployment promo string), like the PCD tracker.
--   KEY: PCL is catalogued as "VCL" in deployment (NOT PLI/PCL) — that's why earlier
--   mnemonic discovery missed it. CRV = "...CC-Instalments-..." names.
--   The numeric ids from the deployment catalog (CRV 87340-87344; PCL 156764/289661...)
--   are OFFER ids, NOT GA4 it_item_id — GA4 is matched on the name; Q0 below discovers
--   the GA4 it_item_id for each.
--
-- Wide LIKE net (discovery) to catch every naming generation:
--   PCL: %vcl% , %pcl% , %limit%increase% , %rcu%pli%      CRV: %instalment%
-- GA4 table = the _reduced variant. Trino dialect (date_diff ok, NO QUALIFY, NO NULLIFZERO).
-- Known names seen in the source screenshots (reference, not exhaustive):
--   CRV:  PB_CC_ALL_21_06_RBC_CC-Instalments-{INT_ONLY,INT_OTF,OTF_ONLY,HowItWorks,NOINT_NOFEE-PIV}
--   PCL:  PB_CC_ALL_23_05_RBC_VCL-LimitIncrease-CLI_{Static,PA,Q};
--         PB_CC_ALL_25_11_RBC_VCL-Joint_*; NBO-PB_*_RBC_VCL_*FinOffersHub;
--         NBO-PB_CC_PCL_*; PB_LN_RCL_*PLI*; PB-MB_DGT_LN_26_06_*PLI*
--   EXCLUDE: PB_CHEQ_ALL_26_05_RBC_VCL_PCD_CCPIJ (Draft, Program=PCD — different campaign)
-- =============================================================================

-- ── Q0 — name <-> GA4 it_item_id map (discovers the GA4 ids; flags recycling) ──
-- A name with >1 it_item_id, or an it_item_id under >1 name, = recycling/reassignment.
SELECT DISTINCT
    it_item_name,
    it_item_id
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2024', '2025', '2026')
  AND lower(event_name) IN ('view_promotion', 'select_promotion')
  AND (   lower(it_item_name) LIKE '%vcl%'
       OR lower(it_item_name) LIKE '%pcl%'
       OR lower(it_item_name) LIKE '%limit%increase%'
       OR lower(it_item_name) LIKE '%rcu%pli%'
       OR lower(it_item_name) LIKE '%instalment%' )
  AND lower(it_item_name) NOT LIKE '%pcd_ccpij%'
ORDER BY it_item_name
;

-- ── Q1 — per-banner lifecycle summary: first/last seen, span, volume ──────────
-- "When can we start trusting this?" first_seen = GA4's earliest record (also a
-- GA4-retention check — GA4 may not reach back to 2024).
SELECT
    it_item_name,
    MIN(event_date)                  AS first_seen,
    MAX(event_date)                  AS last_seen,
    COUNT(DISTINCT year || month)    AS n_months_active,
    SUM(CASE WHEN lower(event_name) = 'view_promotion'   THEN 1 ELSE 0 END) AS n_views,
    SUM(CASE WHEN lower(event_name) = 'select_promotion' THEN 1 ELSE 0 END) AS n_clicks,
    COUNT(DISTINCT up_srf_id2_value) AS n_users
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2024', '2025', '2026')
  AND lower(event_name) IN ('view_promotion', 'select_promotion')
  AND (   lower(it_item_name) LIKE '%vcl%'
       OR lower(it_item_name) LIKE '%pcl%'
       OR lower(it_item_name) LIKE '%limit%increase%'
       OR lower(it_item_name) LIKE '%rcu%pli%'
       OR lower(it_item_name) LIKE '%instalment%' )
  AND lower(it_item_name) NOT LIKE '%pcd_ccpij%'
GROUP BY it_item_name
ORDER BY first_seen, it_item_name
;

-- ── Q2 — monthly detail (pivot in Excel: see the lifecycle, gaps, recycling) ──
SELECT
    it_item_name,
    year,
    month,
    SUM(CASE WHEN lower(event_name) = 'view_promotion'   THEN 1 ELSE 0 END) AS n_views,
    SUM(CASE WHEN lower(event_name) = 'select_promotion' THEN 1 ELSE 0 END) AS n_clicks,
    COUNT(DISTINCT up_srf_id2_value) AS n_users
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2024', '2025', '2026')
  AND lower(event_name) IN ('view_promotion', 'select_promotion')
  AND (   lower(it_item_name) LIKE '%vcl%'
       OR lower(it_item_name) LIKE '%pcl%'
       OR lower(it_item_name) LIKE '%limit%increase%'
       OR lower(it_item_name) LIKE '%rcu%pli%'
       OR lower(it_item_name) LIKE '%instalment%' )
  AND lower(it_item_name) NOT LIKE '%pcd_ccpij%'
GROUP BY it_item_name, year, month
ORDER BY it_item_name, year, month
;
