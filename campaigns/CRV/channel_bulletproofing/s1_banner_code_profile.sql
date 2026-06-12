-- s1_banner_code_profile.sql
-- Identity key = it_item_id ('i_'+offer id) per s7 2026-06-12: format-stable all platforms, zero disagreement, catches rows where promotion_id is absent.
-- STEP 1 — run this first; nothing else in this track runs before its output is reviewed.
-- PURPOSE: For our PCL and CRV banner codes: everything GA4 records about them.
--   No event filter — the data tells us what exists.
-- Run Stmt 1 first (tiny), then Stmt 2.
-- Trino syntax. Counts only.

-- ============================================================
-- STATEMENT 1 — event inventory per code (small output)
-- ============================================================
-- All events where it_promotion_id is in the PCL or CRV lists, Feb–Apr 2026.
-- Shows what event types GA4 actually fires for each code — no assumptions.

SELECT
    CASE
        WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698') THEN 'PCL'
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
    END                                                                  AS banner_family,
    it_item_id,
    event_name,
    COUNT(*)                                                             AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                 AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND it_item_id IN (
        'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698',   -- PCL
        'i_87340','i_87342','i_87343','i_87344'                                                                                               -- CRV
  )
GROUP BY 1, 2, 3
ORDER BY banner_family, it_item_id, n_events DESC
;

-- ============================================================
-- STATEMENT 2 — field profile (the map)
-- ============================================================
-- Every distinct combination of the descriptive fields per event type — this is the
-- menu we choose from; ep_details deliberately excluded (concatenated label, see CB04).
-- Same row scope as Stmt 1.

SELECT
    CASE
        WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698') THEN 'PCL'
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
    END                                                                  AS banner_family,
    event_name,
    it_item_name,
    it_creative_name,
    it_location_id,
    ep_firebase_screen,
    COUNT(*)                                                             AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                 AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND it_item_id IN (
        'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698',   -- PCL
        'i_87340','i_87342','i_87343','i_87344'                                                                                               -- CRV
  )
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY n_events DESC
LIMIT 200
;
