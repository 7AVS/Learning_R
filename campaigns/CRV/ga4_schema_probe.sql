-- =============================================================================
-- GA4 schema probe (Trino / Starburst) — TRIMMED.
-- The FULL non-reduced schema (~130 fields) is already cataloged from Andre's
-- screenshots at schemas/ga4_ecommerce_schema.md — so DESCRIBE-ing the full table is
-- dropped. What's left answers only the still-OPEN questions: what _reduced keeps,
-- history depth, and validating the two promising fields (mnemonic, treatment_code).
-- =============================================================================

-- 1) column list of the _reduced table — the one schema we DON'T have. Tells us whether
--    _reduced KEEPS event_timestamp / ip_sf_treatment_code (if it drops them, we must
--    query the full table for those instead of _reduced).
DESCRIBE edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced;

-- 3) what event types exist (beyond view_promotion / select_promotion)?
SELECT event_name, COUNT(*) AS n_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month = '06'
GROUP BY event_name
ORDER BY n_events DESC;

-- 4) how far back does each table actually go? (tests "remove _reduced to see more")
SELECT 'reduced' AS tbl, MIN(event_date) AS earliest, MAX(event_date) AS latest
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2024','2025','2026')
UNION ALL
SELECT 'full' AS tbl, MIN(event_date) AS earliest, MAX(event_date) AS latest
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year IN ('2024','2025','2026');

-- 5) does the campaign mnemonic carry VCL/CRV cleanly? (could be a cleaner filter)
SELECT ip_sf_campaign_mnemonic, COUNT(*) AS n
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month = '06'
  AND lower(event_name) IN ('view_promotion','select_promotion')
GROUP BY ip_sf_campaign_mnemonic
ORDER BY n DESC;

-- 6) does ip_sf_treatment_code carry Action/Control? (would skip the tactic join!)
--    peek at its values on a known VCL (PCL) banner. SKIP if col absent per DESCRIBE.
SELECT ip_sf_treatment_code, COUNT(*) AS n
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month = '06'
  AND lower(it_item_name) LIKE '%vcl-limitincrease%'
GROUP BY ip_sf_treatment_code
ORDER BY n DESC;
