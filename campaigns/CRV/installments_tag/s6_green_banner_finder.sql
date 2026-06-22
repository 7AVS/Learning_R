-- s6_green_banner_finder.sql
-- Find the "Pay in Installments" GREEN transaction banner — the SECOND CRV entry point
-- (per Tracey Eadie's 2026-06-21 request, CRV/PLI Teams space). It is a per-transaction
-- feature CTA (green text under eligible posted transactions), NOT the Salesforce M1 banner,
-- and is NOT subject to Salesforce CPC capping. Likely logged in the NARROW table as a
-- view/tap on the transaction-details screen. Candidate already seen in s3 STMT3:
-- "view - credit card transaction details - pwp" (pwp = pay-with-plan?) ~275K clients.
--
-- SPELLING GOTCHA: prior sweeps used '%instalment%' (single-L, British — matches the SF
-- banner 'CC-Instalments'). The green UI text is 'Pay in Installments' (double-L, American),
-- which '%instalment%' does NOT match. Use '%install%' to catch BOTH spellings.
-- ENGINE: Starburst/Trino.

-- ============================================================
-- STMT 1 — Find the green banner in the narrow table (transaction screens)
-- ============================================================
SELECT
    event_name,
    ep_details,
    ep_firebase_screen,
    platform,
    COUNT(*)                          AS n_events,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026' AND month = '06'
  AND (
        LOWER(ep_details)         LIKE '%install%'
     OR LOWER(ep_details)         LIKE '%pwp%'
     OR LOWER(ep_details)         LIKE '%pay with plan%'
     OR LOWER(ep_firebase_screen) LIKE '%install%'
     OR LOWER(ep_firebase_screen) LIKE '%pwp%'
  )
GROUP BY 1, 2, 3, 4
ORDER BY n_clients DESC
LIMIT 50
;

-- ============================================================
-- STMT 2 — Same sweep in ecommerce (in case the green banner is also logged as an item)
-- ============================================================
SELECT
    event_name,
    it_item_name,
    it_promotion_name,
    it_location_id,
    COUNT(*)                          AS n_events,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month = '06'
  AND (
        LOWER(it_item_name)      LIKE '%install%'
     OR LOWER(it_promotion_name) LIKE '%install%'
     OR LOWER(it_item_name)      LIKE '%pwp%'
  )
GROUP BY 1, 2, 3, 4
ORDER BY n_clients DESC
LIMIT 50
;
