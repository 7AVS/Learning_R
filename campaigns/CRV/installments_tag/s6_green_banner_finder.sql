-- s6_green_banner_finder.sql  (REWRITTEN)
-- The GREEN "Pay in Installments" banner = the narrow-table tag Andre identified on client
-- 383899457. It is an ORGANIC product feature (green text under eligible posted transactions),
-- NOT a deployed/Salesforce campaign — so it exists ONLY in the narrow table, detected by the
-- COMBINATION of two parameters:
--      event_name = 'tap'
--      ep_details = 'tap - credit card transaction details - posted transaction'
-- (NOT found by keyword '%install%' — the ep_details contains no "install" string. The prior
--  keyword-sweep version of this file was wrong and would have missed it.)
-- ENGINE: Starburst/Trino.

-- ============================================================
-- STMT 1 — Exact green-banner signature: volume by platform / screen
-- ============================================================
SELECT
    platform,
    ep_firebase_screen,
    COUNT(*)                          AS n_events,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026' AND month = '06'
  AND event_name = 'tap'
  AND LOWER(ep_details) = 'tap - credit card transaction details - posted transaction'
GROUP BY 1, 2
ORDER BY n_clients DESC
LIMIT 30
;

-- ============================================================
-- STMT 2 — Census of the transaction screens: is there a DEDICATED green-link event,
--          and is there a VIEW/impression counterpart? (validity check)
-- ============================================================
-- Lists every event_name + ep_details on the account-details / posted-transaction screens.
-- Purpose: confirm whether the generic "tap - ... - posted transaction" IS the Pay-in-Installments
-- tap or just "open a transaction" (navigation), and whether a "view - ..." event exists that
-- would serve as the green banner's impression. Read the labels here to decide.
SELECT
    event_name,
    ep_details,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026' AND month = '06'
  AND ( LOWER(ep_firebase_screen) LIKE '%account details%'
     OR LOWER(ep_firebase_screen) LIKE '%posted transaction%' )
  AND event_name IN ('tap', 'view', 'screen_view', 'select_content')
GROUP BY 1, 2
ORDER BY n_clients DESC
LIMIT 60
;
