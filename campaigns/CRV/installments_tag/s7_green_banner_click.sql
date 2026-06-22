-- s7_green_banner_click.sql
-- The green banner has an IMPRESSION (view - credit card installments - eligible transaction, 1.54M).
-- QUESTION: what is its CLICK — the green-banner equivalent of select_promotion (which only exists
-- for the Salesforce M1 banner)? The green banner is a custom event, so look in its OWN namespace
-- ("credit card installments") for a tap, and check the "pwp" (pay with plan) flow it leads to.
-- ENGINE: Starburst/Trino. Table: narrow (green banner is narrow-only).

-- ============================================================
-- STMT 1 — Everything in the "credit card installments" namespace (impression + any tap/click)
-- ============================================================
-- The impression is "view - credit card installments - eligible transaction". A click on the green
-- link should share the "credit card installments" prefix (e.g. tap - credit card installments - ...).
-- This lists impression + every other event in that namespace so the click — if it fires — appears.
SELECT
    event_name,
    ep_details,
    platform,
    COUNT(*)                          AS n_events,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026' AND month = '06'
  AND LOWER(ep_details) LIKE '%credit card installments%'
GROUP BY 1, 2, 3
ORDER BY n_clients DESC
LIMIT 40
;

-- ============================================================
-- STMT 2 — The "pay with plan" (pwp) flow — is there a tap, and which screens
-- ============================================================
-- pwp is the candidate landing after tapping the green link. Profile its events to see whether a
-- discrete tap/click exists there, or whether it's only a view (i.e. green banner has no literal click).
SELECT
    event_name,
    ep_details,
    ep_firebase_screen,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026' AND month = '06'
  AND (    LOWER(ep_details)         LIKE '%pwp%'
        OR LOWER(ep_details)         LIKE '%pay with plan%'
        OR LOWER(ep_firebase_screen) LIKE '%pwp%' )
GROUP BY 1, 2, 3
ORDER BY n_clients DESC
LIMIT 40
;
