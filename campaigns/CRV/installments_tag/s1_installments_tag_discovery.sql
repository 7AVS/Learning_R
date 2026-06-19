-- s1_installments_tag_discovery.sql
-- ENGINE: Starburst/Trino
-- TABLE:  edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
--         (_reduced has history from Feb-2025; full table holds only ~2 weeks)
--
-- PURPOSE: Profile every GA4 ecommerce event that fires on the mobile credit-card /
--   transaction-details screen so we can locate the inline "pay in installments" tag
--   as a new, distinct UI surface — separate from the known CRV banners (i_87340 series).
--   The query does NOT pre-filter to known banner ids; it scans the full screen to expose
--   any other item ids, event names, or promotion ids present there.
--
-- DATE WINDOW (edit here):
--   Default covers 2025-01 onward to capture any tag introduced in 2025 or 2026.
--   Narrow the month IN-list for faster iteration once you know the launch window.
--   year IN ('2025','2026'), all months — adjust below as needed.
--
-- WHAT TO LOOK FOR:
--   - event_name + it_location_id + it_item_id combos that appear on this screen
--     but have is_known_crv_banner = 'N' — those are candidates for the inline tag surface.
--   - Any event_name values OTHER than view_promotion / select_promotion (e.g. select_item,
--     view_item, a custom event like "tap_installments") — an inline tag wired outside the
--     GA4 promotion framework would NOT appear in view_promotion rows at all.
--   - High-volume it_item_id values with no it_promotion_id (or null it_promotion_id) —
--     some inline UI widgets log under view_item or custom events rather than promotions.
--   - Whether any select_promotion or tap events share the same it_item_id as a new
--     view_promotion id — that confirms it's a tappable surface, not just a display label.
--
-- CAVEAT 1: If the installments tag was instrumented as a non-promotion event (e.g.
--   a custom "view_item" or a named event like "view_installment_offer"), this query's
--   screen-level filter on it_location_id will only catch it if the tag sets that field.
--   Inline tags sometimes fire WITHOUT any item/promotion payload — in that case,
--   profile ep_firebase_screen or ep_details instead (those fields sit at the session/event
--   level, not the item level, and survive even when item fields are null).
-- CAVEAT 2: it_location_id LIKE patterns below are separator-agnostic ('%' spans
--   underscores, spaces, hyphens) because the exact separator in the tag's areaName is
--   unknown. The patterns are proven against the CRV banner location strings; a new
--   surface on the same screen should share the same root string.

-- ============================================================
-- STMT 1 — Screen-level event census (all items, all event types)
-- ============================================================
-- Groups by the full identity of each item on the transaction-details screen.
-- Rows where is_known_crv_banner = 'N' are the candidate new surfaces.

SELECT
    year,
    month,
    event_name,
    it_location_id,
    it_item_id,
    it_item_name,
    it_promotion_id,
    it_promotion_name,
    platform,
    CASE
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'Y'
        ELSE 'N'
    END                                                                 AS is_known_crv_banner,
    COUNT(*)                                                            AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE
    -- ---- EDIT DATE WINDOW HERE ----
    year  IN ('2025', '2026')
    -- All months in scope — narrow once the tag launch window is known:
    -- AND month IN ('01','02','03','04','05','06','07','08','09','10','11','12')
    -- --------------------------------
    AND (
        -- iOS location string (proven against CRV: 'I_IOS_Credit_Card_Details_M1' family)
        LOWER(it_location_id) LIKE '%credit%card%details%'
        -- Android location string (proven against CRV: 'Android_Credit_Card_Details_M1' family)
        OR UPPER(it_location_id) LIKE '%ANDROID%CREDIT%CARD%DETAIL%'
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY is_known_crv_banner ASC, n_events DESC
LIMIT 500
;

-- ============================================================
-- STMT 2 — Non-CRV items only (cleaner view of new surfaces)
-- ============================================================
-- Isolates rows NOT in the known CRV allowlist.
-- Sort by event volume — highest-count unknown ids are the strongest candidates.

SELECT
    year,
    month,
    event_name,
    it_location_id,
    it_item_id,
    it_item_name,
    it_promotion_id,
    it_promotion_name,
    platform,
    COUNT(*)                                                            AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE
    -- ---- EDIT DATE WINDOW HERE ----
    year  IN ('2025', '2026')
    -- --------------------------------
    AND (
        LOWER(it_location_id) LIKE '%credit%card%details%'
        OR UPPER(it_location_id) LIKE '%ANDROID%CREDIT%CARD%DETAIL%'
    )
    -- Exclude known CRV banner ids
    AND it_item_id NOT IN ('i_87340','i_87342','i_87343','i_87344')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY n_events DESC
LIMIT 300
;

-- ============================================================
-- STMT 3 — Installments keyword sweep (backup: event/screen fields)
-- ============================================================
-- Catches the tag IF it fires outside the promotion framework — i.e. as a named
-- event, or with it_item_name / ep_firebase_screen / ep_details containing
-- "install" strings. Not limited to the card-details location filter above.
-- Also profile any event_name distinct from view_promotion / select_promotion
-- that appears on the card-details screen (those are non-standard event types
-- the tag may use if it was wired by a different team or SDK version).

SELECT
    year,
    month,
    event_name,
    it_location_id,
    it_item_id,
    it_item_name,
    it_promotion_id,
    COUNT(*)                                                            AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE
    -- ---- EDIT DATE WINDOW HERE ----
    year  IN ('2025', '2026')
    -- --------------------------------
    AND (
        -- Keyword sweep: installment/BNPL strings wherever they appear
        LOWER(it_item_name)      LIKE '%install%'
        OR LOWER(it_item_name)   LIKE '%bnpl%'
        OR LOWER(it_promotion_name) LIKE '%install%'
        OR LOWER(it_promotion_name) LIKE '%bnpl%'
        -- Non-promotion event types on the card-details screen
        OR (
            event_name NOT IN ('view_promotion','select_promotion')
            AND (
                LOWER(it_location_id) LIKE '%credit%card%details%'
                OR UPPER(it_location_id) LIKE '%ANDROID%CREDIT%CARD%DETAIL%'
            )
        )
    )
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY n_events DESC
LIMIT 300
;
