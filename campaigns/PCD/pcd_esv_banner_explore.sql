-- =============================================================================
-- ESV Banner — Exploratory Queries (GA4 Validation Framework)
-- =============================================================================
--
-- Purpose:
--   Use LIVE ESV banner data in GA4 to validate the tracker framework before
--   PCD launches (~April 20, 2026). ESV promo names are confirmed live in prod
--   now, so we can build and validate the full query pattern against real data.
--   Once we confirm which GA4 field the tags land in and which event_name maps
--   to view vs click, we port the same structure to PCD with no guesswork.
--
-- Context:
--   Confirmed live by Rajani Singineedi (2026-03-18). ESV tags are flowing in
--   production GA4. PCD tags will NOT appear until ~2026-04-20 launch, so this
--   is our dry run.
--
-- Confirmed ESV promo names (field TBD — see Query 1):
--   1. PB_CHEQ_ALL_26_02_RBC_HISA_Offer_Hub_Banner
--   2. PB_CHEQ_ALL_26_02_RBC_HISA_PDA_Product_Page
--   3. PB_CHEQ_ALL_26_02_RBC_ESV_HISA_Acquisition_Campaign_Banner
--
-- Validation goals (answer these before PCD launch):
--   Q1: Which GA4 field do the promo tags land in?
--       (it_item_name, selected_promotion_name, it_creative_name, or ip_sf_campaign_mnemonic?)
--   Q2: Which event_name values correspond to banner view vs click?
--   Q3: Does each individual promo name appear and is traffic volume sensible?
--
-- Once answered: port Query 2 pattern (daily tracker) directly to PCD.
--
-- Tables:
--   edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce  (Trino)
--   GA4 partitioned by year/month/day (varchar) — always filter to avoid full scans.
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 0: Categorical Field EDA — understand the GA4 ecommerce table
-- ---------------------------------------------------------------------------
-- Goal: Before searching for specific promo names, profile the key categorical
-- fields so we know what values exist and can interpret Query 1 results with
-- context. Each sub-query is independent — run them in any order.
--
-- Run all queries in Trino.
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- QUERY 0a — event_name: What event types exist?
-- ---------------------------------------------------------------------------
-- What to look for:
--   - Which event_name maps to a banner view vs. a click?
--     Typically "view_promotion" = view, "select_promotion" = click — but confirm.
--   - Are there checkout / purchase events mixed in? (begin_checkout, purchase)
--   - unique_users vs total_events gap tells you how often users repeat the event.
-- ---------------------------------------------------------------------------

SELECT
    event_name,
    COUNT(*)                       AS total_events,
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year  = '2026'
    AND month IN ('03', '04')
GROUP BY event_name
ORDER BY total_events DESC;


-- ---------------------------------------------------------------------------
-- QUERY 0b — platform: What platforms exist?
-- ---------------------------------------------------------------------------
-- What to look for:
--   - Expect WEB, ANDROID, IOS (or lowercase variants).
--   - Any unexpected platform values that would indicate bad data or test traffic?
--   - Volume split across platforms — useful when filtering for web-only banners.
-- ---------------------------------------------------------------------------

SELECT
    platform,
    COUNT(*)                       AS total_events,
    COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year  = '2026'
    AND month IN ('03', '04')
GROUP BY platform
ORDER BY total_events DESC;


-- ---------------------------------------------------------------------------
-- QUERY 0c — it_item_name: Candidate field for promo tags
-- ---------------------------------------------------------------------------
-- What to look for:
--   - Are any of the 3 ESV promo names (HISA / ESV keywords) present here?
--   - What is the general naming convention? Structured codes vs. free text?
--   - How many distinct values total? A small set = controlled taxonomy.
--     A huge set = likely free text or campaign-level naming.
-- ---------------------------------------------------------------------------

SELECT
    it_item_name,
    COUNT(*) AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year  = '2026'
    AND month IN ('03', '04')
GROUP BY it_item_name
ORDER BY total_events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 0d — selected_promotion_name: Candidate field for promo tags
-- ---------------------------------------------------------------------------
-- What to look for:
--   - Same as 0c — does this field carry structured promo name codes?
--   - Is this field mostly NULL/empty with occasional values, or broadly populated?
--   - Do the values here overlap with it_item_name, or are they distinct?
-- ---------------------------------------------------------------------------

SELECT
    selected_promotion_name,
    COUNT(*) AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year  = '2026'
    AND month IN ('03', '04')
GROUP BY selected_promotion_name
ORDER BY total_events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 0e — it_creative_name: Candidate field for promo tags
-- ---------------------------------------------------------------------------
-- What to look for:
--   - Creative-level naming — may carry banner variant identifiers.
--   - Are ESV/HISA keywords present here?
--   - High cardinality here is normal (one row per creative variant).
-- ---------------------------------------------------------------------------

SELECT
    it_creative_name,
    COUNT(*) AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year  = '2026'
    AND month IN ('03', '04')
GROUP BY it_creative_name
ORDER BY total_events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 0f — ip_sf_campaign_mnemonic: Candidate field for promo tags
-- ---------------------------------------------------------------------------
-- What to look for:
--   - This field may carry the RBC campaign mnemonic (e.g., ESV, PCD, AUH).
--   - If populated: values should be short codes, not full promo names.
--   - If ESV tags land here, the value will likely be "ESV" or a variant,
--     not the full structured promo name string.
-- ---------------------------------------------------------------------------

SELECT
    ip_sf_campaign_mnemonic,
    COUNT(*) AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year  = '2026'
    AND month IN ('03', '04')
GROUP BY ip_sf_campaign_mnemonic
ORDER BY total_events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 1: Field Discovery — where do ESV tags land?
-- ---------------------------------------------------------------------------
-- Goal: Find ESV promo records in GA4 and confirm which field contains the
-- promo name. Uses a broad LIKE search across all 4 candidate fields on the
-- keywords HISA and ESV — catches any ESV-related tags even if naming drifts.
--
-- What to look for in output:
--   - Which column (it_item_name, selected_promotion_name, it_creative_name,
--     ip_sf_campaign_mnemonic) has the confirmed promo names populated?
--   - What are the distinct event_name values? These will map to view vs click.
--   - Are all 3 ESV promo names present?
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    event_name,
    it_item_name,
    it_creative_name,
    selected_promotion_name,
    ip_sf_campaign_mnemonic,
    platform,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(*)                       AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter — March and April 2026 (ESV is live now)
    year = '2026'
    AND month IN ('03', '04')
    -- Broad search across all 4 candidate fields on ESV/HISA keywords
    -- Casts a wide net — catches the tags regardless of which field they land in
    AND (
           it_item_name              LIKE '%HISA%'
        OR it_item_name              LIKE '%ESV%'
        OR selected_promotion_name   LIKE '%HISA%'
        OR selected_promotion_name   LIKE '%ESV%'
        OR it_creative_name          LIKE '%HISA%'
        OR it_creative_name          LIKE '%ESV%'
        OR ip_sf_campaign_mnemonic   LIKE '%HISA%'
        OR ip_sf_campaign_mnemonic   LIKE '%ESV%'
    )
GROUP BY
    event_name,
    it_item_name,
    it_creative_name,
    selected_promotion_name,
    ip_sf_campaign_mnemonic,
    platform
ORDER BY total_events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 2: Daily Tracker — ESV version
-- ---------------------------------------------------------------------------
-- Daily banner event counts using exact IN match on the 3 confirmed ESV promo
-- names. Searches both it_item_name and selected_promotion_name until Query 1
-- confirms which field is the right one.
--
-- TODO (post Query 1):
--   - Once the field is confirmed, drop the other branch of the OR to tighten
--     the filter (reduces scan cost, removes any ambiguity).
--   - Once event_name values are known, pivot view/click into separate columns:
--       Banner Views  = COUNT where event_name = '<view event>'
--       Banner Clicks = COUNT where event_name = '<click event>'
--       Banner CTR    = Clicks / Views
--   - This daily tracker pattern ports directly to PCD — just swap promo names.
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    -- event_date is already YYYY-MM-DD string in GA4 — cast for clean date display
    CAST(event_date AS DATE) AS event_date,
    event_name,
    platform,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(*)                       AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter — March and April 2026
    year = '2026'
    AND month IN ('03', '04')
    -- Confirmed ESV promo names (Rajani Singineedi, 2026-03-18)
    -- Dual-field OR until Query 1 confirms which field to keep
    AND (
           it_item_name IN (
               'PB_CHEQ_ALL_26_02_RBC_HISA_Offer_Hub_Banner',
               'PB_CHEQ_ALL_26_02_RBC_HISA_PDA_Product_Page',
               'PB_CHEQ_ALL_26_02_RBC_ESV_HISA_Acquisition_Campaign_Banner'
           )
        OR selected_promotion_name IN (
               'PB_CHEQ_ALL_26_02_RBC_HISA_Offer_Hub_Banner',
               'PB_CHEQ_ALL_26_02_RBC_HISA_PDA_Product_Page',
               'PB_CHEQ_ALL_26_02_RBC_ESV_HISA_Acquisition_Campaign_Banner'
           )
    )
GROUP BY
    CAST(event_date AS DATE),
    event_name,
    platform
ORDER BY
    event_date,
    event_name;


-- ---------------------------------------------------------------------------
-- QUERY 3: Exact Match Validation — confirm each ESV promo name is flowing
-- ---------------------------------------------------------------------------
-- After Query 1 identifies the correct field, this query validates that each
-- individual ESV promo name is present and generating the expected event types.
-- For now, searches both it_item_name and selected_promotion_name.
--
-- What to look for:
--   - All 3 promo names appear in the output (none missing = tags firing correctly)
--   - Each promo name maps to the expected event_name values (view, click, etc.)
--   - Traffic volumes are plausible (not zero, not suspiciously huge)
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    -- Show both candidate fields so we can see which one is populated
    it_item_name,
    selected_promotion_name,
    event_name,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(*)                       AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter — March and April 2026
    year = '2026'
    AND month IN ('03', '04')
    -- Exact match on all 3 confirmed ESV promo names
    -- Dual-field OR until Query 1 confirms which field to keep
    AND (
           it_item_name IN (
               'PB_CHEQ_ALL_26_02_RBC_HISA_Offer_Hub_Banner',
               'PB_CHEQ_ALL_26_02_RBC_HISA_PDA_Product_Page',
               'PB_CHEQ_ALL_26_02_RBC_ESV_HISA_Acquisition_Campaign_Banner'
           )
        OR selected_promotion_name IN (
               'PB_CHEQ_ALL_26_02_RBC_HISA_Offer_Hub_Banner',
               'PB_CHEQ_ALL_26_02_RBC_HISA_PDA_Product_Page',
               'PB_CHEQ_ALL_26_02_RBC_ESV_HISA_Acquisition_Campaign_Banner'
           )
    )
GROUP BY
    it_item_name,
    selected_promotion_name,
    event_name
ORDER BY
    COALESCE(it_item_name, selected_promotion_name),
    event_name;
