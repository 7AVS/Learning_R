-- =============================================================================
-- PCD Async Banner — Daily Performance Tracker (Production)
-- =============================================================================
--
-- Purpose:
--   Daily tracker for PCD Cards Upgrade async banners. Reports banner views,
--   clicks, and CTR by date and platform. Ready to hand to the dashboard team.
--
-- Context:
--   - Jira: NBA-12268 (requested by Daniel Chin)
--   - Metrics needed: Available PCD Leads, Banner Views, Banner Clicks, Banner CTR
--   - Launch: ~2026-04-20 (async deployment)
--   - Promo names confirmed by Rajani Singineedi (2026-03-18)
--   - Field mapping validated via ESV EDA (2026-04-01)
--
-- Confirmed PCD async promo names (it_item_name):
--   1. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP
--   2. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV
--   3. PB_CC_ALL_26_02_RBC_PCD_PPCN
--   4. PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner
--
-- Field decisions (from GA4 EDA 2026-04-01):
--   - Banner identification: it_item_name (confirmed — promo names land here)
--   - View event: view_promotion
--   - Click event: select_promotion
--   - Campaign filter: ip_sf_campaign_mnemonic = 'PCD'
--   - selected_promotion_name: NOT used (contains SF insight names, not promo names)
--
-- Table: edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
-- Platform: Starburst (Trino-compatible SQL)
--
-- NOTE: Existing PCD banners (NBO_PB_CC_PCD_24_09_*, NBO-PB_CC_PCD_22_10_*)
-- are excluded by the it_item_name IN filter. Only the new async banners
-- are tracked here.
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Daily Banner Performance (Views, Clicks, CTR by date + platform)
-- ---------------------------------------------------------------------------
-- This is the primary production query. Pivot view_promotion and
-- select_promotion into columns for a clean daily report.
--
-- Run in Starburst. Adjust year/month partition filter as needed.
-- ---------------------------------------------------------------------------

SELECT
    CAST(event_date AS DATE)                                          AS event_date,
    platform,
    COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                        THEN user_pseudo_id END)                      AS view_users,
    COUNT(CASE WHEN event_name = 'view_promotion' THEN 1 END)        AS view_events,
    COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                        THEN user_pseudo_id END)                      AS click_users,
    COUNT(CASE WHEN event_name = 'select_promotion' THEN 1 END)      AS click_events,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                                 THEN user_pseudo_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                                     THEN user_pseudo_id END), 0)
        * 100, 2
    )                                                                 AS ctr_users_pct,
    ROUND(
        CAST(COUNT(CASE WHEN event_name = 'select_promotion' THEN 1 END) AS DOUBLE)
        / NULLIF(COUNT(CASE WHEN event_name = 'view_promotion' THEN 1 END), 0)
        * 100, 2
    )                                                                 AS ctr_events_pct
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter (adjust as campaign runs)
    year = '2026'
    AND month IN ('04', '05', '06')
    -- New PCD async banner promo names only
    AND it_item_name IN (
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
        'PB_CC_ALL_26_02_RBC_PCD_PPCN',
        'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
    )
    -- Belt-and-suspenders: only PCD campaign events
    AND ip_sf_campaign_mnemonic = 'PCD'
    -- Only promotion view/click events
    AND event_name IN ('view_promotion', 'select_promotion')
GROUP BY
    CAST(event_date AS DATE),
    platform
ORDER BY
    event_date,
    platform;


-- ---------------------------------------------------------------------------
-- QUERY 2: Daily Banner Performance by Promo Name
-- ---------------------------------------------------------------------------
-- Same as Query 1 but broken out by individual promo name (it_item_name).
-- Use this to see which specific banner/placement drives the most traffic.
-- ---------------------------------------------------------------------------

SELECT
    CAST(event_date AS DATE)                                          AS event_date,
    it_item_name                                                      AS promo_name,
    platform,
    COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                        THEN user_pseudo_id END)                      AS view_users,
    COUNT(CASE WHEN event_name = 'view_promotion' THEN 1 END)        AS view_events,
    COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                        THEN user_pseudo_id END)                      AS click_users,
    COUNT(CASE WHEN event_name = 'select_promotion' THEN 1 END)      AS click_events,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                                 THEN user_pseudo_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                                     THEN user_pseudo_id END), 0)
        * 100, 2
    )                                                                 AS ctr_users_pct
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year = '2026'
    AND month IN ('04', '05', '06')
    AND it_item_name IN (
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
        'PB_CC_ALL_26_02_RBC_PCD_PPCN',
        'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
    )
    AND ip_sf_campaign_mnemonic = 'PCD'
    AND event_name IN ('view_promotion', 'select_promotion')
GROUP BY
    CAST(event_date AS DATE),
    it_item_name,
    platform
ORDER BY
    event_date,
    it_item_name,
    platform;


-- ---------------------------------------------------------------------------
-- QUERY 3: Cumulative Summary (since launch)
-- ---------------------------------------------------------------------------
-- Roll-up across the full post-launch period. Use this for weekly/monthly
-- reporting or quick status checks.
-- ---------------------------------------------------------------------------

SELECT
    it_item_name                                                      AS promo_name,
    platform,
    MIN(CAST(event_date AS DATE))                                     AS first_seen,
    MAX(CAST(event_date AS DATE))                                     AS last_seen,
    COUNT(DISTINCT CAST(event_date AS DATE))                          AS days_active,
    COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                        THEN user_pseudo_id END)                      AS total_view_users,
    COUNT(CASE WHEN event_name = 'view_promotion' THEN 1 END)        AS total_view_events,
    COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                        THEN user_pseudo_id END)                      AS total_click_users,
    COUNT(CASE WHEN event_name = 'select_promotion' THEN 1 END)      AS total_click_events,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                                 THEN user_pseudo_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                                     THEN user_pseudo_id END), 0)
        * 100, 2
    )                                                                 AS ctr_users_pct
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year = '2026'
    AND month IN ('04', '05', '06')
    AND it_item_name IN (
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
        'PB_CC_ALL_26_02_RBC_PCD_PPCN',
        'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
    )
    AND ip_sf_campaign_mnemonic = 'PCD'
    AND event_name IN ('view_promotion', 'select_promotion')
GROUP BY
    it_item_name,
    platform
ORDER BY
    it_item_name,
    platform;
