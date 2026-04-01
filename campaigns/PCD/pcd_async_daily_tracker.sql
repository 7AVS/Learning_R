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


-- =============================================================================
-- TACTIC EVENT TABLE — Deployment Population (Teradata)
-- =============================================================================
-- The GA4 queries above show WHO interacted with the banners.
-- The tactic event table shows WHO was DEPLOYED (offered the banner).
-- Cross-referencing both validates that deployment and tracking are in sync.
--
-- Table: DTZV01.TACTIC_EVNT_IP_AR_H60M (Teradata)
-- TACTIC_ID structure: positions 8-10 = MNE (campaign mnemonic)
--   For PCD: positions 8-10 = 'PCD'
--
-- NOTE: These are Teradata SQL queries. Run in Teradata, NOT Starburst.
-- The tactic ID for the async deployment won't exist until ~April 20.
-- Once deployed, identify the tactic ID by:
--   1. Julian date of deployment + 'PCD' mnemonic
--   2. Or filter TACTIC_ID where SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
--      and deployment date matches
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 4: Discover PCD tactic IDs (run post-deployment)
-- ---------------------------------------------------------------------------
-- Find all PCD tactic IDs to identify the async deployment.
-- Look for the tactic ID matching the April 20+ deployment date.
--
-- Run in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3)       AS mnemonic,
    MIN(EVNT_DT)                  AS first_event_date,
    MAX(EVNT_DT)                  AS last_event_date,
    COUNT(DISTINCT CLNT_NO)       AS unique_clients,
    COUNT(*)                      AS total_events
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
    AND EVNT_DT >= '2026-04-01'
GROUP BY
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3)
ORDER BY first_event_date DESC;


-- ---------------------------------------------------------------------------
-- QUERY 5: Daily deployed population by tactic ID
-- ---------------------------------------------------------------------------
-- Once you identify the correct tactic ID from Query 4, plug it in here
-- to get the daily deployed population (denominator for response rate).
--
-- Replace '<<TACTIC_ID>>' with the actual tactic ID.
--
-- Run in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    EVNT_DT                       AS event_date,
    TACTIC_ID,
    COUNT(DISTINCT CLNT_NO)       AS deployed_clients,
    COUNT(*)                      AS total_events
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    TACTIC_ID = '<<TACTIC_ID>>'
    AND EVNT_DT >= '2026-04-01'
GROUP BY
    EVNT_DT,
    TACTIC_ID
ORDER BY event_date;


-- ---------------------------------------------------------------------------
-- QUERY 6: PCD decision/response table — test vs control population
-- ---------------------------------------------------------------------------
-- The PCD decision/response table has the test/control segmentation.
-- Use this to get the "Available PCD Leads" denominator and understand
-- the act_ctl_seg / test_value split.
--
-- Table: dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp (Teradata)
--
-- Run in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    tactic_id_parent,
    act_ctl_seg,
    test_value,
    COUNT(DISTINCT clnt_no)       AS unique_clients,
    COUNT(*)                      AS total_records
FROM dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE
    response_start >= '2026-04-01'
GROUP BY
    tactic_id_parent,
    act_ctl_seg,
    test_value
ORDER BY unique_clients DESC;


-- ---------------------------------------------------------------------------
-- RECONCILIATION NOTES
-- ---------------------------------------------------------------------------
-- These queries run in DIFFERENT systems (Starburst vs Teradata).
-- To reconcile:
--
-- 1. Run Query 5 (Teradata) → get deployed client count per day
-- 2. Run Query 1 (Starburst) → get banner view/click users per day
-- 3. Compare in Excel:
--    - Deployed clients (Teradata) should >= view users (GA4)
--    - If GA4 shows MORE users than deployed, there's a tagging leak
--    - If GA4 shows significantly FEWER, some clients aren't seeing the banner
--
-- Join feasibility (from exploratory work):
--   GA4 field ep_srf_id2 or user_id may map to clnt_no in Teradata.
--   This has NOT been validated yet — run Query 4 from
--   pcd_async_banner_explore.sql to test after launch.
-- ---------------------------------------------------------------------------
