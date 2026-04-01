-- =============================================================================
-- PCD Async Banner — Exploratory Queries
-- =============================================================================
--
-- Purpose:
--   Exploratory queries to learn the GA4 ecommerce and PCD tables before
--   building the daily async banner performance tracker.
--
-- Context:
--   PCD Cards Upgrade async launch, requested by Daniel Chin (NBA-12268).
--   We need to report: Available PCD Leads (test vs control), Banner Views,
--   Banner Clicks, and Banner CTR.
--
-- Status:
--   CONFIRMED — Rajani Singineedi confirmed the 4 PCD promo names (2026-03-18).
--   All 4 trigger async chat. Launch ~2026-03-25 (next week).
--   Data will NOT contain these tags until launch; queries are READY TO RUN
--   as soon as data starts flowing.
--
--   Confirmed promo names (field TBD — it_item_name? selected_promotion_name?):
--     1. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP
--     2. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV
--     3. PB_CC_ALL_26_02_RBC_PCD_PPCN
--     4. PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner
--
--   POST-LAUNCH VALIDATION NEEDED:
--     - Which GA4 field do these tags land in? (it_item_name, selected_promotion_name, it_creative_name?)
--     - Does ip_sf_campaign_mnemonic follow the PB_CC_ALL_26_02_RBC_PCD_* pattern?
--     - Which event_name values map to view vs click?
--
-- Tables:
--   1. edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce  (Trino — GA4 ecommerce events)
--   2. edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow      (Trino — GA4 narrow events)
--   3. dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp                (Teradata — PCD decision/response)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Validate confirmed PCD tags in GA4 (run post-launch)
-- ---------------------------------------------------------------------------
-- Goal: confirm the 4 promo names appear in the data and discover which
-- GA4 field they land in (it_item_name, selected_promotion_name,
-- it_creative_name, or ip_sf_campaign_mnemonic). Also reveals which
-- event_name values correspond to views vs clicks.
--
-- CONFIRMED promo names — search across multiple fields to find where they land.
-- Run this in Trino AFTER ~2026-03-25.
-- ---------------------------------------------------------------------------

SELECT
    event_name,
    it_item_name,
    it_item_id,
    it_creative_name,
    selected_promotion_name,
    ip_sf_treatment_code,
    ip_sf_campaign_mnemonic,
    platform,
    COUNT(*) AS events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter — only need 2026 data (launch ~2026-03-25)
    year = '2026'
    -- Search across multiple fields: we know the promo names but not which field they land in
    AND (
           it_item_name              LIKE '%PB_CC_ALL_26_02_RBC_PCD%'
        OR selected_promotion_name   LIKE '%PB_CC_ALL_26_02_RBC_PCD%'
        OR it_creative_name          LIKE '%PB_CC_ALL_26_02_RBC_PCD%'
        OR ip_sf_campaign_mnemonic   LIKE '%PB_CC_ALL_26_02_RBC_PCD%'
    )
GROUP BY
    event_name,
    it_item_name,
    it_item_id,
    it_creative_name,
    selected_promotion_name,
    ip_sf_treatment_code,
    ip_sf_campaign_mnemonic,
    platform
ORDER BY events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 2: Daily banner events for PCD — PRODUCTION TRACKER
-- ---------------------------------------------------------------------------
-- Daily tracker with confirmed PCD promo names. Filters on all 4 confirmed
-- tags. Once Query 1 reveals which GA4 field the tags land in, narrow the
-- filter to just that field and drop the others.
--
-- POST-LAUNCH TODO:
--   - Confirm which field (it_item_name vs selected_promotion_name) to keep
--   - Confirm event_name values for view vs click, then pivot into columns:
--       Banner Views  = COUNT where event_name = <view event>
--       Banner Clicks = COUNT where event_name = <click event>
--       Banner CTR    = Clicks / Views
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    -- event_date is already YYYY-MM-DD string in GA4
    event_date,
    event_name,
    platform,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(*)                       AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter — launch ~2026-03-25
    year = '2026'
    -- CONFIRMED promo names (Rajani Singineedi, 2026-03-18)
    -- Searching both candidate fields until Query 1 confirms which one
    AND (
           it_item_name IN (
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
               'PB_CC_ALL_26_02_RBC_PCD_PPCN',
               'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
           )
        OR selected_promotion_name IN (
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
               'PB_CC_ALL_26_02_RBC_PCD_PPCN',
               'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
           )
    )
    -- POST-LAUNCH: once Query 1 confirms the field, drop the OR branch
    -- and optionally add: AND ip_sf_campaign_mnemonic LIKE 'PB_CC_ALL_26_02_RBC_PCD_%'
GROUP BY
    event_date,
    event_name,
    platform
ORDER BY event_date DESC;


-- ---------------------------------------------------------------------------
-- QUERY 3: PCD table — understand the tactic population and test/control split
-- ---------------------------------------------------------------------------
-- Goal: identify which tactic_id_parent is the async PCD upgrade campaign,
-- and understand how test vs control is segmented (act_ctl_seg, test_value).
-- This tells us the "Available PCD Leads" denominator for test and control.
--
-- Run this in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    tactic_id_parent,
    test_description,
    test_value,
    act_ctl_seg,
    test_groups_period,
    COUNT(DISTINCT clnt_no) AS clients,
    COUNT(*)                AS rows_count
FROM dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE
    response_start >= '2025-01-01'
GROUP BY
    tactic_id_parent,
    test_description,
    test_value,
    act_ctl_seg,
    test_groups_period
ORDER BY clients DESC
LIMIT 50;


-- ---------------------------------------------------------------------------
-- QUERY 4 (BONUS): Check join feasibility — can we link GA4 users to PCD clients?
-- ---------------------------------------------------------------------------
-- Hypothesis: ep_srf_id2 in the GA4 ecommerce table (or user_id) maps to
-- clnt_no in the PCD table. If this works, we can join banner engagement
-- back to the PCD population to measure test-vs-control banner CTR.
--
-- Step 1: See what ep_srf_id2 / user_id look like for PCD banner events.
-- Step 2 (not here): attempt a JOIN on CAST(ep_srf_id2 AS BIGINT) = clnt_no.
--
-- Run this in Trino AFTER ~2026-03-25.
-- ---------------------------------------------------------------------------

SELECT
    ep_srf_id2,
    user_id,
    COUNT(*)                       AS events,
    COUNT(DISTINCT user_pseudo_id) AS devices
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year = '2026'
    -- CONFIRMED promo names — same dual-field search as Query 2
    AND (
           it_item_name IN (
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
               'PB_CC_ALL_26_02_RBC_PCD_PPCN',
               'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
           )
        OR selected_promotion_name IN (
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
               'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
               'PB_CC_ALL_26_02_RBC_PCD_PPCN',
               'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
           )
    )
    -- Only rows where we have an identifier to join on
    AND ep_srf_id2 IS NOT NULL
GROUP BY
    ep_srf_id2,
    user_id
ORDER BY events DESC
LIMIT 50;
