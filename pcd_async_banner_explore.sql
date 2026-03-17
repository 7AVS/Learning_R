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
--   Waiting for confirmed banner codes from Melissa's team.
--   The filters below (it_item_name, it_item_id, it_creative_name) are
--   placeholders based on patterns the team has used for similar campaigns.
--
-- Tables:
--   1. ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce  (Trino — GA4 ecommerce events)
--   2. ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow      (Trino — GA4 narrow events)
--   3. dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp                (Teradata — PCD decision/response)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Discover event names and banner patterns for PCD
-- ---------------------------------------------------------------------------
-- Goal: cast a wide net to find which event_name values correspond to
-- banner views vs clicks, and what the actual it_item_name / it_item_id /
-- it_creative_name codes look like for the PCD upgrade campaign.
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    event_name,
    it_item_name,
    it_item_id,
    it_creative_name,
    ip_sf_treatment_code,
    ip_sf_campaign_mnemonic,
    platform,
    COUNT(*) AS events
FROM ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter — keep the scan reasonable
    year IN ('2025', '2026')
    -- Wide net: any of these patterns might surface the PCD banner
    AND (
           it_item_name    LIKE '%PCD%'
        OR it_item_name    LIKE '%CreditCard_Upgrade%'
        OR it_creative_name LIKE '%CRTV%'
    )
GROUP BY
    event_name,
    it_item_name,
    it_item_id,
    it_creative_name,
    ip_sf_treatment_code,
    ip_sf_campaign_mnemonic,
    platform
ORDER BY events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 2: Daily banner events for PCD (skeleton — fill in once codes confirmed)
-- ---------------------------------------------------------------------------
-- Once Query 1 tells us the exact banner codes and which event_name means
-- "view" vs "click", plug them in here. This becomes the skeleton of the
-- daily tracker. We will pivot event_name into columns:
--   Banner Views  = COUNT where event_name = <view event>
--   Banner Clicks = COUNT where event_name = <click event>
--   Banner CTR    = Clicks / Views
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
FROM ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter
    year IN ('2025', '2026')
    -- >>> REPLACE with confirmed banner codes from Melissa's team <<<
    AND it_item_name LIKE 'NBO-PB_CC_PCD%'
    -- Optional: narrow further once known
    -- AND it_item_id       = 'I_113308'
    -- AND it_creative_name = 'CRTV-116596'
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
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    ep_srf_id2,
    user_id,
    COUNT(*)                       AS events,
    COUNT(DISTINCT user_pseudo_id) AS devices
FROM ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year IN ('2025', '2026')
    -- >>> REPLACE with confirmed banner codes <<<
    AND it_item_name LIKE 'NBO-PB_CC_PCD%'
    -- Only rows where we have an identifier to join on
    AND ep_srf_id2 IS NOT NULL
GROUP BY
    ep_srf_id2,
    user_id
ORDER BY events DESC
LIMIT 50;
