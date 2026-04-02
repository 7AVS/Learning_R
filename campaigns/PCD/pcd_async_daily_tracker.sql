-- =============================================================================
-- PCD Async Banner — Daily Performance Tracker (Production)
-- =============================================================================
--
-- Purpose:
--   Daily tracker for PCD Cards Upgrade async banners. Reports the four
--   metrics requested by Daniel Chin (NBA-12268):
--     1. Available PCD Leads (test vs control)
--     2. Banner Views
--     3. Banner Clicks
--     4. Banner CTR
--
-- Context:
--   - Jira: NBA-12268 (requested by Daniel Chin)
--   - Launch: ~2026-04-20 (async deployment)
--   - Promo names confirmed by Rajani Singineedi (2026-03-18)
--   - Field mapping validated via ESV EDA (2026-04-01)
--
-- Architecture:
--   Data lives in TWO systems — queries must be run separately and joined
--   in the dashboard or Excel.
--
--   TERADATA (EDW):
--     - DTZV01.TACTIC_EVNT_IP_AR_H60M → deployed population, tactic IDs
--     - dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp → test/control split
--     Delivers: Metric 1 (Available PCD Leads by test/control)
--
--   STARBURST (GA4):
--     - edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
--     Delivers: Metrics 2-4 (Banner Views, Clicks, CTR)
--
-- Confirmed PCD async promo names (it_item_name in GA4):
--   1. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP
--   2. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV
--   3. PB_CC_ALL_26_02_RBC_PCD_PPCN
--   4. PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner
--
-- Field decisions (from GA4 EDA 2026-04-01):
--   - Banner ID field: it_item_name
--   - View event: view_promotion
--   - Click event: select_promotion
--   - Campaign filter: ip_sf_campaign_mnemonic = 'PCD'
--
-- =============================================================================


-- #############################################################################
-- SECTION A: TERADATA (EDW) — Available PCD Leads + Test/Control
-- #############################################################################
-- Run these in Teradata. These queries deliver Metric 1.
-- #############################################################################


-- ---------------------------------------------------------------------------
-- A1: Discover PCD async tactic IDs (run ONCE post-deployment)
-- ---------------------------------------------------------------------------
-- Find the tactic ID for the async deployment that launched ~April 20.
-- TACTIC_ID positions 8-10 = MNE. For PCD: SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
--
-- Look for the tactic ID with TREATMT_STRT_DT on or after 2026-04-20.
-- That's the async deployment.
--
-- Run in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3)       AS mnemonic,
    TST_GRP_CD,
    RPT_GRP_CD,
    MIN(TREATMT_STRT_DT)          AS first_treatment_date,
    MAX(TREATMT_END_DT)           AS last_treatment_date,
    COUNT(DISTINCT CLNT_NO)       AS unique_clients,
    COUNT(*)                      AS total_events
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
    AND TREATMT_STRT_DT >= '2026-04-01'
GROUP BY
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3),
    TST_GRP_CD,
    RPT_GRP_CD
ORDER BY first_treatment_date DESC;


-- ---------------------------------------------------------------------------
-- A2: Daily Available PCD Leads by Test/Control (Metric 1)
-- ---------------------------------------------------------------------------
-- Once you identify the correct tactic ID from A1, plug it in here.
-- This gives the daily denominator: how many clients were deployed,
-- split by test vs control (TST_GRP_CD).
--
-- Replace '<<TACTIC_ID>>' with the actual tactic ID from A1.
--
-- Run in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    EVNT_DT                       AS event_date,
    TST_GRP_CD                    AS test_control,
    RPT_GRP_CD                    AS report_group,
    COUNT(DISTINCT CLNT_NO)       AS available_leads,
    COUNT(*)                      AS total_events
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    TACTIC_ID = '<<TACTIC_ID>>'
GROUP BY
    EVNT_DT,
    TST_GRP_CD,
    RPT_GRP_CD
ORDER BY
    event_date,
    test_control;


-- ---------------------------------------------------------------------------
-- A3: PCD Decision/Response — Test/Control Population Summary
-- ---------------------------------------------------------------------------
-- Alternative view of test/control from the PCD decision/response table.
-- Uses act_ctl_seg and test_value for segmentation.
-- Cross-reference with A2 to validate population counts match.
--
-- Run in Teradata.
-- ---------------------------------------------------------------------------

SELECT
    tactic_id_parent,
    act_ctl_seg,
    test_value,
    test_description,
    mnemonic,
    COUNT(DISTINCT clnt_no)       AS unique_clients,
    COUNT(DISTINCT acct_no)       AS unique_accounts,
    MIN(response_start)           AS earliest_response_start,
    MAX(response_end)             AS latest_response_end
FROM dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE
    response_start >= '2026-04-01'
GROUP BY
    tactic_id_parent,
    act_ctl_seg,
    test_value,
    test_description,
    mnemonic
ORDER BY unique_clients DESC;


-- ---------------------------------------------------------------------------
-- A4: Client-Level Extract for Join with GA4
-- ---------------------------------------------------------------------------
-- Pull the client list with test/control assignment. Export this to join
-- with GA4 data (in Excel or in HDFS/PySpark when that pipeline is built).
--
-- Replace '<<TACTIC_ID>>' with the actual tactic ID from A1.
--
-- Run in Teradata. Export results for cross-system join.
-- ---------------------------------------------------------------------------

SELECT DISTINCT
    CLNT_NO,
    TACTIC_ID,
    TST_GRP_CD                    AS test_control,
    RPT_GRP_CD                    AS report_group,
    TREATMT_STRT_DT               AS treatment_start,
    TREATMT_END_DT                AS treatment_end
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    TACTIC_ID = '<<TACTIC_ID>>';


-- #############################################################################
-- SECTION B: STARBURST (GA4) — Banner Views, Clicks, CTR
-- #############################################################################
-- Run these in Starburst. These queries deliver Metrics 2, 3, and 4.
-- #############################################################################


-- ---------------------------------------------------------------------------
-- B1: Daily Banner Performance — Views, Clicks, CTR by Platform (Metrics 2-4)
-- ---------------------------------------------------------------------------
-- Primary production query. Pivot view_promotion and select_promotion
-- into columns for a clean daily report.
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
-- B2: Daily Banner Performance by Promo Name
-- ---------------------------------------------------------------------------
-- Same as B1 but broken out by individual promo name (it_item_name).
-- Shows which specific banner/placement drives the most traffic.
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
-- B3: Cumulative Summary Since Launch
-- ---------------------------------------------------------------------------
-- Roll-up across the full post-launch period for weekly/monthly reporting.
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


-- ---------------------------------------------------------------------------
-- B4: Client-Level GA4 Extract for Join with Teradata
-- ---------------------------------------------------------------------------
-- Pull GA4 users who interacted with PCD async banners. The ep_srf_id2
-- field may map to clnt_no in Teradata (not yet validated — see note).
--
-- Export this and join with A4 (Teradata client extract) to get:
--   - Test clients who viewed/clicked (test group response)
--   - Control clients who viewed/clicked (should be zero — leakage check)
--
-- NOTE: The ep_srf_id2 → clnt_no mapping has NOT been validated.
-- Run this post-launch and check whether ep_srf_id2 values look like
-- client numbers (numeric, reasonable length). If they don't match,
-- check user_id as an alternative.
-- ---------------------------------------------------------------------------

SELECT
    ep_srf_id2,
    user_id,
    user_pseudo_id,
    CAST(event_date AS DATE)      AS event_date,
    event_name,
    it_item_name                  AS promo_name,
    platform
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
    AND ep_srf_id2 IS NOT NULL
ORDER BY
    event_date,
    ep_srf_id2;


-- #############################################################################
-- SECTION C: RECONCILIATION — Cross-System Validation
-- #############################################################################


-- ---------------------------------------------------------------------------
-- Reconciliation Workflow
-- ---------------------------------------------------------------------------
-- The tracker output combines results from BOTH systems. To assemble
-- Daniel's four metrics:
--
-- METRIC 1 — Available PCD Leads (test vs control):
--   Source: Query A2 (Teradata)
--   Output: Daily count of deployed clients by TST_GRP_CD
--
-- METRIC 2 — Banner Views:
--   Source: Query B1 (Starburst) → view_users / view_events columns
--
-- METRIC 3 — Banner Clicks:
--   Source: Query B1 (Starburst) → click_users / click_events columns
--
-- METRIC 4 — Banner CTR:
--   Source: Query B1 (Starburst) → ctr_users_pct column
--   Or: click_users / view_users * 100
--
-- CROSS-SYSTEM JOIN (for test/control breakdown of views/clicks):
--   1. Export A4 (Teradata) → client list with test/control flags
--   2. Export B4 (Starburst) → client-level GA4 interactions
--   3. Join on CAST(ep_srf_id2 AS BIGINT) = CLNT_NO (if mapping validates)
--   4. This gives: views/clicks/CTR split by test vs control
--
-- VALIDATION CHECKS:
--   - A2 deployed clients >= B1 view_users (if not, tagging leak)
--   - Control group in B4 should have zero/near-zero banner events
--   - If control clients show banner views, flag as contamination
-- ---------------------------------------------------------------------------
