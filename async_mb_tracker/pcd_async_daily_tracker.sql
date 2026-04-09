-- =============================================================================
-- PCD Async Banner — Daily Performance Tracker (Production)
-- =============================================================================
--
-- Purpose:
--   Unified CTE query delivering all four metrics requested by Daniel Chin
--   (NBA-12268) in a single result set:
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
-- Tables (all accessible from EDW — Teradata/Starburst same system):
--   - DTZV01.TACTIC_EVNT_IP_AR_H60M — deployed population, test/control
--   - dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp — PCD decision/response
--   - edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce — GA4 banner events
--
-- Confirmed PCD async promo names (it_item_name):
--   1. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP
--   2. PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV
--   3. PB_CC_ALL_26_02_RBC_PCD_PPCN
--   4. PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner
--
-- Join key: CAST(ep_srf_id2 AS BIGINT) = CLNT_NO
--   *** NOT YET VALIDATED — run post-launch to confirm ***
--
-- Run in: Starburst (Trino-compatible SQL, accesses all EDW tables)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- DISCOVERY QUERY (run ONCE post-deployment to find the tactic ID)
-- ---------------------------------------------------------------------------
-- Find the PCD async tactic ID from the April 20+ deployment.
-- Once identified, replace '<<TACTIC_ID>>' in the production query below.
--
-- Run this first, then plug the tactic ID into the main query.
-- ---------------------------------------------------------------------------

SELECT
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3)       AS mnemonic,
    TST_GRP_CD,
    RPT_GRP_CD,
    TREATMT_STRT_DT,
    TREATMT_END_DT,
    COUNT(DISTINCT CLNT_NO)       AS unique_clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
    AND TREATMT_STRT_DT >= DATE '2026-04-01'
GROUP BY
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3),
    TST_GRP_CD,
    RPT_GRP_CD,
    TREATMT_STRT_DT,
    TREATMT_END_DT
ORDER BY TREATMT_STRT_DT DESC;


-- ---------------------------------------------------------------------------
-- PRODUCTION QUERY — All 4 Metrics in One Result Set
-- ---------------------------------------------------------------------------
-- Replace '<<TACTIC_ID>>' with the actual tactic ID from the discovery query.
-- Adjust month partition filter as the campaign runs.
--
-- Output columns:
--   event_date | test_control | report_group | available_leads |
--   view_users | view_events | click_users | click_events | ctr_pct
--
-- Run in Starburst.
-- ---------------------------------------------------------------------------

WITH tactic_pop AS (
    -- CTE 1: Deployed PCD population with test/control assignment
    SELECT
        CLNT_NO,
        TST_GRP_CD,
        RPT_GRP_CD,
        TREATMT_STRT_DT,
        TREATMT_END_DT
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE
        TACTIC_ID = '<<TACTIC_ID>>'
),

pop_summary AS (
    -- CTE 2: Available leads count per test/control group (denominator)
    SELECT
        TST_GRP_CD                    AS test_control,
        RPT_GRP_CD                    AS report_group,
        COUNT(DISTINCT CLNT_NO)       AS available_leads
    FROM tactic_pop
    GROUP BY
        TST_GRP_CD,
        RPT_GRP_CD
),

banner_events AS (
    -- CTE 3: GA4 banner events joined to tactic population
    -- Only includes clients who were deployed (INNER JOIN to tactic_pop)
    -- This gives us test/control attribution for each banner interaction
    SELECT
        g.event_date,
        g.event_name,
        g.ep_srf_id2,
        g.it_item_name,
        g.platform,
        t.TST_GRP_CD                 AS test_control,
        t.RPT_GRP_CD                 AS report_group
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    INNER JOIN tactic_pop t
        ON CAST(g.ep_srf_id2 AS BIGINT) = t.CLNT_NO
    WHERE
        g.year = '2026'
        AND g.month IN ('04', '05', '06')
        AND g.it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
        )
        AND g.ip_sf_campaign_mnemonic = 'PCD'
        AND g.event_name IN ('view_promotion', 'select_promotion')
),

daily_metrics AS (
    -- CTE 4: Aggregate daily banner metrics per test/control group
    SELECT
        event_date,
        test_control,
        report_group,
        COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'
                            THEN ep_srf_id2 END)                  AS view_users,
        COUNT(CASE WHEN event_name = 'view_promotion'
                   THEN 1 END)                                    AS view_events,
        COUNT(DISTINCT CASE WHEN event_name = 'select_promotion'
                            THEN ep_srf_id2 END)                  AS click_users,
        COUNT(CASE WHEN event_name = 'select_promotion'
                   THEN 1 END)                                    AS click_events
    FROM banner_events
    GROUP BY
        event_date,
        test_control,
        report_group
)

-- Final output: all 4 metrics joined together
SELECT
    d.event_date,
    d.test_control,
    d.report_group,
    p.available_leads,
    d.view_users,
    d.view_events,
    d.click_users,
    d.click_events,
    ROUND(
        CAST(d.click_users AS DOUBLE)
        / NULLIF(d.view_users, 0) * 100,
        2
    )                                                             AS ctr_pct
FROM daily_metrics d
JOIN pop_summary p
    ON d.test_control = p.test_control
    AND d.report_group = p.report_group
ORDER BY
    d.event_date,
    d.test_control;


-- ---------------------------------------------------------------------------
-- SUPPLEMENTARY: Daily by Promo Name (optional drill-down)
-- ---------------------------------------------------------------------------
-- Same as above but adds promo name breakdown.
-- Uses the same CTEs — copy tactic_pop and pop_summary from above,
-- then replace banner_events/daily_metrics with this version.
-- ---------------------------------------------------------------------------

-- To add promo-level detail, add it_item_name to the GROUP BY in
-- daily_metrics and the final SELECT. Example:
--
--   SELECT
--       d.event_date,
--       d.test_control,
--       d.it_item_name AS promo_name,
--       p.available_leads,
--       d.view_users,
--       d.click_users,
--       ROUND(CAST(d.click_users AS DOUBLE) / NULLIF(d.view_users, 0) * 100, 2) AS ctr_pct
--   ...


-- ---------------------------------------------------------------------------
-- SUPPLEMENTARY: PCD Decision/Response Cross-Check
-- ---------------------------------------------------------------------------
-- Cross-reference tactic event population with the PCD decision/response
-- table to validate test/control counts match.
-- Run separately after the main query.
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
    response_start >= DATE '2026-04-01'
GROUP BY
    tactic_id_parent,
    act_ctl_seg,
    test_value,
    test_description,
    mnemonic
ORDER BY unique_clients DESC;


-- ---------------------------------------------------------------------------
-- POST-LAUNCH VALIDATION CHECKLIST
-- ---------------------------------------------------------------------------
-- 1. Run discovery query → identify tactic ID for async deployment
-- 2. Replace '<<TACTIC_ID>>' in production query
-- 3. Run production query → check if results come back
-- 4. If banner_events CTE returns zero rows:
--    a. Check if promo names match (run ga4 query without join)
--    b. Check if ep_srf_id2 → CLNT_NO join produces matches
--    c. Try user_id instead of ep_srf_id2 as join key
-- 5. Run decision/response cross-check → validate population counts
-- 6. Control group should show zero/near-zero banner events
--    (if not, flag as contamination)
-- ---------------------------------------------------------------------------
