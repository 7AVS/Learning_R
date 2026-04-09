-- =============================================================================
-- O2P Async Banner — Daily Performance Tracker (Production)
-- =============================================================================
--
-- Purpose:
--   Unified CTE query delivering all four metrics in a single result set:
--     1. Available O2P Leads (test vs control)
--     2. Banner Views
--     3. Banner Clicks
--     4. Banner CTR
--
-- Context:
--   - Jira: NBA-12268 (same async tracker initiative as PCD and CTU)
--   - NBA load final prod file: April 10, 2026
--   - Live to clients in mobile: April 13, 2026
--   - Async launch exposure: Week of April 6 (25% exposure — validate
--     engagement and CTR before full ramp)
--   - Lead on async side: Unknown (Tracey Eadie noted "Not sure the O2P lead")
--   - Promo names confirmed by Rajani Singineedi
--
-- NOTE: Only 1 promo tag confirmed — additional tags may be provided by
-- Rajani post-launch. Update the IN list when received.
--
-- Tables (all accessible from EDW — Teradata/Starburst same system):
--   - DTZV01.TACTIC_EVNT_IP_AR_H60M — deployed population, test/control
--   - edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce — GA4 banner events
--
-- NOTE: No O2P decision/response table is known at this time. The
-- decision/response cross-check section is omitted. Add it if a table
-- is identified post-launch.
--
-- Confirmed O2P async promo names (it_item_name):
--   1. PB_CHEQ_ALL_26_02_RBC_O2P_Pre-approved_overdraft_campaign
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
-- Find the O2P async tactic ID from the April 10+ deployment.
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
    SUBSTR(TACTIC_ID, 8, 3) = 'O2P'
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
    -- CTE 1: Deployed O2P population with test/control assignment
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
            'PB_CHEQ_ALL_26_02_RBC_O2P_Pre-approved_overdraft_campaign'
            -- NOTE: Additional promo tags may follow from Rajani post-launch.
            -- Add them here when confirmed.
        )
        AND g.ip_sf_campaign_mnemonic = 'O2P'
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
-- POST-LAUNCH VALIDATION CHECKLIST
-- ---------------------------------------------------------------------------
-- 1. Run discovery query → identify tactic ID for async deployment
-- 2. Replace '<<TACTIC_ID>>' in production query
-- 3. Run production query → check if results come back
-- 4. If banner_events CTE returns zero rows:
--    a. Check if promo names match (run ga4 query without join)
--    b. Check if ep_srf_id2 → CLNT_NO join produces matches
--    c. Try user_id instead of ep_srf_id2 as join key
-- 5. Control group should show zero/near-zero banner events
--    (if not, flag as contamination)
-- ---------------------------------------------------------------------------
