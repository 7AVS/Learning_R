-- =============================================================================
-- PCD Async Banner — Daily Performance Tracker (TEST VERSION)
-- =============================================================================
--
-- PURPOSE OF THIS FILE:
--   This is a structural validation test, NOT a production query.
--   It uses the SAME CTE architecture as pcd_async_daily_tracker.sql
--   but points at EXISTING PCD data confirmed during March 2026 EDA.
--
--   Two things being validated:
--     1. CTE structure executes cleanly end-to-end in Starburst
--     2. ep_srf_id2 → CLNT_NO join actually produces matches
--
--   If this returns results with non-zero banner metrics and recognizable
--   test/control splits, the query skeleton is proven. Swap promo names
--   and tactic filter for production when the async deployment goes live.
--
-- WHAT IS DIFFERENT FROM PRODUCTION:
--   - it_item_name IN (...) → 6 existing PCD promo names (confirmed Mar 2026)
--   - TACTIC_ID = '<<TACTIC_ID>>' → SUBSTR(TACTIC_ID, 8, 3) = 'PCD' (broad)
--   - TREATMT_STRT_DT >= DATE '2025-01-01' to capture older deployments
--   - GA4 month filter = '03' (March 2026, confirmed populated)
--
-- WHAT IS IDENTICAL TO PRODUCTION:
--   - All table names and column names
--   - CTE chain: tactic_pop → pop_summary → banner_events → daily_metrics
--   - Join key: CAST(ep_srf_id2 AS BIGINT) = CLNT_NO
--   - ip_sf_campaign_mnemonic = 'PCD'
--   - event_name filter: view_promotion, select_promotion
--   - Output columns and CTR calculation
--
-- Existing promo names confirmed by EDA (March 2026):
--   - NBO_PB_CC_PCD_24_09_RBC_AVP_70K
--   - NBO-PB_CC_PCD_22_10_RBC_IOP-CreditCard-Upgrade
--   - NBO-PB_CC_PCD_22_10_RBC_ION-CreditCard-Upgrade
--   - NBO_PB_CC_PCD_24_09_RBC_IAV
--   - NBO_PB_CC_PCD_24_09_RBC_AVP_25K
--   - NBO_PB_CC_PCD_24_09_RBC_GCP
--
-- Run in: Starburst (Trino-compatible SQL)
-- =============================================================================


-- ---------------------------------------------------------------------------
-- DISCOVERY QUERY — Find existing PCD tactic IDs (run once to orient)
-- ---------------------------------------------------------------------------
-- Uses SUBSTR filter instead of a specific tactic ID.
-- TREATMT_STRT_DT >= 2025-01-01 to catch all older PCD deployments.
-- This tells us which tactic IDs exist and their group structure,
-- so we know what test_control / report_group values to expect in output.
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
    AND TREATMT_STRT_DT >= DATE '2025-01-01'
GROUP BY
    TACTIC_ID,
    SUBSTR(TACTIC_ID, 8, 3),
    TST_GRP_CD,
    RPT_GRP_CD,
    TREATMT_STRT_DT,
    TREATMT_END_DT
ORDER BY TREATMT_STRT_DT DESC;


-- ---------------------------------------------------------------------------
-- TEST QUERY — Same CTE structure as production, existing data
-- ---------------------------------------------------------------------------

WITH tactic_pop AS (
    -- CTE 1: Deployed PCD population with test/control assignment
    -- Broad filter: any PCD tactic starting 2025-01-01+
    -- Production equivalent uses TACTIC_ID = '<<TACTIC_ID>>'
    SELECT
        CLNT_NO,
        TST_GRP_CD,
        RPT_GRP_CD,
        TREATMT_STRT_DT,
        TREATMT_END_DT
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE
        SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
        AND TREATMT_STRT_DT >= DATE '2025-01-01'
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
    -- This is the key validation: does ep_srf_id2 → CLNT_NO produce matches?
    -- Month = '03' (March 2026) — confirmed populated in EDA
    -- Promo names = existing PCD names, not the new async names
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
        AND g.month IN ('03')
        AND g.it_item_name IN (
            'NBO_PB_CC_PCD_24_09_RBC_AVP_70K',
            'NBO-PB_CC_PCD_22_10_RBC_IOP-CreditCard-Upgrade',
            'NBO-PB_CC_PCD_22_10_RBC_ION-CreditCard-Upgrade',
            'NBO_PB_CC_PCD_24_09_RBC_IAV',
            'NBO_PB_CC_PCD_24_09_RBC_AVP_25K',
            'NBO_PB_CC_PCD_24_09_RBC_GCP'
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
-- Output columns match production exactly
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
-- VALIDATION INTERPRETATION
-- ---------------------------------------------------------------------------
-- If this returns results with proper test/control splits and non-zero
-- banner metrics, the query structure is validated. Swap promo names
-- and tactic ID for production.
--
-- Troubleshooting if banner_events returns zero rows:
--   a. Run the discovery query above — confirm tactic_pop has clients
--   b. Run GA4 standalone (no join) to confirm promo names fire in March
--   c. Check if ep_srf_id2 is populated for these promo names
--      (some promos may not carry srf_id2 — try user_pseudo_id as fallback)
--   d. Check ip_sf_campaign_mnemonic — confirm it is 'PCD' not 'pcd'
-- ---------------------------------------------------------------------------
