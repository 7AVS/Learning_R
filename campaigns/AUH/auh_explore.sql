-- =============================================================================
-- AUH — Exploratory Queries
-- =============================================================================
--
-- Purpose:
--   Exploratory queries to learn AUH campaign data before Phase 2 launch.
--   Discover tactic MNE code, success event definition, and test/control groups.
--
-- Campaign:
--   Authorized User Acquisition (AUH)
--   Phase 1 (complete): Non-rewards cards, email only, ~190K clients, Feb 12 2026.
--     Reported significant lift in net authorized user adds over control.
--   Phase 2 (Apr 30 2026): Rewards cards (ADP, GPR, GCP, MC2). Email + OLB banner.
--     Points accelerator offers. Mobile banner CANNOT be fulfilled.
--
-- Status:
--   CONFIRMED (Phase 1):
--     - MNE = 'AUH' (SUBSTR(TACTIC_ID, 8, 3) = 'AUH')
--     - Tactic ID = '2026042AUH'
--     - Treatment dates: 2026-02-12 to 2026-03-12
--     - Product codes: PLT, CLO, MC1, MCP, VPR
--     - Success table: D3CV12A.ACCT_CRD_OWN_DLY_DELTA (relationship_cd='Z', card_sts IN ('A',''))
--     - Test groups: NRGA, NRR, NRS (test) / NRGA_C, NRR_C, NRS_C (control)
--     - Email disposition codes: 1=sent, 2=opened, 3=clicked, 4=unsubscribed, 5=hardbounce, 6=complaint
--   STILL UNKNOWN:
--     - Phase 2 OLB banner codes (launches Apr 30 2026)
--
-- DOE (Phase 2):
--   539.6K population, 10% control (53.96K), 90% contact (485.68K)
--   50/50 offer split nested within contact (242.84K each)
--
-- Tables:
--   1. DTZV01.TACTIC_EVNT_IP_AR_H60M       (Teradata via Trino — tactic event history)
--   2. DG6V01.CLNT_DERIV_DTA_HIST           (Teradata via Trino — segment / client derivations)
--   3. DDWV01.EXT_CDP_CHNL_EVNT             (Teradata via Trino — channel events)
--   4. DTZV01.VENDOR_FEEDBACK_MASTER         (Teradata via Trino — email master)
--   5. DTZV01.VENDOR_FEEDBACK_EVENT          (Teradata via Trino — email events)
--   6. ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce  (Trino — GA4 ecommerce)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Confirm AUH tactics in the tactic event history (CONFIRMED)
-- ---------------------------------------------------------------------------
-- CONFIRMED: MNE = 'AUH', Tactic ID = '2026042AUH'.
-- Keeping this query as a reference / validation tool.
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    SUBSTR(TACTIC_ID, 8, 3)         AS MNE,
    TACTIC_ID,
    TST_GRP_CD,
    RPT_GRP_CD,
    COUNT(DISTINCT CLNT_NO)          AS clients,
    MIN(TREATMT_STRT_DT)             AS min_start,
    MAX(TREATMT_STRT_DT)             AS max_start
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    TREATMT_STRT_DT >= DATE '2025-01-01'
    -- Wide net: any of these patterns might surface AUH
    AND (
           TACTIC_ID              LIKE '%AUH%'
        OR TACTIC_ID              LIKE '%AUTH%'
        OR TACTIC_ID              LIKE '%AUA%'
        OR TACTIC_DECISN_VRB_INFO LIKE '%AUTHORIZED%USER%'
        OR TACTIC_DECISN_VRB_INFO LIKE '%AUTH%USER%'
        OR TACTIC_DECISN_VRB_INFO LIKE '%AUH%'
    )
GROUP BY
    SUBSTR(TACTIC_ID, 8, 3),
    TACTIC_ID,
    TST_GRP_CD,
    RPT_GRP_CD
ORDER BY clients DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- QUERY 2: Profile the AUH Phase 1 population (CONFIRMED)
-- ---------------------------------------------------------------------------
-- Goal: validate the ~190K Phase 1 population and test/control groups.
-- CONFIRMED: MNE = 'AUH', Tactic ID = '2026042AUH'.
--   Test groups: NRGA, NRR, NRS (test) / NRGA_C, NRR_C, NRS_C (control).
--   Treatment dates: 2026-02-12 to 2026-03-12.
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    TST_GRP_CD,
    RPT_GRP_CD,
    COUNT(DISTINCT CLNT_NO)          AS clients,
    MIN(TREATMT_STRT_DT)             AS min_start,
    MAX(TREATMT_STRT_DT)             AS max_start
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE
    TREATMT_STRT_DT >= DATE '2026-01-01'
    AND TREATMT_STRT_DT <  DATE '2026-04-01'
    AND SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
GROUP BY
    TST_GRP_CD,
    RPT_GRP_CD
ORDER BY clients DESC;


-- ---------------------------------------------------------------------------
-- QUERY 3: Explore channel events around Phase 1 window (reference)
-- ---------------------------------------------------------------------------
-- CONFIRMED: Success is NOT from EXT_CDP_CHNL_EVNT. Success is measured via:
--   Table: D3CV12A.ACCT_CRD_OWN_DLY_DELTA
--   Filters: relationship_cd = 'Z', card_sts IN ('A', '')
--   Product codes: PLT, CLO, MC1, MCP, VPR
--
-- Keeping this query for general channel-event exploration around Phase 1.
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    ACTVY_TYP_CD,
    CHNL_TYP_CD,
    SRC_DTA_STORE_CD,
    COUNT(*)                         AS events,
    COUNT(DISTINCT CLNT_NO)          AS clients,
    MIN(CAPTR_DT)                    AS min_captr,
    MAX(CAPTR_DT)                    AS max_captr
FROM DDWV01.EXT_CDP_CHNL_EVNT
WHERE
    -- Narrow to Phase 1 window to keep scan manageable
    CAPTR_DT >= DATE '2026-02-01'
    AND CAPTR_DT <  DATE '2026-04-01'
    -- If too slow, first get the client list from Query 2 and add:
    --   AND CLNT_NO IN (SELECT CLNT_NO FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    --                    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH')
GROUP BY
    ACTVY_TYP_CD,
    CHNL_TYP_CD,
    SRC_DTA_STORE_CD
ORDER BY events DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- QUERY 4: Email performance for Phase 1 (CONFIRMED)
-- ---------------------------------------------------------------------------
-- Goal: get sent / delivered / opened / clicked counts for AUH Phase 1.
-- Phase 1 was email-only, so these are the primary channel metrics.
--
-- CONFIRMED disposition codes (DISPOSITION_CD):
--   1=sent, 2=opened, 3=clicked, 4=unsubscribed, 5=hardbounce, 6=complaint
-- Tactic ID = '2026042AUH'.
--
-- Run this in Trino.
-- ---------------------------------------------------------------------------

SELECT
    m.TACTIC_ID,
    e.EVENT_TYPE,
    COUNT(*)                         AS event_count,
    COUNT(DISTINCT m.CLNT_NO)        AS clients
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
LEFT JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
    ON m.FEEDBACK_ID = e.FEEDBACK_ID
WHERE
    SUBSTR(m.TACTIC_ID, 8, 3) = 'AUH'
    -- Limit to Phase 1 time window
    AND m.SEND_DT >= DATE '2026-02-01'
    AND m.SEND_DT <  DATE '2026-04-01'
GROUP BY
    m.TACTIC_ID,
    e.EVENT_TYPE
ORDER BY event_count DESC;


-- ---------------------------------------------------------------------------
-- QUERY 5: Phase 2 prep — OLB banner in GA4 (STILL UNKNOWN)
-- ---------------------------------------------------------------------------
-- Goal: skeleton query for OLB banner tracking when Phase 2 launches Apr 30.
-- Phase 2 adds OLB banner to email. Mobile banner CANNOT be fulfilled.
-- OLB banner codes are STILL UNKNOWN — keeping wide-net search.
-- MNE confirmed as 'AUH' so that filter is updated.
--
-- Run this in Trino. Not expected to return results until after Apr 30.
-- ---------------------------------------------------------------------------

SELECT
    event_name,
    it_item_name,
    it_item_id,
    it_creative_name,
    ip_sf_treatment_code,
    ip_sf_campaign_mnemonic,
    platform,
    COUNT(*)                         AS events
FROM ed10_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    -- Partition filter
    year = '2026'
    -- Wide net: banner codes still unknown — keeping broad search
    -- >>> REPLACE with confirmed banner codes once Phase 2 launches <<<
    AND (
           it_item_name             LIKE '%AUH%'
        OR it_item_name             LIKE '%AUTH%USER%'
        OR it_item_name             LIKE '%Authorized%'
        OR ip_sf_campaign_mnemonic  LIKE '%AUH%'
        OR ip_sf_treatment_code     LIKE '%2026042AUH%'
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
