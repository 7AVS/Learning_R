-- q04_conversion_by_platform.sql
-- Purpose: Q04 headline cannibalization gap (Action vs Control PCL conversion)
--          broken down by mobile platform (iOS / Android / no_impression).
-- Population: IDENTICAL to 04_ci_on_cannibalization_gap.sql — PCL-mobile leads,
--             treatmt_strt_dt >= 2024-10-01, channel LIKE '%MB%', CRV overlap via
--             date-range semi-join on cards_crv_install_decis_resp.
-- Arm definition: overlap_action_flag=1 → Action (CRV IM deployed, action_control='Action')
--                 overlap_control_flag=1 → Control (action_control='Control')
--                 both flags = 0 → no_overlap (excluded from gap calculation; included here
--                 for completeness and denominator audit).
-- Platform: GA4 mobile banner events (PCL item IDs, platform IN ('IOS','ANDROID')) joined
--           to each lead within its treatmt_strt_dt – treatmt_end_dt window.
--
-- DENOMINATOR INTEGRITY:
--   One row per (clnt_no × treatmt_strt_dt × treatmt_end_dt) — matching the PCL lead grain.
--   Platform is collapsed to a single label per lead via the ranked-CTE pattern below.
--   Collapse rule: if a lead has events on both iOS and Android within the treatment window,
--   the platform with the LOWER RANK wins: IOS=1, ANDROID=2. This is a deterministic
--   tie-break; empirically rare but must be enforced so SUM(leads) across platforms equals
--   the total lead count in Q04 exactly. COALESCE(platform,'no_impression') handles leads
--   with no mobile GA4 event — they land in the 'no_impression' bucket, not dropped.
--   ∴  SUM(leads) over platform_label = total arm population (no drop, no double-count).
--
-- CAVEATS (read before interpreting):
--   1. GA4 platform coverage begins ~Dec 2025. PCL waves from Oct–Nov 2024 will appear
--      as 'no_impression' not because of non-exposure but because GA4 data does not exist
--      for that window. The no_impression bucket is therefore inflated for early cohorts.
--   2. iOS/Android buckets CONDITION ON mobile engagement (post-treatment). Within-platform
--      gaps are DESCRIPTIVE, not causal. Engaged users self-select; compare cautiously.
--   3. The full-population pooled gap (Action vs Control regardless of platform) from Q04
--      remains the causal headline. This breakdown is a diagnostic layer, not a replacement.
--   4. no_impression = leads with zero captured mobile banner events in GA4 within their
--      treatment window. CRV serving suppression cannot affect these leads — they are a
--      key diagnostic: if the gap persists in no_impression, the mechanism is NOT banner
--      display interference.
--   5. PCL item IDs reused from verified list in view_to_conversion.sql / 28_view_to_click_journey.sql.
--      CRV item IDs are NOT filtered here — we care only about the PCL banner for platform detection
--      (consistent with the channel LIKE '%MB%' mobile deployment filter on the lead itself).

WITH
-- ── 1. PCL lead universe (identical to Q04) ──────────────────────────────────
pcl_universe AS (
    SELECT
        clnt_no,
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),

-- ── 2. CRV Action / Control populations (identical to Q04) ───────────────────
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),

-- ── 3. Overlap flags (LEFT JOIN pattern instead of EXISTS to carry acct_no forward) ─
-- We need acct_no/clnt_no to join GA4 so we switch to the LEFT JOIN pattern.
-- Semantics identical to Q04 EXISTS: date-range overlap, deduped at acct_no level.
crv_action_keys AS (
    SELECT DISTINCT acct_no
    FROM crv_action
),
crv_control_keys AS (
    SELECT DISTINCT acct_no
    FROM crv_control
),
-- Check overlap for each PCL lead individually (date-range match, not just acct_no).
-- Replicates Q04 EXISTS logic: offer window overlaps treatment window.
pcl_action_overlap AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_action ca
      ON ca.acct_no          = p.acct_no
     AND ca.offer_start_date <= p.treatmt_end_dt
     AND ca.offer_end_date   >= p.treatmt_strt_dt
),
pcl_control_overlap AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_control cc
      ON cc.acct_no          = p.acct_no
     AND cc.offer_start_date <= p.treatmt_end_dt
     AND cc.offer_end_date   >= p.treatmt_strt_dt
),
pcl_flagged AS (
    SELECT
        p.clnt_no,
        p.acct_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt,
        p.responder_cli,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_control_flag
    FROM pcl_universe p
    LEFT JOIN pcl_action_overlap oa
      ON oa.acct_no        = p.acct_no
     AND oa.treatmt_strt_dt = p.treatmt_strt_dt
     AND oa.treatmt_end_dt  = p.treatmt_end_dt
    LEFT JOIN pcl_control_overlap oc
      ON oc.acct_no        = p.acct_no
     AND oc.treatmt_strt_dt = p.treatmt_strt_dt
     AND oc.treatmt_end_dt  = p.treatmt_end_dt
),

-- ── 4. GA4 PCL mobile banner events ──────────────────────────────────────────
-- Filter to PCL item IDs only (platform detection is about PCL exposure, not CRV).
-- platform IN ('IOS','ANDROID') — no NULL rows enter here; no_impression is handled
-- downstream by the LEFT JOIN + COALESCE.
ga4_pcl_events AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        platform
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE ((year = '2024' AND month IN ('10','11','12'))
        OR (year = '2025' AND month IN ('01','02','03','04','05','06','07','08','09','10','11','12'))
        OR (year = '2026' AND month IN ('01','02','03','04','05','06')))
      AND event_date >= DATE '2024-10-01'
      AND it_item_id IN (
            'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
            'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
          )
      AND event_name IN ('view_promotion','select_promotion')
      AND platform IN ('IOS','ANDROID')
),

-- ── 5. Per-lead platform candidates (within treatment window) ─────────────────
-- One row per (clnt_no × treatmt_strt_dt × treatmt_end_dt × platform).
-- A lead that triggered both iOS and Android events within the window yields 2 rows here.
lead_platform_candidates AS (
    SELECT DISTINCT
        f.clnt_no,
        f.treatmt_strt_dt,
        f.treatmt_end_dt,
        g.platform
    FROM pcl_flagged f
    INNER JOIN ga4_pcl_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
),

-- ── 6. Collapse to single platform per lead (denominator integrity) ───────────
-- Tie-break rule: IOS=1 wins over ANDROID=2. Deterministic; empirically rare.
-- ROW_NUMBER() requires no QUALIFY (Trino syntax) — filter in outer WHERE.
lead_platform_ranked AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        platform,
        ROW_NUMBER() OVER (
            PARTITION BY clnt_no, treatmt_strt_dt, treatmt_end_dt
            ORDER BY CASE platform WHEN 'IOS' THEN 1 WHEN 'ANDROID' THEN 2 ELSE 3 END
        ) AS rn
    FROM lead_platform_candidates
),
lead_platform AS (
    -- Keep only the single winning platform per lead.
    -- Leads absent from lead_platform_candidates (no GA4 event) are NOT in this CTE —
    -- they get NULL when LEFT JOINed below, then COALESCE → 'no_impression'.
    SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt, platform
    FROM lead_platform_ranked
    WHERE rn = 1
),

-- ── 7. Attach platform to each PCL lead (LEFT JOIN preserves all leads) ───────
-- NULL platform → lead had no PCL mobile GA4 event within window → 'no_impression'.
pcl_with_platform AS (
    SELECT
        f.clnt_no,
        f.acct_no,
        f.treatmt_strt_dt,
        f.treatmt_end_dt,
        f.responder_cli,
        f.overlap_action_flag,
        f.overlap_control_flag,
        COALESCE(lp.platform, 'no_impression') AS platform_label
    FROM pcl_flagged f
    LEFT JOIN lead_platform lp
      ON lp.clnt_no        = f.clnt_no
     AND lp.treatmt_strt_dt = f.treatmt_strt_dt
     AND lp.treatmt_end_dt  = f.treatmt_end_dt
)

-- ── 8. Final output ───────────────────────────────────────────────────────────
-- GROUP BY arm × platform × responder_cli. Counts only — no rate columns.
-- User computes: conversion rate = SUM(leads WHERE responder_cli=1) / SUM(leads) per arm×platform.
-- Denominator check: SUM(leads) over platform_label within each arm = total arm leads from Q04.
SELECT
    CASE
        WHEN overlap_action_flag  = 1 THEN 'overlap_action'
        WHEN overlap_control_flag = 1 THEN 'overlap_control'
        ELSE                               'no_overlap'
    END                        AS overlap_status,
    platform_label,            -- 'IOS', 'ANDROID', or 'no_impression'
    responder_cli,             -- 1 = converted, 0 = did not convert
    COUNT(*)                   AS leads
FROM pcl_with_platform
GROUP BY
    CASE
        WHEN overlap_action_flag  = 1 THEN 'overlap_action'
        WHEN overlap_control_flag = 1 THEN 'overlap_control'
        ELSE                               'no_overlap'
    END,
    platform_label,
    responder_cli
ORDER BY
    overlap_status,
    platform_label,
    responder_cli
;
