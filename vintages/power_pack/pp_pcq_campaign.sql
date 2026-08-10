-- pcq_campaign_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED
--   2026-08-10; `mne` ADDED 2026-08-10 as first column). Emits EXACTLY 8 columns: mne VARCHAR(3)
--   [CAST('PCQ' AS VARCHAR(3)) — constant, campaign mnemonic], cohort_month VARCHAR(7) 'YYYY-MM',
--   segment VARCHAR(20) [CAST('All' AS VARCHAR(20)) — constant, no pre-treatment split above
--   test_group_latest for PCQ], grp VARCHAR(20) [binary], vintage_day INTEGER (0..90 continuous),
--   base INTEGER (fixed per cohort x segment x grp), responders INTEGER, responders_cum INTEGER.
--   Counts only.
--
-- NOTE: mne is the campaign mnemonic only. This file shares its mne with
--   pp_pcq_sales_modal.sql. If both are stacked into one cube they are not
--   distinguishable by mne alone - keep them on separate sheets, or add a
--   scope column.
--
-- SCOPE: *** CAMPAIGN *** — whole PCQ campaign. The test_group_latest IN (...) filter used by the
--   Modal Sales experiment file is DROPPED entirely, per task instruction.
--   (The EXPERIMENT-scope sibling is pcq_sales_modal.sql — Modal Sales NG3_* codes only.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_tpa_pcq_decision_resp
-- Grain    : client (clnt_no)
-- Anchor   : treatmt_start_dt (treatment start), per wave.
--
-- Population filter: tpa_ita = 'TPA' (mandatory for every PCQ measurement query — PCQ has no ITA
--   arm; canon: reference_pcq_measurement_filters.md) AND treatmt_start_dt >= DATE '2026-01-01'
--   (contract floor). The test_group_latest filter is DROPPED — campaign-wide, all test groups,
--   not just the Modal Sales NG3_CHMP/CHLN/CHLG set. decsn_year = 2026 DROPPED — see [LANDMINE]
--   below, unchanged reasoning from the 8-column version.
--
-- *** deployment DROPPED, 2026-08-10 *** — was tactic_id. Curve grain is now COHORT MONTH.
--
-- DEDUP IDENTIFIER: clnt_no — this file's grain and success are both clnt_no throughout.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
--
-- *** DEDUP — one row per (clnt_no, cohort_month), anchored on first wave ***
--   1. mapped_rows: population filter + grp mapping, exactly as the 8-column version (unmapped
--      test_group_latest codes stay excluded from the population entirely — see [VERIFY] below).
--   2. cohort_first: QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no, cohort_month
--      ORDER BY treatmt_start_dt ASC) = 1 — first-touch wave wins grp and becomes Day 0.
--   3. Success is reconstructed to an absolute date (treatmt_start_dt + days_to_respond, only
--      when app_approved=1 AND Period-ASC) and pooled across EVERY mapped wave the client
--      touched that month — success_pooled takes MIN(success_dt_abs), then rebases to the
--      first-touch anchor date. A client who didn't respond on wave 1 but did on wave 2 (same
--      month) is no longer silently dropped, and base is not inflated by counting them once per
--      wave.
--
-- grp (arm) — *** [VERIFY] BLOCKING, PARTIAL COVERAGE ONLY — READ BEFORE TRUSTING grp OR base ***
--   Campaign-wide, test_group_latest is NOT limited to the 3 Modal Sales codes. The repo confirms
--   other/unrecognized values exist (n_unmapped bucket in value_capture_report_v2*.sql). grp below
--   maps ONLY the confirmed Modal Sales codes: NG3_CHMP -> 'Champion'; NG3_CHLN / NG3_CHLG ->
--   'Challenger'. Any client whose test_group_latest is something else resolves to NULL and is
--   FILTERED OUT of this file's population entirely. PRACTICAL CONSEQUENCE: this file's `base` is
--   a floor, not the true whole-campaign population, for any cohort_month where non-NG3 test
--   groups ran. Before trusting this as a genuine "whole campaign" cube, profile test_group_latest
--   campaign-wide (no repo query does this today).
--
-- Success: app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'. Period-ASC gates the
--   NUMERATOR only (canon: reference_pcq_measurement_filters.md) — it is NOT in the population
--   WHERE clause, so `base` stays all mapped TPA-targeted clients in the cell, not just
--   responders. Absolute success date = treatmt_start_dt + days_to_respond (both fields live on
--   the same curated row). This is the 'approved' metric only (matches the primary metric already
--   established in vintages/pcq_vintage_monthly.sql); the source pcq_ms_vintage.sql also tracks
--   'completed' as a second metric — dropped here per contract rule 4 (one success metric per
--   file).
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcq_camp_cells;
--   DROP TABLE vt_pcq_camp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_camp_cells AS (
    WITH mapped_rows AS (
        SELECT
            clnt_no,
            treatmt_start_dt,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
            AS VARCHAR(7))                                AS cohort_month,
            CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
                 WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
            END                                            AS grp          -- [VERIFY] partial coverage, see header
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE tpa_ita           = 'TPA'
          AND treatmt_start_dt  >= DATE '2026-01-01'
          AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; silently returns nothing from 2027-01-01. Revisit before FY27.
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
        SELECT clnt_no, cohort_month, grp
        FROM mapped_rows
        WHERE grp IS NOT NULL   -- unmapped test_group_latest codes excluded, see [VERIFY] header note
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month ORDER BY treatmt_start_dt ASC
        ) = 1
    )
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_camp_cells COLUMN (cohort_month, grp);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_camp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_camp_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
mapped_rows AS (
    SELECT
        clnt_no,
        treatmt_start_dt,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
             WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
        END                                            AS grp,
        CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'
             THEN treatmt_start_dt + days_to_respond
        END                                            AS success_dt_abs
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE tpa_ita           = 'TPA'
      AND treatmt_start_dt  >= DATE '2026-01-01'
      AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; silently returns nothing from 2027-01-01. Revisit before FY27.
),

population_mapped AS (
    SELECT * FROM mapped_rows WHERE grp IS NOT NULL   -- see [VERIFY] header note: unmapped codes dropped
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT clnt_no, cohort_month, grp, treatmt_start_dt AS anchor_dt
    FROM population_mapped
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort_month ORDER BY treatmt_start_dt ASC
    ) = 1
),

-- pool success across every mapped wave the client touched this cohort_month
success_pooled AS (
    SELECT clnt_no, cohort_month, MIN(success_dt_abs) AS success_dt_abs
    FROM population_mapped
    WHERE success_dt_abs IS NOT NULL
    GROUP BY clnt_no, cohort_month
),

population AS (
    SELECT
        cf.cohort_month, cf.grp, cf.clnt_no,
        CAST(sp.success_dt_abs - cf.anchor_dt AS INTEGER) AS vintage_day_raw
    FROM cohort_first cf
    LEFT JOIN success_pooled sp
        ON sp.clnt_no = cf.clnt_no AND sp.cohort_month = cf.cohort_month
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day_raw AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM population
    WHERE vintage_day_raw BETWEEN 0 AND 90
    GROUP BY cohort_month, grp, vintage_day_raw
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vt_pcq_camp_cells c
    CROSS JOIN vt_pcq_camp_spine s
)

SELECT
    CAST('PCQ' AS VARCHAR(3))                               AS mne,
    g.cohort_month,
    CAST('All' AS VARCHAR(20))                              AS segment,
    g.grp,
    CAST(g.vintage_day AS INTEGER)                          AS vintage_day,
    CAST(g.base AS INTEGER)                                 AS base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER)              AS responders,
    CAST(
        SUM(COALESCE(dc.responders, 0)) OVER (
            PARTITION BY g.cohort_month, g.grp
            ORDER BY g.vintage_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    AS INTEGER)                                              AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.grp           = g.grp
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;

DROP TABLE vt_pcq_camp_cells;
DROP TABLE vt_pcq_camp_spine;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             clnt_no,
--             CAST(
--                 CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
--                 CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
--                 CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
--                  WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
--             END AS grp
--         FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
--         WHERE tpa_ita = 'TPA' AND treatmt_start_dt >= DATE '2026-01-01' AND decsn_year = 2026
--     ) raw
--     WHERE grp IS NOT NULL
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
