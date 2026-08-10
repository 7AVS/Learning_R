-- pcq_campaign_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). Emits EXACTLY 7 columns:
--   cohort_month VARCHAR(7) 'YYYY-MM', deployment VARCHAR(30), grp VARCHAR(20) [binary],
--   vintage_day INTEGER (0..90 continuous), base INTEGER (fixed per cohort x deployment x grp),
--   responders INTEGER, responders_cum INTEGER. Counts only, no rates.
--
-- SCOPE: *** CAMPAIGN *** — whole PCQ campaign. The test_group_latest IN (...) filter used by the
--   Modal Sales experiment file is DROPPED entirely, per task instruction.
--   (The EXPERIMENT-scope sibling is pcq_experiment_vintage.sql — Modal Sales NG3_* codes only.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_tpa_pcq_decision_resp
-- Grain    : client (clnt_no)
-- Anchor   : treatmt_start_dt (treatment start), per deployment.
--
-- Population filter: tpa_ita = 'TPA' (mandatory for every PCQ measurement query — PCQ has no ITA
--   arm; canon: reference_pcq_measurement_filters.md) AND treatmt_start_dt >= DATE '2026-01-01'
--   (contract floor). The test_group_latest filter is DROPPED — this file is campaign-wide, all
--   test groups, not just the Modal Sales NG3_CHMP/CHLN/CHLG set.
--   decsn_year = 2026 (present in the source vintages/pcq_vintage_monthly.sql and
--   campaigns/sales_modal/pcq/pcq_ms_vintage.sql) is also DROPPED here: it would silently exclude
--   any 2024/2025 cohort and directly contradicts the contract's >= 2026-01-01 floor. If
--   decsn_year in fact carries structural meaning beyond "current year" (e.g. required for
--   partition pruning), that was not documented anywhere found in this repo — flagging as a
--   judgment call, not a verified fact.
--
-- grp (arm) — *** [VERIFY] BLOCKING, PARTIAL COVERAGE ONLY — READ BEFORE TRUSTING grp OR base ***
--   Campaign-wide, test_group_latest is NOT limited to the 3 Modal Sales codes. The repo confirms
--   other/unrecognized values exist: value_capture/value_capture_report_v2.sql (and v2_cohort,
--   v2_deployment, v3) all compute a separate "n_unmapped" bucket for clients whose
--   test_group_latest never resolves to Champion/Challenger under the NG3_CHMP / NG3_CHLN+CHLG
--   mapping, and explicitly call out "NG3_CHLD and anything else unrecognised" as real, present
--   values. No repo source documents the FULL campaign-wide code list or how non-NG3 waves should
--   collapse to binary Test/Control — so this file does NOT guess one.
--   grp below maps ONLY the confirmed Modal Sales codes: NG3_CHMP -> 'Champion';
--   NG3_CHLN / NG3_CHLG -> 'Challenger'. Any client whose test_group_latest is something else
--   resolves to NULL and is FILTERED OUT of this file's population entirely (not shown as a third
--   bucket, since the contract requires grp to be strictly binary). PRACTICAL CONSEQUENCE: this
--   file's `base` is a floor, not the true whole-campaign population, for any cohort_month /
--   deployment where non-NG3 test groups ran. Before trusting this as a genuine "whole campaign"
--   cube, profile test_group_latest campaign-wide (no repo query does this today) to find what
--   other codes exist and confirm/extend the binary collapse rule.
--
-- Deployment: tactic_id, TRIM'd, CAST VARCHAR(30). Curated table is one row per (clnt_no,
--   deployment) so COUNT(DISTINCT clnt_no) per (cohort_month, deployment, grp) is a direct
--   aggregate — curves never pool across deployments (contract rule).
--
-- Success: app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'. Period-ASC gates the
--   NUMERATOR only (canon: reference_pcq_measurement_filters.md) — it is NOT in the population
--   WHERE clause, so `base` stays all TPA-targeted clients in the cell, not just responders.
--   Event day = days_to_respond (precomputed on the same row). This is the 'approved' metric only
--   (matches the primary metric already established in vintages/pcq_vintage_monthly.sql); the
--   source pcq_ms_vintage.sql also tracks 'completed' as a second metric — dropped here per
--   contract rule 4 (one success metric per file).
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcq_camp_cells;
--   DROP TABLE vt_pcq_camp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x deployment x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_camp_cells AS (
    WITH client_base AS (
        SELECT
            clnt_no,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
            AS VARCHAR(7))                                AS cohort_month,
            CAST(TRIM(tactic_id) AS VARCHAR(30))           AS deployment,
            CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
                 WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
            END                                            AS grp          -- [VERIFY] partial coverage, see header
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE tpa_ita           = 'TPA'
          AND treatmt_start_dt  >= DATE '2026-01-01'
          AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; silently returns nothing from 2027-01-01. Revisit before FY27.
    )
    SELECT cohort_month, deployment, grp, COUNT(DISTINCT clnt_no) AS base
    FROM client_base
    WHERE grp IS NOT NULL   -- unmapped test_group_latest codes excluded, see [VERIFY] header note
    GROUP BY cohort_month, deployment, grp
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_camp_cells COLUMN (cohort_month, deployment, grp);

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
population AS (
    SELECT
        clnt_no,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(TRIM(tactic_id) AS VARCHAR(30))           AS deployment,
        CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
             WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
        END                                            AS grp,
        CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'
             THEN days_to_respond
        END                                            AS vintage_day_raw
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE tpa_ita           = 'TPA'
      AND treatmt_start_dt  >= DATE '2026-01-01'
      AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; silently returns nothing from 2027-01-01. Revisit before FY27.
),

population_mapped AS (
    SELECT * FROM population WHERE grp IS NOT NULL   -- see [VERIFY] header note: unmapped codes dropped
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day_raw AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM population_mapped
    WHERE vintage_day_raw BETWEEN 0 AND 90
    GROUP BY cohort_month, deployment, grp, vintage_day_raw
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_pcq_camp_cells c
    CROSS JOIN vt_pcq_camp_spine s
)

SELECT
    g.cohort_month,
    g.deployment,
    g.grp,
    CAST(g.vintage_day AS INTEGER)                          AS vintage_day,
    CAST(g.base AS INTEGER)                                 AS base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER)              AS responders,
    CAST(
        SUM(COALESCE(dc.responders, 0)) OVER (
            PARTITION BY g.cohort_month, g.deployment, g.grp
            ORDER BY g.vintage_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    AS INTEGER)                                              AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.deployment    = g.deployment
    AND dc.grp           = g.grp
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_month, g.deployment, g.grp, g.vintage_day;

DROP TABLE vt_pcq_camp_cells;
DROP TABLE vt_pcq_camp_spine;
