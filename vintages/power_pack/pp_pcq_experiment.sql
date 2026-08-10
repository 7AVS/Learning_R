-- pcq_experiment_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). Emits EXACTLY 7 columns:
--   cohort_month VARCHAR(7) 'YYYY-MM', deployment VARCHAR(30), grp VARCHAR(20) [binary],
--   vintage_day INTEGER (0..90 continuous), base INTEGER (fixed per cohort x deployment x grp),
--   responders INTEGER, responders_cum INTEGER. Counts only, no rates.
--
-- SCOPE: *** EXPERIMENT *** — PCQ Modal Sales (MS) champion/challenger split ONLY.
--   (The CAMPAIGN-scope sibling is pcq_campaign_vintage.sql — whole PCQ campaign, test_group_latest
--   filter dropped.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_tpa_pcq_decision_resp
--            (Population + success logic ported from campaigns/sales_modal/pcq/pcq_ms_vintage.sql
--            and vintages/pcq_vintage_monthly.sql — both already EXPERIMENT-scoped; this file
--            restates them under the new 7-column contract.)
-- Grain    : client (clnt_no)
-- Anchor   : treatmt_start_dt (treatment start), per deployment.
--
-- Population filter:
--   tpa_ita = 'TPA' (mandatory, canon: reference_pcq_measurement_filters.md — PCQ has no ITA arm)
--   AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
--   AND treatmt_start_dt >= DATE '2026-01-01' (contract floor; source files used >= 2026-06-01 —
--       widened here per contract rule 6; if the MS wave only ran from mid-2026, earlier
--       cohort_months simply will not appear in the data)
--   decsn_year = 2026 (present in both source files) DROPPED here — same reasoning as the
--   campaign-scope file: it contradicts the >= 2026-01-01 floor and is not documented as a
--   structural requirement anywhere in this repo.
--
-- grp: NG3_CHMP -> 'Champion'; NG3_CHLN and NG3_CHLG -> 'Challenger'. Confirmed test-group codes
--   per campaigns/sales_modal/pcq/pcq_ms_vintage.sql header ("confirmed from tactic table
--   DG6V01.TACTIC_EVNT_IP_AR_HIST, 2026-06-19"). NG3_CHLG is small (~2-4k/wave) per that same note.
--
-- *** CRITICAL — assignment vs delivery, do not swap *** (campaigns/sales_modal/README.md,
--   "OPEN DECISIONS" #2): `test_group_latest` is the design ASSIGNMENT (pre-treatment,
--   intent-to-treat) and is what `grp` is built from here. `ms_targeted` is the tactic-EVENT
--   delivery flag — it is POST-treatment (whether the modal actually fired for that client) and
--   is NOT used anywhere in this file, for grp or for filtering. Conditioning population or arm
--   assignment on a post-treatment variable (ms_targeted) would invalidate any causal/lift read
--   off this curve. If a delivery-truth descriptive read is ever needed, that is a different
--   file/question (see pcq_ms_vs_benchmark.sql's ms_targeted split) — not this one.
--
-- Deployment: tactic_id, TRIM'd, CAST VARCHAR(30). Curated table is one row per (clnt_no,
--   deployment) so COUNT(DISTINCT clnt_no) per (cohort_month, deployment, grp) is a direct
--   aggregate — curves never pool across deployments (contract rule).
--
-- Success: app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'. Known PCQ measurement
--   rule (canon: reference_pcq_measurement_filters.md, and restated in pcq_ms_vintage.sql's own
--   header): Period-ASC gates the NUMERATOR only — it is NOT in the population WHERE clause, so
--   `base` stays ALL targeted clients in the (cohort_month, deployment, grp) cell, not just
--   responders. Event day = days_to_respond (precomputed on the same row). This is the 'approved'
--   metric — the source pcq_ms_vintage.sql also tracks 'completed' as a second metric and a
--   decile_scope slicer (overall/top5); both dropped here per contract rule 4 (one metric per
--   file) and the contract's ban on slicer dimensions.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days. (pcq_ms_vintage.sql
--   used a dynamic MAX(days_to_respond)-based spine length; fixed at 0-90 here for consistency
--   with the other 3 contract files and vintages/pcq_vintage_monthly.sql.)
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcq_exp_cells;
--   DROP TABLE vt_pcq_exp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x deployment x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_exp_cells AS (
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(TRIM(tactic_id) AS VARCHAR(30))           AS deployment,
        CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
             WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
        END                                            AS grp,
        COUNT(DISTINCT clnt_no)                        AS base
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE tpa_ita           = 'TPA'
      AND treatmt_start_dt  >= DATE '2026-01-01'
      AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; silently returns nothing from 2027-01-01. Revisit before FY27.
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_exp_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_exp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_exp_spine COLUMN (vintage_day);

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
        -- SUCCESS = Period-ASC attributed only; gates the NUMERATOR only (see header note)
        CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'
             THEN days_to_respond
        END                                            AS vintage_day_raw
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE tpa_ita           = 'TPA'
      AND treatmt_start_dt  >= DATE '2026-01-01'
      AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; silently returns nothing from 2027-01-01. Revisit before FY27.
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day_raw AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM population
    WHERE vintage_day_raw BETWEEN 0 AND 90
    GROUP BY cohort_month, deployment, grp, vintage_day_raw
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_pcq_exp_cells c
    CROSS JOIN vt_pcq_exp_spine s
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

DROP TABLE vt_pcq_exp_cells;
DROP TABLE vt_pcq_exp_spine;
