-- pcq_experiment_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED
--   2026-08-10). Emits EXACTLY 7 columns: cohort_month VARCHAR(7) 'YYYY-MM', segment VARCHAR(20)
--   [CAST('All' AS VARCHAR(20)) — constant, no pre-treatment split above Champion/Challenger
--   Modal Sales arm], grp VARCHAR(20) [binary], vintage_day INTEGER (0..90 continuous), base
--   INTEGER (fixed per cohort x segment x grp), responders INTEGER, responders_cum INTEGER.
--   Counts only.
--
-- SCOPE: *** EXPERIMENT *** — PCQ Modal Sales (MS) champion/challenger split ONLY.
--   (The CAMPAIGN-scope sibling is pcq_campaign.sql — whole PCQ campaign, test_group_latest
--   filter dropped.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_tpa_pcq_decision_resp
--            (Population + success logic ported from campaigns/sales_modal/pcq/pcq_ms_vintage.sql
--            and vintages/pcq_vintage_monthly.sql — both already EXPERIMENT-scoped.)
-- Grain    : client (clnt_no)
-- Anchor   : treatmt_start_dt (treatment start), per wave.
--
-- Population filter:
--   tpa_ita = 'TPA' (mandatory, canon: reference_pcq_measurement_filters.md — PCQ has no ITA arm)
--   AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
--   AND treatmt_start_dt >= DATE '2026-01-01' (contract floor)
--   decsn_year = 2026 DROPPED — same reasoning as the campaign-scope file.
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
--   1. cohort_first: QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no, cohort_month
--      ORDER BY treatmt_start_dt ASC) = 1 — first-touch wave wins grp and becomes Day 0.
--   2. Success is reconstructed to an absolute date (treatmt_start_dt + days_to_respond, only
--      when app_approved=1 AND Period-ASC) and pooled across EVERY NG3 wave the client touched
--      that month — success_pooled takes MIN(success_dt_abs), then rebases to the first-touch
--      anchor date. A client who didn't respond on wave 1 but did on wave 2 (same month) is no
--      longer silently dropped, and base is not inflated by counting them once per wave.
--
-- grp: NG3_CHMP -> 'Champion'; NG3_CHLN and NG3_CHLG -> 'Challenger', taken from the client's
--   FIRST-TOUCH wave this cohort_month. Confirmed test-group codes per
--   campaigns/sales_modal/pcq/pcq_ms_vintage.sql header. NG3_CHLG is small (~2-4k/wave).
--
-- *** CRITICAL — assignment vs delivery, do not swap *** (campaigns/sales_modal/README.md,
--   "OPEN DECISIONS" #2): `test_group_latest` is the design ASSIGNMENT (pre-treatment,
--   intent-to-treat) and is what `grp` is built from here. `ms_targeted` is the tactic-EVENT
--   delivery flag — POST-treatment — and is NOT used anywhere in this file. Conditioning
--   population or arm assignment on a post-treatment variable would invalidate any causal/lift
--   read off this curve.
--
-- Success: app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'. Period-ASC gates the
--   NUMERATOR only — `base` stays ALL targeted clients in the cell, not just responders.
--   Absolute success date = treatmt_start_dt + days_to_respond (both fields live on the same
--   curated row). This is the 'approved' metric — the source pcq_ms_vintage.sql also tracks
--   'completed' and a decile_scope slicer; both dropped here per contract rule 4.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcq_exp_cells;
--   DROP TABLE vt_pcq_exp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_exp_cells AS (
    WITH raw_rows AS (
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
            END                                            AS grp
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE tpa_ita           = 'TPA'
          AND treatmt_start_dt  >= DATE '2026-01-01'
          AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
        SELECT clnt_no, cohort_month, grp
        FROM raw_rows
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month ORDER BY treatmt_start_dt ASC
        ) = 1
    )
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_exp_cells COLUMN (cohort_month, grp);

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
raw_rows AS (
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
        -- SUCCESS = Period-ASC attributed only; gates the NUMERATOR only (see header note)
        CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'
             THEN treatmt_start_dt + days_to_respond
        END                                            AS success_dt_abs
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE tpa_ita           = 'TPA'
      AND treatmt_start_dt  >= DATE '2026-01-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT clnt_no, cohort_month, grp, treatmt_start_dt AS anchor_dt
    FROM raw_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort_month ORDER BY treatmt_start_dt ASC
    ) = 1
),

-- pool success across every NG3 wave the client touched this cohort_month
success_pooled AS (
    SELECT clnt_no, cohort_month, MIN(success_dt_abs) AS success_dt_abs
    FROM raw_rows
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
    FROM vt_pcq_exp_cells c
    CROSS JOIN vt_pcq_exp_spine s
)

SELECT
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

DROP TABLE vt_pcq_exp_cells;
DROP TABLE vt_pcq_exp_spine;

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
--         WHERE tpa_ita = 'TPA' AND treatmt_start_dt >= DATE '2026-01-01'
--           AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
