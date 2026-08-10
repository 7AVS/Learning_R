-- pcq_experiment_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED
--   2026-08-10; `mne` ADDED 2026-08-10 as first column). Emits EXACTLY 8 columns: mne VARCHAR(20)
--   [CAST('PCQ Sales Modal' AS VARCHAR(20))  campaign mnemonic, or the experiment name where the
--   file measures an experiment], cohort_month VARCHAR(7) 'YYYY-MM',
--   segment VARCHAR(20) [CAST('All' AS VARCHAR(20))  constant, no pre-treatment split above
--   Champion/Challenger Modal Sales arm], grp VARCHAR(20) [binary], vintage_day INTEGER (0..90
--   continuous), base INTEGER (fixed per cohort x segment x grp), responders INTEGER,
--   responders_cum INTEGER. Counts only.
--
-- mne distinguishes this file from its campaign sibling (pp_pcq_campaign.sql), so both
--   can be stacked into one cube safely.
--
-- SCOPE: *** EXPERIMENT ***  PCQ Modal Sales (MS) champion/challenger split ONLY.
--   (The CAMPAIGN-scope sibling is pcq_campaign.sql  whole PCQ campaign, test_group_latest
--   filter dropped.)
--
-- ENGINE: Teradata-direct. Touches only Teradata tables (dl_mr_prod.cards_tpa_pcq_decision_resp)
--    no dw00 catalog prefix, no Trino functions (DATE_TRUNC/DATE_DIFF/DATE_ADD/UNNEST/SEQUENCE).
--   Uses QUALIFY for dedup and a SYS_CALENDAR-backed VOLATILE TABLE for the day spine  both
--   native Teradata, both unavailable on Trino/Starburst. (CRV and VBA are the exceptions in this
--   folder  they reach EDL and stay on Trino/Starburst.)
--
-- TABLE NAME: dl_mr_prod.cards_tpa_pcq_decision_resp  bare, no dw00_im catalog prefix.
--   Teradata-direct does not need a Starburst catalog prefix at all; the prior Trino build of
--   this file carried dw00_im.dl_mr_prod.*  that prefix is REMOVED here per Andre (2026-08-10):
--   "why are we including the dw00_im over there, it never works when I'm querying Teradata, I
--   always have to fix this. Just dl_mr_prod."
--
-- Source   : dl_mr_prod.cards_tpa_pcq_decision_resp
--            (Population + success logic ported from campaigns/sales_modal/pcq/pcq_ms_vintage.sql
--            and vintages/pcq_vintage_monthly.sql  both already EXPERIMENT-scoped  adapted here
--            for Teradata-direct execution.)
-- Grain    : client (clnt_no)
-- Anchor   : treatmt_start_dt (treatment start), per wave.
--
-- Population filter:
--   tpa_ita = 'TPA' (mandatory, canon: reference_pcq_measurement_filters.md  PCQ has no ITA arm)
--   AND TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
--   AND treatmt_start_dt >= DATE '2026-01-01' (contract floor)
--   decsn_year = 2026 RESTORED 2026-08-10  it is in pcq_ms_vintage.sql:53 and was lost here.
--
-- *** RECONCILING AGAINST THE DASHBOARD ***
--   If this does not tie to the dashboard, the <<WINDOW>> filter is the first thing to check,
--   not the success definition. The success gate here is identical to pcq_ms_vintage.sql:
--   app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC', numerator only.
--
--   What differs is POPULATION. pcq_ms_vintage.sql has NO treatmt_end_dt filter at all  it
--   takes treatmt_start_dt >= DATE '2026-06-01' and nothing else. The dashboard, per Andre,
--   just reports by "month" with no stated deployment-end rule, which reads as the same
--   no-end-filter behaviour.
--
--   This file adds an end-date window because the Power Pack question is "campaigns that ENDED
--   in Q3". That is a deliberately different population, so it will NOT equal the dashboard.
--
--   TO RECONCILE: comment out the two <<WINDOW>> lines in BOTH WHERE clauses below and set the
--   start floor to DATE '2026-06-01' to match pcq_ms_vintage.sql. That should reproduce the
--   dashboard population. Re-enable the window afterwards for the Q3 pack.
--
-- *** deployment DROPPED, 2026-08-10 ***  was tactic_id. Curve grain is now COHORT MONTH.
--
-- DEDUP IDENTIFIER: clnt_no  this file's grain and success are both clnt_no throughout.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it  expect zeros. Dedup uses QUALIFY ROW_NUMBER() ... = 1 (Teradata-native), not a
--   ranked subquery with an outer WHERE rn = 1.
--
-- *** DEDUP  one row per (clnt_no, cohort_month), anchored on first wave ***
--   1. cohort_first: QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no, cohort_month
--      ORDER BY treatmt_start_dt ASC) = 1  first-touch wave wins grp and becomes Day 0.
--   2. Success is reconstructed to an absolute date (treatmt_start_dt + days_to_respond, only
--      when app_approved=1 AND Period-ASC) and pooled across EVERY NG3 wave the client touched
--      that month  success_pooled takes MIN(success_dt_abs), then rebases to the first-touch
--      anchor date. A client who didn't respond on wave 1 but did on wave 2 (same month) is no
--      longer silently dropped, and base is not inflated by counting them once per wave.
--
-- grp: NG3_CHMP -> 'Champion'; NG3_CHLN and NG3_CHLG -> 'Challenger', taken from the client's
--   FIRST-TOUCH wave this cohort_month. Confirmed test-group codes per
--   campaigns/sales_modal/pcq/pcq_ms_vintage.sql header. NG3_CHLG is small (~2-4k/wave).
--
-- *** CRITICAL  assignment vs delivery, do not swap *** (campaigns/sales_modal/README.md,
--   "OPEN DECISIONS" #2): `test_group_latest` is the design ASSIGNMENT (pre-treatment,
--   intent-to-treat) and is what `grp` is built from here. `ms_targeted` is the tactic-EVENT
--   delivery flag  POST-treatment  and is NOT used anywhere in this file. Conditioning
--   population or arm assignment on a post-treatment variable would invalidate any causal/lift
--   read off this curve.
--
-- Success: app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'. Period-ASC gates the
--   NUMERATOR only  `base` stays ALL targeted clients in the cell, not just responders.
--   Absolute success date = treatmt_start_dt + days_to_respond (both fields live on the same
--   curated row)  plain Teradata date + integer-days arithmetic; Trino needed
--   DATE_ADD('day', ...) for this, Teradata does not. This is the 'approved' metric  the source
--   pcq_ms_vintage.sql also tracks 'completed' and a decile_scope slicer; both dropped here per
--   contract rule 4.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days. Built as a Teradata
--            VOLATILE TABLE off SYS_CALENDAR.CALENDAR, not UNNEST(SEQUENCE(...))  that is a
--            Trino-only function. TDWM blocks an unconstrained product join against SYS_CALENDAR,
--            so both the spine AND the denominator cells (vt_pcq_exp_cells) are materialized as
--            VOLATILE TABLEs with COLLECT STATISTICS before the CROSS JOIN that builds the grid.
-- ----------------------------------------------------------------------------
-- SCOPE: this file is scoped to deployments ENDING in the quarter window below
--   (population filtered on treatmt_end_dt, confirmed column on
--   dl_mr_prod.cards_tpa_pcq_decision_resp  see value_capture/value_capture_report_v3.sql:95-98
--   which uses the identical pattern). cohort_month and day-0 still anchor on treatmt_start_dt
--   (the START column)  unchanged. Success (app_approved/asc_on_app_source, success date =
--   treatmt_start_dt + days_to_respond) is read from the SAME curated row as population, so the
--   end-date filter below tightens both the population AND the success-side scan in one change 
--   no separate event table to bound here. Retargeting a quarter = editing the two <<WINDOW>>
--   literals below only.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW  EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in the window.
--   Cohort month and day 0 still anchor on treatmt_start_dt (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

-- ============================================================================
-- RERUN GUARD  if re-running this file in the SAME Teradata session, the volatile tables
-- below will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_pcq_exp_spine;
--   DROP TABLE vt_pcq_exp_cells;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_pcq_exp_cells below without
-- tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_pcq_exp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_pcq_exp_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (cohort_month x grp x base). VOLATILE for the same TDWM reason  it is the
-- other side of the spine CROSS JOIN. Rebuilds raw_rows -> cohort_first internally; the same CTE
-- chain is repeated in the main query below (Teradata volatile-table creation is a standalone
-- statement, it cannot see CTEs defined outside it).
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_pcq_exp_cells AS (
    WITH
    raw_rows AS (
        SELECT
            clnt_no,
            treatmt_start_dt,
            CAST(
              CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
              CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
              CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
            AS VARCHAR(7))                          AS cohort_month,
            CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
                 WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
            END                                      AS grp
        FROM dl_mr_prod.cards_tpa_pcq_decision_resp
        WHERE tpa_ita           = 'TPA'
          AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; returns nothing from 2027-01-01
          AND treatmt_start_dt  >= DATE '2026-01-01'                            -- floor guard
          AND treatmt_end_dt    >= DATE '2026-05-01'                            -- <<WINDOW>>
          AND treatmt_end_dt    <  DATE '2026-08-01'                            -- <<WINDOW>>
          AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire  see header)
        SELECT clnt_no, cohort_month, grp, treatmt_start_dt AS anchor_dt
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

-- ----------------------------------------------------------------------------
-- Main query  numerator side rebuilds the same raw_rows/cohort_first chain (regular CTEs are
-- fine here; only the spine CROSS JOIN needed the volatile-table workaround), pools success,
-- then joins the dense grid off the two volatile tables built above.
-- ----------------------------------------------------------------------------
WITH
raw_rows AS (
    SELECT
        clnt_no,
        treatmt_start_dt,
        CAST(
          CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
          CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
          CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
        AS VARCHAR(7))                          AS cohort_month,
        CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
             WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
        END                                      AS grp,
        -- SUCCESS = Period-ASC attributed only; gates the NUMERATOR only (see header note)
        CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'
             THEN treatmt_start_dt + days_to_respond
        END                                      AS success_dt_abs
    FROM dl_mr_prod.cards_tpa_pcq_decision_resp
    WHERE tpa_ita           = 'TPA'
      AND decsn_year        = 2026  -- [LANDMINE] hard-coded year; returns nothing from 2027-01-01
      AND treatmt_start_dt  >= DATE '2026-01-01'                            -- floor guard
      AND treatmt_end_dt    >= DATE '2026-05-01'                            -- <<WINDOW>>
      AND treatmt_end_dt    <  DATE '2026-08-01'                            -- <<WINDOW>>
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire  see header)
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
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('PCQ Sales Modal' AS VARCHAR(20))                  AS mne,
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

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             clnt_no,
--             CAST(
--               CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) || '-' ||
--               CASE WHEN EXTRACT(MONTH FROM treatmt_start_dt) < 10 THEN '0' ELSE '' END ||
--               CAST(EXTRACT(MONTH FROM treatmt_start_dt) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CASE WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(20))
--                  WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(20))
--             END AS grp
--         FROM dl_mr_prod.cards_tpa_pcq_decision_resp
--         WHERE tpa_ita = 'TPA' AND treatmt_start_dt >= DATE '2026-01-01'
--           AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
