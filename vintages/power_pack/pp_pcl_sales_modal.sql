-- pcl_experiment_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED
--   2026-08-10; `mne` ADDED 2026-08-10 as first column). Emits EXACTLY 8 columns: mne VARCHAR(20)
--   [CAST('PCL Sales Modal' AS VARCHAR(20))  campaign mnemonic, or the experiment name where the
--   file measures an experiment], cohort_month VARCHAR(7) 'YYYY-MM',
--   segment VARCHAR(20) [CAST('All' AS VARCHAR(20))  constant, no pre-treatment split above the
--   Champion/Challenger modal arm], grp VARCHAR(20) [binary], vintage_day INTEGER (0..90
--   continuous), base INTEGER (fixed per cohort x segment x grp), responders INTEGER,
--   responders_cum INTEGER. Counts only.
--
-- mne distinguishes this file from its campaign sibling (pp_pcl_campaign.sql), so both
--   can be stacked into one cube safely.
--
-- SCOPE: *** EXPERIMENT ***  PLI sales-modal challenger/champion split ONLY.
--   (The CAMPAIGN-scope sibling is pcl_campaign.sql  whole PCL campaign, no modal filter.)
--
-- ENGINE: Teradata-direct. Touches only Teradata tables (dl_mr_prod.cards_pli_decision_resp) 
--   no dw00 catalog prefix, no Trino functions (DATE_TRUNC/DATE_DIFF/UNNEST/SEQUENCE). Uses
--   QUALIFY for dedup and a SYS_CALENDAR-backed VOLATILE TABLE for the day spine  both native
--   Teradata, both unavailable on Trino/Starburst. (CRV and VBA are the exceptions in this
--   folder  they reach EDL and stay on Trino/Starburst.)
--
-- TABLE NAME: dl_mr_prod.cards_pli_decision_resp  bare, no dw00_im catalog prefix. Teradata-
--   direct does not need a Starburst catalog prefix at all; the prior Trino build of this file
--   carried dw00_im.dl_mr_prod.*  that prefix is REMOVED here per Andre (2026-08-10): "why are
--   we including the dw00_im over there, it never works when I'm querying Teradata, I always
--   have to fix this. Just dl_mr_prod."
--
-- Source   : dl_mr_prod.cards_pli_decision_resp
--            (Modal-experiment logic ported from campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql
--            and p10_vintage_curves.sql, adapted here for Teradata-direct execution. The
--            CONVERSION metric (responder_cli/dt_cl_change) lives entirely on the curated row and
--            needs no GA4 join; p10's second metric, ENGAGEMENT (first GA4 modal view), requires a
--            separate GA4/EDL join and is deliberately NOT carried into this file  see Success
--            note below, and it would force this file onto Trino/Starburst if it were added.)
-- Grain    : client (clnt_no)  matches p9/p10, NOT acct_no (differs from the campaign-scope file,
--            which is account-grain; the modal experiment's population CTEs in p9/p10 are built
--            on clnt_no).
-- Anchor   : treatmt_strt_dt (treatment start), consistent with p9/p10 and the campaign-scope file.
--
-- Population filter (verbatim from p9/p10, task-specified):
--   WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
--     AND treatmt_strt_dt >= DATE '2026-01-01'   -- contract floor (p9/p10 used >= 2026-05-01;
--                                                    widened here per contract rule 6)
--   NOTE: strategy_id (BAU/NTC in p9/p10) is an ORTHOGONAL audience dimension, NOT the treatment
--   split. It is deliberately NOT used for grp and NOT filtered on here.
--
-- *** deployment DROPPED, 2026-08-10 ***  was parent_tactic_id. Curve grain is now COHORT MONTH.
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
--      ORDER BY treatmt_strt_dt ASC) = 1  first-touch wave wins grp and becomes Day 0.
--   2. Success (dt_cl_change, already an absolute date on the curated row when responder_cli=1)
--      is pooled across EVERY wave the client touched that month: success_pooled takes
--      MIN(dt_cl_change) across all the client's rows in the cohort_month, then rebases to the
--      first-touch anchor date. A client who didn't respond on wave 1 but did on wave 2 (same
--      month) is no longer silently dropped, and base is not inflated by counting them once per
--      wave.
--
-- grp: WMS (report_groups_period) -> 'Challenger' (modal served); NMS -> 'Champion' (no modal),
--   taken from the client's FIRST-TOUCH wave this cohort_month. This is the confirmed,
--   behavior-verified split from p7/p8 arm contrast (see
--   campaigns/sales_modal/pcl/modal_item_id_lookup.md)  NOT the same unresolved tst_grp_cd
--   question that blocks the campaign-scope file. This mapping is treated as CONFIRMED.
--
-- Success: responder_cli = 1 (CLI response flag); event date = dt_cl_change (already absolute).
--   This is the CONVERSION metric only (matches p10's 'conversion' metric and the campaign-scope
--   file's metric  one shared primary success definition across both PCL files). p10's second
--   metric, ENGAGEMENT, is NOT included: contract rule 4 caps this file at one success metric,
--   engagement needs a separate GA4 join that is out of scope for this file.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days. Built as a Teradata
--            VOLATILE TABLE off SYS_CALENDAR.CALENDAR, not UNNEST(SEQUENCE(...))  that is a
--            Trino-only function. TDWM blocks an unconstrained product join against SYS_CALENDAR,
--            so both the spine AND the denominator cells (vt_pcl_exp_cells) are materialized as
--            VOLATILE TABLEs with COLLECT STATISTICS before the CROSS JOIN that builds the grid.
-- ----------------------------------------------------------------------------
-- SCOPE: this file is scoped to deployments ENDING in the quarter window below
--   (population filtered on treatmt_end_dt, confirmed column on
--   schemas/crv_pcl_curated_schemas.md 3a). cohort_month and day-0 still anchor
--   on treatmt_strt_dt (the START column)  unchanged. Success (dt_cl_change) is
--   read from the SAME curated row as population, so the end-date filter below
--   tightens both the population AND the success-side scan in one change  no
--   separate event table to bound here. Retargeting a quarter = editing the two
--   <<WINDOW>> literals below only.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW  EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in the window.
--   Cohort month and day 0 still anchor on treatmt_strt_dt (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

-- ============================================================================
-- RERUN GUARD  if re-running this file in the SAME Teradata session, the volatile tables
-- below will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_pcl_exp_spine;
--   DROP TABLE vt_pcl_exp_cells;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_pcl_exp_cells below without
-- tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_pcl_exp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_pcl_exp_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (cohort_month x grp x base). VOLATILE for the same TDWM reason  it is the
-- other side of the spine CROSS JOIN. Rebuilds raw_rows -> cohort_first internally; the same CTE
-- chain is repeated in the main query below (Teradata volatile-table creation is a standalone
-- statement, it cannot see CTEs defined outside it).
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_pcl_exp_cells AS (
    WITH
    raw_rows AS (
        SELECT
            clnt_no,
            treatmt_strt_dt,
            CAST(
              CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
              CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
              CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
            AS VARCHAR(7))                          AS cohort_month,
            CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
                 WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
            END                                      AS grp
        FROM dl_mr_prod.cards_pli_decision_resp
        WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
          AND treatmt_strt_dt >= DATE '2026-01-01'                              -- floor guard
          AND treatmt_end_dt  >= DATE '2026-05-01'                              -- <<WINDOW>>
          AND treatmt_end_dt  <  DATE '2026-08-01'                              -- <<WINDOW>>
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire  see header)
        SELECT clnt_no, cohort_month, grp, treatmt_strt_dt AS anchor_dt
        FROM raw_rows
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_pcl_exp_cells COLUMN (cohort_month, grp);

-- ----------------------------------------------------------------------------
-- Main query  numerator side rebuilds the same raw_rows/cohort_first chain (regular CTEs are
-- fine here; only the spine CROSS JOIN needed the volatile-table workaround), pools success,
-- then joins the dense grid off the two volatile tables built above.
-- ----------------------------------------------------------------------------
WITH
raw_rows AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        CAST(
          CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
          CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
          CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                          AS cohort_month,
        CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
             WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
        END                                      AS grp,
        CASE WHEN responder_cli = 1 THEN dt_cl_change END AS success_dt_abs
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
      AND treatmt_strt_dt >= DATE '2026-01-01'                              -- floor guard
      AND treatmt_end_dt  >= DATE '2026-05-01'                              -- <<WINDOW>>
      AND treatmt_end_dt  <  DATE '2026-08-01'                              -- <<WINDOW>>
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire  see header)
    SELECT clnt_no, cohort_month, grp, treatmt_strt_dt AS anchor_dt
    FROM raw_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort_month ORDER BY treatmt_strt_dt ASC
    ) = 1
),

-- pool success across every wave the client touched this cohort_month
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
    FROM vt_pcl_exp_cells c
    CROSS JOIN vt_pcl_exp_spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('PCL Sales Modal' AS VARCHAR(20))                  AS mne,
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
--               CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
--               CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
--               CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
--                  WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
--             END AS grp
--         FROM dl_mr_prod.cards_pli_decision_resp
--         WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
--           AND treatmt_strt_dt >= DATE '2026-01-01'
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
