-- pcl_experiment_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED
--   2026-08-10; `mne` ADDED 2026-08-10 as first column). Emits EXACTLY 8 columns: mne VARCHAR(3)
--   [CAST('PCL' AS VARCHAR(3)) — constant, campaign mnemonic], cohort_month VARCHAR(7) 'YYYY-MM',
--   segment VARCHAR(20) [CAST('All' AS VARCHAR(20)) — constant, no pre-treatment split above the
--   Champion/Challenger modal arm], grp VARCHAR(20) [binary], vintage_day INTEGER (0..90
--   continuous), base INTEGER (fixed per cohort x segment x grp), responders INTEGER,
--   responders_cum INTEGER. Counts only.
--
-- NOTE: mne is the campaign mnemonic only. This file shares its mne with
--   pp_pcl_campaign.sql. If both are stacked into one cube they are not
--   distinguishable by mne alone - keep them on separate sheets, or add a
--   scope column.
--
-- SCOPE: *** EXPERIMENT *** — PLI sales-modal challenger/champion split ONLY.
--   (The CAMPAIGN-scope sibling is pcl_campaign.sql — whole PCL campaign, no modal filter.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_pli_decision_resp
--            (Modal-experiment logic ported from campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql
--            and p10_vintage_curves.sql, which run on Starburst/Trino against dw00_im.dl_mr_prod.
--            Same underlying table, same columns — re-pointed here at Teradata-direct per contract
--            rule 8. The CONVERSION metric (responder_cli/dt_cl_change) lives entirely on the
--            curated row and needs no GA4 join, so it is engine-portable; p10's second metric,
--            ENGAGEMENT (first GA4 modal view), requires a GA4/Trino federation join and is
--            deliberately NOT carried into this file — see Success note below.)
-- Grain    : client (clnt_no) — matches p9/p10, NOT acct_no (differs from the campaign-scope file,
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
-- *** deployment DROPPED, 2026-08-10 *** — was parent_tactic_id. Curve grain is now COHORT MONTH.
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
--      ORDER BY treatmt_strt_dt ASC) = 1 — first-touch wave wins grp and becomes Day 0.
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
--   campaigns/sales_modal/pcl/modal_item_id_lookup.md) — NOT the same unresolved tst_grp_cd
--   question that blocks the campaign-scope file. This mapping is treated as CONFIRMED.
--
-- Success: responder_cli = 1 (CLI response flag); event date = dt_cl_change (already absolute).
--   This is the CONVERSION metric only (matches p10's 'conversion' metric and the campaign-scope
--   file's metric — one shared primary success definition across both PCL files). p10's second
--   metric, ENGAGEMENT, is NOT included: contract rule 4 caps this file at one success metric,
--   engagement needs a GA4/Trino join that is out of scope for a Teradata-direct file.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcl_exp_cells;
--   DROP TABLE vt_pcl_exp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_exp_cells AS (
    WITH raw_rows AS (
        SELECT
            clnt_no,
            treatmt_strt_dt,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
            AS VARCHAR(7))                                AS cohort_month,
            CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
                 WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
            END                                            AS grp
        FROM DL_MR_PROD.cards_pli_decision_resp
        WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
          AND treatmt_strt_dt >= DATE '2026-01-01'
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
        SELECT clnt_no, cohort_month, grp
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

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_exp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_exp_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
raw_rows AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
             WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
        END                                            AS grp,
        CASE WHEN responder_cli = 1 THEN dt_cl_change END AS success_dt_abs
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
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
    CAST('PCL' AS VARCHAR(3))                               AS mne,
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

DROP TABLE vt_pcl_exp_cells;
DROP TABLE vt_pcl_exp_spine;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             clnt_no,
--             CAST(
--                 CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
--                 CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
--                 CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
--                  WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
--             END AS grp
--         FROM DL_MR_PROD.cards_pli_decision_resp
--         WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
--           AND treatmt_strt_dt >= DATE '2026-01-01'
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
