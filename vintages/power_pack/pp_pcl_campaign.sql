-- pcl_campaign_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, segment column added
--   2026-08-10). Emits EXACTLY 8 columns: cohort_month VARCHAR(7) 'YYYY-MM', deployment
--   VARCHAR(30), segment VARCHAR(20) [CAST('All' AS VARCHAR(20)) — constant, no pre-treatment
--   split above tst_grp_cd for PCL], grp VARCHAR(20) [binary], vintage_day INTEGER
--   (0..90 continuous), base INTEGER (fixed per cohort x deployment x segment x grp),
--   responders INTEGER, responders_cum INTEGER. Counts only, no rates.
--
-- SCOPE: *** CAMPAIGN *** — whole PCL campaign. NO modal / sales-modal population filter.
--   (The EXPERIMENT-scope sibling is pcl_experiment_vintage.sql — sales-modal WMS/NMS only.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_pli_decision_resp
-- Grain    : account (acct_no)
-- Anchor   : treatmt_strt_dt (treatment start). [VERIFY] reliability of this field on THIS curated
--            table was never confirmed (see prior probe in vintages/pcl_vintage_monthly.sql header —
--            it flagged treatmt_strt_dt as unreliable on DG6V01.tactic_evnt_ip_ar_hist; unconfirmed
--            whether the same issue exists here). Not re-run here per task scope ("do not run
--            anything") — carried forward, not resolved.
-- Population filter: treatmt_strt_dt >= DATE '2026-01-01' only. No report_groups_period filter,
--            no tst_grp_cd exclusion — this is the WHOLE campaign, every arm, every deployment.
-- Deployment: parent_tactic_id, TRIM'd, CAST VARCHAR(30). Curves are grouped by
--            (cohort_month, deployment, grp) and never pooled across deployments (contract rule).
--            Because grouping is now per-deployment (not per-cohort-bin like the old
--            vintages/pcl_vintage_monthly.sql), the old "first-in-bin deployment wins" dedup logic
--            is retired: the curated table is one row per (acct_no, deployment), so
--            COUNT(DISTINCT acct_no) at (cohort_month, deployment, grp) grain is a direct, safe
--            aggregate — no tie-break needed.
--
-- grp (arm) — *** [VERIFY] BLOCKING, UNRESOLVED — READ BEFORE TRUSTING grp ON THIS FILE ***
--   PCL tst_grp_cd Test/Control code mapping is UNCONFIRMED anywhere in this repo. Two separate
--   files carry open TODOs on this exact point (campaigns/CRV/crv_pcl_overlap_summary.sql lines
--   33 & 74: "TODO: exclude PCL Control once tst_grp_cd code is confirmed"). No code value for
--   PCL Control/Test has ever been resolved in this codebase — profiling query C4 in that same
--   file was written to do it but has apparently never been run/logged.
--   The contract requires grp to be BINARY (Test/Control). Because the actual code->arm mapping
--   cannot be derived from anything in the repo, this file does NOT invent one. `grp` below is
--   TRIM(tst_grp_cd) passed through RAW, CAST to VARCHAR(20) — i.e. it will show whatever raw
--   codes exist (may be more than 2 values), NOT a true binary collapse. This is a deliberate
--   deviation from contract rule 5, flagged here rather than guessed.
--   BEFORE using this file's grp for any Test-vs-Control read: run
--   campaigns/CRV/crv_pcl_overlap_summary.sql §C4 (or equivalent) to get the actual code list,
--   then edit the two CASE-free `grp` expressions below into a real
--   CASE WHEN TRIM(tst_grp_cd) IN (...) THEN 'Test' WHEN ... THEN 'Control' END.
--
-- Success  : responder_cli = 1 (CLI response flag); event day = dt_cl_change - treatmt_strt_dt.
--            Same primary success metric as pcl_vintage_monthly.sql / pcl_experiment_vintage.sql —
--            preserved unchanged, not invented. One metric per file per contract rule 4.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcl_camp_cells;
--   DROP TABLE vt_pcl_camp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x deployment x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_camp_cells AS (
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(TRIM(parent_tactic_id) AS VARCHAR(30))    AS deployment,
        CAST(TRIM(tst_grp_cd) AS VARCHAR(20))          AS grp,          -- [VERIFY] raw pass-through, see header
        COUNT(DISTINCT acct_no)                        AS base
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND TRIM(tst_grp_cd) IS NOT NULL
      AND TRIM(tst_grp_cd) <> ''
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_camp_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_camp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_camp_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
population AS (
    SELECT
        acct_no,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(TRIM(parent_tactic_id) AS VARCHAR(30))    AS deployment,
        CAST(TRIM(tst_grp_cd) AS VARCHAR(20))          AS grp,          -- [VERIFY] raw pass-through, see header
        CASE WHEN responder_cli = 1
             THEN CAST(dt_cl_change - treatmt_strt_dt AS INTEGER)
        END                                            AS vintage_day_raw
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND TRIM(tst_grp_cd) IS NOT NULL
      AND TRIM(tst_grp_cd) <> ''
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day_raw AS vintage_day,
           COUNT(DISTINCT acct_no) AS responders
    FROM population
    WHERE vintage_day_raw BETWEEN 0 AND 90
    GROUP BY cohort_month, deployment, grp, vintage_day_raw
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_pcl_camp_cells c
    CROSS JOIN vt_pcl_camp_spine s
)

SELECT
    g.cohort_month,
    g.deployment,
    CAST('All' AS VARCHAR(20))                              AS segment,
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

DROP TABLE vt_pcl_camp_cells;
DROP TABLE vt_pcl_camp_spine;
