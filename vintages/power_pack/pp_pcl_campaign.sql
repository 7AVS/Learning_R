-- pcl_campaign_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED
--   2026-08-10; `mne` ADDED 2026-08-10 as first column). Emits EXACTLY 8 columns: mne VARCHAR(20)
--   [CAST('PCL' AS VARCHAR(20)) — campaign mnemonic, or the experiment name where the file
--   measures an experiment], cohort_month VARCHAR(7) 'YYYY-MM',
--   segment VARCHAR(20) [CAST('All' AS VARCHAR(20)) — constant, no pre-treatment split above
--   tst_grp_cd for PCL], grp VARCHAR(20) [see grp note below — NOT true binary in this file],
--   vintage_day INTEGER (0..90 continuous), base INTEGER (fixed per cohort x segment x grp),
--   responders INTEGER, responders_cum INTEGER. Counts only, no rates.
--
-- mne distinguishes this file from its experiment sibling (pp_pcl_sales_modal.sql), so both
--   can be stacked into one cube safely.
--
-- SCOPE: *** CAMPAIGN *** — whole PCL campaign. NO modal / sales-modal population filter.
--   (The EXPERIMENT-scope sibling is pcl_sales_modal.sql — sales-modal WMS/NMS only.)
--
-- ENGINE: Trino / Starburst. ALL Power Pack files use ONE engine - EDW tables are reached
--         through Starburst federation. No volatile tables, no QUALIFY, no SYS_CALENDAR.
--
-- CATALOG PREFIX: dw00_im.dl_mr_prod.cards_pli_decision_resp — proven in
--   campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql, p10_vintage_curves.sql and every
--   other file in that directory, all headed "Engine: ... Trino/Starburst" and matches
--   CLAUDE.md's own EDW table citation for this table.
--
-- Source   : dw00_im.dl_mr_prod.cards_pli_decision_resp
-- Grain    : account (acct_no)
-- Anchor   : treatmt_strt_dt (treatment start). [VERIFY] reliability of this field on THIS curated
--            table was never confirmed (see prior probe in vintages/pcl_vintage_monthly.sql header —
--            it flagged treatmt_strt_dt as unreliable on DG6V01.tactic_evnt_ip_ar_hist; unconfirmed
--            whether the same issue exists here). Not re-run here per task scope ("do not run
--            anything") — carried forward, not resolved.
-- Population filter: treatmt_strt_dt >= DATE '2026-01-01' only. No report_groups_period filter,
--            no tst_grp_cd exclusion — this is the WHOLE campaign, every arm, every deployment.
--
-- *** deployment DROPPED, 2026-08-10 *** — was parent_tactic_id. PCL runs ~25 deployments/quarter
--   and Andre will not build a curve per deployment. Curve grain is now COHORT MONTH.
--
-- DEDUP IDENTIFIER: acct_no — this file's grain and success are both acct_no throughout.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
--
-- *** DEDUP — one row per (acct_no, cohort_month), anchored on first wave ***
--   1. cohort_first: ROW_NUMBER() OVER (PARTITION BY acct_no, cohort_month
--      ORDER BY treatmt_strt_dt ASC), keep rn = 1 — first-touch wave wins grp and becomes Day 0.
--   2. Success (dt_cl_change, already an absolute date on the curated row when responder_cli=1)
--      is pooled across EVERY wave the account touched that month: success_pooled takes
--      MIN(dt_cl_change) across all the account's rows in the cohort_month, then rebases to the
--      first-touch anchor date. An account that didn't respond on wave 1 but did on wave 2 (same
--      month) is no longer silently dropped, and base is not inflated by counting the account
--      once per wave.
--
-- grp (arm) — *** [VERIFY] BLOCKING, UNRESOLVED — READ BEFORE TRUSTING grp ON THIS FILE ***
--   PCL tst_grp_cd Action/Control code mapping is UNCONFIRMED anywhere in this repo (unchanged by
--   the deployment drop — this was already true in the 8-column version). `grp` below is
--   TRIM(tst_grp_cd) passed through RAW from the account's first-touch wave, CAST to VARCHAR(20)
--   — i.e. it will show whatever raw codes exist (may be more than 2 values), NOT a true binary
--   collapse. This is a deliberate deviation from contract rule 5, flagged here rather than
--   guessed. BEFORE using this file's grp for any Action-vs-Control read: run
--   campaigns/CRV/crv_pcl_overlap_summary.sql §C4 (or equivalent) to get the actual code list,
--   then edit the grp expressions below into a real
--   CASE WHEN TRIM(tst_grp_cd) IN (...) THEN 'Action' WHEN ... THEN 'Control' END.
--
-- Success  : responder_cli = 1 (CLI response flag); event date = dt_cl_change (already absolute).
--            Same primary success metric as pcl_vintage_monthly.sql / pcl_sales_modal.sql —
--            preserved unchanged, not invented. One metric per file per contract rule 4.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days. UNNEST(SEQUENCE(0,90)),
--            Trino has no SYS_CALENDAR.
-- ----------------------------------------------------------------------------
-- SCOPE: this file is scoped to deployments ENDING in the quarter window below
--   (population filtered on treatmt_end_dt, confirmed column on
--   schemas/crv_pcl_curated_schemas.md §3a). cohort_month and day-0 still anchor
--   on treatmt_strt_dt (the START column) — unchanged. Success (dt_cl_change) is
--   read from the SAME curated row as population, so the end-date filter below
--   tightens both the population AND the success-side scan in one change — no
--   separate event table to bound here. Retargeting a quarter = editing the two
--   <<WINDOW>> literals below only.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW — EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in the window.
--   Cohort month and day 0 still anchor on treatmt_strt_dt (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

WITH
raw_rows AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        DATE_TRUNC('month', treatmt_strt_dt)           AS cohort_month,
        CAST(TRIM(tst_grp_cd) AS VARCHAR(20))          AS grp,          -- [VERIFY] raw pass-through, see header
        CASE WHEN responder_cli = 1 THEN dt_cl_change END AS success_dt_abs
    FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-01-01'                              -- floor guard
      AND treatmt_end_dt  >= DATE '2026-05-01'                              -- <<WINDOW>>
      AND treatmt_end_dt  <  DATE '2026-08-01'                              -- <<WINDOW>>
      AND TRIM(tst_grp_cd) IS NOT NULL
      AND TRIM(tst_grp_cd) <> ''
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT acct_no, cohort_month, grp, treatmt_strt_dt AS anchor_dt
    FROM (
        SELECT acct_no, cohort_month, grp, treatmt_strt_dt,
               ROW_NUMBER() OVER (
                   PARTITION BY acct_no, cohort_month ORDER BY treatmt_strt_dt ASC
               ) AS rn
        FROM raw_rows
    ) ranked
    WHERE rn = 1
),

pcl_cells AS (
    SELECT cohort_month, grp, COUNT(DISTINCT acct_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
),

-- pool success across every wave the account touched this cohort_month
success_pooled AS (
    SELECT acct_no, cohort_month, MIN(success_dt_abs) AS success_dt_abs
    FROM raw_rows
    WHERE success_dt_abs IS NOT NULL
    GROUP BY acct_no, cohort_month
),

population AS (
    SELECT
        cf.cohort_month, cf.grp, cf.acct_no,
        DATE_DIFF('day', cf.anchor_dt, sp.success_dt_abs) AS vintage_day_raw
    FROM cohort_first cf
    LEFT JOIN success_pooled sp
        ON sp.acct_no = cf.acct_no AND sp.cohort_month = cf.cohort_month
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day_raw AS vintage_day,
           COUNT(DISTINCT acct_no) AS responders
    FROM population
    WHERE vintage_day_raw BETWEEN 0 AND 90
    GROUP BY cohort_month, grp, vintage_day_raw
),

-- day spine 0-90
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS s(vintage_day)
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM pcl_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('PCL' AS VARCHAR(20))                              AS mne,
    CAST(SUBSTR(CAST(g.cohort_month AS VARCHAR), 1, 7) AS VARCHAR(7)) AS cohort_month,
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
-- DIAGNOSTIC (commented out): how many accounts hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_accounts FROM (
--     SELECT acct_no, cohort_month FROM (
--         SELECT
--             acct_no,
--             DATE_TRUNC('month', treatmt_strt_dt) AS cohort_month,
--             CAST(TRIM(tst_grp_cd) AS VARCHAR(20)) AS grp
--         FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
--         WHERE treatmt_strt_dt >= DATE '2026-01-01'
--           AND TRIM(tst_grp_cd) IS NOT NULL
--           AND TRIM(tst_grp_cd) <> ''
--     ) raw
--     GROUP BY acct_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
