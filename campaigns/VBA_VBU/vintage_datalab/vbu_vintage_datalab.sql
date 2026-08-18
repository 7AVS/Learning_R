-- ENGINE: Teradata-direct
-- VBU monthly vintage — one statement, no volatile tables, no DDL.
-- Source: DL_MR_PROD.cards_bizups_vbu_descresp_clnt (bare schema — this table
--   carries no `mnc` column, unlike the VBA table; mnc is a literal constant here).
--
-- OUTPUT: mnc | cohort_month | arm | success_def | vintage_day | base | conv_day | conv_cum
--   Every day 0..max is present for every cohort+arm+success_def. conv_day is 0 on
--   days nobody converted; conv_cum carries forward. No gaps.
--
-- STRUCTURE mirrors campaigns/VBA_VBU/vintage_datalab/vba_vintage_datalab.sql:
--   recursive day spine, base -> agg -> cells -> final join, LEAST(horizon, maturity)
--   cap. Differences from VBA are called out inline where they diverge (dual
--   success_def, no source `mnc` column, 90-day horizon instead of 60).
--
-- POPULATION / GRAIN (probe P1, vbu_probe_results_2026-08-18.md):
--   Table grain is ~(clnt_no, tactic_id) but NOT clean — 810 dup rows out of
--   1,071,165 (0.08%). This build dedups to one row per (clnt_no, cohort_month, arm)
--   in `base`, same pattern as VBA's base CTE: GROUP BY clnt_no/cohort_month/arm,
--   taking MIN(response_start) as the cohort anchor and MIN(dt_prod_change_client)
--   among responders as the conversion date (done separately per success_def — see
--   SUCCESS below). A client with duplicate rows in a cohort+arm collapses to one.
--
-- ARM (probe P2a): `control` holds literal 'Action' / 'Control' directly on this
--   table — no join needed (contrast with VBA's arm field on the same table).
--   Filtered TRIM(control) IN ('Action','Control').
--
-- COHORT (probe P1): anchor = `response_start` (DATE, matches `year_mon_start`).
--   cohort_month = EXTRACT(YEAR FROM response_start)*100 + EXTRACT(MONTH FROM
--   response_start). Floored response_start >= DATE '2024-01-01' — same floor
--   convention as every other curated-table build in this repo (2024_data_floor).
--
-- SUCCESS (probe P3) — THE CHAR-RESPONDER TRAP:
--   `responder` is CHAR ('0.No Change' / '1.Change t…' / '2.Change t…'). Probes P4/P7
--   FAILED on 2026-08-18 with Teradata error [2621] "Bad character in format or data
--   of ...Responder" from comparing it to a number. NEVER compare `responder` to an
--   integer or use it in arithmetic. This build uses the pre-built NUMERIC flags
--   instead: `responder_anyproduct = 1` (any product change) and
--   `responder_targetproduct = 1` (change specifically to the target product).
--   P3 confirms no NULLs and a clean partition: anyproduct = responder IN ('1.','2.'),
--   targetproduct = responder = '1.' only (14,530 / 1,136 / 337,234 clients resp.).
--   Conversion date = `dt_prod_change_client`. Both flavours are produced, stacked
--   via UNION ALL (see `base2` below), because P3 shows they are genuinely different
--   populations (targetproduct is a strict subset of anyproduct), not a display
--   choice — downstream consumers need both.
--
-- HORIZON (probe P5): `response_end` is a FIXED 61-90 day window after
--   `response_start` for every single row (868,938/868,938 rows land in the
--   3_61_90 bucket) — this is the table's own standard measurement window, not
--   an assumption. Horizon = 90, capped per cohort by
--   maturity = CURRENT_DATE - response_start:
--     max_day = LEAST(90, maturity)   (0 if maturity < 0, i.e. clock skew)
--   Cohorts younger than 90 days stop early on purpose — missing observation time,
--   not a missing value. Never fill it forward.
--
-- RELATIONSHIP TO THE OTHER TWO VBU BUILDS IN THIS REPO:
--   vintages/power_pack/pp_vbu_campaign.sql (v1) — ALSO sources this same curated
--     table, but joins it to a separate tactic table to recover the arm; that join
--     returned ZERO rows before a 2026-08-10 key-normalization fix (v1's own header
--     documents this). THIS FILE NEEDS NO JOIN AT ALL: probe P2a confirms `control`
--     already carries the literal Action/Control arm directly on
--     cards_bizups_vbu_descresp_clnt, so the fragile join v1 depends on is avoided
--     entirely here.
--   vintages/power_pack/pp_vbu_campaign_v2.sql — a from-scratch rebuild off raw
--     tactic + portfolio tables (DG6V01.tactic_evnt_ip_ar_hist, d3cv12a.cr_crd_rpts_acct,
--     D3CV12A.dly_full_portfolio), deliberately NOT touching the curated table at all.
--     v2's own header records its success detection running short of the dashboard
--     target by 42 (Action) / 5 (Control) on the 2026-04-13 wave before a fix, and
--     flags Control's near-zero success rate as unexplained. This file is the
--     opposite bet: trust the curated table's own pre-built responder flags instead
--     of rebuilding detection from raw account history. See vbu_reconcile_dashboard.sql
--     for whether that bet pays off against the same dashboard numbers v2 targets.
--
-- Spine 0..90. Change 90 in BOTH places (here and max_day in `cells`) to move the
-- horizon. Seed must reference a table (Teradata error 8842) — returns 1 row.
WITH RECURSIVE spine (vintage_day) AS (
        SELECT 0
        FROM SYS_CALENDAR.CALENDAR
        WHERE calendar_date = DATE '2024-01-01'
    UNION ALL
        SELECT vintage_day + 1 FROM spine WHERE vintage_day < 90
)
-- One row per (clnt_no, cohort_month, arm) — dedups the 810 dup rows found in P1.
-- conv_dt_any / conv_dt_target computed separately so both success flavours share
-- the exact same population and anchor date; only the conversion-date column differs.
, base AS (
    SELECT
        clnt_no
      , CAST(EXTRACT(YEAR FROM response_start) * 100
             + EXTRACT(MONTH FROM response_start) AS INTEGER)          AS cohort_month
      , TRIM(control)                                                  AS arm
      , MIN(response_start)                                            AS anchor_dt
      , MIN(CASE WHEN responder_anyproduct = 1
                 THEN dt_prod_change_client END)                       AS conv_dt_any
      , MIN(CASE WHEN responder_targetproduct = 1
                 THEN dt_prod_change_client END)                       AS conv_dt_target
    FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
    WHERE TRIM(control) IN ('Action','Control')
      AND response_start >= DATE '2024-01-01'
    GROUP BY 1,2,3
)
-- Fan `base` out to two success_defs (one statement, no second query). First
-- literal cast to VARCHAR(20) explicitly — Teradata sizes a UNION ALL column off
-- the first branch's literal length (7 chars 'anyproduct' would truncate 13-char
-- 'targetproduct' otherwise).
, base2 AS (
    SELECT
        clnt_no, cohort_month, arm
      , CAST('anyproduct' AS VARCHAR(20))     AS success_def
      , anchor_dt
      , conv_dt_any                           AS conv_dt
    FROM base
    UNION ALL
    SELECT
        clnt_no, cohort_month, arm
      , 'targetproduct'                       AS success_def
      , anchor_dt
      , conv_dt_target                        AS conv_dt
    FROM base
)
-- one pass over base2; agg is small (cohort_month x arm x success_def x vintage_day)
, agg AS (
    SELECT
        cohort_month, arm, success_def
      , conv_dt - anchor_dt                 AS vintage_day   -- NULL = never converted
      , COUNT(*)                            AS n
      , MIN(CURRENT_DATE - anchor_dt)       AS maturity      -- observation time so far
    FROM base2
    GROUP BY 1,2,3,4
)
-- base = distinct clients per (cohort_month, arm, success_def). Because base2 fans
-- every client to BOTH success_defs regardless of conversion status, this SUM is
-- identical for anyproduct and targetproduct within a given cohort_month+arm —
-- i.e. "same base for both success_defs," as required.
, cells AS (
    SELECT
        cohort_month, arm, success_def
      , SUM(n)          AS base
      , MIN(maturity)   AS maturity_day
      , CASE WHEN MIN(maturity) < 0  THEN 0
             WHEN MIN(maturity) > 90 THEN 90     -- fixed 90-day horizon (P5)
             ELSE MIN(maturity) END AS max_day
    FROM agg
    GROUP BY 1,2,3
)
SELECT
    CAST('VBU' AS VARCHAR(10)) AS mnc
  , c.cohort_month
  , c.arm
  , c.success_def
  , s.vintage_day
  , c.base
  , COALESCE(d.n,0) AS conv_day
  , SUM(COALESCE(d.n,0)) OVER (
        PARTITION BY c.cohort_month, c.arm, c.success_def
        ORDER BY s.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS conv_cum
FROM cells c
JOIN spine s
  ON s.vintage_day <= c.max_day
LEFT JOIN agg d
  ON  c.cohort_month = d.cohort_month
  AND c.arm          = d.arm
  AND c.success_def  = d.success_def
  AND s.vintage_day  = d.vintage_day
ORDER BY 1,2,3,4,5;
