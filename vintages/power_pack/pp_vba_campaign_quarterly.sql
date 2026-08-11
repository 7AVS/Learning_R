-- pp_vba_campaign_quarterly.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (8-column shape), QUARTERLY GRAIN VARIANT.
--             mne, quarter, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : VBA (Visa Benefit Add)  CAMPAIGN scope
--
-- WHY THIS FILE EXISTS SEPARATELY FROM pp_vba_campaign.sql (the monthly file)
--   Andre wants three quarters side by side: FY25-Q3, FY26-Q2, FY26-Q3. That is NOT the same as
--   pivoting the monthly cube up to quarter: clients are DEDUPLICATED WITHIN THE QUARTER here.
--   A client hit by three VBA waves inside one quarter counts ONCE in base and ONCE (at most)
--   in responders. Summing three monthly rows for that client in a pivot would count them up to
--   three times and inflate base -- the monthly cube's dedup grain is cohort_month, not quarter,
--   so it does not protect against this on its own. This file re-dedups at the quarter grain
--   from raw tactic history, independently of the monthly file's output.
--   CONSEQUENCE: base in this file will be LOWER than the sum of the monthly cube's base across
--   the quarter's 3 months. That gap IS the duplication being removed -- expected, not a bug.
--
-- ONE QUARTER PER RUN (2026-08-10 fix -- was three quarters in one statement, ran out of spool)
--   Running all three quarters in a single pass scans roughly two years of Casper+SCOT
--   application data (floor 2025-01-01 .. upper bound 2026-10-30) plus a spine cross join, and
--   TDWM/Starburst spool could not hold it. This file now runs ONE quarter at a time: edit the
--   parameter block below, run, save the 8-column output, move to the next quarter, then stack
--   the three saved outputs (UNION ALL, no further dedup needed -- each run's dedup is already
--   scoped to its own quarter) to reproduce the three-quarter comparison.
--   CAVEAT: base is a LEVEL comparison, not automatically like-for-like. If VBA's deployed
--   volume changed materially between quarters (e.g. campaign scaled up/down between FY25-Q3
--   and FY26-Q3), base is NOT comparable across quarters on its own -- check base size before
--   reading a response-rate difference as a real shift.
--
-- QUARTERS (RBC fiscal, Nov-Oct)  run separately, stack afterwards:
--     FY25-Q3 = 2025-05-01 .. 2025-07-31   (YEAR-OVER-YEAR comparator for FY26-Q3)
--     FY26-Q2 = 2026-02-01 .. 2026-04-30   (SEQUENTIAL comparator -- quarter right before Q3)
--     FY26-Q3 = 2026-05-01 .. 2026-07-31
--   Deployment selection is by END date (treatmt_end_dt), exactly like the monthly file: a
--   deployment belongs to the quarter its end date falls in.
--
-- ANCHOR  treatmt_strt_dt (same column the monthly file uses -- VBA's raw tactic history has no
--   separate "effective" date column the way CRV does). Day 0 = that client's FIRST
--   treatmt_strt_dt inside the quarter (first-touch anchor, see DEDUP below).
--
-- ENGINE: Trino/Starburst. Same reason as the monthly file -- success reaches edl0_im (SCOT,
--   prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot), and touching any
--   edl0_im table forces the whole statement onto Starburst/Trino syntax
--   (query_engine_guidelines.md, "SYNTAX FOLLOWS THE ENGINE, NOT THE TABLE"). NO QUALIFY
--   (ROW_NUMBER in a subquery + outer WHERE rn = 1 instead), NO VOLATILE TABLE (CTEs only), NO
--   SYS_CALENDAR (UNNEST(SEQUENCE(0,90)) instead), DATE_DIFF for day math. No dw00 catalog
--   prefix anywhere in this file (DG6V01 / d3cv12a / edl0_im never carried one -- see the
--   monthly file's header for the proof trail).
--
-- SOURCE, unchanged from the monthly file:
--   DG6V01.tactic_evnt_ip_ar_hist (population) + d3cv12a.appl_fact_dly (Casper) +
--   edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot (SCOT)
--
-- mne     : CAST('VBA' AS VARCHAR(20))  same literal as the monthly file.
-- segment : CAST('All' AS VARCHAR(20))  constant, same as the monthly file.
-- grp     : same derivation as the monthly file -- SUBSTR(TRIM(tst_grp_cd),1,1): 'C'->Control,
--           'T'->Action, from the client's first-touch wave. Anything else excluded from the
--           population (grp is strictly binary per the contract).
--
-- SEED EXCLUSION, unchanged from the monthly file: deployments (tactic_id) with an empty
--   Control arm are dropped before population is built -- a deployment with no control clients
--   is not an experiment. Data-driven (COUNT DISTINCT clnt_no in grp='Control' = 0), not a
--   hardcoded tactic_id list. Applied at the tactic_id grain, before the quarter rollup.
--
-- DEDUP -- THIS IS THE POINT OF THE FILE
--   One row per clnt_no (within the single quarter this run targets), anchored on that client's
--   FIRST treatmt_strt_dt in the quarter:
--     ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC) = 1
--   A client touched by 3 VBA waves this quarter counts ONCE in base. grp comes from the
--   first-touch row. Success is POOLED across every wave the client touched in the quarter (not
--   just the first-touch wave) -- take MIN(event_date) across the client's pooled window so a
--   conversion attributed to wave 2 or 3 is not lost -- then rebased (DATE_DIFF) to the
--   first-touch anchor date. The success search window per client is widened to
--   [anchor treatmt_strt_dt, MAX(treatmt_end_dt) across all that client's waves in the quarter]
--   so a later wave's own window is never truncated by only looking at the first-touch wave.
--   Structurally identical to the monthly file's cohort_first / cohort_window / population
--   chain -- only the grouping key changes from cohort_month to (single-quarter) clnt_no.
--   Because this run only ever targets one quarter, PARTITION BY no longer needs a quarter key --
--   there is nothing to cross into. Stacking three single-quarter outputs afterwards reproduces
--   the cross-quarter comparison without re-introducing the spool problem.
--
-- SUCCESS DEFINITION, unchanged from the monthly file: Casper + SCOT unioned and deduped to one
--   earliest-approval event before cohort attribution (plain UNION on (clnt_no, event_date), not
--   UNION ALL). Casper: Status='A', PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND='N', visa_prod_cd
--   NOT IN ('CCL','BXX'), Cell_Code NOT IN ('PATACT','GV0320'), event date = app_rcv_dt. SCOT:
--   productcategory='CREDIT_CARD', statuscode='FULFILLED', event date =
--   creditapplication_createddatetime.
--
-- SPOOL RELIEF (2026-08-10, beyond the one-quarter split):
--   1. Casper and SCOT are no longer scanned in full and joined after. The population CTE
--      (already reduced to this quarter's clients only, post seed-exclusion and dedup) is
--      INNER JOINed INTO the event scan in the same query block, so the client filter can push
--      down into the scan instead of the optimizer materializing the whole table first.
--   2. Each event source is aggregated to one row per client (earliest qualifying event) inside
--      that same CTE, before the Casper/SCOT union and before anything touches the spine, so the
--      spine cross join (vba_cells x spine) always runs against a small deduped set.
--   Net effect: no more 2025-01-01..2026-10-30 whole-table scans. Every scan is bounded to one
--   quarter's SCAN FLOOR .. EVENT UPPER BOUND (see parameter block) AND to this quarter's client
--   list.
--
-- Spine: fixed 0..90, continuous, per the output contract (deliberate cap, preserved from
--   the monthly file). Counts only -- COUNT(DISTINCT clnt_no) / COUNT(*) on the already-deduped
--   success set, never a double-count.

-- ============================================================================
-- RUN ONE QUARTER AT A TIME. Edit the parameters below, run, save the output,
-- then move to the next quarter and stack the results.
-- Running all three at once exhausts spool - roughly two years of Casper+SCOT
-- application data in one pass.
--
--   FY25-Q3   quarter end 2025-05-01 .. 2025-07-31   scan floor 2025-02-01   event upper bound 2025-10-30
--   FY26-Q2   quarter end 2026-02-01 .. 2026-04-30   scan floor 2025-11-01   event upper bound 2026-07-30
--   FY26-Q3   quarter end 2026-05-01 .. 2026-07-31   scan floor 2026-02-01   event upper bound 2026-10-30
--
-- Event upper bound = QUARTER END TO + 90 days, precomputed above so nothing is computed at
-- runtime. Every occurrence below is tagged -- <<QUARTER>> -- grep that tag to find and edit all
-- four dates (label, end-from, end-to, scan floor) plus the derived event upper bound in one pass.
--
-- CURRENTLY SET TO: FY26-Q3
-- ============================================================================
-- QUARTER LABEL      : 'FY26-Q3'          -- <<QUARTER>>
-- QUARTER END FROM   : DATE '2026-05-01'  -- <<QUARTER>>
-- QUARTER END TO     : DATE '2026-08-01'  -- <<QUARTER>> exclusive
-- SCAN FLOOR         : DATE '2026-02-01'  -- <<QUARTER>> earliest possible anchor
-- EVENT UPPER BOUND  : DATE '2026-10-30'  -- <<QUARTER>> exclusive; QUARTER END TO + 90 days
-- ============================================================================

WITH
-- stage 1 dedup: one row per (clnt_no, tactic_id) -- earliest treatmt_strt_dt wins. Single quarter
-- literal, no CASE needed (one run = one quarter).
pop_wave AS (
    SELECT clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt, grp, quarter
    FROM (
        SELECT
            clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt,
            CASE
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'T' THEN CAST('Action'  AS VARCHAR(20))
            END AS grp,
            CAST('FY26-Q3' AS VARCHAR(7)) AS quarter,                       -- <<QUARTER>>
            ROW_NUMBER() OVER (
                PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
            ) AS rn
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE treatmt_strt_dt >= DATE '2026-02-01'                          -- <<QUARTER>> scan floor
          AND treatmt_end_dt  >= DATE '2026-05-01'                          -- <<QUARTER>> quarter end from
          AND treatmt_end_dt  <  DATE '2026-08-01'                          -- <<QUARTER>> quarter end to (excl)
          AND SUBSTR(tactic_id, 8, 3) = 'VBA'
          AND SUBSTR(TRIM(tst_grp_cd), 1, 1) IN ('C', 'T')
    ) ranked
    WHERE rn = 1
),
-- seed exclusion: keep only tactic_ids with a non-empty Control arm (see header note above)
valid_deployments AS (
    SELECT tactic_id
    FROM pop_wave
    GROUP BY tactic_id
    HAVING COUNT(DISTINCT CASE WHEN grp = 'Control' THEN clnt_no END) > 0
),
pop_raw AS (
    SELECT
        w.clnt_no, w.treatmt_strt_dt, w.treatmt_end_dt, w.grp, w.quarter
    FROM pop_wave w
    INNER JOIN valid_deployments vd ON vd.tactic_id = w.tactic_id
),

-- THE DEDUP: one row per clnt_no (this run's single quarter only), first-touch wave wins grp +
-- Day 0. A client in 3 waves this quarter is ONE row here -- that is why this file exists
-- separately from the monthly cube (see header).
cohort_first AS (
    SELECT clnt_no, quarter, grp, treatmt_strt_dt AS anchor_dt
    FROM (
        SELECT clnt_no, quarter, grp, treatmt_strt_dt,
               ROW_NUMBER() OVER (
                   PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC
               ) AS rn
        FROM pop_raw
    ) ranked
    WHERE rn = 1
),

-- widen the success search window to the LATEST end date across ALL of that client's waves in
-- the quarter, so a later wave's conversion is not truncated by the first-touch wave's own
-- (possibly shorter) window.
cohort_window AS (
    SELECT clnt_no, MAX(treatmt_end_dt) AS window_end
    FROM pop_raw
    GROUP BY clnt_no
),

-- population is small by construction: this run's single quarter, post seed-exclusion, post
-- dedup. Everything downstream (Casper scan, SCOT scan) joins INTO this set so the client filter
-- pushes down into the event scans instead of scanning full tables first.
population AS (
    SELECT cf.clnt_no, cf.quarter, cf.grp, cf.anchor_dt, cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.clnt_no = cf.clnt_no
),

-- base -- DEDUPED, one row per client this quarter. FIXED down the spine.
vba_cells AS (
    SELECT quarter, grp, COUNT(DISTINCT clnt_no) AS base
    FROM population
    GROUP BY quarter, grp
),

-- Casper (PRIMARY) success candidates -- population JOINed INTO the scan (client filter pushed
-- down), pre-filtered to each client's own [anchor_dt, window_end], and pre-aggregated to one
-- row per client (earliest qualifying approval) before the union with SCOT.
casper_events AS (
    SELECT p.clnt_no, MIN(app.app_rcv_dt) AS event_date
    FROM population p
    INNER JOIN d3cv12a.appl_fact_dly app
        ON app.bus_clnt_no = p.clnt_no
    WHERE app.app_rcv_dt >= DATE '2026-02-01'                              -- <<QUARTER>> scan floor
      AND app.app_rcv_dt <  DATE '2026-10-30'                              -- <<QUARTER>> event upper bound (excl)
      AND app.app_rcv_dt BETWEEN p.anchor_dt AND p.window_end
      AND app.Status IN ('A')
      AND app.PROD_APPRVD IN ('B', 'E')
      AND app.CR_LMT_CHG_IND = 'N'
      AND app.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND (app.Cell_Code IS NULL OR app.Cell_Code NOT IN ('PATACT', 'GV0320'))
    GROUP BY p.clnt_no
),

-- SCOT (SECONDARY) success candidates -- same treatment: population JOINed INTO the scan, client
-- filter pushed down, pre-aggregated to one row per client before the union.
scot_events_raw AS (
    SELECT
        p.clnt_no,
        MIN(CAST(s.creditapplication_createddatetime AS DATE))           AS event_date,
        MAX(CASE
            WHEN s.creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                             AS approved
    FROM population p
    INNER JOIN edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot s
        ON CAST(s.creditapplication_borrowers_borrowersrfnumber AS INTEGER) = p.clnt_no
    WHERE s.creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory
          IN ('CREDIT_CARD')
      AND s.creditapplication_createddatetime >= DATE '2026-02-01'         -- <<QUARTER>> scan floor
      AND s.creditapplication_createddatetime <  DATE '2026-10-30'         -- <<QUARTER>> event upper bound (excl)
      AND CAST(s.creditapplication_createddatetime AS DATE) BETWEEN p.anchor_dt AND p.window_end
    GROUP BY p.clnt_no
),
scot_events AS (
    SELECT clnt_no, event_date FROM scot_events_raw WHERE approved = 1
),

-- union FIRST, dedupe the physical event (clnt_no, event_date) -- plain UNION, not UNION ALL --
-- each side is already one row per client, so this union is small.
events AS (
    SELECT clnt_no, event_date FROM casper_events
    UNION
    SELECT clnt_no, event_date FROM scot_events
),

-- each client is measured against its OWN pooled window only (anchor_dt to window_end, already
-- enforced inside casper_events / scot_events_raw above); success pooled across both sources,
-- MIN(event_date), rebased to the first-touch anchor -- a conversion from either source is not
-- lost.
success AS (
    SELECT
        p.quarter,
        p.grp,
        p.clnt_no,
        MIN(DATE_DIFF('day', p.anchor_dt, e.event_date)) AS vintage_day
    FROM population p
    INNER JOIN events e
        ON e.clnt_no = p.clnt_no
    GROUP BY p.clnt_no, p.quarter, p.grp
),

daily_counts AS (
    SELECT quarter, grp, vintage_day, COUNT(*) AS responders
    FROM success
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY quarter, grp, vintage_day
),

-- day spine 0-90 (deliberate fixed cap, preserved from the monthly file)
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS s(vintage_day)
),

dense_grid AS (
    SELECT
        c.quarter, c.grp, c.base, s.vintage_day
    FROM vba_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) on purpose, matching the monthly file / pack-wide convention (see
    -- OUTPUT_CONTRACT.md truncation note on Teradata UNION ALL).
    CAST('VBA' AS VARCHAR(20)) AS mne,
    CAST(g.quarter AS VARCHAR(7)) AS quarter,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    COALESCE(dc.responders, 0) AS responders,
    SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.quarter, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.quarter     = g.quarter
    AND dc.grp          = g.grp
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms this quarter?
-- (Same guard as the monthly file's bottom diagnostic -- expect zeros; see that file's header.)
-- Edit the same four -- <<QUARTER>> dates as the main query above before running.
-- ============================================================================
-- SELECT COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no FROM (
--         SELECT
--             clnt_no,
--             CASE
--                 WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
--                 WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'T' THEN CAST('Action'  AS VARCHAR(20))
--             END AS grp
--         FROM DG6V01.tactic_evnt_ip_ar_hist
--         WHERE treatmt_strt_dt >= DATE '2026-02-01'                        -- <<QUARTER>> scan floor
--           AND treatmt_end_dt  >= DATE '2026-05-01'                        -- <<QUARTER>> quarter end from
--           AND treatmt_end_dt  <  DATE '2026-08-01'                        -- <<QUARTER>> quarter end to (excl)
--           AND SUBSTR(tactic_id, 8, 3) = 'VBA'
--           AND SUBSTR(TRIM(tst_grp_cd), 1, 1) IN ('C', 'T')
--     ) raw
--     GROUP BY clnt_no
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x;
