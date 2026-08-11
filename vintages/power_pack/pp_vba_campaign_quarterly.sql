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
-- QUARTERS (RBC fiscal, Nov-Oct)  THREE quarters, all in one output:
--     FY25-Q3 = 2025-05-01 .. 2025-07-31   (YEAR-OVER-YEAR comparator for FY26-Q3)
--     FY26-Q2 = 2026-02-01 .. 2026-04-30   (SEQUENTIAL comparator -- quarter right before Q3)
--     FY26-Q3 = 2026-05-01 .. 2026-07-31
--   Deployment selection is by END date (treatmt_end_dt), exactly like the monthly file: a
--   deployment belongs to the quarter its end date falls in.
--   CAVEAT: base is a LEVEL comparison, not automatically like-for-like. If VBA's deployed
--   volume changed materially between quarters (e.g. campaign scaled up/down between FY25-Q3
--   and FY26-Q3), base is NOT comparable across quarters on its own -- check base size before
--   reading a response-rate difference as a real shift.
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
--   One row per (clnt_no, quarter), anchored on that client's FIRST treatmt_strt_dt inside the
--   quarter:
--     ROW_NUMBER() OVER (PARTITION BY clnt_no, quarter ORDER BY treatmt_strt_dt ASC) = 1
--   A client touched by 3 VBA waves in one quarter counts ONCE in base. grp comes from the
--   first-touch row. Success is POOLED across every wave the client touched in the quarter (not
--   just the first-touch wave) -- take MIN(event_date) across the client's pooled window so a
--   conversion attributed to wave 2 or 3 is not lost -- then rebased (DATE_DIFF) to the
--   first-touch anchor date. The success search window per client is widened to
--   [anchor treatmt_strt_dt, MAX(treatmt_end_dt) across all that client's waves in the quarter]
--   so a later wave's own window is never truncated by only looking at the first-touch wave.
--   Structurally identical to the monthly file's cohort_first / cohort_window / population
--   chain -- only the grouping key changes from cohort_month to quarter.
--   CROSS-QUARTER NOTE: dedup is WITHIN a quarter only (PARTITION BY includes quarter). A client
--   who shows up in both FY25-Q3 and FY26-Q3 is NOT deduped across those quarters -- they
--   correctly get ONE row in FY25-Q3 AND a separate ONE row in FY26-Q3. Do not read this
--   PARTITION BY as collapsing a client to a single row for the whole file.
--
-- SUCCESS DEFINITION, unchanged from the monthly file: Casper + SCOT unioned and deduped to one
--   earliest-approval event before cohort attribution (plain UNION on (clnt_no, event_date), not
--   UNION ALL). Casper: Status='A', PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND='N', visa_prod_cd
--   NOT IN ('CCL','BXX'), Cell_Code NOT IN ('PATACT','GV0320'), event date = app_rcv_dt. SCOT:
--   productcategory='CREDIT_CARD', statuscode='FULFILLED', event date =
--   creditapplication_createddatetime.
--
-- EVENT DATE FLOOR: DATE '2025-01-01' on both Casper and SCOT scans, matching the widened
--   population floor (treatmt_strt_dt >= 2025-01-01) so no real anchor's own events (including
--   FY25-Q3 anchors) can fall below it.
--   Upper bound DATE '2026-10-30' (exclusive) = 90 days past the latest quarter end (FY26-Q3
--   ends 2026-07-31; +90 days = 2026-10-29) so a late FY26-Q3 conversion is not truncated.
--
-- Spine: fixed 0..90, continuous, per the output contract (deliberate cap, preserved from
--   the monthly file). Counts only -- COUNT(DISTINCT clnt_no) / COUNT(*) on the already-deduped
--   success set, never a double-count.

-- ============================================================================
-- QUARTER WINDOW  <<WINDOW>>  EDIT THESE SIX DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in one of three windows; tags
--   the surviving row with which quarter it belongs to. Day 0 still anchors on treatmt_strt_dt
--   (START), not these.
--     FY25-Q3 = 2025-05-01 .. 2025-07-31   (year-over-year comparator)
--     FY26-Q2 = 2026-02-01 .. 2026-04-30   (sequential comparator)
--     FY26-Q3 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- Q1 (FY25-Q3) START : DATE '2025-05-01'                                    -- <<WINDOW>>
-- Q1 (FY25-Q3) END   : DATE '2025-07-31'   (inclusive)                      -- <<WINDOW>>
-- Q2 (FY26-Q2) START : DATE '2026-02-01'                                    -- <<WINDOW>>
-- Q2 (FY26-Q2) END   : DATE '2026-04-30'   (inclusive)                      -- <<WINDOW>>
-- Q3 (FY26-Q3) START : DATE '2026-05-01'                                    -- <<WINDOW>>
-- Q3 (FY26-Q3) END   : DATE '2026-07-31'   (inclusive)                      -- <<WINDOW>>

WITH
-- stage 1 dedup: one row per (clnt_no, tactic_id) -- earliest treatmt_strt_dt wins. Quarter tag
-- attached here, off treatmt_end_dt, same rule as the monthly file's cohort_month tag.
pop_wave AS (
    SELECT clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt, grp, quarter
    FROM (
        SELECT
            clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt,
            CASE
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'T' THEN CAST('Action'  AS VARCHAR(20))
            END AS grp,
            CASE
                WHEN treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31' THEN 'FY25-Q3'  -- <<WINDOW>>
                WHEN treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' THEN 'FY26-Q2'  -- <<WINDOW>>
                WHEN treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31' THEN 'FY26-Q3'  -- <<WINDOW>>
            END AS quarter,
            ROW_NUMBER() OVER (
                PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
            ) AS rn
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE treatmt_strt_dt >= DATE '2025-01-01'                          -- floor guard
          AND (
                treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31'    -- <<WINDOW>>
             OR treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-07-31'    -- <<WINDOW>>
              )
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

-- THE DEDUP: one row per (clnt_no, quarter), first-touch wave wins grp + Day 0. A client in 3
-- waves this quarter is ONE row here -- that is why this file exists separately from the
-- monthly cube (see header).
cohort_first AS (
    SELECT clnt_no, quarter, grp, treatmt_strt_dt AS anchor_dt
    FROM (
        SELECT clnt_no, quarter, grp, treatmt_strt_dt,
               ROW_NUMBER() OVER (
                   PARTITION BY clnt_no, quarter ORDER BY treatmt_strt_dt ASC
               ) AS rn
        FROM pop_raw
    ) ranked
    WHERE rn = 1
),

-- widen the success search window to the LATEST end date across ALL of that client's waves in
-- the quarter, so a later wave's conversion is not truncated by the first-touch wave's own
-- (possibly shorter) window.
cohort_window AS (
    SELECT clnt_no, quarter, MAX(treatmt_end_dt) AS window_end
    FROM pop_raw
    GROUP BY clnt_no, quarter
),

population AS (
    SELECT cf.clnt_no, cf.quarter, cf.grp, cf.anchor_dt, cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.clnt_no = cf.clnt_no AND cw.quarter = cf.quarter
),

-- base -- DEDUPED, one row per client per quarter. FIXED down the spine.
vba_cells AS (
    SELECT quarter, grp, COUNT(DISTINCT clnt_no) AS base
    FROM population
    GROUP BY quarter, grp
),

-- raw candidate success events, Casper (PRIMARY) -- no deployment key on the event table itself
casper_events AS (
    SELECT DISTINCT app.bus_clnt_no AS clnt_no, app.app_rcv_dt AS event_date
    FROM d3cv12a.appl_fact_dly app
    WHERE app.app_rcv_dt >= DATE '2025-01-01'                               -- floor, matches population floor
      AND app.app_rcv_dt <  DATE '2026-10-30'                               -- <<WINDOW>> upper: 90 days past latest quarter end
      AND app.Status IN ('A')
      AND app.PROD_APPRVD IN ('B', 'E')
      AND app.CR_LMT_CHG_IND = 'N'
      AND app.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND (app.Cell_Code IS NULL OR app.Cell_Code NOT IN ('PATACT', 'GV0320'))
),

-- raw candidate success events, SCOT (SECONDARY) -- one row per client (SCOT's snapshot
-- structure has no per-deployment key; MIN date + fulfilled flag is the most it supports)
scot_events_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS event_date,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory
          IN ('CREDIT_CARD')
      AND creditapplication_createddatetime >= DATE '2025-01-01'            -- floor, matches population floor
      AND creditapplication_createddatetime <  DATE '2026-10-30'            -- <<WINDOW>> upper: 90 days past latest quarter end
    GROUP BY 1
),
scot_events AS (
    SELECT clnt_no, event_date FROM scot_events_raw WHERE approved = 1
),

-- union FIRST, dedupe the physical event (clnt_no, event_date) -- plain UNION, not UNION ALL --
-- BEFORE joining to a cohort window
events AS (
    SELECT clnt_no, event_date FROM casper_events
    UNION
    SELECT clnt_no, event_date FROM scot_events
),

-- each client-quarter is measured against its OWN pooled window only (anchor_dt to window_end);
-- success pooled across every wave the client touched, MIN(event_date), rebased to the
-- first-touch anchor -- a conversion on wave 2 or 3 is not lost.
success AS (
    SELECT
        p.quarter,
        p.grp,
        p.clnt_no,
        MIN(DATE_DIFF('day', p.anchor_dt, e.event_date)) AS vintage_day
    FROM population p
    INNER JOIN events e
        ON  e.clnt_no    = p.clnt_no
        AND e.event_date BETWEEN p.anchor_dt AND p.window_end
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
        c.quarter, c.grp, c.base, s.vintage_day,
        -- explicit sort key: plain alphabetical ORDER BY on quarter happens to also give the
        -- right order here (FY25 < FY26, Q2 < Q3), but this is spelled out so the emit order
        -- (FY25-Q3, FY26-Q2, FY26-Q3) is guaranteed, not an accident of string sort.
        CASE c.quarter
            WHEN 'FY25-Q3' THEN 1
            WHEN 'FY26-Q2' THEN 2
            WHEN 'FY26-Q3' THEN 3
        END AS quarter_sort
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
ORDER BY g.quarter_sort, g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one quarter?
-- (Same guard as the monthly file's bottom diagnostic -- expect zeros; see that file's header.)
-- ============================================================================
-- SELECT quarter, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, quarter FROM (
--         SELECT
--             clnt_no,
--             CASE
--                 WHEN treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31' THEN 'FY25-Q3'
--                 WHEN treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' THEN 'FY26-Q2'
--                 WHEN treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31' THEN 'FY26-Q3'
--             END AS quarter,
--             CASE
--                 WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
--                 WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'T' THEN CAST('Action'  AS VARCHAR(20))
--             END AS grp
--         FROM DG6V01.tactic_evnt_ip_ar_hist
--         WHERE treatmt_strt_dt >= DATE '2025-01-01'
--           AND (
--                 treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31'
--             OR treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-07-31'
--               )
--           AND SUBSTR(tactic_id, 8, 3) = 'VBA'
--           AND SUBSTR(TRIM(tst_grp_cd), 1, 1) IN ('C', 'T')
--     ) raw
--     GROUP BY clnt_no, quarter
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
