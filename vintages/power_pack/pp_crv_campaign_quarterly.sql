-- pp_crv_campaign_quarterly.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (8-column shape), QUARTERLY GRAIN VARIANT.
--             mne, quarter, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : CRV (Credit Card Installment Plan)  CAMPAIGN scope
--
-- WHY THIS FILE EXISTS SEPARATELY FROM pp_crv_campaign.sql (the monthly file)
--   Andre wants three quarters side by side: FY25-Q3, FY26-Q2, FY26-Q3. That is NOT the same as
--   pivoting the monthly cube up to quarter: clients are DEDUPLICATED WITHIN THE QUARTER here.
--   A client hit by three CRV waves inside one quarter counts ONCE in base and ONCE (at most)
--   in responders. Summing three monthly rows for that client in a pivot would count them up to
--   three times and inflate base -- that is a real bug the monthly cube does not protect against
--   on its own (its dedup grain is cohort_month, not quarter). This file re-dedups at the
--   quarter grain from raw tactic history, independently of the monthly file's output.
--   CONSEQUENCE: base in this file will be LOWER than the sum of the monthly cube's base across
--   the quarter's 3 months. That gap IS the duplication being removed -- expected, not a bug.
--
-- QUARTERS (RBC fiscal, Nov-Oct)  THREE quarters, all in one output:
--     FY25-Q3 = 2025-05-01 .. 2025-07-31   (YEAR-OVER-YEAR comparator for FY26-Q3)
--     FY26-Q2 = 2026-02-01 .. 2026-04-30   (SEQUENTIAL comparator -- quarter right before Q3)
--     FY26-Q3 = 2026-05-01 .. 2026-07-31
--   Deployment selection is by END date (treatmt_end_dt), exactly like the monthly file: a
--   deployment belongs to the quarter its end date falls in.
--   CAVEAT: base is a LEVEL comparison, not automatically like-for-like. If CRV's deployed
--   volume changed materially between quarters (e.g. campaign scaled up/down between FY25-Q3
--   and FY26-Q3), base is NOT comparable across quarters on its own -- check base size before
--   reading a response-rate difference as a real shift.
--
-- ANCHOR  treatmt_eff_dt, per references/vintage_datalab_method.md and the monthly file's own
--   header ("Do NOT switch to treatmt_strt_dt"). Day 0 = that client's FIRST treatmt_eff_dt
--   inside the quarter (first-touch anchor, see DEDUP below).
--
-- ENGINE: Trino/Starburst. Same reason as the monthly file -- success lives in the EDL Success
--   Library (edl0_im.prod_zp10_prod_staging.measurement_events_v2), and touching any edl0_im
--   table forces the whole statement onto Starburst/Trino syntax (query_engine_guidelines.md,
--   "SYNTAX FOLLOWS THE ENGINE, NOT THE TABLE"). NO QUALIFY (ROW_NUMBER in a subquery + outer
--   WHERE rn = 1 instead), NO VOLATILE TABLE (CTEs only), NO SYS_CALENDAR (UNNEST(SEQUENCE(0,90))
--   instead), DATE_DIFF for day math. No dw00 catalog prefix anywhere in this file.
--
-- SOURCE, unchanged from the monthly file (raw tables, not the curated Data Lab table):
--   1. dg6v01.tactic_evnt_ip_ar_hist                       population / arm / treatment window
--   2. edl0_im.prod_zp10_prod_staging.measurement_events_v2  success (Success Library events)
--
-- mne     : CAST('CRV' AS VARCHAR(20))  same literal as the monthly file.
-- segment : CAST('All' AS VARCHAR(20))  constant, same as the monthly file.
-- grp     : same derivation formula as the monthly file --
--             CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
--           DEVIATION FROM THE MONTHLY FILE'S GRAIN, ON PURPOSE: the monthly file keeps arm
--           IN the dedup grain (GROUP BY visa_acct_no, cohort_month, arm), so one account can
--           appear under both Action and Control in the same month -- that was validated
--           against the production dashboard and is correct FOR THE MONTHLY SUMMARY. This
--           quarterly file's whole purpose is a different, stricter dedup (one row per client
--           per quarter, period), so grp here comes from the client's FIRST-TOUCH row only, not
--           from a grain that lets one client double-count across arms. See DEDUP below.
--
-- DEDUP -- THIS IS THE POINT OF THE FILE
--   One row per (visa_acct_no, quarter), anchored on that client's FIRST treatmt_eff_dt inside
--   the quarter:
--     ROW_NUMBER() OVER (PARTITION BY visa_acct_no, quarter ORDER BY treatmt_eff_dt ASC) = 1
--   A client touched by 3 CRV waves in one quarter counts ONCE in base. grp comes from the
--   first-touch row. Success is POOLED across every wave the client touched in the quarter (not
--   just the first-touch wave) -- take MIN(event_date) across the client's pooled window so a
--   conversion attributed to wave 2 or 3 is not lost -- then rebased (DATE_DIFF) to the
--   first-touch anchor date. The success search window per client is widened to
--   [anchor treatmt_eff_dt, MAX(treatmt_end_dt) across all that client's waves in the quarter]
--   so a later wave's own window is never truncated by only looking at the first-touch wave.
--   CROSS-QUARTER NOTE: dedup is WITHIN a quarter only (PARTITION BY includes quarter). A client
--   who shows up in both FY25-Q3 and FY26-Q3 is NOT deduped across those quarters -- they
--   correctly get ONE row in FY25-Q3 AND a separate ONE row in FY26-Q3. Do not read this
--   PARTITION BY as collapsing a client to a single row for the whole file.
--
-- JOIN KEY TRAP, unchanged from the monthly file: measurement_events_v2.acct_no is a
--   zero-padded VARCHAR (~23 chars); visa_acct_no is a bare number. Normalize BOTH sides through
--   DECIMAL(38,0) or the join silently returns zero rows.
--
-- EVENT DATE FLOOR: DATE '2025-01-01' (widened for FY25-Q3 -- see floor guard below).
--   treatmt_eff_dt can run earlier than the quarter's own start date; the population floor
--   below keeps every anchor >= this date, so this floor cannot cut a real anchor's own events.
--
-- Spine: fixed 0..90, continuous, per the output contract. Counts only --
--   COUNT(DISTINCT visa_acct_no) throughout, never COUNT(*).

-- ============================================================================
-- QUARTER WINDOW  <<WINDOW>>  EDIT THESE SIX DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in one of three windows; tags
--   the surviving row with which quarter it belongs to. Day 0 still anchors on treatmt_eff_dt
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
-- raw waves in scope: both quarters, tagged. Grain here is still raw (one row can repeat per
-- account across multiple deployments) -- the dedup happens in first_touch below.
raw_waves AS (
    SELECT
        visa_acct_no,
        treatmt_eff_dt,
        treatmt_end_dt,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END AS grp,
        CASE
            WHEN treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31' THEN 'FY25-Q3'  -- <<WINDOW>>
            WHEN treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' THEN 'FY26-Q2'  -- <<WINDOW>>
            WHEN treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31' THEN 'FY26-Q3'  -- <<WINDOW>>
        END AS quarter
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_eff_dt IS NOT NULL
      AND treatmt_eff_dt >= DATE '2025-01-01'                                                   -- floor guard
      AND (
            treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31'                      -- <<WINDOW>>
         OR treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-07-31'                      -- <<WINDOW>>
          )
),

-- THE DEDUP: one row per (visa_acct_no, quarter), first-touch anchor wins grp + Day 0.
-- A client in 3 waves this quarter is ONE row here -- that is why this file exists separately
-- from the monthly cube (see header).
first_touch AS (
    SELECT visa_acct_no, quarter, grp, treatmt_eff_dt AS anchor_dt
    FROM (
        SELECT
            visa_acct_no, quarter, grp, treatmt_eff_dt,
            ROW_NUMBER() OVER (
                PARTITION BY visa_acct_no, quarter ORDER BY treatmt_eff_dt ASC
            ) AS rn
        FROM raw_waves
    ) ranked
    WHERE rn = 1
),

-- widen the success search window to the LATEST end date across ALL of that client's waves in
-- the quarter, so a later wave's conversion is not truncated by the first-touch wave's own
-- (possibly shorter) window.
quarter_window AS (
    SELECT visa_acct_no, quarter, MAX(treatmt_end_dt) AS window_end
    FROM raw_waves
    GROUP BY visa_acct_no, quarter
),

population AS (
    SELECT ft.visa_acct_no, ft.quarter, ft.grp, ft.anchor_dt, qw.window_end
    FROM first_touch ft
    INNER JOIN quarter_window qw
        ON qw.visa_acct_no = ft.visa_acct_no AND qw.quarter = ft.quarter
),

-- base -- DEDUPED, one row per client per quarter. FIXED down the spine.
cohort_cells AS (
    SELECT quarter, grp, COUNT(DISTINCT visa_acct_no) AS base
    FROM population
    GROUP BY quarter, grp
),

-- success pooled across every wave the client touched in the quarter: MIN(event_date) across
-- the client's own pooled window (anchor_dt to window_end), rebased to the first-touch anchor.
-- FIX 2026-08-10: was DATE_DIFF('day', p.anchor_dt, MIN(m.event_date)), which Trino
-- rejects -- anchor_dt is neither aggregated nor in the GROUP BY. Taking MIN of the
-- day-difference is equivalent (anchor_dt is constant within the group and DATE_DIFF
-- is monotonic in its second argument) and is what pp_vba_campaign_quarterly.sql uses.
success AS (
    SELECT
        p.quarter,
        p.grp,
        p.visa_acct_no,
        MIN(DATE_DIFF('day', p.anchor_dt, m.event_date)) AS vintage_day
    FROM population p
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(p.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2025-01-01'
      AND m.event_date BETWEEN p.anchor_dt AND p.window_end
    GROUP BY p.quarter, p.grp, p.visa_acct_no
),

daily_counts AS (
    SELECT quarter, grp, vintage_day, COUNT(DISTINCT visa_acct_no) AS responders
    FROM success
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY quarter, grp, vintage_day
),

-- fixed spine, 0..90, continuous
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
    FROM cohort_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) on purpose, matching the monthly file / pack-wide convention (see
    -- OUTPUT_CONTRACT.md truncation note on Teradata UNION ALL).
    CAST('CRV' AS VARCHAR(20)) AS mne,
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
