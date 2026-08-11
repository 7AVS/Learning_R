-- pp_vba_westjet_quarterly.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (8-column shape), QUARTERLY GRAIN VARIANT.
--             mne, quarter, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : VBA (Visa Benefit Add)  WestJet PRODUCT SLICE (segment = 'WestJet')
--
-- WHAT WESTJET IS
--   WestJet = visa_offer_prod = 'MCB'. Proof: campaigns/VBA_VBU/jupyter_vba_local.py:221
--   `& (df['visa_offer_prod'] == 'MCB')`, under the heading "WestJet (MCB) approver client
--   profile". Other observed values of that column in the same file: CPX, MC6, AIB.
--
-- SOURCE -- built DIRECTLY off the curated combined table, no Casper/SCOT join needed:
--   dl_mr_prod.nbo_vba_rbol_combined (campaigns/VBA_VBU/vba_deep_dive_cell.py:97,106). This
--   table already carries the product, the arm, and the approval flags on one row -- unlike
--   the campaign-scope pp_vba_campaign_quarterly.sql, which has to reach Casper + SCOT because
--   raw tactic history alone has no product or approval data. No dw00_im prefix (Andre,
--   2026-08-10: the dw00 prefix never resolves on Teradata) -- table is bare dl_mr_prod.
--
-- CONFIRMED COLUMNS ONLY -- vba_deep_dive_cell.py:93-110 (the Q1 rollup query) is the only
--   place in the repo that lists real column names off this table. Columns used below are
--   restricted to that list: control, treatmt_strt_dt, visa_offer_prod, visa_acct_no,
--   visa_app_approved, visa_app_started, visa_app_completed, visa_app_declined, gross_response,
--   net_response, mnc. Nothing outside that list is referenced. Where the file needs something
--   not on that list, it is flagged [VERIFY] below rather than invented.
--
-- FILTER: mnc = 'VBA' AND visa_offer_prod = 'MCB'.
--
-- SUCCESS: visa_app_approved = 1 (per campaign-scope convention: one primary success metric,
--   contract rule 4). Pooled across every wave the client touched this quarter -- MAX() over
--   the client's rows, so an approval on a later wave is not lost because the client's
--   first-touch wave declined.
--
-- [VERIFY -- LOUD, READ BEFORE TRUSTING THE CURVE] NO APPROVAL DATE COLUMN CONFIRMED.
--   vba_deep_dive_cell.py's column list has visa_app_approved as a 0/1 FLAG, not a date. There
--   is no confirmed column recording WHEN the approval happened -- only treatmt_strt_dt (the
--   deployment date) is confirmed. Contract rule: vintage_day = days from treatment start to
--   the success event. Without a success DATE, that day-level curve cannot be computed as
--   designed, and this file does NOT invent one.
--   WHAT THIS FILE DOES INSTEAD: every approval this quarter is posted at vintage_day = 0 (a
--   same-quarter yes/no flag, not a real time-to-approval curve). base/responders/responders_cum
--   are still correct COUNTS -- what is NOT correct is the shape of the curve across days 1-90,
--   which will read as a single spike at day 0 then flat. Do not read this file's curve shape as
--   "everyone approves instantly" -- it means "no approval date exists to place them elsewhere."
--   WHAT WOULD FIX IT: confirm whether nbo_vba_rbol_combined carries an approval-date column
--   not visible in the deep-dive script (check the live table's full column list), or rebuild
--   this file joining a real approval-date source (Casper app_rcv_dt / SCOT
--   creditapplication_createddatetime) the way pp_vba_campaign_quarterly.sql does.
--
-- [VERIFY] CLIENT KEY = visa_acct_no, NOT clnt_no. OUTPUT_CONTRACT.md's file inventory lists
--   clnt_no as VBA's dedup id (campaign-scope file, off raw tactic history). clnt_no is NOT a
--   confirmed column on nbo_vba_rbol_combined -- only visa_acct_no is (vba_deep_dive_cell.py
--   does not select clnt_no in the Q1 query). This file dedups and bases everything on
--   visa_acct_no instead. If a client can hold more than one Visa account, base here is at
--   ACCOUNT grain, not CLIENT grain, and will not match the campaign-scope file's base 1:1 --
--   do not diff the two files' base counts as a QA check without accounting for this.
--
-- [VERIFY] QUARTER SELECTION BY treatmt_strt_dt, NOT treatmt_end_dt. The parent file
--   (pp_vba_campaign_quarterly.sql) selects deployments by treatmt_end_dt falling in the
--   quarter. treatmt_end_dt is NOT a confirmed column on nbo_vba_rbol_combined -- it is not in
--   the vba_deep_dive_cell.py column list. This file selects by treatmt_strt_dt instead (the
--   confirmed date column, also the Day-0 anchor). END FROM / END TO below bound
--   treatmt_strt_dt directly, not a deployment end date. Quarter counts here may differ from
--   the parent file's for deployments that start in one quarter and end in the next.
--
-- [VERIFY] grp DERIVATION -- assumes the `control` column already stores the literal strings
--   'Action' / 'Control'. Inferred from campaigns/VBA_VBU/jupyter_vba_local.py, which compares
--   `df['control'] == 'Action'` directly (line ~172, ~222) and elsewhere groups by `control`
--   then unstacks to columns named rate_Action / rate_Control -- both only work if those are the
--   literal stored values. This has NOT been independently confirmed against the live Teradata
--   column (case, trailing whitespace, or a coded value like 'C'/'T' would all break the CASE
--   below silently into NULL grp). Guard: TRIM() + exact match, non-matching values are excluded
--   from population entirely (contract rule 5: grp is strictly binary) -- if base comes back
--   empty or unexpectedly small, check `SELECT DISTINCT control FROM dl_mr_prod.nbo_vba_rbol_combined
--   WHERE mnc='VBA' AND visa_offer_prod='MCB'` before assuming zero WestJet volume.
--
-- mne     : CAST('VBA WestJet' AS VARCHAR(20)).
-- segment : CAST('WestJet' AS VARCHAR(20))  constant, this file is a single-product slice.
--
-- DEDUP: one row per visa_acct_no (within the single quarter this run targets), anchored on
--   that account's FIRST treatmt_strt_dt in the quarter (first-touch), same first-touch pattern
--   as every other pp_*_quarterly.sql file:
--     ROW_NUMBER() OVER (PARTITION BY visa_acct_no ORDER BY treatmt_strt_dt ASC) = 1
--   grp comes from the first-touch row. Success (visa_app_approved) is pooled with MAX() across
--   every row (wave) the account touched this quarter, not just the first-touch row.
--
-- ENGINE: Teradata-direct. This file reaches ONLY dl_mr_prod.nbo_vba_rbol_combined, a Teradata
--   curated table -- no edl0_im / SCOT reach, so nothing forces Starburst/Trino syntax the way
--   the campaign-scope pp_vba_campaign_quarterly.sql needs it (that file joins SCOT). QUALIFY
--   and VOLATILE TABLE both used (QUALIFY for the first-touch dedup, VOLATILE TABLE for the day
--   spine and the base cells -- TDWM blocks an unconstrained product join against
--   SYS_CALENDAR.CALENDAR, so both sides of that CROSS JOIN are built as VOLATILE TABLEs with
--   COLLECT STATISTICS first, same pattern as every Teradata-direct file in this pack). Volatile
--   tables named vt_vba_wj_* so this file can coexist with any other pp_vba_*.sql volatile
--   tables in the same session. Pure ASCII only.
--
-- Spine: fixed 0-90, continuous, per the output contract. Counts only -- COUNT(DISTINCT
--   visa_acct_no), never COUNT(*).

-- ============================================================================
-- RUN ONE QUARTER AT A TIME, same discipline as pp_vba_campaign_quarterly.sql. Edit the four
-- <<QUARTER>> parameters below, run, save the output, move to the next quarter, then stack the
-- three saved outputs (UNION ALL, no further dedup needed -- each run's dedup is already scoped
-- to its own quarter).
--
--   FY25-Q3   quarter start 2025-05-01 .. 2025-07-31 (excl 2025-08-01)   scan floor 2025-02-01
--   FY26-Q2   quarter start 2026-02-01 .. 2026-04-30 (excl 2026-05-01)   scan floor 2025-11-01
--   FY26-Q3   quarter start 2026-05-01 .. 2026-07-31 (excl 2026-08-01)   scan floor 2026-02-01
--
-- Every occurrence below is tagged -- <<QUARTER>> -- grep that tag to find and edit all four
-- values (label, end-from, end-to, scan floor) in one pass.
--
-- CURRENTLY SET TO: FY26-Q3
-- ============================================================================
-- QUARTER LABEL      : 'FY26-Q3'          -- <<QUARTER>>
-- QUARTER END FROM   : DATE '2026-05-01'  -- <<QUARTER>> (bounds treatmt_strt_dt, see VERIFY above)
-- QUARTER END TO     : DATE '2026-08-01'  -- <<QUARTER>> exclusive
-- SCAN FLOOR         : DATE '2026-02-01'  -- <<QUARTER>> earliest possible anchor
-- ============================================================================

-- ============================================================================
-- RERUN GUARD -- if re-running this file in the SAME Teradata session, the volatile tables
-- below will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_vba_wj_spine;
--   DROP TABLE vt_vba_wj_cells;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_vba_wj_cells below without
-- tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vba_wj_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vba_wj_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (grp x base), deduped to one row per visa_acct_no this quarter, first-touch
-- wins grp. VOLATILE for the same TDWM reason -- it is the other side of the spine CROSS JOIN
-- below. Teradata volatile-table creation is a standalone statement (cannot see CTEs defined
-- outside it), so the population-dedup chain is repeated verbatim in the main query below.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vba_wj_cells AS (
    WITH raw_waves AS (
        SELECT
            r.visa_acct_no,
            r.treatmt_strt_dt,
            CASE
                WHEN TRIM(r.control) = 'Action'  THEN 'Action'
                WHEN TRIM(r.control) = 'Control' THEN 'Control'
            END AS grp                                              -- [VERIFY] see header note
        FROM dl_mr_prod.nbo_vba_rbol_combined r
        WHERE r.mnc = 'VBA'
          AND r.visa_offer_prod = 'MCB'
          AND r.treatmt_strt_dt >= DATE '2026-02-01'                 -- <<QUARTER>> scan floor
          AND r.treatmt_strt_dt >= DATE '2026-05-01'                 -- <<QUARTER>> quarter end from
          AND r.treatmt_strt_dt <  DATE '2026-08-01'                 -- <<QUARTER>> quarter end to (excl)
          AND TRIM(r.control) IN ('Action', 'Control')
    ),
    first_touch AS (
        SELECT visa_acct_no, grp
        FROM raw_waves
        QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no ORDER BY treatmt_strt_dt ASC) = 1
    )
    SELECT grp, COUNT(DISTINCT visa_acct_no) AS base
    FROM first_touch
    GROUP BY grp
) WITH DATA PRIMARY INDEX (grp) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vba_wj_cells COLUMN (grp);

-- ----------------------------------------------------------------------------
-- Main query -- population/dedup/success reproduced, then joined onto the two volatile tables
-- built above.
-- ----------------------------------------------------------------------------
WITH
raw_waves AS (
    SELECT
        r.visa_acct_no,
        r.treatmt_strt_dt,
        r.visa_app_approved,
        CASE
            WHEN TRIM(r.control) = 'Action'  THEN 'Action'
            WHEN TRIM(r.control) = 'Control' THEN 'Control'
        END AS grp                                                  -- [VERIFY] see header note
    FROM dl_mr_prod.nbo_vba_rbol_combined r
    WHERE r.mnc = 'VBA'
      AND r.visa_offer_prod = 'MCB'
      AND r.treatmt_strt_dt >= DATE '2026-02-01'                     -- <<QUARTER>> scan floor
      AND r.treatmt_strt_dt >= DATE '2026-05-01'                     -- <<QUARTER>> quarter end from
      AND r.treatmt_strt_dt <  DATE '2026-08-01'                     -- <<QUARTER>> quarter end to (excl)
      AND TRIM(r.control) IN ('Action', 'Control')
),

-- first-touch anchor + grp, one row per visa_acct_no this quarter
first_touch AS (
    SELECT visa_acct_no, grp, treatmt_strt_dt AS anchor_dt
    FROM raw_waves
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no ORDER BY treatmt_strt_dt ASC) = 1
),

-- success POOLED across every wave the account touched this quarter -- MAX(), not first-touch
-- row only, so an approval on a later wave is not lost.
success_pool AS (
    SELECT visa_acct_no, MAX(visa_app_approved) AS approved_any
    FROM raw_waves
    GROUP BY visa_acct_no
),

population AS (
    SELECT ft.visa_acct_no, ft.grp, ft.anchor_dt, sp.approved_any
    FROM first_touch ft
    INNER JOIN success_pool sp ON sp.visa_acct_no = ft.visa_acct_no
),

-- [VERIFY -- see loud header note] no approval date exists on this table -- every approval this
-- quarter is posted at vintage_day = 0. This is a same-quarter yes/no flag, not a real
-- time-to-approval curve. base/responders/responders_cum counts are correct; the day-1..90 shape
-- is not.
success_day0 AS (
    SELECT grp, COUNT(DISTINCT visa_acct_no) AS responders
    FROM population
    WHERE approved_any >= 1
    GROUP BY grp
),

dense_grid AS (
    SELECT c.grp, c.base, s.vintage_day
    FROM vt_vba_wj_cells c
    CROSS JOIN vt_vba_wj_spine s
)

SELECT
    CAST('VBA WestJet' AS VARCHAR(20)) AS mne,
    CAST('FY26-Q3' AS VARCHAR(7)) AS quarter,                        -- <<QUARTER>>
    CAST('WestJet' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(CASE WHEN g.vintage_day = 0 THEN COALESCE(d0.responders, 0) ELSE 0 END AS INTEGER)
        AS responders,
    CAST(COALESCE(d0.responders, 0) AS INTEGER) AS responders_cum
    -- responders_cum is flat from day 0 onward on purpose: everything posts at day 0 (see
    -- VERIFY note above), so the cumulative sum never changes across the rest of the spine.
FROM dense_grid g
LEFT JOIN success_day0 d0
    ON d0.grp = g.grp
ORDER BY g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many accounts hit both arms this quarter?
-- (Same guard as every other pp_*_quarterly.sql file's bottom diagnostic -- expect zeros.)
-- Edit the same <<QUARTER>> dates as the main query above before running.
-- ============================================================================
-- SELECT COUNT(*) AS conflicted_accounts FROM (
--     SELECT visa_acct_no FROM (
--         SELECT
--             r.visa_acct_no,
--             CASE
--                 WHEN TRIM(r.control) = 'Action'  THEN 'Action'
--                 WHEN TRIM(r.control) = 'Control' THEN 'Control'
--             END AS grp
--         FROM dl_mr_prod.nbo_vba_rbol_combined r
--         WHERE r.mnc = 'VBA'
--           AND r.visa_offer_prod = 'MCB'
--           AND r.treatmt_strt_dt >= DATE '2026-02-01'                 -- <<QUARTER>> scan floor
--           AND r.treatmt_strt_dt >= DATE '2026-05-01'                 -- <<QUARTER>> quarter end from
--           AND r.treatmt_strt_dt <  DATE '2026-08-01'                 -- <<QUARTER>> quarter end to (excl)
--           AND TRIM(r.control) IN ('Action', 'Control')
--     ) raw
--     GROUP BY visa_acct_no
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x;
