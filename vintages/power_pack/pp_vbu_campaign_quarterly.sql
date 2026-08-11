-- pp_vbu_campaign_quarterly.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (8-column shape), QUARTERLY GRAIN VARIANT.
--             mne, quarter, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : VBU (Visa Benefit Upgrade)  CAMPAIGN scope
--
-- BASE FILE: vintages/power_pack/pp_vbu_campaign_v2.sql. ALL population, eligibility, and
--   success logic (base -> elig -> acct_changes -> success_primary) is reproduced VERBATIM from
--   that file. Only the grain changes: v2 emits one cohort_month per deployment window; this file
--   dedups clients to ONE row per (clnt_no, quarter) and pools success across every wave the
--   client touched inside that quarter. Do not "improve" the population/eligibility/success CTEs
--   here -- they are copied from v2's reconciled logic, not re-derived.
--
-- WHY THIS FILE EXISTS SEPARATELY FROM pp_vbu_campaign_v2.sql (the monthly file)
--   Andre wants three quarters side by side: FY25-Q3, FY26-Q2, FY26-Q3. That is NOT the same as
--   pivoting the monthly cube up to quarter: clients are DEDUPLICATED WITHIN THE QUARTER here.
--   A client hit by three VBU waves inside one quarter counts ONCE in base and ONCE (at most)
--   in responders. Summing three monthly rows for that client in a pivot would count them up to
--   three times and inflate base -- the monthly cube's dedup grain is cohort_month (and tactic
--   wave), not quarter, so it does not protect against this on its own. This file re-dedups at
--   the quarter grain from raw tactic history, independently of the monthly file's output.
--   CONSEQUENCE: base in this file will be LOWER than the sum of the monthly cube's base across
--   the quarter's 3 months. That gap IS the duplication being removed -- expected, not a bug.
--
-- QUARTERS (RBC fiscal, Nov-Oct)  THREE quarters, all in one output:
--     FY25-Q3 = 2025-05-01 .. 2025-07-31   (YEAR-OVER-YEAR comparator for FY26-Q3)
--     FY26-Q2 = 2026-02-01 .. 2026-04-30   (SEQUENTIAL comparator -- quarter right before Q3)
--     FY26-Q3 = 2026-05-01 .. 2026-07-31
--   Deployment selection is by END date (treatmt_end_dt), exactly like v2: a deployment belongs
--   to the quarter its end date falls in. FY25-Q3 is NOT contiguous with the FY26 quarters, so
--   the WHERE clause is an OR of two ranges (isolated FY25-Q3 window, plus the combined
--   FY26-Q2..FY26-Q3 span) -- same pattern as pp_vba_campaign_quarterly.sql /
--   pp_crv_campaign_quarterly.sql.
--   CAVEAT: base is a LEVEL comparison, not automatically like-for-like. If VBU's deployed
--   volume changed materially between quarters, base is NOT comparable across quarters on its
--   own -- check base size before reading a response-rate difference as a real shift.
--
-- ANCHOR  Treat_Start_DT (treatmt_strt_dt), same column v2 anchors on. Day 0 = that client's
--   FIRST Treat_Start_DT inside the quarter (first-touch anchor, see DEDUP below).
--
-- FLOOR WIDENED to DATE '2025-01-01' (v2's floor was 2025-10-01) -- tagged "floor guard" below --
--   so FY25-Q3 (starting 2025-05-01) is reachable. The change-detection window in acct_changes
--   and the prior-AIB window in prior_aib are both RELATIVE to Treat_Start_DT (no independent
--   absolute floor of their own), so widening the population floor widens them automatically --
--   nothing further to change there.
--
-- DEDUP -- THIS IS THE POINT OF THE FILE
--   One row per (clnt_no, quarter), anchored on that client's FIRST Treat_Start_DT inside the
--   quarter:
--     ROW_NUMBER() OVER (PARTITION BY clnt_no, quarter ORDER BY Treat_Start_DT ASC) = 1
--   A client touched by 3 VBU waves in one quarter counts ONCE in base. grp comes from the
--   first-touch row. Success is POOLED across every wave the client touched in the quarter: v2's
--   elig -> acct_changes -> prior_aib -> success_primary chain runs per WAVE (unchanged, exactly
--   as v2 built it -- addnl_data_dt and Treat_Start_DT differ per wave so eligibility and the
--   change-detection window cannot be deduped before this point), then the earliest
--   first_change_dt across all of that client's waves in the quarter is taken (success_by_quarter
--   below), and rebased using v2's own day-offset expression to the QUARTER's first-touch
--   anchor_dt, not each wave's own Treat_Start_DT.
--   CROSS-QUARTER NOTE: dedup is WITHIN a quarter only (PARTITION BY includes quarter). A client
--   who shows up in both FY25-Q3 and FY26-Q3 is NOT deduped across those quarters -- they
--   correctly get ONE row in FY25-Q3 AND a separate ONE row in FY26-Q3. Do not read this
--   PARTITION BY as collapsing a client to a single row for the whole file.
--
-- CARRIED FORWARD FROM v2, still open (do not re-decide here):
--   - elig product-match gate REMOVED (it dropped 19,706 of 39,954 -- half the base). Kept
--     removed; see the PRODUCT-MATCH GATE REMOVED block at elig below.
--   - prior-AIB exclusion DISABLED (commented out, LEVER 2 in v2). Kept disabled; see the
--     LEVER 2 block at success_primary below.
--   - [UNRESOLVED / OPEN CAVEAT] success is detected by diffing D3CV12A.dly_full_portfolio
--     snapshots and stamping DT_record_ext, which is the snapshot EXTRACT date, not a true
--     product-change date. Against the dashboard this runs short on Action and over on Control
--     (see v2's RECONCILIATION section). Deduping to quarter grain does NOT fix this -- do not
--     report a quarterly lift off this file without knowing the underlying dt_prod_change
--     measurement is a proxy, not an exact change date.
--
-- ENGINE: Teradata-direct, same as v2. All three source tables (DG6V01.tactic_evnt_ip_ar_hist,
--   d3cv12a.cr_crd_rpts_acct, D3CV12A.dly_full_portfolio) and every function used (INTERVAL,
--   ADD_MONTHS, LAST_DAY, EXTRACT, SYS_CALENDAR) are native Teradata. No dw00 catalog prefix, no
--   Trino functions. No curated dl_mr_prod table anywhere. QUALIFY and VOLATILE TABLE are both
--   used (QUALIFY for the two ROW_NUMBER dedups, VOLATILE TABLE for the day spine and the
--   denominator cells -- TDWM blocks an unconstrained product join against SYS_CALENDAR.CALENDAR,
--   so both sides of that CROSS JOIN are built as VOLATILE TABLEs with COLLECT STATISTICS first,
--   same pattern as v2 and every other pp_*.sql file). Volatile tables named vt_vbu_q_* (not
--   vt_vbu2_*) so this file can coexist with v1 and v2 in the same session without a name
--   collision. Pure ASCII only.
--
-- Spine: fixed 0-90, matching v2's cap and Andre's own BETWEEN 0 AND 90 filter.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW  <<WINDOW>>  EDIT THESE SIX DATES TO RETARGET THE PACK
--   Selects deployments whose END date (Treat_End_DT / treatmt_end_dt) falls in one of three
--   windows; tags the surviving row with which quarter it belongs to. Day 0 still anchors on
--   Treat_Start_DT (START), not these.
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

-- ============================================================================
-- RERUN GUARD -- if re-running this file in the SAME Teradata session, the volatile tables below
-- will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_vbu_q_spine;
--   DROP TABLE vt_vbu_q_cells;
-- Names are vt_vbu_q_* (not vt_vbu2_*) so this file can coexist with v1 (vt_vbu_*, if any) and
-- v2 (vt_vbu2_*) in the same session without a volatile-table name collision.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_vbu_q_cells below without
-- tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu_q_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu_q_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (quarter x grp x base), DEDUPED at the quarter grain -- one row per client
-- per quarter, first-touch wins. Rolling the MONTHLY cube up in a pivot would count a
-- multi-wave client once per month and inflate base; this rebuilds the population chain
-- directly off tactic history so that never happens. VOLATILE for the same TDWM reason -- it is
-- the other side of the spine CROSS JOIN below. Teradata volatile-table creation is a standalone
-- statement (cannot see CTEs defined outside it), so the population-dedup chain is repeated
-- verbatim in the main query below.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu_q_cells AS (
    WITH raw_waves AS (
        SELECT DISTINCT
            E.clnt_no,
            E.treatmt_strt_dt AS Treat_Start_DT,
            CASE WHEN UPPER(SUBSTR(E.tst_grp_cd, 1, 1)) = 'C' THEN 'Control' ELSE 'Action' END AS grp,
            CASE
                WHEN E.treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31' THEN 'FY25-Q3'  -- <<WINDOW>>
                WHEN E.treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' THEN 'FY26-Q2'  -- <<WINDOW>>
                WHEN E.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31' THEN 'FY26-Q3'  -- <<WINDOW>>
            END AS quarter
        FROM DG6V01.tactic_evnt_ip_ar_hist E
        WHERE E.treatmt_strt_dt >= DATE '2025-01-01'                                  -- floor guard
          AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
          AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
          AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
          AND (
                E.treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31'      -- <<WINDOW>>
             OR E.treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-07-31'      -- <<WINDOW>>
              )
    ),
    first_touch AS (
        SELECT clnt_no, quarter, grp, Treat_Start_DT AS anchor_dt
        FROM raw_waves
        QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no, quarter ORDER BY Treat_Start_DT ASC) = 1
    )
    SELECT quarter, grp, COUNT(DISTINCT clnt_no) AS base
    FROM first_touch
    GROUP BY quarter, grp
) WITH DATA PRIMARY INDEX (quarter, grp) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu_q_cells COLUMN (quarter, grp);

-- ----------------------------------------------------------------------------
-- Main query -- base / elig / acct_changes / prior_aib / success_primary reproduced verbatim
-- from v2 (per-wave, unchanged except carrying the quarter tag through), then pooled to
-- (clnt_no, quarter) and rebased to the quarter's first-touch anchor, then the day axis joined
-- off the two volatile tables built above.
-- ----------------------------------------------------------------------------
WITH
base AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50)) AS tactic_id,
        E.treatmt_strt_dt                AS Treat_Start_DT,
        E.treatmt_end_dt                 AS Treat_End_DT,
        E.addnl_data_dt,
        E.tst_grp_cd,
        CASE WHEN UPPER(SUBSTR(E.tst_grp_cd, 1, 1)) = 'C' THEN 'Control' ELSE 'Action' END AS grp,
        CASE
            WHEN E.treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31' THEN 'FY25-Q3'  -- <<WINDOW>>
            WHEN E.treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' THEN 'FY26-Q2'  -- <<WINDOW>>
            WHEN E.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31' THEN 'FY26-Q3'  -- <<WINDOW>>
        END AS quarter
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2025-01-01'                                  -- floor guard
      AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
      AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
      AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
      AND (
            E.treatmt_end_dt BETWEEN DATE '2025-05-01' AND DATE '2025-07-31'      -- <<WINDOW>>
         OR E.treatmt_end_dt BETWEEN DATE '2026-02-01' AND DATE '2026-07-31'      -- <<WINDOW>>
          )
),

-- quarter-grain dedup: one row per (clnt_no, quarter), first-touch wave wins grp + Day-0 anchor.
-- A client in 3 waves this quarter is ONE row here -- that is why this file exists separately
-- from the monthly cube (see header).
first_touch AS (
    SELECT clnt_no, quarter, grp, Treat_Start_DT AS anchor_dt
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no, quarter ORDER BY Treat_Start_DT ASC) = 1
),

-- [VERIFY] SELECT list reconstructed from a partially-obscured screenshot 2026-08-10 (v2's own
-- flag, reproduced here unchanged). Coherent reading: must carry clnt_no, tactic_id, both dates,
-- grp, quarter, acct_no, prod_before, from_product_code -- every later CTE (acct_changes,
-- prior_aib, success_primary) references all of those fields. Not independently reconfirmed.
-- Runs PER WAVE (against base, not first_touch) -- addnl_data_dt differs per wave, so
-- eligibility cannot be deduped to the quarter grain before this point.
elig AS (
    SELECT DISTINCT
        b.clnt_no, b.tactic_id, b.Treat_Start_DT, b.Treat_End_DT, b.grp, b.quarter,
        a.acct_no,
        a.prod_cd_current           AS prod_before,
        SUBSTR(b.tst_grp_cd, 6, 3)  AS from_product_code
    FROM base b
    JOIN d3cv12a.cr_crd_rpts_acct a
        ON a.clnt_no = b.clnt_no
       AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
       -- ==== PRODUCT-MATCH GATE REMOVED, carried forward from v2 (2026-08-10) ============
       -- v2 measured this on the 2026-04-13 wave: gating elig on the from-product code
       -- dropped 19,706 of 39,954 clients (49% of base) with no possible route to the
       -- numerator -- a one-way loss, never a gain. Kept removed here for the same reason.
       -- Restore by uncommenting the two lines below.
       --   AND ( (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
       --      OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX') )
       -- ===================================================================================
       AND a.status = 'OPEN'
),

-- change-detection window anchored on THIS WAVE's own Treat_Start_DT (already floor-guarded via
-- base above, so it widens automatically for FY25-Q3 -- no separate absolute floor needed here),
-- running the full 90-day spine per v2's FIX (2026-08-10) so the last days of the curve are
-- never structurally empty. Runs per wave; pooled downstream in success_by_quarter.
acct_changes AS (
    SELECT
        e.clnt_no, e.tactic_id, e.Treat_Start_DT, e.Treat_End_DT, e.grp, e.quarter, e.acct_no,
        d.visa_prod_cd  AS new_product,
        d.DT_record_ext AS dt_prod_change
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON d.acct_no = e.acct_no
       AND d.DT_record_ext BETWEEN (e.Treat_Start_DT - INTERVAL '1' DAY)
                               AND (e.Treat_Start_DT + INTERVAL '90' DAY)
       AND d.visa_prod_cd <> e.prod_before
       AND d.visa_prod_cd <> e.from_product_code
),

-- prior-AIB-holder exclusion -- clients who already held AIB before Treat_Start_DT are not
-- eligible to count as a new AIB "success". Unchanged from v2, keyed by tactic_id/acct_no
-- (per wave; TACTIC_ID is unique per deployment, see reference_tactic_id_unique_per_deployment).
prior_aib AS (
    SELECT DISTINCT e.tactic_id, e.acct_no
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio p
        ON p.acct_no = e.acct_no
       AND p.visa_prod_cd = 'AIB'
       AND p.dt_record_ext < e.Treat_Start_DT
),

-- SUCCESS METRIC per wave (the ONE metric, per contract rule 4) -- unchanged from v2.
-- first_change_dt = MIN(dt_prod_change) per (clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT,
-- grp, quarter). Pooling across waves happens in success_by_quarter below, NOT here.
success_primary AS (
    SELECT
        ac.clnt_no, ac.tactic_id, ac.Treat_Start_DT, ac.Treat_End_DT, ac.grp, ac.quarter,
        MIN(ac.dt_prod_change) AS first_change_dt
    FROM acct_changes ac
    LEFT JOIN prior_aib prior
        ON prior.tactic_id = ac.tactic_id AND prior.acct_no = ac.acct_no
    WHERE ac.new_product = 'AIB'
      -- ==== LEVER 2, carried forward from v2 (currently OFF, disabled 2026-08-10) =======
      -- Re-enable by uncommenting the line below.
      --   AND prior.acct_no IS NULL
      -- ===================================================================================
    GROUP BY ac.clnt_no, ac.tactic_id, ac.Treat_Start_DT, ac.Treat_End_DT, ac.grp, ac.quarter
),

-- THE POOL: earliest first_change_dt across every wave the client touched in the quarter.
-- This step does not exist in v2 -- v2 stays at (clnt_no, tactic_id) grain. Here, all of a
-- client's waves inside one quarter collapse to a single success row.
success_by_quarter AS (
    SELECT clnt_no, quarter, MIN(first_change_dt) AS first_change_dt
    FROM success_primary
    GROUP BY clnt_no, quarter
),

-- day axis: v2's own day-offset expression, reused exactly, but rebased to the QUARTER's
-- first-touch anchor_dt (not each wave's own Treat_Start_DT).
numerator AS (
    SELECT
        ft.clnt_no,
        ft.quarter,
        ft.grp,
        CAST(CASE WHEN sq.first_change_dt < ft.anchor_dt THEN 0
                  ELSE sq.first_change_dt - ft.anchor_dt END AS INTEGER) AS vintage_day
    FROM first_touch ft
    INNER JOIN success_by_quarter sq
        ON sq.clnt_no = ft.clnt_no AND sq.quarter = ft.quarter
),

daily_counts AS (
    SELECT quarter, grp, vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM numerator
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY quarter, grp, vintage_day
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
    FROM vt_vbu_q_cells c
    CROSS JOIN vt_vbu_q_spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character length is
    -- fixed by the FIRST SELECT block, so stacking a 3-char 'VBU' block ahead of a longer label
    -- would silently truncate it.
    CAST('VBU' AS VARCHAR(20)) AS mne,
    CAST(g.quarter AS VARCHAR(7)) AS quarter,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.quarter, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.quarter     = g.quarter
    AND dc.grp          = g.grp
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.quarter_sort, g.grp, g.vintage_day;
