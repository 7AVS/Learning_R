-- pp_vbu_westjet_quarterly.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (8-column shape), QUARTERLY GRAIN VARIANT.
--             mne, quarter, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : VBU (Visa Benefit Upgrade)  WestJet PRODUCT SLICE (segment = 'WestJet')
--
-- ============================================================================
-- WESTJET PRODUCT FILTER -- [VERIFY] NOT CONFIRMED FOR VBU
-- WestJet is 'MCB' in VBA's visa_offer_prod (jupyter_vba_local.py:221).
-- VBU has NO product-brand split anywhere in the repo - its only product
-- logic is the AIB upgrade. This filter is a BEST GUESS by analogy with
-- VBU's own from-product encoding, SUBSTR(tst_grp_cd, 6, 3), which is how
-- vbu_vintage_original.sql:65 reads the from-product.
-- CONFIRM the code before trusting any number from this file.
-- ============================================================================
-- WESTJET CODE : 'MCB'   -- <<WESTJET>>
--
-- BASE FILE: vintages/power_pack/pp_vbu_campaign_quarterly.sql. ALL population, eligibility, and
--   success logic (base -> elig -> acct_changes -> success_primary) is reproduced VERBATIM from
--   that file, itself reproduced verbatim from pp_vbu_campaign_v2.sql. The ONLY additions here
--   are (1) the WestJet product filter in elig, tagged -- <<WESTJET>> -- below, and (2) collapsing
--   the parent's three-quarters-in-one-run pattern to ONE QUARTER PER RUN (see next section). Do
--   not "improve" the population/eligibility/success CTEs here -- they are copied, not re-derived.
--
-- ONE QUARTER PER RUN (deliberate deviation from pp_vbu_campaign_quarterly.sql, which runs all
--   three quarters in a single statement via an OR-of-two-ranges WHERE + CASE quarter tag). This
--   file follows the pp_vba_campaign_quarterly.sql / pp_vba_westjet_quarterly.sql convention
--   instead: one quarter selected by a literal parameter block, run three times, stack the saved
--   outputs afterwards (UNION ALL, no further dedup needed -- each run's dedup is already scoped
--   to its own quarter).
--
--   FY25-Q3   quarter end 2025-05-01 .. 2025-07-31 (excl 2025-08-01)   scan floor 2025-02-01
--   FY26-Q2   quarter end 2026-02-01 .. 2026-04-30 (excl 2026-05-01)   scan floor 2025-11-01
--   FY26-Q3   quarter end 2026-05-01 .. 2026-07-31 (excl 2026-08-01)   scan floor 2026-02-01
--
-- Every occurrence below is tagged -- <<QUARTER>> -- grep that tag to find and edit all four
-- values (label, end-from, end-to, scan floor) in one pass.
--
-- CURRENTLY SET TO: FY26-Q3
--
-- Deployment selection is by END date (treatmt_end_dt), exactly like the parent files: a
--   deployment belongs to the quarter its end date falls in. Day 0 still anchors on
--   Treat_Start_DT (treatmt_strt_dt), not the end date.
--
-- ANCHOR  Treat_Start_DT (treatmt_strt_dt), same column the parent files anchor on. Day 0 = that
--   client's FIRST Treat_Start_DT inside the quarter (first-touch anchor, see DEDUP below).
--
-- DEDUP -- THIS IS THE POINT OF THE FILE (unchanged from the parent quarterly file)
--   One row per (clnt_no, quarter), anchored on that client's FIRST Treat_Start_DT inside the
--   quarter:
--     ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY Treat_Start_DT ASC) = 1
--   (PARTITION BY drops the quarter key relative to the parent file -- this run only ever
--   targets one quarter, so there is nothing to cross into, same simplification
--   pp_vba_westjet_quarterly.sql / pp_vba_campaign_quarterly.sql make for the same reason.)
--   A client touched by multiple VBU waves in this quarter counts ONCE in base. grp comes from
--   the first-touch row. Success is POOLED across every wave the client touched in the quarter:
--   the parent's elig -> acct_changes -> prior_aib -> success_primary chain runs per WAVE
--   (unchanged -- addnl_data_dt and Treat_Start_DT differ per wave so eligibility and the
--   change-detection window cannot be deduped before this point), then the earliest
--   first_change_dt across all of that client's waves in the quarter is taken
--   (success_by_quarter below), and rebased to the quarter's first-touch anchor_dt.
--
-- CARRIED FORWARD FROM THE PARENT FILES, still open (do not re-decide here):
--   - elig product-match gate REMOVED (see the PRODUCT-MATCH GATE REMOVED block at elig below).
--   - prior-AIB exclusion DISABLED (LEVER 2, commented out at success_primary below).
--   - [UNRESOLVED / OPEN CAVEAT] success is detected by diffing D3CV12A.dly_full_portfolio
--     snapshots and stamping DT_record_ext, which is the snapshot EXTRACT date, not a true
--     product-change date. See the parent file's RECONCILIATION note (via v2). Restricting to
--     WestJet does NOT fix this -- do not report a lift off this file without knowing the
--     underlying dt_prod_change measurement is a proxy, not an exact change date.
--   - [VERIFY, THIS FILE'S OWN ADD] the WestJet filter itself is unconfirmed -- see the loud
--     block at the top of this header. If this file returns ZERO rows, that is NOT proof VBU has
--     no WestJet volume -- it may mean the code guess is wrong. Run the diagnostic query at the
--     bottom of this file (SUBSTR(tst_grp_cd,6,3) value counts against real VBU population) to
--     see the actual product codes before concluding zero volume.
--
-- ENGINE: Teradata-direct, same as the parent files. All three source tables
--   (DG6V01.tactic_evnt_ip_ar_hist, d3cv12a.cr_crd_rpts_acct, D3CV12A.dly_full_portfolio) and
--   every function used (INTERVAL, ADD_MONTHS, LAST_DAY, EXTRACT, SYS_CALENDAR) are native
--   Teradata. No dw00 catalog prefix, no Trino functions, no curated dl_mr_prod table anywhere.
--   QUALIFY and VOLATILE TABLE both used (QUALIFY for the two ROW_NUMBER dedups, VOLATILE TABLE
--   for the day spine and the denominator cells -- TDWM blocks an unconstrained product join
--   against SYS_CALENDAR.CALENDAR, so both sides of that CROSS JOIN are built as VOLATILE TABLEs
--   with COLLECT STATISTICS first). Volatile tables named vt_vbu_wj_* (not vt_vbu_q_* or
--   vt_vbu2_*) so this file can coexist with the parent quarterly file and v1/v2 in the same
--   session without a volatile-table name collision. Pure ASCII only.
--
-- Spine: fixed 0-90, matching every other file in this pack.
-- ============================================================================

-- ============================================================================
-- QUARTER PARAMETER BLOCK -- EDIT THESE FOUR VALUES TO RETARGET THE RUN
-- ============================================================================
-- QUARTER LABEL      : 'FY26-Q3'          -- <<QUARTER>>
-- QUARTER END FROM   : DATE '2026-05-01'  -- <<QUARTER>> (bounds treatmt_end_dt, inclusive)
-- QUARTER END TO     : DATE '2026-08-01'  -- <<QUARTER>> exclusive
-- SCAN FLOOR         : DATE '2026-02-01'  -- <<QUARTER>> earliest possible anchor (treatmt_strt_dt)
-- ============================================================================

-- ============================================================================
-- RERUN GUARD -- if re-running this file in the SAME Teradata session, the volatile tables below
-- will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_vbu_wj_spine;
--   DROP TABLE vt_vbu_wj_cells;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_vbu_wj_cells below without
-- tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu_wj_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu_wj_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (grp x base), deduped at the quarter grain with the WestJet filter applied,
-- one row per client this quarter, first-touch wins. VOLATILE for the same TDWM reason -- it is
-- the other side of the spine CROSS JOIN below. Teradata volatile-table creation is a standalone
-- statement (cannot see CTEs defined outside it), so the population-dedup chain (including the
-- WestJet filter) is repeated verbatim in the main query below.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu_wj_cells AS (
    WITH raw_waves AS (
        SELECT DISTINCT
            E.clnt_no,
            E.treatmt_strt_dt AS Treat_Start_DT,
            CASE WHEN UPPER(SUBSTR(E.tst_grp_cd, 1, 1)) = 'C' THEN 'Control' ELSE 'Action' END AS grp
        FROM DG6V01.tactic_evnt_ip_ar_hist E
        WHERE E.treatmt_strt_dt >= DATE '2026-02-01'                          -- <<QUARTER>> scan floor
          AND E.treatmt_end_dt  >= DATE '2026-05-01'                          -- <<QUARTER>> quarter end from
          AND E.treatmt_end_dt  <  DATE '2026-08-01'                          -- <<QUARTER>> quarter end to (excl)
          AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
          AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
          AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
          AND SUBSTR(TRIM(E.tst_grp_cd), 6, 3) = 'MCB'                        -- <<WESTJET>>
    ),
    first_touch AS (
        SELECT clnt_no, grp
        FROM raw_waves
        QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY Treat_Start_DT ASC) = 1
    )
    SELECT grp, COUNT(DISTINCT clnt_no) AS base
    FROM first_touch
    GROUP BY grp
) WITH DATA PRIMARY INDEX (grp) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu_wj_cells COLUMN (grp);

-- ----------------------------------------------------------------------------
-- Main query -- base / elig / acct_changes / prior_aib / success_primary reproduced verbatim
-- from the parent quarterly file (per wave), with the WestJet filter added in elig, then pooled
-- to clnt_no and rebased to the quarter's first-touch anchor, then the day axis joined off the
-- two volatile tables built above.
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
        CAST('FY26-Q3' AS VARCHAR(7)) AS quarter                              -- <<QUARTER>>
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt >= DATE '2026-02-01'                              -- <<QUARTER>> scan floor
      AND E.treatmt_end_dt  >= DATE '2026-05-01'                              -- <<QUARTER>> quarter end from
      AND E.treatmt_end_dt  <  DATE '2026-08-01'                              -- <<QUARTER>> quarter end to (excl)
      AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
      AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
      AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
),

-- quarter-grain dedup: one row per clnt_no, first-touch wave wins grp + Day-0 anchor. A client
-- in multiple waves this quarter is ONE row here.
first_touch AS (
    SELECT clnt_no, quarter, grp, Treat_Start_DT AS anchor_dt
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY Treat_Start_DT ASC) = 1
),

-- [VERIFY] SELECT list reconstructed from a partially-obscured screenshot 2026-08-10 (parent
-- file's own flag, reproduced here unchanged). Coherent reading: must carry clnt_no, tactic_id,
-- both dates, grp, quarter, acct_no, prod_before, from_product_code -- every later CTE
-- (acct_changes, prior_aib, success_primary) references all of those fields. Not independently
-- reconfirmed. Runs PER WAVE (against base, not first_touch) -- addnl_data_dt differs per wave,
-- so eligibility cannot be deduped to the quarter grain before this point.
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
       -- ==== PRODUCT-MATCH GATE REMOVED, carried forward from the parent file (2026-08-10) ====
       -- The parent file measured this on the 2026-04-13 wave: gating elig on the from-product
       -- code dropped 19,706 of 39,954 clients (49% of base) with no possible route to the
       -- numerator -- a one-way loss, never a gain. Kept removed here for the same reason.
       -- Restore by uncommenting the two lines below.
       --   AND ( (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
       --      OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX') )
       -- =======================================================================================
       AND a.status = 'OPEN'
       AND SUBSTR(TRIM(b.tst_grp_cd), 6, 3) = 'MCB'                          -- <<WESTJET>>
),

-- change-detection window anchored on THIS WAVE's own Treat_Start_DT, running the full 90-day
-- spine per the parent file's fix (2026-08-10) so the last days of the curve are never
-- structurally empty. Runs per wave; pooled downstream in success_by_quarter.
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
-- eligible to count as a new AIB "success". Unchanged from the parent file, keyed by
-- tactic_id/acct_no (per wave; TACTIC_ID is unique per deployment).
prior_aib AS (
    SELECT DISTINCT e.tactic_id, e.acct_no
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio p
        ON p.acct_no = e.acct_no
       AND p.visa_prod_cd = 'AIB'
       AND p.dt_record_ext < e.Treat_Start_DT
),

-- SUCCESS METRIC per wave (the ONE metric, per contract rule 4) -- unchanged from the parent
-- file. first_change_dt = MIN(dt_prod_change) per (clnt_no, tactic_id, Treat_Start_DT,
-- Treat_End_DT, grp, quarter). Pooling across waves happens in success_by_quarter below, NOT
-- here.
success_primary AS (
    SELECT
        ac.clnt_no, ac.tactic_id, ac.Treat_Start_DT, ac.Treat_End_DT, ac.grp, ac.quarter,
        MIN(ac.dt_prod_change) AS first_change_dt
    FROM acct_changes ac
    LEFT JOIN prior_aib prior
        ON prior.tactic_id = ac.tactic_id AND prior.acct_no = ac.acct_no
    WHERE ac.new_product = 'AIB'
      -- ==== LEVER 2, carried forward from the parent file (currently OFF) ===================
      -- Re-enable by uncommenting the line below.
      --   AND prior.acct_no IS NULL
      -- =======================================================================================
    GROUP BY ac.clnt_no, ac.tactic_id, ac.Treat_Start_DT, ac.Treat_End_DT, ac.grp, ac.quarter
),

-- THE POOL: earliest first_change_dt across every wave the client touched in the quarter.
success_by_quarter AS (
    SELECT clnt_no, quarter, MIN(first_change_dt) AS first_change_dt
    FROM success_primary
    GROUP BY clnt_no, quarter
),

-- day axis: parent file's own day-offset expression, reused exactly, rebased to the quarter's
-- first-touch anchor_dt (not each wave's own Treat_Start_DT).
numerator AS (
    SELECT
        ft.clnt_no,
        ft.grp,
        CAST(CASE WHEN sq.first_change_dt < ft.anchor_dt THEN 0
                  ELSE sq.first_change_dt - ft.anchor_dt END AS INTEGER) AS vintage_day
    FROM first_touch ft
    INNER JOIN success_by_quarter sq
        ON sq.clnt_no = ft.clnt_no AND sq.quarter = ft.quarter
),

daily_counts AS (
    SELECT grp, vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM numerator
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY grp, vintage_day
),

dense_grid AS (
    SELECT c.grp, c.base, s.vintage_day
    FROM vt_vbu_wj_cells c
    CROSS JOIN vt_vbu_wj_spine s
)

SELECT
    -- VARCHAR(20) on purpose, matching the pack-wide convention (see OUTPUT_CONTRACT.md
    -- truncation note on Teradata UNION ALL).
    CAST('VBU WestJet' AS VARCHAR(20)) AS mne,
    CAST('FY26-Q3' AS VARCHAR(7)) AS quarter,                                 -- <<QUARTER>>
    CAST('WestJet' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.grp          = g.grp
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): if the main query above returns ZERO rows, the WestJet code guess
-- ('MCB') may be wrong, or VBU may have no WestJet volume. Run this first to see the real
-- product codes in VBU's tst_grp_cd before concluding zero volume. Not scoped to any single
-- quarter -- floored at 2025-01-01 for a broad read.
-- ============================================================================
-- SELECT SUBSTR(TRIM(tst_grp_cd),6,3) AS prod, COUNT(DISTINCT clnt_no)
-- FROM DG6V01.tactic_evnt_ip_ar_hist
-- WHERE SUBSTR(tactic_id,8,3)='VBU' AND treatmt_strt_dt >= DATE '2025-01-01'
-- GROUP BY 1 ORDER BY 2 DESC;
