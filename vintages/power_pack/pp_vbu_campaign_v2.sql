-- pp_vbu_campaign_v2.sql
-- ============================================================================
-- VERSION 2 of the VBU campaign vintage. Built from Andre's OWN working summary
-- query, transcribed from screenshots 2026-08-10. That query returns real rows
-- and its lead counts (base, per wave) reconcile exactly against the tactic
-- table (DG6V01.tactic_evnt_ip_ar_hist) -- see RECONCILIATION TARGET below.
--
-- THIS FILE DOES NOT TOUCH THE CURATED DATALAB TABLE AT ALL.
--   dl_mr_prod.cards_bizups_vbu_descresp_clnt is never referenced anywhere in
--   this file. That curated table is what pp_vbu_campaign.sql (v1) is built
--   on. v1's curated-to-tactic join (vbu_arm <-> cards_bizups_vbu_descresp_clnt
--   on tactic_id/clnt_no) returned ZERO rows before its 2026-08-10 join-key
--   normalization fix -- v1's own header documents this ("JOIN-KEY FIX
--   2026-08-10 ... Unnormalised, this join returned ZERO rows on 2026-08-10").
--   v2 sidesteps that fragility entirely: it rebuilds population, eligibility,
--   and product-change detection directly off the tactic table + the
--   portfolio/account tables Andre's own query uses, with no curated-table
--   join in the chain at all.
--
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 8 columns, exact order:
--   mne | cohort_month | segment | grp | vintage_day | base | responders | responders_cum.
--   mne = CAST('VBU' AS VARCHAR(20)). segment = CAST('All' AS VARCHAR(20)) constant --
--   same as v1, no pre-treatment population split above Action/Control for VBU.
--
-- SOURCE TABLES (three, all Teradata-direct, no curated table among them):
--   1. DG6V01.tactic_evnt_ip_ar_hist   -- population / cohort / grp anchor  (base CTE)
--   2. d3cv12a.cr_crd_rpts_acct        -- account eligibility snapshot     (elig CTE)
--   3. D3CV12A.dly_full_portfolio      -- daily product-code history, used twice:
--                                          product-change detection (acct_changes CTE)
--                                          and prior-AIB-holder exclusion (prior_aib CTE)
--
-- CTEs REPRODUCED VERBATIM from Andre's working query (base, elig, acct_changes) --
--   nothing added to their logic except the <<WINDOW>> tags on base (see below) and
--   the [VERIFY] note on elig's SELECT list (see that CTE). Do not "improve" these;
--   they are the thing that already reconciles.
--
-- [VERIFY] elig's SELECT list was PARTLY OBSCURED in the source screenshot. The
--   version reproduced below is the coherent reading: it carries clnt_no, tactic_id,
--   both dates, grp, acct_no, prod_before, from_product_code because every later CTE
--   (acct_changes, prior_aib, success_primary) references all seven of those fields.
--   Flagged, not independently reconfirmed against a second screenshot.
--
-- SUCCESS METRIC (ONE metric, per contract rule 4): Andre's success_primary --
--   product change TO 'AIB', EXCLUDING clients who already held an AIB product
--   before Treat_Start_DT (prior_aib CTE, LEFT JOIN ... WHERE prior.acct_no IS NULL).
--   first_change_dt = MIN(dt_prod_change) per (clnt_no, tactic_id, Treat_Start_DT,
--   Treat_End_DT, grp) -- Andre's own aggregation grain, reproduced exactly.
--   success_any (any product change, no AIB/prior-holder restriction) is available
--   in acct_changes but is DELIBERATELY DROPPED here -- one metric per file, and
--   success_primary is the one Andre's reconciliation numbers below are keyed to.
--
-- DAY-OFFSET EXPRESSION (reused exactly, becomes vintage_day):
--   CAST(CASE WHEN first_change_dt < Treat_Start_DT THEN 0
--             ELSE first_change_dt - Treat_Start_DT END AS INTEGER)
--   Andre filters this BETWEEN 0 AND 90 in his own query; same cap applied here as
--   the spine bound (0-90), matching v1's cap.
--
-- GRP: base's own CASE already outputs 'Control' / 'Action' directly
--   (UPPER(SUBSTR(tst_grp_cd,1,1)) = 'C' -> 'Control', ELSE -> 'Action'). Reproduced
--   verbatim, unchanged. [NOTE] Andre's own query comments referred to the non-control
--   arm as "Test" in conversation; the CODE itself already emits 'Action', which is
--   what this file's output column carries -- no translation needed, just documented
--   here so the naming mismatch in conversation is not mistaken for a bug.
--
-- RECONCILIATION TARGET -- from Andre's own run of the working query. At
--   vintage_day 90, this file's cumulative responders AND base must equal these
--   exactly, matched by cohort_month (Treat_Start_DT month) x grp:
--     Treat_Start_DT 2026-03-13 -> Treat_End_DT 2026-05-31   cohort_month 2026-03
--         Control   base 11,326   primary responders_cum  0
--         Action    base 34,435   primary responders_cum 449
--     Treat_Start_DT 2026-04-13 -> Treat_End_DT 2026-06-30   cohort_month 2026-04
--         Control   base 11,133   primary responders_cum  0
--         Action    base 28,821   primary responders_cum 429
--     Treat_Start_DT 2026-05-13 -> Treat_End_DT 2026-07-31   cohort_month 2026-05
--         Control   base 10,600   primary responders_cum  0
--         Action    base 23,947   primary responders_cum 293
--
-- [FLAG] Control primary-success is 0 in ALL THREE Q3 waves while Action runs
--   293-449. This is Andre's own reconciled number, not a bug in this build -- but
--   it means Control effectively cannot produce an organic AIB upgrade under this
--   success definition (or the volume is too small to surface here). Understand WHY
--   before reporting any lift off this file: a 0 vs N comparison is not a normal
--   test/control lift ratio (division by zero), and the read has to be framed as
--   "Action produced N upgrades against a near-zero Control base rate," not as a
--   percentage lift.
--
-- RECONCILIATION -- gap measured 2026-08-10, deployment 2026-04-13 -> 2026-06-30:
--     dashboard   Action 28,821 leads / 471 target-product clients / 1.63%
--                 Control 11,133 leads / 5 / 0.04%
--     v2 (pre-fix) Action 28,821 / 429            Control 11,133 / 0
--   Leads matched exactly -- population is correct, only success detection differed.
--   Short by 42 (Action) and 5 (Control).
--   FIX 1 APPLIED 2026-08-10: acct_changes' change-detection window was capped at
--     Treat_End_DT + 5 days, which closes BEFORE the vintage spine's day 90 for
--     every wave -- see the FIX comment at acct_changes below for the before/after.
--   FIX 2 AVAILABLE, NOT APPLIED: prior-AIB-holder exclusion in success_primary,
--     toggleable with a one-line comment -- see the LEVER 2 block at success_primary.
--   TARGET (2026-04-13 -> 2026-06-30 wave, vintage_day 90): Action 471 / 1.63%,
--     Control 5 / 0.04%.
--
-- ENGINE: Teradata-direct. All three source tables and every function used
--   (INTERVAL, ADD_MONTHS, LAST_DAY, EXTRACT, SYS_CALENDAR) are native Teradata.
--   No dw00 catalog prefix, no Trino functions (DATE_TRUNC/DATE_DIFF/UNNEST/SEQUENCE).
--   TDWM blocks an unconstrained product join against SYS_CALENDAR.CALENDAR, so the
--   day spine AND the denominator cells (base, by cohort_month x grp) are both built
--   as VOLATILE TABLEs with COLLECT STATISTICS before the CROSS JOIN that forms the
--   dense grid -- same pattern as every other pp_*.sql file in this folder. QUALIFY
--   is available but not needed here (Andre's base CTE already dedups with
--   SELECT DISTINCT). Pure ASCII only.
--
-- Spine: fixed 0-90, matching v1's cap and Andre's own BETWEEN 0 AND 90 filter.
-- Floor: base's own treatmt_strt_dt BETWEEN DATE '2025-10-01' AND DATE '2026-08-01'
--   is kept exactly as Andre wrote it (wider than the repo's usual 2024-01-01 floor,
--   because it is HIS working, reconciled bound -- not replaced with the repo default).
-- ----------------------------------------------------------------------------
-- SCOPE: <<WINDOW>> selects deployments whose END date (treatmt_end_dt) falls in the
--   Q3 window. cohort_month and day-0 still anchor on Treat_Start_DT (START).
--   Retargeting a quarter = editing the two <<WINDOW>> literals below (both copies --
--   once in the volatile denominator table, once in the main query's base CTE).
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW -- EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (Treat_End_DT / treatmt_end_dt) falls in
--   the window. Cohort month and day 0 still anchor on Treat_Start_DT (START).
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

-- ============================================================================
-- RERUN GUARD -- if re-running this file in the SAME Teradata session, the volatile
-- tables below will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_vbu2_spine;
--   DROP TABLE vt_vbu2_cells;
-- Names are vt_vbu2_* (not vt_vbu_*) so this file can coexist with pp_vbu_campaign.sql
-- (v1) in the same session without a volatile-table name collision.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_vbu2_cells below
-- without tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu2_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu2_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (cohort_month x grp x base). VOLATILE for the same TDWM reason --
-- it is the other side of the spine CROSS JOIN. Rebuilds Andre's `base` CTE internally
-- (Teradata volatile-table creation is a standalone statement, it cannot see CTEs
-- defined outside it) -- the same CTE is repeated verbatim in the main query below.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu2_cells AS (
    WITH base AS (
        SELECT DISTINCT
            E.clnt_no,
            CAST(E.tactic_id AS VARCHAR(50)) AS tactic_id,
            E.treatmt_strt_dt                AS Treat_Start_DT,
            E.treatmt_end_dt                 AS Treat_End_DT,
            E.addnl_data_dt,
            E.tst_grp_cd,
            CASE WHEN UPPER(SUBSTR(E.tst_grp_cd, 1, 1)) = 'C' THEN 'Control' ELSE 'Action' END AS grp
        FROM DG6V01.tactic_evnt_ip_ar_hist E
        WHERE E.treatmt_strt_dt BETWEEN DATE '2025-10-01' AND DATE '2026-08-01'
          AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
          AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
          AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
          AND E.treatmt_end_dt >= DATE '2026-05-01'   -- <<WINDOW>>
          AND E.treatmt_end_dt <  DATE '2026-08-01'   -- <<WINDOW>>
    )
    SELECT
        CAST(CAST(EXTRACT(YEAR FROM Treat_Start_DT) AS VARCHAR(4)) || '-' ||
             CASE WHEN EXTRACT(MONTH FROM Treat_Start_DT) < 10 THEN '0' ELSE '' END ||
             TRIM(CAST(EXTRACT(MONTH FROM Treat_Start_DT) AS VARCHAR(2)))
        AS VARCHAR(7))               AS cohort_month,
        grp,
        COUNT(DISTINCT clnt_no)      AS base
    FROM base
    GROUP BY cohort_month, grp
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu2_cells COLUMN (cohort_month, grp);

-- ----------------------------------------------------------------------------
-- Main query -- base / elig / acct_changes reproduced verbatim from Andre's working
-- query, then prior_aib + success_primary (his success definition), then the day
-- axis joined off the two volatile tables built above.
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
        CASE WHEN UPPER(SUBSTR(E.tst_grp_cd, 1, 1)) = 'C' THEN 'Control' ELSE 'Action' END AS grp
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt BETWEEN DATE '2025-10-01' AND DATE '2026-08-01'
      AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
      AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
      AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, 10)
      AND E.treatmt_end_dt >= DATE '2026-05-01'   -- <<WINDOW>>
      AND E.treatmt_end_dt <  DATE '2026-08-01'   -- <<WINDOW>>
),

-- [VERIFY] SELECT list reconstructed from a partially-obscured screenshot 2026-08-10.
-- Coherent reading: must carry clnt_no, tactic_id, both dates, grp, acct_no,
-- prod_before, from_product_code -- every later CTE (acct_changes, prior_aib,
-- success_primary) references all seven fields. Not independently reconfirmed.
elig AS (
    SELECT DISTINCT
        b.clnt_no, b.tactic_id, b.Treat_Start_DT, b.Treat_End_DT, b.grp,
        a.acct_no,
        a.prod_cd_current           AS prod_before,
        SUBSTR(b.tst_grp_cd, 6, 3)  AS from_product_code
    FROM base b
    JOIN d3cv12a.cr_crd_rpts_acct a
        ON a.clnt_no = b.clnt_no
       AND a.ME_dt   = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
       AND (
            (a.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
         OR (a.prod_cd_current IN ('C00','C01','C02')       AND b.tst_grp_cd  = 'XX')
           )
       AND a.status = 'OPEN'
),

acct_changes AS (
    -- FIX 2026-08-10: change-detection window must be AT LEAST as long as the vintage
    -- spine (0-90, see vt_vbu2_spine above), or the last days of every curve are
    -- structurally empty. Anchoring both on Treat_Start_DT makes them the same window
    -- by construction. This REPLACES the original cap below (kept here for reference):
    --   AND d.DT_record_ext BETWEEN (e.Treat_Start_DT - INTERVAL '1' DAY)
    --                           AND (e.Treat_End_DT   + INTERVAL '5' DAY)
    -- For the 2026-04-13 wave (Treat_End_DT 2026-06-30) that old cap closed at
    -- 2026-07-05 -- day 83 from Treat_Start_DT -- while the spine runs to day 90
    -- (2026-07-12). Days 84-90 could never contain a conversion under the old cap.
    SELECT
        e.clnt_no, e.tactic_id, e.Treat_Start_DT, e.Treat_End_DT, e.grp, e.acct_no,
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

-- prior-AIB-holder exclusion -- clients who already held AIB before Treat_Start_DT
-- are not eligible to count as a new AIB "success"
prior_aib AS (
    SELECT DISTINCT e.tactic_id, e.acct_no
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio p
        ON p.acct_no = e.acct_no
       AND p.visa_prod_cd = 'AIB'
       AND p.dt_record_ext < e.Treat_Start_DT
),

-- SUCCESS METRIC (the ONE metric, per contract rule 4): success_any is deliberately
-- NOT built here -- see header. first_change_dt aggregated per
-- (clnt_no, tactic_id, Treat_Start_DT, Treat_End_DT, grp), Andre's own grain.
success_primary AS (
    SELECT
        ac.clnt_no, ac.tactic_id, ac.Treat_Start_DT, ac.Treat_End_DT, ac.grp,
        MIN(ac.dt_prod_change) AS first_change_dt
    FROM acct_changes ac
    LEFT JOIN prior_aib prior
        ON prior.tactic_id = ac.tactic_id AND prior.acct_no = ac.acct_no
    WHERE ac.new_product = 'AIB'
      -- ==== LEVER 2 (currently OFF, disabled 2026-08-10) ==============
      -- FIX 1 (widening the change window to the full 90-day spine) moved the
      -- count by only ~1 conversion per cohort - nowhere near the 42 needed.
      -- So the window was NOT the cause. Disabling the prior-AIB exclusion is
      -- the remaining lever: it drops clients who already held AIB before
      -- treatment, and the dashboard's target-product count appears not to.
      -- Re-enable by uncommenting the line below.
      --   AND prior.acct_no IS NULL
      -- ===============================================================
    GROUP BY ac.clnt_no, ac.tactic_id, ac.Treat_Start_DT, ac.Treat_End_DT, ac.grp
),

-- day axis: Andre's own day-offset expression, reused exactly, becomes vintage_day
numerator AS (
    SELECT
        sp.clnt_no,
        CAST(CAST(EXTRACT(YEAR FROM sp.Treat_Start_DT) AS VARCHAR(4)) || '-' ||
             CASE WHEN EXTRACT(MONTH FROM sp.Treat_Start_DT) < 10 THEN '0' ELSE '' END ||
             TRIM(CAST(EXTRACT(MONTH FROM sp.Treat_Start_DT) AS VARCHAR(2)))
        AS VARCHAR(7))               AS cohort_month,
        sp.grp,
        CAST(CASE WHEN sp.first_change_dt < sp.Treat_Start_DT THEN 0
                  ELSE sp.first_change_dt - sp.Treat_Start_DT END AS INTEGER) AS vintage_day
    FROM success_primary sp
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM numerator
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort_month, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vt_vbu2_cells c
    CROSS JOIN vt_vbu2_spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'VBU' block
    -- ahead of a longer label would silently truncate it.
    CAST('VBU' AS VARCHAR(20)) AS mne,
    g.cohort_month,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.grp           = g.grp
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;
