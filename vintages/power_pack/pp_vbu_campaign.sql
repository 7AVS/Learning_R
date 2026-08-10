-- vbu_campaign_vintage.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED 2026-08-10;
--             `mne` ADDED 2026-08-10 as first column).
--             8 columns only: mne, cohort_month, segment, grp, vintage_day, base,
--             responders, responders_cum.
-- mne       : CAST('VBU' AS VARCHAR(20)) — campaign mnemonic, or the experiment name where the
--             file measures an experiment.
-- segment   : CAST('All' AS VARCHAR(20)) — constant. VBU has no pre-treatment population split
--             above Action/Control; segment carries real values only for AUH.
-- Campaign  : VBU (Visa Benefit Upgrade) — CAMPAIGN scope
--
-- ENGINE: Trino / Starburst. ALL Power Pack files use ONE engine - EDW tables are reached
--         through Starburst federation. No volatile tables, no QUALIFY, no SYS_CALENDAR.
--
-- CATALOG PREFIXES (verified against proven-running Trino files in this repo, not guessed):
--   DG6V01.tactic_evnt_ip_ar_hist — bare, no dw00 prefix. Proven in pp_crv_campaign.sql,
--     campaigns/CRV/vintage_reconciliation/crv_vintage_v2_production.sql, and
--     campaigns/PCD/async_banner_summary.sql — all three headed "Engine: Trino/Starburst".
--   d3cv12a.appl_fact_dly (Casper) — bare, no dw00 prefix. This exact table isn't independently
--     proven, but the D3CV12A schema's no-prefix pattern is proven for sibling tables
--     (dly_full_portfolio, cr_crd_rpts_acct, visa_txn_dly, lkup_txn_cd_catg) in
--     campaigns/CRV/suppression_experiment/c3_banner_reach_2026.sql, c4_demand_shaping.sql,
--     and campaigns/PCD/async_banner_summary.sql / pcd_success_validation.sql /
--     async_banner_vintage_tracker.sql — all Engine: Trino/Starburst. Same schema, same rule.
--     (Note: query_engine_guidelines.md §Catalog Naming lists "dw00_im.d3cv12a.*" as the
--     federated form — that line does not match what actually runs in this repo's Trino files.
--     Going with the proven code, not the stale doc line. Flag if this errors.)
--   edl0_im.prod_yg80_pcbsharedzone.tsz_00222_... (SCOT) — 3-part EDL form, unambiguous.
--
-- Source    : DG6V01.tactic_evnt_ip_ar_hist (population) + d3cv12a.appl_fact_dly (Casper) +
--             edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot (SCOT)
-- [FLAG] Success definition for VBU is Casper+SCOT application approval — the SAME definition
--             as VBA — per the 2026-07-22 methodology swap documented in
--             campaigns/VBA_VBU/vbu_summary_vintage_cell.py, REPLACING the earlier AIB/
--             dly_full_portfolio product-change definition ("Daniel Chin's original
--             framework", campaigns/VBA_VBU/vbu_vintage_original.sql — left in the repo,
--             untouched). Carried forward unchanged by the deployment drop.
-- Population: SUBSTR(tactic_id,8,3)='VBU', treatmt_strt_dt >= DATE '2026-01-01'. DELETED the
--             dead filter SUBSTR(tactic_id,8,1)<>'J' — position 8 is always 'V' for VBA/VBU.
--
-- *** deployment DROPPED, 2026-08-10 *** — was tactic_id. Curve grain is now COHORT MONTH.
--
-- DEDUP IDENTIFIER: clnt_no — this file's success join keys on clnt_no throughout.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
--
-- *** DEDUP — one row per (clnt_no, cohort_month), anchored on first wave ***
--   1. pop_wave: one row per (clnt_no, tactic_id) — the original per-wave dedup (multiple event
--      rows for the same client+wave collapse to the earliest treatmt_strt_dt), unchanged.
--   2. cohort_first: ROW_NUMBER() OVER (PARTITION BY clnt_no, cohort_month
--      ORDER BY treatmt_strt_dt ASC), keep rn=1 — first-touch wave wins grp and becomes Day 0.
--   3. cohort_window: MAX(treatmt_end_dt) across ALL of that client's waves in the cohort_month,
--      so a later wave's window is never truncated by only looking at the first-touch wave's own
--      end date. Search window for success = [anchor_dt, window_end].
-- grp       : SUBSTR(TRIM(tst_grp_cd),1,1): 'C'->Control, 'T'->Action, from the client's
--             first-touch wave (Trino has no LEFT() — SUBSTR(x,1,1) is the equivalent; TRIM
--             guards against CHAR padding on the code column). Anything else is EXCLUDED from
--             the population (not a third bucket — contract requires grp strictly binary).
--             [VERIFY] the C/T-prefix split itself is not documented in the original
--             campaigns/VBA_VBU/vbu_vintage_original.sql (which uses tst_grp_cd positions 6-8
--             as a FROM-product code, not an Action/Control split); confirmed instead in
--             campaigns/VBA_VBU/vbu_summary_vintage_cell.py (`tc()`, ~line 64) as VBA/VBU's real
--             Action/Control rollup rule.
-- Success   : UNCHANGED definition — Casper + SCOT unioned and deduped to one earliest-approval
--             event. Same tables/filters as vba_campaign.sql. Union is a plain UNION (not UNION
--             ALL) on (clnt_no, event_date) to dedupe the same physical event reported by both
--             systems, done BEFORE cohort attribution. Only the cross-wave pooling layer
--             changed — see DEDUP above; each client's cohort-month window now spans ALL their
--             VBU waves that month instead of measuring against a single wave's window.
-- Spine     : fixed 0-90 — preserved as a deliberate cap. UNNEST(SEQUENCE(0,90)), Trino has no
--             SYS_CALENDAR.
-- Floor     : DATE '2026-01-01' on every scan (contract rule 6), including Casper/SCOT event
--             tables.
-- ----------------------------------------------------------------------------
-- SCOPE: this file is scoped to deployments ENDING in the quarter window below
--   (population filtered on treatmt_end_dt). cohort_month and day-0 still anchor
--   on treatmt_strt_dt (the START column) — unchanged. Success-side scan (Casper
--   + SCOT event tables) tightened separately — see <<WINDOW>> tags below.
--   Retargeting a quarter = editing the two <<WINDOW>> literals below only.
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
-- stage 1 dedup: one row per (clnt_no, tactic_id) — earliest treatmt_strt_dt wins
pop_wave AS (
    SELECT clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt, grp
    FROM (
        SELECT
            clnt_no, tactic_id, treatmt_strt_dt, treatmt_end_dt,
            CASE
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'T' THEN CAST('Action'  AS VARCHAR(20))
            END AS grp,
            ROW_NUMBER() OVER (
                PARTITION BY clnt_no, tactic_id ORDER BY treatmt_strt_dt ASC
            ) AS rn
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE treatmt_strt_dt >= DATE '2026-01-01'                          -- floor guard
          AND treatmt_end_dt  >= DATE '2026-05-01'                          -- <<WINDOW>>
          AND treatmt_end_dt  <  DATE '2026-08-01'                          -- <<WINDOW>>
          AND SUBSTR(tactic_id, 8, 3) = 'VBU'
          AND SUBSTR(TRIM(tst_grp_cd), 1, 1) IN ('C', 'T')
    ) ranked
    WHERE rn = 1
),
pop_raw AS (
    SELECT
        clnt_no, treatmt_strt_dt, treatmt_end_dt, grp,
        DATE_TRUNC('month', treatmt_strt_dt) AS cohort_month
    FROM pop_wave
),
-- stage 2 dedup: one row per (clnt_no, cohort_month) — first-touch wave wins grp + anchor date
-- (never expected to fire — see header)
cohort_first AS (
    SELECT clnt_no, cohort_month, grp, treatmt_strt_dt AS anchor_dt
    FROM (
        SELECT clnt_no, cohort_month, grp, treatmt_strt_dt,
               ROW_NUMBER() OVER (
                   PARTITION BY clnt_no, cohort_month ORDER BY treatmt_strt_dt ASC
               ) AS rn
        FROM pop_raw
    ) ranked
    WHERE rn = 1
),
vbu_cells AS (
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
),
cohort_window AS (
    -- widen search window to the latest end date across ALL the client's waves this month
    SELECT clnt_no, cohort_month, MAX(treatmt_end_dt) AS window_end
    FROM pop_raw
    GROUP BY clnt_no, cohort_month
),
population AS (
    SELECT cf.clnt_no, cf.cohort_month, cf.grp, cf.anchor_dt, cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.clnt_no = cf.clnt_no AND cw.cohort_month = cf.cohort_month
),

-- raw candidate success events, Casper (PRIMARY) — no deployment key on the event table itself
casper_events AS (
    SELECT DISTINCT app.bus_clnt_no AS clnt_no, app.app_rcv_dt AS event_date
    FROM d3cv12a.appl_fact_dly app
    WHERE app.app_rcv_dt >= DATE '2026-02-01'                               -- <<WINDOW>> lower: earliest start of a Q3-ending deployment
      AND app.app_rcv_dt <  DATE '2026-08-01'                               -- <<WINDOW>> upper: no selected deployment runs past this
      AND app.Status IN ('A')
      AND app.PROD_APPRVD IN ('B', 'E')
      AND app.CR_LMT_CHG_IND = 'N'
      AND app.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND (app.Cell_Code IS NULL OR app.Cell_Code NOT IN ('PATACT', 'GV0320'))
),

-- raw candidate success events, SCOT (SECONDARY) — one row per client (SCOT's snapshot
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
      AND creditapplication_createddatetime >= DATE '2026-02-01'            -- <<WINDOW>> lower: earliest start of a Q3-ending deployment
      AND creditapplication_createddatetime <  DATE '2026-08-01'            -- <<WINDOW>> upper: no selected deployment runs past this
    GROUP BY 1
),
scot_events AS (
    SELECT clnt_no, event_date FROM scot_events_raw WHERE approved = 1
),

-- union FIRST, dedupe the physical event (clnt_no, event_date) — plain UNION, not UNION ALL —
-- BEFORE joining to a cohort window
events AS (
    SELECT clnt_no, event_date FROM casper_events
    UNION
    SELECT clnt_no, event_date FROM scot_events
),

-- each client-month is measured against its OWN pooled window only (anchor_dt to window_end)
success AS (
    SELECT
        p.cohort_month,
        p.grp,
        p.clnt_no,
        MIN(DATE_DIFF('day', p.anchor_dt, e.event_date)) AS vintage_day
    FROM population p
    INNER JOIN events e
        ON  e.clnt_no    = p.clnt_no
        AND e.event_date BETWEEN p.anchor_dt AND p.window_end
    GROUP BY p.clnt_no, p.cohort_month, p.grp
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(*) AS responders
    FROM success
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort_month, grp, vintage_day
),

-- day spine 0-90 (deliberate fixed cap, preserved from source)
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS s(vintage_day)
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vbu_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('VBU' AS VARCHAR(20)) AS mne,
    CAST(SUBSTR(CAST(g.cohort_month AS VARCHAR), 1, 7) AS VARCHAR(7)) AS cohort_month,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    COALESCE(dc.responders, 0) AS responders,
    SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.grp           = g.grp
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             clnt_no,
--             DATE_TRUNC('month', treatmt_strt_dt) AS cohort_month,
--             CASE
--                 WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'C' THEN CAST('Control' AS VARCHAR(20))
--                 WHEN SUBSTR(TRIM(tst_grp_cd), 1, 1) = 'T' THEN CAST('Action'  AS VARCHAR(20))
--             END AS grp
--         FROM DG6V01.tactic_evnt_ip_ar_hist
--         WHERE treatmt_strt_dt >= DATE '2026-01-01'
--           AND SUBSTR(tactic_id, 8, 3) = 'VBU'
--           AND SUBSTR(TRIM(tst_grp_cd), 1, 1) IN ('C', 'T')
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
