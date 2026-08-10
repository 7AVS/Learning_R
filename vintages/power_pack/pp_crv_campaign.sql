-- pp_crv_campaign.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 8 columns only:
--             mne, cohort_month, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : CRV (Credit Card Installment Plan) — CAMPAIGN scope
--
-- ENGINE: Trino/Starburst — THE ONLY NON-TERADATA FILE IN THE PACK.
-- Every other pp_*.sql file is Teradata-direct (SYS_CALENDAR volatile spine). This one
-- flips because the success signal now lives in the EDL Success Library
-- (edl0_im.prod_zp10_prod_staging.measurement_events_v2), and touching any edl0_im table
-- forces the whole statement into Starburst/Trino syntax (see query_engine_guidelines.md,
-- "SYNTAX FOLLOWS THE ENGINE, NOT THE TABLE"). Consequences applied throughout this file:
-- NO QUALIFY (ROW_NUMBER() in a subquery + outer WHERE rn = 1 instead), NO VOLATILE TABLE
-- (CTEs only), NO SYS_CALENDAR spine (UNNEST(SEQUENCE(0,90)) instead), NO Teradata date
-- subtraction (DATE_DIFF('day', a, b) instead).
--
-- RAW SOURCE, not the curated Data Lab table. Per Andre's direction: this file does NOT
-- read DL_MR_PROD.cards_crv_install_decis_resp / cards_crv_install_details (the curated
-- datalab dataset the OLD version of this file used). It rebuilds population, arm, window,
-- and success independently from two raw tables:
--   1. dg6v01.tactic_evnt_ip_ar_hist       — population / arm / treatment window (raw tactic history)
--   2. edl0_im.prod_zp10_prod_staging.measurement_events_v2 — success (raw Success Library events)
-- This mirrors campaigns/CRV/vintage_reconciliation/crv_vintage_v2_production.sql (the
-- production-side reconciliation build), not crv_vintage_v1_datalab.sql.
--
-- mne       : CAST('CRV' AS VARCHAR(3)) — constant per file, campaign mnemonic.
-- segment   : CAST('All' AS VARCHAR(20)) — constant. CRV has no pre-treatment population
--             split above Action/Control; segment carries real values only for AUH.
-- DEDUP IDENTIFIER: visa_acct_no — the raw tactic table's account column (it has no column
--             literally named acct_no). This replaces the old file's acct_no (curated-side
--             column name); same grain, different source column name.
--
-- ANCHOR DATE — treatmt_eff_dt, deliberately NOT treatmt_strt_dt.
--             treatmt_strt_dt runs a few days later than treatmt_eff_dt and pushes ~1.7% of
--             accounts into the next cohort month (proven 2026-07-09, RECONCILIATION_FINDINGS.md
--             FINDING 1). treatmt_eff_dt matches the curated offer_start_date and is what both
--             production reconciliation files anchor on. cohort_month, day-0, the success
--             window start, and the per-account day axis are ALL anchored on treatmt_eff_dt here.
--
-- JOIN KEY TRAP — visa_acct_no (tactic side) is a bare numeric account number.
--             measurement_events_v2.acct_no (events side) is a ZERO-PADDED VARCHAR (~23 chars,
--             e.g. '00000000000000284474803'; the real account number is the numeric tail).
--             A raw string compare (visa_acct_no = acct_no) silently returns ZERO ROWS.
--             The proven fix (crv_vintage_v2_production.sql) is to normalize BOTH sides through
--             DECIMAL(38,0): CAST(visa_acct_no AS DECIMAL(38,0)) = CAST(acct_no AS DECIMAL(38,0)).
--             This is applied in success_events below. Note the CAST-in-join can block
--             federation pushdown on the events side; correctness first, revisit if slow.
--
-- ARM — CORRECTED, fixes a known bug in crv_vintage_v2_production.sql.
--             That file used CASE WHEN tst_grp_cd='TG8' THEN 'Control' ELSE 'Action' END, which
--             lumps ~1M accounts from tst_grp_cd groups OTHER than TG4/TG8 into 'Action'
--             (RECONCILIATION_FINDINGS.md:56-59: "Arm-logic bug to fix in the summary/vintage:
--             ELSE 'Action' lumps non-TG4/TG8 groups into Action; should be
--             TG4→Action, TG8→Control, exclude the rest"). This file implements the corrected
--             version: TG4 -> Action, TG8 -> Control, every other tst_grp_cd is EXCLUDED from
--             the population entirely (WHERE tst_grp_cd IN ('TG4','TG8')), not bucketed.
--
-- DEDUP — first-touch, one row per (visa_acct_no, cohort_month), per OUTPUT_CONTRACT.md.
--             1. raw_pop: row-level population, CRV-scoped, TG4/TG8-only, floored >= 2026-01-01.
--             2. pop_raw: attach row-level cohort_month = date_trunc('month', treatmt_eff_dt).
--             3. cohort_first: ROW_NUMBER() OVER (PARTITION BY visa_acct_no, cohort_month
--                ORDER BY treatmt_eff_dt ASC) in a subquery, outer WHERE rn = 1 (NOT QUALIFY —
--                Trino doesn't have it). The winning row's treatmt_eff_dt is, by construction,
--                the MIN(treatmt_eff_dt) for that account in that cohort_month — this IS the
--                "aggregate per account: MIN(treatmt_eff_dt)" the reconciliation build describes,
--                expressed as an explicit first-touch tie-break (grp + anchor both come from the
--                same winning row, so an account can never straddle both arms in one cohort_month
--                — see OUTPUT_CONTRACT.md's "grp conflict rule = FIRST-TOUCH", expected never to
--                fire under trigger-style decisioning).
--             4. cohort_window: MAX(treatmt_end_dt) pooled across ALL of that account's
--                deployments in the cohort_month — the "aggregate per account: MAX(treatmt_end_dt)"
--                half — so a later deployment's window is never truncated by only looking at the
--                first-touch deployment's own end date.
--
-- ATTRIBUTION — success event_date BETWEEN the account's [anchor_dt, window_end], i.e. the
--             pooled [MIN(treatmt_eff_dt), MAX(treatmt_end_dt)] window described above.
--
-- SPINE — fixed 0..90, continuous, per the output contract (not data-driven / not capped
--             per-cohort like the old curated-side file — this pack's contract wants a flat
--             uniform spine across every campaign).
--
-- WHY THIS NUMBER WON'T MATCH THE CURATED BUILD, BY DESIGN (canon, vintage_datalab_method.md:95-97):
--   "Production != Data Lab by design: production counts in-window post-start conversions;
--    Data Lab counts the responder flag incl. pre-offer responses (clamped to day 0).
--    Production ~= Data Lab minus the pre-offer bucket - production is the causally cleaner
--    number."
--   Do NOT validate this file by comparing cohort-month AGGREGATE success counts against the
--   curated build (RECONCILIATION_FINDINGS.md:46-51) — the ~1.7% month-boundary accounts and
--   the pre-offer bucket both leak a spurious gap at that grain regardless of correctness.
--   Validate at ACCOUNT level instead: for the same accounts, does measurement_events_v2
--   (p_card_installmt_purch, in-window) detect success where the curated responder flag = 1?
--
-- Floor     : DATE '2026-01-01' on every scan (contract rule 6; matches the reconciliation build).
-- Counts only. COUNT(DISTINCT visa_acct_no) throughout, never COUNT(*).

WITH
-- Step 1: row-level population, CRV-scoped, TG4/TG8-only (the arm fix — excludes, doesn't bucket)
raw_pop AS (
    SELECT
        visa_acct_no,
        treatmt_eff_dt,
        treatmt_end_dt,
        CASE
            WHEN tst_grp_cd = 'TG4' THEN 'Action'
            WHEN tst_grp_cd = 'TG8' THEN 'Control'
        END AS grp
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE SUBSTR(tactic_id, 8, 3) = 'CRV'
      AND tst_grp_cd IN ('TG4', 'TG8')
      AND treatmt_eff_dt IS NOT NULL
      AND treatmt_eff_dt >= DATE '2026-01-01'
),

-- Step 2: attach row-level cohort_month (VARCHAR 'YYYY-MM') off treatmt_eff_dt
pop_raw AS (
    SELECT
        visa_acct_no,
        treatmt_eff_dt,
        treatmt_end_dt,
        grp,
        CAST(SUBSTR(CAST(DATE_TRUNC('month', treatmt_eff_dt) AS VARCHAR), 1, 7) AS VARCHAR(7)) AS cohort_month
    FROM raw_pop
),

-- Step 3: first-touch dedup — ROW_NUMBER subquery + outer WHERE, NOT QUALIFY (Trino has no QUALIFY)
ranked AS (
    SELECT
        visa_acct_no,
        cohort_month,
        grp,
        treatmt_eff_dt AS anchor_dt,
        ROW_NUMBER() OVER (
            PARTITION BY visa_acct_no, cohort_month
            ORDER BY treatmt_eff_dt ASC
        ) AS rn
    FROM pop_raw
),
cohort_first AS (
    SELECT visa_acct_no, cohort_month, grp, anchor_dt
    FROM ranked
    WHERE rn = 1
),

-- Step 4: window_end = MAX(treatmt_end_dt) pooled across every deployment the account had
-- in that cohort_month (mirrors the reconciliation build's per-account aggregation)
cohort_window AS (
    SELECT visa_acct_no, cohort_month, MAX(treatmt_end_dt) AS window_end
    FROM pop_raw
    GROUP BY visa_acct_no, cohort_month
),

population AS (
    SELECT
        cf.visa_acct_no,
        cf.cohort_month,
        cf.grp,
        cf.anchor_dt,
        cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.visa_acct_no = cf.visa_acct_no
       AND cw.cohort_month = cf.cohort_month
),

-- Step 5: base (denominator) per cohort_month x grp — fixed down the spine
cells AS (
    SELECT
        cohort_month,
        grp,
        COUNT(DISTINCT visa_acct_no) AS base
    FROM population
    GROUP BY cohort_month, grp
),

-- Step 6: success events, in-window, joined on the normalized numeric account key.
-- JOIN KEY TRAP: visa_acct_no is bare numeric; measurement_events_v2.acct_no is a
-- zero-padded VARCHAR. A raw string compare silently returns zero rows. Normalizing BOTH
-- sides through DECIMAL(38,0) is the proven fix (crv_vintage_v2_production.sql).
success_events AS (
    SELECT
        p.visa_acct_no,
        p.cohort_month,
        p.grp,
        DATE_DIFF('day', p.anchor_dt, m.event_date) AS vintage_day
    FROM population p
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(p.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN p.anchor_dt AND p.window_end
),

-- Step 7: first in-window success per account per cell (naturally >= 0, no clamp needed —
-- events are restricted to on/after anchor_dt above)
acct_success AS (
    SELECT visa_acct_no, cohort_month, grp, MIN(vintage_day) AS vintage_day
    FROM success_events
    GROUP BY visa_acct_no, cohort_month, grp
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(DISTINCT visa_acct_no) AS responders
    FROM acct_success
    GROUP BY cohort_month, grp, vintage_day
),

-- Step 8: fixed spine, 0..90, continuous
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS s(vintage_day)
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM cells c
    CROSS JOIN spine s
)

SELECT
    CAST('CRV' AS VARCHAR(3))  AS mne,
    g.cohort_month,
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
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): distinct tst_grp_cd values + account counts for CRV,
-- so Andre can see how many accounts the TG4/TG8-only filter drops.
-- ============================================================================
-- SELECT
--     tst_grp_cd,
--     COUNT(DISTINCT visa_acct_no) AS accounts
-- FROM dg6v01.tactic_evnt_ip_ar_hist
-- WHERE SUBSTR(tactic_id, 8, 3) = 'CRV'
--   AND treatmt_eff_dt IS NOT NULL
--   AND treatmt_eff_dt >= DATE '2026-01-01'
-- GROUP BY tst_grp_cd
-- ORDER BY accounts DESC;
