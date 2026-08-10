-- pp_crv_campaign.sql
-- Contract  : vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 8 columns only:
--             mne, cohort_month, segment, grp, vintage_day, base, responders, responders_cum.
-- Campaign  : CRV (Credit Card Installment Plan) — CAMPAIGN scope
--
-- SOURCE OF TRUTH: Andre's validated, reconciled CRV summary query, supplied 2026-08-10
--             (transcribed from screenshots). The cohort_month/arm/base/success logic below
--             (tactic_cohort, cohort_cells CTEs) is REPRODUCED VERBATIM from that query.
--             Do NOT "improve" the arm rule or the grain — both were validated against the
--             dashboard. This file adds exactly one thing on top: a per-account first-event
--             day offset, so the validated summary becomes a vintage curve.
--
-- RECONCILIATION TARGET — the validated summary's own output. At vintage_day = 90, this
--             file's cumulative base and responders must equal these, per cohort_month x arm:
--               2026-02  Action  base 1,831,018  responders 20,069
--               2026-02  Control base    97,923  responders    850
--               2026-03  Action  base   784,601  responders 11,108
--               2026-03  Control base    40,919  responders    471
--               2026-04  Action  base   969,058  responders 11,169
--               2026-04  Control base    50,815  responders    458
--
-- ENGINE: Trino/Starburst — THE ONLY NON-TERADATA FILE IN THE PACK.
-- Every other pp_*.sql file is Teradata-direct (SYS_CALENDAR volatile spine). This one
-- flips because the success signal lives in the EDL Success Library
-- (edl0_im.prod_zp10_prod_staging.measurement_events_v2), and touching any edl0_im table
-- forces the whole statement into Starburst/Trino syntax (see query_engine_guidelines.md,
-- "SYNTAX FOLLOWS THE ENGINE, NOT THE TABLE"). Consequences applied throughout this file:
-- NO QUALIFY, NO VOLATILE TABLE (CTEs only), NO SYS_CALENDAR spine (UNNEST(SEQUENCE(0,90))
-- instead), NO Teradata date subtraction (DATE_DIFF('day', a, b) instead).
--
-- CATALOG — edl0_im (L-zero), NOT ed10_im. One screenshot rendered it as ed10_im; that is
--             the known wrong-catalog trap. Confirmed edl0_im below.
--
-- RAW SOURCE, not the curated Data Lab table. This file does NOT read
-- DL_MR_PROD.cards_crv_install_decis_resp / cards_crv_install_details. It rebuilds
-- population, arm, window, and success from two raw tables, exactly as the validated query:
--   1. dg6v01.tactic_evnt_ip_ar_hist       — population / arm / treatment window (raw tactic history)
--   2. edl0_im.prod_zp10_prod_staging.measurement_events_v2 — success (raw Success Library events)
--
-- mne       : CAST('CRV' AS VARCHAR(20)) — campaign mnemonic, or the experiment name where the
--             file measures an experiment.
-- segment   : CAST('All' AS VARCHAR(20)) — constant. CRV has no pre-treatment population
--             split above Action/Control; segment carries real values only for AUH.
-- DEDUP IDENTIFIER: visa_acct_no — the raw tactic table's account column (it has no column
--             literally named acct_no).
--
-- ANCHOR DATE — treatmt_eff_dt (MIN per account/cohort_month/arm), matching the validated
--             query. Do NOT switch to treatmt_strt_dt.
--
-- JOIN KEY TRAP — visa_acct_no (tactic side) is a bare numeric account number.
--             measurement_events_v2.acct_no (events side) is a ZERO-PADDED VARCHAR (~23 chars,
--             e.g. '00000000000000284474803'; the real account number is the numeric tail).
--             A raw string compare (visa_acct_no = acct_no) silently returns ZERO ROWS.
--             The fix, matching the validated query, is to normalize BOTH sides through
--             DECIMAL(38,0): CAST(visa_acct_no AS DECIMAL(38,0)) = CAST(acct_no AS DECIMAL(38,0)).
--             Note the CAST-in-join can block federation pushdown on the events side;
--             correctness first, revisit if slow.
--
-- ===========================================================================================
-- THREE PRIOR DEVIATIONS FROM THE VALIDATED QUERY — ALL REVERTED IN THIS REWRITE
-- ===========================================================================================
--
-- 1. ARM — a prior version of this file excluded non-TG4/TG8 groups
--             (WHERE tst_grp_cd IN ('TG4','TG8')). THAT WAS WRONG. The validated query uses
--             CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END, with NO exclusion,
--             and it RECONCILES: it produces Feb Action = 1,831,018 against the curated
--             1,831,016 from RECONCILIATION_FINDINGS.md — a ~2-account difference, not the
--             ~1M-account gap the exclusion theory predicted. Excluding groups would drop real
--             accounts and break that match. The ~1M-extra-accounts concern in
--             RECONCILIATION_FINDINGS.md:56-59 does not materialise once the treatmt_end_dt
--             window filter (treatmt_end_dt BETWEEN the quarter bounds) is applied — most of
--             those "extra" accounts belong to deployments outside the selected quarter and are
--             filtered out by the window predicate, not by an arm exclusion. Restored:
--             ELSE 'Action', no exclusion filter.
--
-- 2. DEDUP — a prior version used a ROW_NUMBER() first-touch tie-break
--             (PARTITION BY visa_acct_no, cohort_month ORDER BY treatmt_eff_dt, keep rn=1).
--             The validated query does NOT do this. It uses
--             GROUP BY visa_acct_no, cohort_month, arm with MIN(treatmt_eff_dt) /
--             MAX(treatmt_end_dt) — arm is part of the grain, so ONE ACCOUNT CAN APPEAR UNDER
--             BOTH ARMS in the same cohort_month (e.g. touched by a TG4 deployment and a TG8
--             deployment in the same month). This is deliberate and matches the validated
--             build. The Power Pack first-touch rule (OUTPUT_CONTRACT.md) is WAIVED for CRV
--             specifically — the validated query defines the grain here, not the pack default.
--             ROW_NUMBER/first-touch logic has been removed entirely.
--
-- 3. EVENT DATE FLOOR — a prior version tightened the success-side scan to
--             event_date >= DATE '2026-02-01'. THAT WAS A BUG: treatmt_eff_dt runs EARLIER
--             than treatmt_strt_dt, so a deployment whose treatmt_end_dt falls inside the
--             quarter window can still have a treatmt_eff_dt in January — a Feb floor would
--             silently cut that account's early events out of the success window. Restored:
--             m.event_date >= DATE '2026-01-01', exactly as validated. No upper bound is added
--             on event_date — the m.event_date BETWEEN t.treatmt_eff_dt AND t.treatmt_end_dt
--             predicate already bounds it from above.
--
-- ===========================================================================================
-- VINTAGE EXTENSION — the one thing added on top of the validated logic
-- ===========================================================================================
-- The validated query is a SUMMARY (one row per cohort_month x arm): it counts an account as
-- a responder if it has >=1 in-window event, but never captures WHEN. A vintage needs the day
-- offset. first_event below runs the SAME table, SAME predicates, SAME join as the validated
-- success_events, just with MIN(event_date) instead of DISTINCT visa_acct_no — it is a strict
-- superset. COUNT(DISTINCT visa_acct_no) over first_event equals the validated `responders`
-- count exactly; this is the reconciliation guarantee for this file. vintage_day is then
-- DATE_DIFF('day', treatmt_eff_dt, first_event_date), which is always >= 0 because event_date
-- is bounded below by treatmt_eff_dt in the WHERE clause.
--
-- SPINE — fixed 0..90, continuous, per the output contract.
-- Counts only. COUNT(DISTINCT visa_acct_no) throughout, never COUNT(*).

-- ============================================================================
-- QUARTER WINDOW — EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in the window.
--   Cohort month and day 0 still anchor on treatmt_eff_dt (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive)

WITH
-- Validated CTE 1/2 — reproduced verbatim. Grain = visa_acct_no x cohort_month x arm.
-- arm has NO exclusion (ELSE 'Action'); do not add tst_grp_cd IN (...) here.
tactic_cohort AS (
    SELECT
        visa_acct_no,
        date_trunc('month', treatmt_eff_dt) AS cohort_month,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END AS arm,
        MIN(treatmt_eff_dt) AS treatmt_eff_dt,
        MAX(treatmt_end_dt) AS treatmt_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'      -- <<WINDOW>>
      AND treatmt_eff_dt IS NOT NULL
    GROUP BY
        visa_acct_no,
        date_trunc('month', treatmt_eff_dt),
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
),

-- Validated CTE 2/2 — reproduced verbatim. This is the base/denominator, fixed down the spine.
cohort_cells AS (
    SELECT cohort_month, arm, COUNT(DISTINCT visa_acct_no) AS cohort_size
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- VINTAGE EXTENSION — strict superset of the validated success_events CTE (same table, same
-- predicates, same join key normalization), swapping DISTINCT visa_acct_no for MIN(event_date)
-- so we capture the day offset instead of just the yes/no flag.
first_event AS (
    SELECT
        t.visa_acct_no,
        t.cohort_month,
        t.arm,
        t.treatmt_eff_dt,
        MIN(m.event_date) AS first_event_date
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'                                   -- restored, NOT 2026-02-01
      AND m.event_date BETWEEN t.treatmt_eff_dt AND t.treatmt_end_dt
    GROUP BY t.visa_acct_no, t.cohort_month, t.arm, t.treatmt_eff_dt
),

acct_vintage AS (
    SELECT
        visa_acct_no,
        cohort_month,
        arm,
        DATE_DIFF('day', treatmt_eff_dt, first_event_date) AS vintage_day
    FROM first_event
),

daily_counts AS (
    SELECT cohort_month, arm, vintage_day, COUNT(DISTINCT visa_acct_no) AS responders
    FROM acct_vintage
    GROUP BY cohort_month, arm, vintage_day
),

-- Fixed spine, 0..90, continuous
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS s(vintage_day)
),

dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size AS base, s.vintage_day
    FROM cohort_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('CRV' AS VARCHAR(20)) AS mne,
    CAST(SUBSTR(CAST(g.cohort_month AS VARCHAR), 1, 7) AS VARCHAR(7)) AS cohort_month,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.arm AS grp,
    g.vintage_day,
    g.base,
    COALESCE(dc.responders, 0) AS responders,
    SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm           = g.arm
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day;
