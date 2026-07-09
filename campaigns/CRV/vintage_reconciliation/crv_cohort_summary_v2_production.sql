-- crv_cohort_summary_v2_production.sql
-- Per-cohort FULL-WINDOW summary version of crv_vintage_v2_production.sql.
-- Grain: ACCOUNT (visa_acct_no) for the population/denominator. dg6v01.tactic_evnt_ip_ar_hist
-- (the tactic/decisioning population source) carries the account number under the
-- column name visa_acct_no -- it does NOT have a column literally named acct_no.
-- edl0_im.prod_zp10_prod_staging.measurement_events_v2 (the Success Library events)
-- stores acct_no as a ZERO-PADDED VARCHAR (~23 chars, e.g. '00000000000000284474803';
-- the real account number is the numeric tail). The two account keys are therefore in
-- different formats and are joined on the NORMALIZED NUMERIC value, stripping leading
-- zeros on BOTH sides: CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS
-- DECIMAL(38,0)). Joining on the raw string values would not match. Account identity
-- throughout (population, success, counting) is the tactic-side visa_acct_no -- the
-- events-side acct_no is used only to find matching events, never counted separately.
-- Same population, cohort anchor, and arm derivation as the vintage file; the
-- vintage_day spine and cumulative curve are dropped in favor of one row per
-- (cohort_month, arm) covering the entire [treatmt_strt_dt, treatmt_end_dt] window.
-- Success is DERIVED (>=1 in-window p_card_installmt_purch event), not a precomputed
-- flag.
-- Inclusion window: treatmt_end_dt in [2026-05-01, 2026-07-31] (deployments
-- ENDING in this window). cohort_month is still keyed on treatmt_strt_dt (the
-- wave's start month) -- a deployment can start earlier (e.g. Feb) and end
-- inside the window (e.g. May) and is included under its START-month cohort.
-- This is the validation gate (denominator + conversions) that must pass
-- before trusting the vintage.
-- Grain caveat: CRV randomization is per-wave, not sticky. Collapsing to
-- (visa_acct_no, cohort_month, arm) can put the same account in both arms across
-- different waves. Multiple waves landing in the same cell are collapsed
-- using MIN(treatmt_strt_dt) / MAX(treatmt_end_dt) as the cell's anchor/
-- window bound -- a simplification, not a wave-level solve.
-- Engine: Starburst/Trino, cross-catalog federated join (dg6v01 <-> edl0_im).
-- Trino syntax only: no QUALIFY, no TOP, no NULLIFZERO.
-- Note: the join predicate wraps both sides in CAST(... AS DECIMAL(38,0)), which
-- can block partition/predicate pushdown on the federated event side (the engine
-- may not push the cast expression down to the remote catalog the way it would a
-- bare column comparison). Correctness (matching zero-padded strings to bare
-- numerics) takes priority over pushdown efficiency here; flagged for follow-up
-- if this query proves slow at full volume.
-- Output: campaign, cohort_month, arm, cohort_size (formatted), responders
-- (formatted), response_rate (percentage with % sign). cohort_size and
-- responders are thousands-separated whole numbers; response_rate is a
-- 2-decimal percentage string (divide-by-zero guarded via NULLIF). Raw
-- integers are computed in an inner CTE; formatting happens only in the outer
-- SELECT. Matches the Data Lab summary (crv_cohort_summary_v1_datalab.sql)
-- column layout exactly.

WITH
tactic_cohort AS (
    SELECT
        visa_acct_no,
        date_trunc('month', treatmt_strt_dt)                            AS cohort_month,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END    AS arm,
        MIN(treatmt_strt_dt)                                            AS treatmt_strt_dt,
        MAX(treatmt_end_dt)                                             AS treatmt_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
    GROUP BY
        visa_acct_no,
        date_trunc('month', treatmt_strt_dt),
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
),

cohort_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT visa_acct_no) AS cohort_size
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- success events anywhere in-window (no vintage_day, no day binning). Joined on
-- the normalized numeric account key -- tactic-side visa_acct_no vs. events-side
-- zero-padded acct_no, both CAST to DECIMAL(38,0) to strip the padding.
success_events AS (
    SELECT DISTINCT
        t.visa_acct_no,
        t.cohort_month,
        t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
),

-- per-cell responders: accounts (visa_acct_no) with >=1 matching in-window event
success_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT visa_acct_no) AS responders
    FROM success_events
    GROUP BY cohort_month, arm
),

final_counts AS (
    SELECT
        CAST('CRV' AS VARCHAR(10))     AS campaign,
        c.cohort_month,
        c.arm,
        c.cohort_size,
        COALESCE(s.responders, 0)      AS responders
    FROM cohort_cells c
    LEFT JOIN success_cells s
        ON  s.cohort_month = c.cohort_month
        AND s.arm           = c.arm
)

SELECT
    campaign,
    cohort_month,
    arm,
    format('%,d', cohort_size)                                              AS cohort_size,
    format('%,d', responders)                                               AS responders,
    format('%.2f', 100.0 * responders / NULLIF(cohort_size, 0)) || '%'      AS response_rate
FROM final_counts
ORDER BY cohort_month, arm;
