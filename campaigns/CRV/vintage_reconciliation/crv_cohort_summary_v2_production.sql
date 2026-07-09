-- crv_cohort_summary_v2_production.sql
-- Per-cohort FULL-WINDOW summary version of crv_vintage_v2_production.sql.
-- Grain: ACCOUNT (acct_no) -- tactic history joined to measurement_events_v2
-- directly on acct_no, no clnt_no bridge (acct_no is the identifier on BOTH
-- tables). Same population, cohort anchor, arm derivation and success logic
-- as the vintage file; the vintage_day spine and cumulative curve are dropped
-- in favor of one row per (cohort_month, arm) covering the entire
-- [treatmt_strt_dt, treatmt_end_dt] window. Responder is DERIVED (>=1
-- in-window p_card_installmt_purch event), not a precomputed flag. The
-- vintage file's 0-90 day cap only bounded the daily curve -- success here is
-- "any event inside the window", with no artificial day cap re-applied.
-- Inclusion window: treatmt_end_dt in [2026-05-01, 2026-07-31] (deployments
-- ENDING in this window). cohort_month is still keyed on treatmt_strt_dt (the
-- wave's start month) -- a deployment can start earlier (e.g. Feb) and end
-- inside the window (e.g. May) and is included under its START-month cohort.
-- This is the validation gate (denominator + conversions) that must pass
-- before trusting the vintage.
-- Grain caveat: CRV randomization is per-wave, not sticky. Collapsing to
-- (acct_no, cohort_month, arm) can put the same account in both arms across
-- different waves. Multiple waves landing in the same cell are collapsed
-- using MIN(treatmt_strt_dt) / MAX(treatmt_end_dt) as the cell's anchor/
-- window bound -- a simplification, not a wave-level solve.
-- Engine: Starburst/Trino, cross-catalog federated join (dg6v01 <-> edl0_im).
-- Trino syntax only: no QUALIFY, no TOP, no NULLIFZERO.
-- Output: campaign, cohort_month, arm, cohort_size (formatted), responders
-- (formatted), response_rate (percentage with % sign). cohort_size and
-- responders are thousands-separated whole numbers; response_rate is a
-- 2-decimal percentage string (divide-by-zero guarded via NULLIF). No
-- population/success logic changed -- formatting/rate are computed off the
-- same raw counts.

WITH
tactic_cohort AS (
    SELECT
        acct_no,
        date_trunc('month', treatmt_strt_dt)                            AS cohort_month,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END    AS arm,
        MIN(treatmt_strt_dt)                                            AS treatmt_strt_dt,
        MAX(treatmt_end_dt)                                             AS treatmt_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'
    GROUP BY
        acct_no,
        date_trunc('month', treatmt_strt_dt),
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
),

cohort_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT acct_no) AS cohort_size
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- success events anywhere in-window (no vintage_day, no day binning)
success_events AS (
    SELECT
        t.acct_no,
        t.cohort_month,
        t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON m.acct_no = t.acct_no
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
),

-- one row per account per cell that had at least one in-window success
acct_success AS (
    SELECT DISTINCT
        acct_no,
        cohort_month,
        arm
    FROM success_events
),

responder_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT acct_no) AS responders
    FROM acct_success
    GROUP BY cohort_month, arm
),

final_counts AS (
    SELECT
        CAST('CRV' AS VARCHAR(10))       AS campaign,
        c.cohort_month,
        c.arm,
        c.cohort_size,
        COALESCE(r.responders, 0)        AS responders
    FROM cohort_cells c
    LEFT JOIN responder_cells r
        ON  r.cohort_month = c.cohort_month
        AND r.arm           = c.arm
)

SELECT
    campaign,
    cohort_month,
    arm,
    format('%,d', cohort_size)                                             AS cohort_size,
    format('%,d', responders)                                              AS responders,
    format('%.2f', 100.0 * responders / NULLIF(cohort_size, 0)) || '%'     AS response_rate
FROM final_counts
ORDER BY cohort_month, arm;
