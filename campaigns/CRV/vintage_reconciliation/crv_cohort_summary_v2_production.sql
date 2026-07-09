-- crv_cohort_summary_v2_production.sql
-- Per-cohort FULL-WINDOW summary version of crv_vintage_v2_production.sql.
-- Same population, cohort anchor, arm derivation and success logic as the vintage
-- file; the vintage_day spine and cumulative curve are dropped in favor of one row
-- per (cohort_month, arm) covering the entire [treatmt_strt_dt, treatmt_end_dt]
-- window. Built for source reconciliation (dashboard vs Data Lab vs Production)
-- with the time axis removed so population/anchor/success differences are
-- isolated. The vintage file's 0-90 day cap only bounded the daily curve — success
-- here is "any event inside the window", with no artificial day cap re-applied.
-- Engine: Starburst/Trino, cross-catalog federated join (dg6v01 <-> edl0_im).
-- Trino syntax only: no QUALIFY, no TOP, no NULLIFZERO.
-- Counts only — no rate/percentage/ratio columns.

WITH
tactic_cohort AS (
    SELECT
        clnt_no,
        date_trunc('month', treatmt_strt_dt)                            AS cohort_month,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END    AS arm,
        MIN(treatmt_strt_dt)                                            AS treatmt_strt_dt,
        MAX(treatmt_end_dt)                                             AS treatmt_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-01-01'
    GROUP BY
        clnt_no,
        date_trunc('month', treatmt_strt_dt),
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
),

cohort_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no) AS cohort_size
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- success events anywhere in-window (no vintage_day, no day binning)
success_events AS (
    SELECT
        t.clnt_no,
        t.cohort_month,
        t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON m.clnt_no = t.clnt_no
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
),

-- one row per client per cell that had at least one in-window success
client_success AS (
    SELECT DISTINCT
        clnt_no,
        cohort_month,
        arm
    FROM success_events
),

responder_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no) AS responders
    FROM client_success
    GROUP BY cohort_month, arm
)

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
ORDER BY c.cohort_month, c.arm;
