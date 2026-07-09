-- crv_vintage_v2_production.sql
-- Campaign : CRV (Credit Card Installment Plan) — SOURCE RECONCILIATION, PRODUCTION SIDE
-- Source   : DG6V01.TACTIC_EVNT_IP_AR_HIST (decisioning) + EDL0_IM measurement_events_v2
--            (success events). This file is fully self-contained: only these two tables,
--            no shared tables with crv_vintage_v1_datalab.sql, and NEVER filtered by the
--            curated cards_crv_install_decis_resp table.
-- Engine   : Starburst/Trino, cross-catalog federated join (dg6v01 <-> edl0_im).
--            Trino syntax only: no QUALIFY (MIN() aggregation used instead), no TOP,
--            strict typing, UNNEST(SEQUENCE(...)) spine. Smaller side (tactic cohort,
--            pre-filtered to CRV) is joined first against the much larger events table.
-- Grain    : ACCOUNT (acct_no). Tactic history and measurement_events_v2 are joined on
--            acct_no directly — acct_no is the identifier on BOTH tables, no clnt_no bridge.
-- Inclusion window : deployments whose treatmt_end_dt falls in [2026-05-01, 2026-07-31].
--            cohort_month is still keyed on treatmt_strt_dt (the wave's start month) — a
--            deployment can start earlier (e.g. Feb) and end inside the window (e.g. May)
--            and is included under its START-month cohort.
-- Day axis : no hardcoded 0..90. Each cohort's curve is bounded by its own deployment
--            window length (treatmt_end_dt - treatmt_strt_dt), mirroring the datalab file.
--            The spine runs 0 .. MAX(cohort_max_day) across all cohorts; dense_grid caps
--            each cohort at its own cohort_max_day.
-- Success  : event_cd = 'p_card_installmt_purch' inside the [treatmt_strt_dt, treatmt_end_dt]
--            window — matches the datalab's in-window definition. First in-window success
--            per acct_no (MIN(vintage_day)).
-- Arm      : tst_grp_cd = 'TG8' -> Control, else Action
--
-- Grain caveat: CRV randomization is per-wave, not sticky. Collapsing to
-- (acct_no, cohort_month, arm) can put the same account in both arms across different
-- waves. Flagged for a downstream multi-arm prevalence check, not solved here.
-- Multiple waves landing in the same (acct_no, cohort_month, arm) cell are collapsed
-- using MIN(treatmt_strt_dt) / MAX(treatmt_end_dt) as the cell's anchor/window bound —
-- a simplification, not a wave-level solve.

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

-- per-cell size and each cohort's own day-axis bound (its deployment window length)
cohort_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT acct_no)                                 AS cohort_size,
        MAX(date_diff('day', treatmt_strt_dt, treatmt_end_dt))  AS cohort_max_day
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- success events in-window; vintage_day = days since cell's treatment start
success_events AS (
    SELECT
        t.acct_no,
        t.cohort_month,
        t.arm,
        date_diff('day', t.treatmt_strt_dt, m.event_date) AS vintage_day
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON m.acct_no = t.acct_no
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
),

-- first in-window success per account per cell (vintage_day is naturally >= 0 since
-- events are restricted to on/after treatmt_strt_dt above; no clamp needed)
acct_success AS (
    SELECT
        acct_no,
        cohort_month,
        arm,
        MIN(vintage_day) AS acct_vintage_day
    FROM success_events
    GROUP BY acct_no, cohort_month, arm
),

daily_counts AS (
    SELECT
        cohort_month,
        arm,
        acct_vintage_day        AS vintage_day,
        COUNT(DISTINCT acct_no) AS n_events
    FROM acct_success
    GROUP BY cohort_month, arm, acct_vintage_day
),

-- spine spans 0 .. the longest deployment window across ALL cohorts; each cohort is
-- capped at its own cohort_max_day in dense_grid below
spine AS (
    SELECT s.vintage_day
    FROM (SELECT MAX(cohort_max_day) AS mx FROM cohort_cells) m
    CROSS JOIN UNNEST(SEQUENCE(0, m.mx)) AS s(vintage_day)
),

-- dense grid: cohort_month x arm x vintage_day, capped per-cohort at its own window
dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, s.vintage_day
    FROM cohort_cells c
    CROSS JOIN spine s
    WHERE s.vintage_day <= c.cohort_max_day
)

SELECT
    CAST('CRV' AS VARCHAR(10))                          AS campaign,
    g.cohort_month,
    g.arm,
    CAST('crv_install_conversion' AS VARCHAR(30))       AS metric,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                    AS cum_events
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm          = g.arm
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day;
