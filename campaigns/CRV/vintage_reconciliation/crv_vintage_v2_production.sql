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
-- Grain    : client (clnt_no)
-- Success  : event_cd = 'p_card_installmt_purch' inside the [treatmt_strt_dt, treatmt_end_dt]
--            window — matches the datalab's in-window definition.
-- Arm      : tst_grp_cd = 'TG8' -> Control, else Action
--
-- Grain caveat: CRV randomization is per-wave, not sticky. Collapsing to
-- (clnt_no, cohort_month, arm) can put the same client in both arms across different
-- waves. Flagged for a downstream multi-arm prevalence check, not solved here.
-- Multiple waves landing in the same (clnt_no, cohort_month, arm) cell are collapsed
-- using MIN(treatmt_strt_dt) / MAX(treatmt_end_dt) as the cell's anchor/window bound —
-- a simplification, not a wave-level solve.

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

-- success events in-window; vintage_day = days since cell's treatment start
success_events AS (
    SELECT
        t.clnt_no,
        t.cohort_month,
        t.arm,
        date_diff('day', t.treatmt_strt_dt, m.event_date) AS vintage_day
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON m.clnt_no = t.clnt_no
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN t.treatmt_strt_dt AND t.treatmt_end_dt
),

-- first in-window success per client per cell
client_success AS (
    SELECT
        clnt_no,
        cohort_month,
        arm,
        MIN(vintage_day) AS client_vintage_day
    FROM success_events
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY clnt_no, cohort_month, arm
),

daily_counts AS (
    SELECT
        cohort_month,
        arm,
        client_vintage_day       AS vintage_day,
        COUNT(DISTINCT clnt_no)  AS n_events
    FROM client_success
    GROUP BY cohort_month, arm, client_vintage_day
),

spine AS (
    SELECT vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS t(vintage_day)
),

-- dense grid: cohort_month x arm x vintage_day
dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, s.vintage_day
    FROM cohort_cells c
    CROSS JOIN spine s
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
