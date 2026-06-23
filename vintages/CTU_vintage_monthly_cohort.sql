-- Campaign : CTU (Card Type Upgrade)
-- Source   : DL_MR_PROD.nbo_pba_upgrade (curated)
-- Success  : primary_success = 1 (primary upgrade success, confirmed in source script)
--            Earliest response_dt per client.
-- Anchor   : treatmt_strt_dt
-- Arm      : No test/control split in CTU — arm fixed as 'ALL' (per source script)
-- Engine   : Starburst/Trino (federation: DL_MR_PROD via Starburst)
-- Range    : treatmt_strt_dt >= 2026-01-01

-- NOTE: tactic_id filter '2026098CTU' is specific to the current deployment wave.
-- Remove or broaden for multi-wave monthly cohorts; monthly cohort_month groups waves naturally.

WITH cohort AS (
    SELECT
        clnt_no,
        treatmt_strt_dt                                               AS anchor_dt,
        date_trunc('month', treatmt_strt_dt)                          AS cohort_month,
        primary_success,
        response_dt,
        CAST('ALL' AS VARCHAR(10))                                    AS arm
    FROM DL_MR_PROD.nbo_pba_upgrade
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
),

cohort_size AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size
    FROM cohort
    GROUP BY 1, 2
),

first_success AS (
    SELECT
        clnt_no,
        cohort_month,
        arm,
        anchor_dt,
        MIN(response_dt)                                              AS first_success_dt
    FROM cohort
    WHERE primary_success = 1
      AND response_dt IS NOT NULL
    GROUP BY clnt_no, cohort_month, arm, anchor_dt
),

vintage_days_raw AS (
    SELECT
        cohort_month,
        arm,
        date_diff('day', anchor_dt, first_success_dt)                AS vintage_day,
        COUNT(DISTINCT clnt_no)                                       AS new_events
    FROM first_success
    WHERE date_diff('day', anchor_dt, first_success_dt) BETWEEN 0 AND 180
    GROUP BY 1, 2, 3
),

spine AS (
    SELECT
        cs.cohort_month,
        cs.arm,
        cs.cohort_size,
        t.vintage_day
    FROM cohort_size cs
    CROSS JOIN UNNEST(sequence(0, 180)) AS t(vintage_day)
),

joined AS (
    SELECT
        s.cohort_month,
        s.arm,
        s.vintage_day,
        s.cohort_size,
        COALESCE(r.new_events, 0)                                     AS new_events
    FROM spine s
    LEFT JOIN vintage_days_raw r
        ON  r.cohort_month = s.cohort_month
        AND r.arm          = s.arm
        AND r.vintage_day  = s.vintage_day
)

SELECT
    CAST('CTU' AS VARCHAR(10))                                        AS campaign,
    cohort_month,
    arm,
    vintage_day,
    cohort_size,
    SUM(new_events) OVER (
        PARTITION BY cohort_month, arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                 AS cum_events
FROM joined
ORDER BY cohort_month, arm, vintage_day
;
