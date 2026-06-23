-- pcd_vintage_monthly.sql
-- Campaign : PCD (Product Card Upgrade — Async Banner)
-- Source   : dl_mr_prod.cards_pcd_ongoing_decis_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine; TDWM cross-join guard: pop_cells is small)
-- Success  : responder_targetproduct = 1; event date = dt_prod_change
-- Arm      : test_groups_period suffix '%T' = TEST, '%C' = CONTROL
-- Cohort   : MONTH(response_start), Jan 2026 onward; vintage_day 0–90
-- NOTE: If TDWM blocks the cross-join, materialize pop_cells and days_spine as
--       volatile tables with COLLECT STATISTICS before the final SELECT.

WITH
days_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 90
),

cohort AS (
    SELECT
        clnt_no,
        response_start,
        dt_prod_change,
        responder_targetproduct,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
        END                                             AS arm,
        (response_start - (EXTRACT(DAY FROM response_start) - 1)) AS cohort_month
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start   >= DATE '2026-01-01'
      AND (   TRIM(test_groups_period) LIKE '%T'
           OR TRIM(test_groups_period) LIKE '%C'
          )
),

pop_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no) AS cohort_size
    FROM cohort
    WHERE arm IS NOT NULL
    GROUP BY cohort_month, arm
),

first_event AS (
    SELECT
        clnt_no,
        arm,
        cohort_month,
        response_start,
        MIN(dt_prod_change) AS first_event_dt
    FROM cohort
    WHERE arm IS NOT NULL
      AND responder_targetproduct = 1
      AND dt_prod_change IS NOT NULL
    GROUP BY clnt_no, arm, cohort_month, response_start
),

client_vintage AS (
    SELECT
        cohort_month,
        arm,
        clnt_no,
        CAST(first_event_dt - response_start AS INTEGER) AS vintage_day
    FROM first_event
    WHERE CAST(first_event_dt - response_start AS INTEGER) BETWEEN 0 AND 90
),

daily_counts AS (
    SELECT cohort_month, arm, vintage_day, COUNT(DISTINCT clnt_no) AS n_events
    FROM client_vintage
    GROUP BY cohort_month, arm, vintage_day
),

grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM pop_cells c CROSS JOIN days_spine d
)

SELECT
    CAST('PCD' AS VARCHAR(10))                          AS campaign,
    g.cohort_month,
    CAST(g.arm AS VARCHAR(20))                          AS arm,
    CAST('responder_targetproduct' AS VARCHAR(30))      AS metric,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                   AS cum_events
FROM grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm          = g.arm
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day
;
