-- ENGINE: Teradata-direct
-- VBA monthly vintage — one statement, no volatile tables.
-- Source: DL_MR_PROD.NBO_VBA_RBOL_COMBINED
--
-- OUTPUT: mnc | cohort_month | arm | vintage_day | base | conv_day | conv_cum
--   Every day 0..max is present for every cohort+arm. conv_day is 0 on days
--   nobody converted; conv_cum carries forward. No gaps.
--
-- Population : mnc='VBA' (the table also carries BOL/RBOL — different track)
-- Arm        : control IN ('Action','Control')
-- Cohort     : month of treatmt_strt_dt
-- Conversion : net_response > 0, dated by response_dt
--
-- Horizon is derived per cohort+arm from the data, not hardcoded:
--   window_day   = MAX(treatmt_end_dt - treatmt_strt_dt)  -- the deployment length
--   maturity_day = MIN(CURRENT_DATE   - treatmt_strt_dt)  -- observation time so far
--   max_day      = the smaller of the two
-- A recent cohort shows a short curve on purpose — that is missing observation
-- time, not a missing value. Do not fill it forward.

-- 400 is a safety rail on the recursion, not a business horizon. The real cut is
-- per cell in `cells` below, derived from the deployment window in the data.
-- Seed must reference a table (Teradata 8842) — this returns exactly 1 row.
WITH RECURSIVE spine (vintage_day) AS (
        SELECT 0
        FROM SYS_CALENDAR.CALENDAR
        WHERE calendar_date = DATE '2024-01-01'
    UNION ALL
        SELECT vintage_day + 1 FROM spine WHERE vintage_day < 400
)
, base AS (
    SELECT
        TRIM(mnc)                                                              AS mnc
      , CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month
      , TRIM(control)                                                          AS arm
      , clnt_no
      , MIN(treatmt_strt_dt)                                                   AS anchor_dt
      , MAX(treatmt_end_dt)                                                    AS end_dt
      , MIN(CASE WHEN net_response > 0 THEN response_dt END)                   AS conv_dt
    FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
    WHERE TRIM(mnc) = 'VBA'
      AND TRIM(control) IN ('Action','Control')
      AND treatmt_strt_dt >= DATE '2024-01-01'
    GROUP BY 1,2,3,4
)
-- one pass over base; agg is ~thousands of rows so referencing it twice is free
, agg AS (
    SELECT
        mnc, cohort_month, arm
      , conv_dt - anchor_dt                AS vintage_day   -- NULL = never converted
      , COUNT(*)                           AS n
      , MAX(end_dt - anchor_dt)            AS window_day    -- deployment length
      , MIN(CURRENT_DATE - anchor_dt)      AS maturity      -- observation time so far
    FROM base
    GROUP BY 1,2,3,4
)
-- Horizon per cell = the deployment window, cut short if the cohort is still open.
-- Probe P4: all 235,889 responders land inside the window (max day 117), so the
-- window is the true bound. A cohort month with several deployments takes the
-- LONGEST of them, so the slowest deployment in the month runs its full course.
, cells AS (
    SELECT
        mnc, cohort_month, arm
      , SUM(n)           AS base
      , MAX(window_day)  AS window_day
      , MIN(maturity)    AS maturity_day
      , CASE WHEN MIN(maturity)   < 0 THEN 0
             WHEN MAX(window_day) < 0 THEN 0
             WHEN MAX(window_day) <= MIN(maturity) THEN MAX(window_day)
             ELSE MIN(maturity) END AS max_day
    FROM agg
    GROUP BY 1,2,3
)
SELECT
    c.mnc
  , c.cohort_month
  , c.arm
  , s.vintage_day
  , c.base
  , COALESCE(d.n,0) AS conv_day
  , SUM(COALESCE(d.n,0)) OVER (
        PARTITION BY c.mnc, c.cohort_month, c.arm
        ORDER BY s.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS conv_cum
FROM cells c
JOIN spine s
  ON s.vintage_day <= c.max_day
LEFT JOIN agg d
  ON  c.mnc          = d.mnc
  AND c.cohort_month = d.cohort_month
  AND c.arm          = d.arm
  AND s.vintage_day  = d.vintage_day
ORDER BY 1,2,3,4;
