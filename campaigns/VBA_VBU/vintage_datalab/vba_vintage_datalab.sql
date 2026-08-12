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
-- Each cohort stops at its own maturity (CURRENT_DATE - treatmt_strt_dt),
-- capped at 120. A cohort treated last month shows a short curve on purpose —
-- that is missing observation time, not a missing value. Do not fill it forward.

WITH RECURSIVE spine (vintage_day) AS (
        SELECT 0
    UNION ALL
        SELECT vintage_day + 1 FROM spine WHERE vintage_day < 120
)
, base AS (
    SELECT
        TRIM(mnc)                                                              AS mnc
      , CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month
      , TRIM(control)                                                          AS arm
      , clnt_no
      , MIN(treatmt_strt_dt)                                                   AS anchor_dt
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
      , MIN(CURRENT_DATE - anchor_dt)      AS maturity
    FROM base
    GROUP BY 1,2,3,4
)
, cells AS (
    SELECT
        mnc, cohort_month, arm
      , SUM(n) AS base
      , CASE WHEN MIN(maturity) < 0   THEN 0
             WHEN MIN(maturity) > 120 THEN 120
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
