-- ENGINE: Teradata-direct
-- VBA monthly vintage — one statement, no volatile tables.
-- Source: DL_MR_PROD.NBO_VBA_RBOL_COMBINED
--
-- OUTPUT: mnc | cohort_month | arm | vintage_day | base | conv_day | conv_cum
--
-- Population : mnc='VBA' (the table also carries BOL/RBOL — different track)
-- Arm        : control IN ('Action','Control')
-- Cohort     : month of treatmt_strt_dt
-- Conversion : net_response > 0, dated by response_dt
-- vintage_day: response_dt - treatmt_strt_dt
--
-- Rows are emitted only for days on which someone converted. Days with zero
-- conversions are absent — conv_cum is still correct at every row it prints.

WITH base AS (
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
, agg AS (
    SELECT
        mnc, cohort_month, arm
      , conv_dt - anchor_dt   AS vintage_day     -- NULL for non-converters
      , COUNT(*)              AS n
    FROM base
    GROUP BY 1,2,3,4
)
, with_base AS (
    SELECT
        mnc, cohort_month, arm, vintage_day, n
      , SUM(n) OVER (PARTITION BY mnc, cohort_month, arm) AS base   -- includes non-converters
    FROM agg
)
SELECT
    mnc
  , cohort_month
  , arm
  , vintage_day
  , base
  , n AS conv_day
  , SUM(n) OVER (
        PARTITION BY mnc, cohort_month, arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS conv_cum
FROM with_base
WHERE vintage_day IS NOT NULL
ORDER BY 1,2,3,4;
