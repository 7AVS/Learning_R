-- ENGINE: Teradata-direct
-- VBA monthly vintage off the curated Data Lab table
-- Source: DL_MR_PROD.NBO_VBA_RBOL_COMBINED
--
-- OUTPUT: mnc | cohort_month | arm | vintage_day | base | conv_day | conv_cum
-- Counts only. Divide in the pivot.
--
-- Conversion = net_response > 0, dated by response_dt.
-- Day 0 clamp: responses before treatmt_strt_dt are pinned to day 0, not dropped.
-- Horizon per cell = MIN(CURRENT_DATE - treatmt_strt_dt), capped at 180 days,
--   so the curve stops where every client in the cell has been observed.

-- DROP TABLE vt_vba_base; DROP TABLE vt_vba_cells; DROP TABLE vt_vba_spine;


-- 1. One row per client per mnemonic per cohort month
CREATE VOLATILE TABLE vt_vba_base AS (
    SELECT
        TRIM(mnc)                                                                AS mnc
      , CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE)   AS cohort_month
      , TRIM(control)                                                            AS arm
      , clnt_no
      , MIN(treatmt_strt_dt)                                                     AS anchor_dt
      , MAX(CASE WHEN COALESCE(net_response,0) > 0 THEN 1 ELSE 0 END)            AS conv_flag
      , MIN(CASE WHEN COALESCE(net_response,0) > 0 THEN response_dt END)         AS conv_dt
    FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
    WHERE treatmt_strt_dt IS NOT NULL
      AND treatmt_strt_dt >= DATE '2024-01-01'
      AND TRIM(control) IN ('Action','Control')
    GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (mnc, cohort_month, arm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_base COLUMN (mnc, cohort_month, arm);


-- 2. Base counts + per-cell horizon
CREATE VOLATILE TABLE vt_vba_cells AS (
    SELECT
        mnc, cohort_month, arm
      , COUNT(DISTINCT clnt_no)                                AS base
      , CASE WHEN MIN(CURRENT_DATE - anchor_dt) < 0   THEN 0
             WHEN MIN(CURRENT_DATE - anchor_dt) > 180 THEN 180
             ELSE MIN(CURRENT_DATE - anchor_dt) END            AS max_day
    FROM vt_vba_base
    GROUP BY 1,2,3
) WITH DATA
PRIMARY INDEX (mnc, cohort_month, arm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_cells COLUMN (mnc, cohort_month, arm);
COLLECT STATISTICS ON vt_vba_cells COLUMN (max_day);


-- 3. Day spine (materialized + stats before the cross join — TDWM blocks it otherwise)
CREATE VOLATILE TABLE vt_vba_spine AS (
    SELECT CAST(calendar_date - DATE '2000-01-01' AS INTEGER) AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date >= DATE '2000-01-01'
      AND calendar_date <= DATE '2000-01-01' + (SELECT MAX(max_day) FROM vt_vba_cells)
) WITH DATA
PRIMARY INDEX (vintage_day)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_spine COLUMN (vintage_day);


-- 4. Output
WITH daily AS (
    SELECT
        mnc, cohort_month, arm
      , CASE WHEN conv_dt - anchor_dt < 0 THEN 0 ELSE conv_dt - anchor_dt END AS vintage_day
      , COUNT(DISTINCT clnt_no) AS n
    FROM vt_vba_base
    WHERE conv_flag = 1 AND conv_dt IS NOT NULL
    GROUP BY 1,2,3,4
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
FROM vt_vba_cells c
JOIN vt_vba_spine s
  ON s.vintage_day <= c.max_day
LEFT JOIN daily d
  ON  c.mnc = d.mnc
  AND c.cohort_month = d.cohort_month
  AND c.arm = d.arm
  AND s.vintage_day = d.vintage_day
ORDER BY 1,2,3,4;


-- CHECK: terminal conv_cum vs flat conversion count. Should match except for
-- converters with no response_dt (which have no day to sit on).
/*
SELECT mnc, cohort_month, arm
     , COUNT(DISTINCT clnt_no)                                            AS base
     , SUM(conv_flag)                                                     AS conv_flat
     , SUM(CASE WHEN conv_flag=1 AND conv_dt IS NULL THEN 1 ELSE 0 END)   AS conv_undated
FROM vt_vba_base GROUP BY 1,2,3 ORDER BY 1,2,3;
*/

-- DROP TABLE vt_vba_base; DROP TABLE vt_vba_cells; DROP TABLE vt_vba_spine;
