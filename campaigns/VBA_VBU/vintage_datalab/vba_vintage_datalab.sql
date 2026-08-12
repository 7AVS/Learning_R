-- ENGINE: Teradata-direct
-- VBA monthly vintage off the curated Data Lab table
-- Source: DL_MR_PROD.NBO_VBA_RBOL_COMBINED
--
-- OUTPUT: mnc | cohort_month | arm | vintage_day | base | conv_day | conv_cum
-- Counts only. Divide in the pivot.
--
-- Conversion = net_response > 0, dated by response_dt.
--   net_response is NULL for non-responders (98.4% of rows), 0/1 otherwise — probe P3.
--   response_dt is never NULL when a response exists — no undated converters.
--
-- Horizon per cell = the SMALLER of
--   (a) the treatment window   MAX(treatmt_end_dt - treatmt_strt_dt)   [canon]
--   (b) maturity               MIN(CURRENT_DATE  - treatmt_strt_dt)    [no survivorship]
--   Probe P4: all 235,889 responses land inside the treatment window, days 0-117.
--   So (a) is the real bound; (b) only bites on cohorts that are still open.
--
-- Day-0 clamp kept but it is a no-op here — P4 found zero negative days.
--
-- GRAIN WARNING (probe P1): the source is a reporting snapshot, ~4.1 rows per
--   (clnt_no, tactic_id) across report_date/comparison/segment. The GROUP BY in
--   step 1 is what makes the base a client count. Do not remove it.
--
-- POPULATION: mnc = 'VBA' only. The table stacks two campaigns (VBA + BOL/RBOL);
--   BOL is a different track and is excluded. Matches vba_deployment_baseline.sql.

-- Re-run: these must be dropped first or you get "table already exists".
-- DROP TABLE vt_vba_base; DROP TABLE vt_vba_cells; DROP TABLE vt_vba_spine;


-- 1. One row per client per mnemonic per cohort month
CREATE VOLATILE TABLE vt_vba_base AS (
    SELECT
        TRIM(mnc)                                                                AS mnc
      , CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE)   AS cohort_month
      , TRIM(control)                                                            AS arm
      , clnt_no
      , MIN(treatmt_strt_dt)                                                     AS anchor_dt
      , MAX(treatmt_end_dt)                                                      AS end_dt
      , MAX(CASE WHEN COALESCE(net_response,0) > 0 THEN 1 ELSE 0 END)            AS conv_flag
      , MIN(CASE WHEN COALESCE(net_response,0) > 0 THEN response_dt END)         AS conv_dt
    FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
    WHERE TRIM(mnc) = 'VBA'                        -- VBA only. BOL is the RBOL track, not this campaign.
      AND treatmt_strt_dt IS NOT NULL
      AND treatmt_strt_dt >= DATE '2024-01-01'
      AND TRIM(control) IN ('Action','Control')
    GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (clnt_no)                            -- high cardinality. Do NOT use (mnc,cohort_month,arm):
ON COMMIT PRESERVE ROWS;                           -- ~64 distinct values over millions of rows = AMP skew = hang.

COLLECT STATISTICS ON vt_vba_base COLUMN (clnt_no);
COLLECT STATISTICS ON vt_vba_base COLUMN (mnc, cohort_month, arm);


-- 2. Base counts + per-cell horizon = LEAST(treatment window, maturity), floored at 0
CREATE VOLATILE TABLE vt_vba_cells AS (
    SELECT
        mnc, cohort_month, arm
      , COUNT(DISTINCT clnt_no)                                AS base
      , MAX(end_dt - anchor_dt)                                AS window_day
      , MIN(CURRENT_DATE - anchor_dt)                          AS maturity_day
      , CASE
            WHEN MIN(CURRENT_DATE - anchor_dt) < 0 THEN 0
            WHEN MAX(end_dt - anchor_dt)       < 0 THEN 0
            WHEN MAX(end_dt - anchor_dt) <= MIN(CURRENT_DATE - anchor_dt)
                 THEN MAX(end_dt - anchor_dt)
            ELSE MIN(CURRENT_DATE - anchor_dt)
        END                                                    AS max_day
    FROM vt_vba_base
    GROUP BY 1,2,3
) WITH DATA
PRIMARY INDEX (mnc, cohort_month, arm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_cells COLUMN (mnc, cohort_month, arm);
COLLECT STATISTICS ON vt_vba_cells COLUMN (max_day);


-- 3. Day spine 0..365, flat. Materialized + stats before the cross join (TDWM).
--    No scalar subquery here on purpose — it made the calendar scan unpredictable.
--    The per-cell cap happens in the join in step 4.
CREATE VOLATILE TABLE vt_vba_spine AS (
    SELECT CAST(calendar_date - DATE '2000-01-01' AS INTEGER) AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-12-31'
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


-- CHECK A: flat counts + horizon per cell. conv_flat is the number the terminal
-- conv_cum must reach. window_day vs maturity_day shows which bound is binding.
/*
SELECT c.mnc, c.cohort_month, c.arm, c.base, c.window_day, c.maturity_day, c.max_day
     , b.conv_flat, b.conv_undated
FROM vt_vba_cells c
JOIN (
    SELECT mnc, cohort_month, arm
         , SUM(conv_flag)                                                  AS conv_flat
         , SUM(CASE WHEN conv_flag=1 AND conv_dt IS NULL THEN 1 ELSE 0 END) AS conv_undated
    FROM vt_vba_base GROUP BY 1,2,3
) b ON c.mnc=b.mnc AND c.cohort_month=b.cohort_month AND c.arm=b.arm
ORDER BY 1,2,3;
*/

-- CHECK B: clients sitting in BOTH arms in the same mnc+month. Should be 0 rows.
-- If not 0, those clients are double-counted in the base and the arm split is dirty.
/*
SELECT mnc, cohort_month, COUNT(*) AS clients_in_both_arms
FROM (
    SELECT mnc, cohort_month, clnt_no
    FROM vt_vba_base
    GROUP BY 1,2,3
    HAVING COUNT(DISTINCT arm) > 1
) x
GROUP BY 1,2 ORDER BY 1,2;
*/

-- DROP TABLE vt_vba_base; DROP TABLE vt_vba_cells; DROP TABLE vt_vba_spine;
