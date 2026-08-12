-- ENGINE: Teradata-direct  (pure EDW, no edl0_im table touched -> volatile tables allowed)
-- VBA vintage off the curated Data Lab table — STEP 2 of 2
-- Source : DL_MR_PROD.NBO_VBA_RBOL_COMBINED
-- Method : references/vintage_datalab_method.md
-- Schema : schemas/nbo_vba_rbol_combined.md
--
-- DO NOT RUN THIS BEFORE vba_datalab_probe.sql. Four things in here are
-- assumptions the probe confirms or kills:
--   A1. net_response is a 0/1 flag, not a count.        (probe P3)
--   A2. response_dt is the response date for net_response. (probe P3/P4)
--   A3. control holds the literal strings 'Action'/'Control'. (probe P2)
--   A4. grain is (clnt_no, tactic_id).                  (probe P1)
--
-- DEPARTURE FROM THE CANON — READ THIS:
--   references/vintage_datalab_method.md sets the per-cell horizon to
--   MAX(offer_end - offer_start). That is a TREATMENT-window cap. VBA responses
--   land well past treatmt_end_dt (probe P4 buckets 4/5), so that cap would
--   truncate the curve mid-climb.
--   This file instead caps each cell at its MATURITY floor:
--       cohort_max_day = MIN(AS_OF_DT - treatmt_strt_dt) across the cell
--   i.e. the curve stops at the last day on which EVERY client in the cell has
--   been observed. That removes survivorship bias (later-treated clients
--   dropping out of the tail and flattening it). Cost: the curve is shorter.
--   If you want the canon behaviour instead, swap the marked line in vt_vba_cells.
--
-- NO MNEMONIC FILTER. Repo rule never_filter_mnemonic: mnc is emitted as a
--   slicer, never used in a WHERE clause.
--
-- OUTPUT: counts only, never rates (repo rule no_rates_in_outputs). Divide in the pivot.
--   Grain: (slicer_dim, slicer_value, cohort_month, arm, vintage_day)


-------------------------------------------------------------------------------
-- PARAMETERS — edit these three, nothing else
-------------------------------------------------------------------------------
--   COHORT_FLOOR   DATE '2024-01-01'   hard repo floor, do not go earlier
--   AS_OF_DT       CURRENT_DATE        observation cutoff for maturity capping
--   HORIZON_DAYS   180                 hard cap on the day axis (bounds the spine)
-------------------------------------------------------------------------------

-- Re-run safety: uncomment if a prior run left tables behind.
-- DROP TABLE vt_vba_base;  DROP TABLE vt_vba_long;
-- DROP TABLE vt_vba_cells; DROP TABLE vt_vba_spine;


-------------------------------------------------------------------------------
-- 1. BASE — one row per (clnt_no, cohort_month). Deduped denominator.
--    A client in two waves in the same month is counted ONCE, anchored on the
--    earliest treatment; their response is the earliest response across those
--    waves. A client in two different months appears in both bins (standard).
-------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vba_base AS (
    WITH src AS (
        SELECT
            clnt_no
          , TRIM(tactic_id)                                                       AS tactic_id
          , TRIM(mnc)                                                             AS mnc
          , TRIM(control)                                                         AS arm
          , TRIM(COALESCE(test_group,'(none)'))                                   AS test_group
          , CAST(COALESCE(decile,-1) AS INTEGER)                                  AS decile
          , treatmt_strt_dt
          , CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month
          , CASE WHEN COALESCE(net_response,0) > 0 THEN 1 ELSE 0 END              AS resp_flag
          , CASE WHEN COALESCE(net_response,0) > 0 THEN response_dt END           AS resp_dt
        FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
        WHERE treatmt_strt_dt IS NOT NULL
          AND treatmt_strt_dt >= DATE '2024-01-01'          -- COHORT_FLOOR
          AND TRIM(control) IN ('Action','Control')          -- exact codes only
    )
    -- client-month roll-up: anchor, response, and the dims of the anchor row
  , rolled AS (
        SELECT
            clnt_no
          , cohort_month
          , MIN(treatmt_strt_dt)                             AS anchor_dt
          , MAX(resp_flag)                                   AS resp_flag
          , MIN(resp_dt)                                     AS resp_dt
        FROM src
        GROUP BY 1,2
    )
  , anchor_dims AS (
        SELECT clnt_no, cohort_month, mnc, arm, test_group, decile
        FROM src
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month
            ORDER BY treatmt_strt_dt, tactic_id
        ) = 1
    )
    SELECT
        r.clnt_no
      , r.cohort_month
      , r.anchor_dt
      , d.mnc
      , d.arm
      , d.test_group
      , d.decile
      , r.resp_flag
      , CASE
            WHEN r.resp_flag = 1 AND r.resp_dt IS NOT NULL
            THEN CASE WHEN r.resp_dt - r.anchor_dt < 0 THEN 0
                      ELSE r.resp_dt - r.anchor_dt END       -- clamp pre-treatment to day 0
        END                                                  AS vintage_day
    FROM rolled r
    JOIN anchor_dims d
      ON r.clnt_no = d.clnt_no AND r.cohort_month = d.cohort_month
) WITH DATA
PRIMARY INDEX (clnt_no, cohort_month)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_base COLUMN (cohort_month);
COLLECT STATISTICS ON vt_vba_base COLUMN (arm);


-------------------------------------------------------------------------------
-- 2. LONG — unpivot the dims into (slicer_dim, slicer_value).
--    First UNION ALL branch is CAST explicitly: Teradata sizes the whole
--    UNION's char columns off branch 1 and silently truncates the rest.
--    Comment out any slicer you do not want; 'overall' must stay.
-------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vba_long AS (
    SELECT CAST('overall'    AS VARCHAR(30)) AS slicer_dim
         , CAST('all'        AS VARCHAR(50)) AS slicer_value
         , cohort_month, arm, anchor_dt, clnt_no, resp_flag, vintage_day
    FROM vt_vba_base
  UNION ALL
    SELECT 'mnc',        mnc,
           cohort_month, arm, anchor_dt, clnt_no, resp_flag, vintage_day
    FROM vt_vba_base
  UNION ALL
    SELECT 'test_group', test_group,
           cohort_month, arm, anchor_dt, clnt_no, resp_flag, vintage_day
    FROM vt_vba_base
  UNION ALL
    SELECT 'decile',     CAST(decile AS VARCHAR(50)),
           cohort_month, arm, anchor_dt, clnt_no, resp_flag, vintage_day
    FROM vt_vba_base
) WITH DATA
PRIMARY INDEX (slicer_dim, slicer_value, cohort_month, arm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_long COLUMN (slicer_dim, slicer_value, cohort_month, arm);
COLLECT STATISTICS ON vt_vba_long COLUMN (vintage_day);


-------------------------------------------------------------------------------
-- 3. CELLS — denominator + per-cell horizon. Small table, gets stats, so the
--    cross join in step 5 is constrained (TDWM blocks unconstrained product joins).
-------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vba_cells AS (
    SELECT
        slicer_dim
      , slicer_value
      , cohort_month
      , arm
      , COUNT(DISTINCT clnt_no)                              AS cohort_size
      , SUM(resp_flag)                                       AS responders_total
      , SUM(CASE WHEN resp_flag = 1 AND vintage_day IS NULL
                 THEN 1 ELSE 0 END)                          AS responders_no_date
        -- MATURITY CAP (see header). Canon alternative, if you want it:
        --   MAX(treatmt_end_dt - anchor_dt)  -- requires carrying treatmt_end_dt through
      , CASE WHEN MIN(CURRENT_DATE - anchor_dt) < 0   THEN 0
             WHEN MIN(CURRENT_DATE - anchor_dt) > 180 THEN 180   -- HORIZON_DAYS
             ELSE MIN(CURRENT_DATE - anchor_dt) END          AS cohort_max_day
    FROM vt_vba_long
    GROUP BY 1,2,3,4
) WITH DATA
PRIMARY INDEX (slicer_dim, slicer_value, cohort_month, arm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_cells COLUMN (slicer_dim, slicer_value, cohort_month, arm);
COLLECT STATISTICS ON vt_vba_cells COLUMN (cohort_max_day);


-------------------------------------------------------------------------------
-- 4. SPINE — 0 .. max horizon. Materialized + stats BEFORE any cross join.
-------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vba_spine AS (
    SELECT CAST(calendar_date - DATE '2000-01-01' AS INTEGER) AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date >= DATE '2000-01-01'
      AND calendar_date <= DATE '2000-01-01' + (SELECT MAX(cohort_max_day) FROM vt_vba_cells)
) WITH DATA
PRIMARY INDEX (vintage_day)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_spine COLUMN (vintage_day);


-------------------------------------------------------------------------------
-- 5. OUTPUT — dense grid, continuous curve, cumulative counts.
--    Export this to CSV. Rates are computed in the pivot, not here.
-------------------------------------------------------------------------------
WITH daily AS (
    SELECT
        slicer_dim, slicer_value, cohort_month, arm, vintage_day
      , COUNT(DISTINCT clnt_no) AS n_responders
    FROM vt_vba_long
    WHERE resp_flag = 1
      AND vintage_day IS NOT NULL
    GROUP BY 1,2,3,4,5
)
, grid AS (
    SELECT
        c.slicer_dim, c.slicer_value, c.cohort_month, c.arm
      , s.vintage_day
      , c.cohort_size
      , c.responders_total
      , c.responders_no_date
    FROM vt_vba_cells c
    JOIN vt_vba_spine s
      ON s.vintage_day <= c.cohort_max_day
)
SELECT
    CAST('VBA' AS VARCHAR(20))                               AS campaign
  , g.slicer_dim
  , g.slicer_value
  , g.cohort_month
  , g.arm
  , g.vintage_day
  , g.cohort_size
  , COALESCE(d.n_responders,0)                               AS responders_day
  , SUM(COALESCE(d.n_responders,0)) OVER (
        PARTITION BY g.slicer_dim, g.slicer_value, g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                        AS responders_cum
  , g.responders_total                                       AS responders_flat_check
  , g.responders_no_date                                     AS responders_undated
FROM grid g
LEFT JOIN daily d
  ON  g.slicer_dim   = d.slicer_dim
  AND g.slicer_value = d.slicer_value
  AND g.cohort_month = d.cohort_month
  AND g.arm          = d.arm
  AND g.vintage_day  = d.vintage_day
ORDER BY 2,3,4,5,6;


-------------------------------------------------------------------------------
-- 6. ACCEPTANCE TEST — run this, it must return ZERO rows.
--    Terminal cum_responders per cell must equal that cell's flat responder
--    count, minus the responders that have no response_dt to place on the axis.
-------------------------------------------------------------------------------
/*
WITH daily AS (
    SELECT slicer_dim, slicer_value, cohort_month, arm, vintage_day,
           COUNT(DISTINCT clnt_no) AS n
    FROM vt_vba_long WHERE resp_flag = 1 AND vintage_day IS NOT NULL
    GROUP BY 1,2,3,4,5
)
, terminal AS (
    SELECT c.slicer_dim, c.slicer_value, c.cohort_month, c.arm
         , c.responders_total, c.responders_no_date, c.cohort_max_day
         , COALESCE(SUM(CASE WHEN d.vintage_day <= c.cohort_max_day THEN d.n END),0) AS cum_at_horizon
         , COALESCE(SUM(d.n),0)                                                      AS cum_all_days
    FROM vt_vba_cells c
    LEFT JOIN daily d
      ON c.slicer_dim=d.slicer_dim AND c.slicer_value=d.slicer_value
     AND c.cohort_month=d.cohort_month AND c.arm=d.arm
    GROUP BY 1,2,3,4,5,6,7
)
SELECT * FROM terminal
WHERE slicer_dim = 'overall'
  AND cum_all_days <> responders_total - responders_no_date;
*/
-- If it returns rows: cum_all_days <> flat count means the day axis lost
-- responders somewhere other than the undated bucket. Stop and tell me.
-- Separately, (responders_total - responders_no_date - cum_at_horizon) is the
-- tail chopped off by the maturity cap — expected, not an error.


-- DROP TABLE vt_vba_base;
-- DROP TABLE vt_vba_long;
-- DROP TABLE vt_vba_cells;
-- DROP TABLE vt_vba_spine;
