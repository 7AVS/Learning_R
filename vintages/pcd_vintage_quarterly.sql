-- pcd_vintage_quarterly.sql
-- Campaign : PCD (Product Card Upgrade — Async Banner)
-- Source   : dl_mr_prod.cards_pcd_ongoing_decis_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : responder_targetproduct = 1; event date = dt_prod_change
-- Anchor   : response_start = TREATMENT WINDOW START in this table (verified 2026-07-22:
--          schemas/pcd_curated_schemas.md line 191 "PCD exposure window = `response_start` ->
--          `response_end`"; schemas/nbo_pba_curated_schemas.md line 210 cross-table mapping
--          "Treatment window | `response_start` / `response_end` | `treatmt_strt_dt` /
--          `treatmt_end_dt`" — response_start IS PCD's own name for what other curated tables
--          call treatmt_strt_dt, not a response-side field), per deployment
-- Grain    : client (clnt_no)
-- Arm      : test_groups_period suffix — '%T' -> Action, '%C' -> Control (relabeled from the
--          source's own TEST/CONTROL derivation to the standard Action/Control vocabulary)
-- Population filter: tactic_id_parent = '2026111PCD'
-- Cohort bin: CALENDAR quarter 'YYYYQn' (Jan-Mar=Q1) of a deployment's own response_start
-- Day window: 0-60 (canon window, per async_banner_vintage_success.sql's vintage_days spine.
--          REVERTED 2026-07-22 review — was extended to 90 in the first pass; cross-campaign
--          comparability was not requested, canon windows stand as-is)
-- Denominator: one row per (clnt_no, bin) = first in-bin deployment (MIN response_start within
--          the bin). Arm = that deployment's arm; first-anchor wins on conflict. Quarterly
--          cohort_size <= sum of the 3 monthly cohort_sizes — gap = clients contacted in more
--          than one month of the quarter.
-- Numerator: NOT deduped — every deployment gets its own success lookup. dt_prod_change already
--          lives on that same row (curated table = one row per deployment), so no cross-
--          deployment / last-touch attribution is needed here. Rolls up under the client's bin
--          arm. cum_responses = cumulative SUCCESS EVENTS (one per deployment window), NOT
--          clients — sums cleanly: quarterly cum_responses = sum of the 3 monthly files'
--          cum_responses.
-- Sourced from: campaigns/PCD/async_banner_vintage_success.sql BLOCK 1 (PCD), secondary/upgrade
--          metrics and segment slicer dropped here per the simple-version spec
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcd_quarterly_cells;
--   DROP TABLE vt_pcd_quarterly_spine;

-- ============================================================================
-- STEP 1: denominator cells
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_quarterly_cells AS (
    WITH bin_arm_lookup AS (
        SELECT
            clnt_no,
            CAST(
                CAST(EXTRACT(YEAR FROM response_start) AS VARCHAR(4)) ||
                CASE
                    WHEN EXTRACT(MONTH FROM response_start) IN (1,2,3)    THEN 'Q1'
                    WHEN EXTRACT(MONTH FROM response_start) IN (4,5,6)    THEN 'Q2'
                    WHEN EXTRACT(MONTH FROM response_start) IN (7,8,9)    THEN 'Q3'
                    WHEN EXTRACT(MONTH FROM response_start) IN (10,11,12) THEN 'Q4'
                END
            AS VARCHAR(10))                                AS cohort,
            CAST(TRIM(test_groups_period) AS VARCHAR(30))   AS arm_raw,
            CASE
                WHEN TRIM(test_groups_period) LIKE '%T' THEN CAST('Action'  AS VARCHAR(30))
                WHEN TRIM(test_groups_period) LIKE '%C' THEN CAST('Control' AS VARCHAR(30))
            END                                              AS arm
        FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
        WHERE tactic_id_parent = '2026111PCD'
          AND response_start   >= DATE '2026-01-01'
          AND (   TRIM(test_groups_period) LIKE '%T'
               OR TRIM(test_groups_period) LIKE '%C'
              )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort
            ORDER BY response_start ASC
        ) = 1
    )
    SELECT cohort, arm_raw, arm, COUNT(DISTINCT clnt_no) AS cohort_size
    FROM bin_arm_lookup
    WHERE arm IS NOT NULL
    GROUP BY cohort, arm_raw, arm
) WITH DATA PRIMARY INDEX (cohort, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_quarterly_cells COLUMN (cohort, arm);

-- ============================================================================
-- STEP 2: day spine 0-60
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_quarterly_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 60
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_quarterly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
bin_arm_lookup AS (
    SELECT
        clnt_no,
        CAST(
            CAST(EXTRACT(YEAR FROM response_start) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM response_start) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM response_start) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM response_start) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM response_start) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                                AS cohort,
        CAST(TRIM(test_groups_period) AS VARCHAR(30))   AS arm_raw,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%T' THEN CAST('Action'  AS VARCHAR(30))
            WHEN TRIM(test_groups_period) LIKE '%C' THEN CAST('Control' AS VARCHAR(30))
        END                                              AS arm
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start   >= DATE '2026-01-01'
      AND (   TRIM(test_groups_period) LIKE '%T'
           OR TRIM(test_groups_period) LIKE '%C'
          )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort
        ORDER BY response_start ASC
    ) = 1
),

-- every deployment (NOT deduped); collapse duplicate rows for the exact same wave to one
-- vintage_day via MIN (a deployment can carry >1 dt_prod_change row, e.g. multiple products)
all_deployments_raw AS (
    SELECT
        clnt_no,
        response_start,
        CAST(
            CAST(EXTRACT(YEAR FROM response_start) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM response_start) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM response_start) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM response_start) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM response_start) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                                AS cohort,
        CASE WHEN responder_targetproduct = 1 AND dt_prod_change IS NOT NULL
             THEN CAST(dt_prod_change - response_start AS INTEGER)
        END                                              AS vintage_day_raw
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start   >= DATE '2026-01-01'
      AND (   TRIM(test_groups_period) LIKE '%T'
           OR TRIM(test_groups_period) LIKE '%C'
          )
),

all_deployments AS (
    SELECT clnt_no, response_start, cohort, MIN(vintage_day_raw) AS vintage_day_raw
    FROM all_deployments_raw
    GROUP BY clnt_no, response_start, cohort
),

deployment_success AS (
    SELECT clnt_no, cohort, vintage_day_raw AS vintage_day
    FROM all_deployments
    WHERE vintage_day_raw IS NOT NULL
),

-- roll up under the client's BIN arm (first-in-bin deployment), not this deployment's own arm
numerator_binned AS (
    SELECT bl.cohort, bl.arm_raw, bl.arm, ds.vintage_day
    FROM deployment_success ds
    INNER JOIN bin_arm_lookup bl
        ON bl.clnt_no = ds.clnt_no AND bl.cohort = ds.cohort
),

daily_counts AS (
    SELECT cohort, arm_raw, arm, vintage_day, COUNT(*) AS n_events
    FROM numerator_binned
    WHERE vintage_day BETWEEN 0 AND 60
    GROUP BY cohort, arm_raw, arm, vintage_day
),

dense_grid AS (
    SELECT c.cohort, c.arm_raw, c.arm, c.cohort_size, s.vintage_day
    FROM vt_pcd_quarterly_cells c
    CROSS JOIN vt_pcd_quarterly_spine s
)

SELECT
    CAST('PCD' AS VARCHAR(10)) AS campaign,
    g.cohort,
    g.arm_raw,
    g.arm,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort, g.arm_raw, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_responses
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort      = g.cohort
    AND dc.arm_raw     = g.arm_raw
    AND dc.arm         = g.arm
    AND dc.vintage_day = g.vintage_day
ORDER BY g.cohort, g.arm, g.vintage_day;

DROP TABLE vt_pcd_quarterly_cells;
DROP TABLE vt_pcd_quarterly_spine;
