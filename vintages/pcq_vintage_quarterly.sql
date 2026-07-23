-- pcq_vintage_quarterly.sql
-- Campaign : PCQ Modal Sales (MS)
-- Source   : DL_MR_PROD.cards_tpa_pcq_decision_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : app_approved = 1 AND asc_on_app_source = 'Period-ASC' (Period-ASC gates the
--          NUMERATOR only); event day = days_to_respond (precomputed on the same row)
-- Anchor   : treatmt_start_dt (treatment start), per deployment
-- Grain    : client (clnt_no)
-- Arm      : test_group_latest — 'NG3_CHMP' -> Champion; IN ('NG3_CHLN','NG3_CHLG') ->
--          Challenger (PCQ is the one campaign using Champion/Challenger, not Action/Control)
-- Population filters: decsn_year=2026, tpa_ita='TPA' (mandatory — PCQ has no ITA arm)
-- Cohort bin: CALENDAR quarter 'YYYYQn' (Jan-Mar=Q1) of a deployment's own treatmt_start_dt
-- Day window: 0-90
-- Denominator: one row per (clnt_no, bin) = first in-bin deployment (MIN treatmt_start_dt within
--          the bin). Arm = that deployment's arm; first-anchor wins on conflict. PCQ deploys
--          monthly, so quarterly cohort_size <= sum of the 3 monthly cohort_sizes is expected —
--          gap = clients contacted in more than one month of the quarter.
-- Numerator: NOT deduped — every deployment gets its own success lookup. days_to_respond already
--          lives on that same row (curated table = one row per deployment/wave), so no cross-
--          deployment / last-touch attribution is needed here. Rolls up under the client's bin
--          arm. cum_responses = cumulative SUCCESS EVENTS (one per deployment window), NOT
--          clients — sums cleanly: quarterly cum_responses = sum of the 3 monthly files'
--          cum_responses.
-- Sourced from: campaigns/sales_modal/pcq/pcq_ms_vintage.sql (production, decile-scope slicer
--          dropped here per the simple-version spec — single metric, no slicers)
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcq_quarterly_cells;
--   DROP TABLE vt_pcq_quarterly_spine;

-- ============================================================================
-- STEP 1: denominator cells
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_quarterly_cells AS (
    WITH bin_arm_lookup AS (
        SELECT
            clnt_no,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) ||
                CASE
                    WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (1,2,3)    THEN 'Q1'
                    WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (4,5,6)    THEN 'Q2'
                    WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (7,8,9)    THEN 'Q3'
                    WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (10,11,12) THEN 'Q4'
                END
            AS VARCHAR(10))                                        AS cohort,
            CAST(TRIM(test_group_latest) AS VARCHAR(30))            AS arm_raw,
            CASE
                WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(30))
                WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(30))
            END                                                      AS arm
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE decsn_year       = 2026
          AND tpa_ita          = 'TPA'
          AND treatmt_start_dt >= DATE '2026-01-01'
          AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort
            ORDER BY treatmt_start_dt ASC
        ) = 1
    )
    SELECT cohort, arm_raw, arm, COUNT(DISTINCT clnt_no) AS cohort_size
    FROM bin_arm_lookup
    GROUP BY cohort, arm_raw, arm
) WITH DATA PRIMARY INDEX (cohort, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_quarterly_cells COLUMN (cohort, arm);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_quarterly_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_quarterly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
bin_arm_lookup AS (
    SELECT
        clnt_no,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                                        AS cohort,
        CAST(TRIM(test_group_latest) AS VARCHAR(30))            AS arm_raw,
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN CAST('Champion'   AS VARCHAR(30))
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN CAST('Challenger' AS VARCHAR(30))
        END                                                      AS arm
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE decsn_year       = 2026
      AND tpa_ita          = 'TPA'
      AND treatmt_start_dt >= DATE '2026-01-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort
        ORDER BY treatmt_start_dt ASC
    ) = 1
),

-- every deployment (NOT deduped); collapse any duplicate rows for the exact same wave first
all_deployments_raw AS (
    SELECT
        clnt_no,
        treatmt_start_dt,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_start_dt) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM treatmt_start_dt) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                                         AS cohort,
        CASE
            WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC'
            THEN days_to_respond
        END                                                      AS vintage_day_raw
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE decsn_year       = 2026
      AND tpa_ita          = 'TPA'
      AND treatmt_start_dt >= DATE '2026-01-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
),

all_deployments AS (
    SELECT clnt_no, treatmt_start_dt, cohort, MIN(vintage_day_raw) AS vintage_day_raw
    FROM all_deployments_raw
    GROUP BY clnt_no, treatmt_start_dt, cohort
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
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort, arm_raw, arm, vintage_day
),

dense_grid AS (
    SELECT c.cohort, c.arm_raw, c.arm, c.cohort_size, s.vintage_day
    FROM vt_pcq_quarterly_cells c
    CROSS JOIN vt_pcq_quarterly_spine s
)

SELECT
    CAST('PCQ' AS VARCHAR(10)) AS campaign,
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

DROP TABLE vt_pcq_quarterly_cells;
DROP TABLE vt_pcq_quarterly_spine;
