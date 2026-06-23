-- pcq_vintage_monthly.sql
-- Campaign : PCQ Modal Sales (MS)
-- Source   : campaigns/PCQ/modal_sales/pcq_ms_vintage.sql
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : DL_MR_PROD.cards_tpa_pcq_decision_resp — app_approved=1 AND asc_on_app_source='Period-ASC'
--            (Period-ASC gates NUMERATOR only; denominator = all targeted clients in the arm)
--            Anchor: days_to_respond (pre-computed field in curated table)
--            Primary metric: 'approved' (app_approved=1); drop 'completed' per simplification spec
-- Grain    : client (clnt_no)
-- Arm field: test_group_latest — 'NG3_CHMP'→champion, IN('NG3_CHLN','NG3_CHLG')→challenger
-- Population filters: decsn_year=2026, tpa_ita='TPA' (mandatory — PCQ has no ITA arm)
-- Cohort   : calendar month of treatmt_start_dt, Jan 2026 onward
-- Window   : 0–90 vintage days (source was dynamic max; fixed at 90 for standard output)
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcq_monthly_cells;
--   DROP TABLE vt_pcq_monthly_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size per (cohort_month, arm)
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_monthly_cells AS (
    WITH client_base AS (
        SELECT
            clnt_no,
            CAST(
                CAST(YEAR(treatmt_start_dt) AS CHAR(4))
                || '-'
                || TRIM(CAST(MONTH(treatmt_start_dt) AS CHAR(2)) (FORMAT 'Z9'))
                || '-01'
                AS DATE FORMAT 'YYYY-MM-DD'
            )                                              AS cohort_month,
            CASE
                WHEN TRIM(test_group_latest) = 'NG3_CHMP'
                    THEN 'champion'
                WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG')
                    THEN 'challenger'
            END                                            AS arm
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE decsn_year       = 2026
          AND tpa_ita          = 'TPA'
          AND treatmt_start_dt >= DATE '2026-01-01'
          AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
        GROUP BY clnt_no,
                 CAST(
                     CAST(YEAR(treatmt_start_dt) AS CHAR(4))
                     || '-'
                     || TRIM(CAST(MONTH(treatmt_start_dt) AS CHAR(2)) (FORMAT 'Z9'))
                     || '-01'
                     AS DATE FORMAT 'YYYY-MM-DD'
                 ),
                 CASE
                     WHEN TRIM(test_group_latest) = 'NG3_CHMP'  THEN 'champion'
                     WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN 'challenger'
                 END
    )
    SELECT
        cohort_month,
        CAST(arm AS VARCHAR(20)) AS arm,
        COUNT(DISTINCT clnt_no)  AS cohort_size
    FROM client_base
    WHERE arm IS NOT NULL
    GROUP BY cohort_month, arm
) WITH DATA PRIMARY INDEX (cohort_month, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_monthly_cells COLUMN (cohort_month, arm);

-- ============================================================================
-- STEP 2: days spine 0–90
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_monthly_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_monthly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve — approved metric only, Period-ASC numerator gating
-- ============================================================================
WITH
client_base AS (
    SELECT
        clnt_no,
        CAST(
            CAST(YEAR(treatmt_start_dt) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(treatmt_start_dt) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        )                                              AS cohort_month,
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'
                THEN 'champion'
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG')
                THEN 'challenger'
        END                                            AS arm,
        -- first approved event in the Period-ASC window (numerator gate)
        MIN(
            CASE
                WHEN app_approved = 1
                 AND TRIM(asc_on_app_source) = 'Period-ASC'
                THEN days_to_respond
            END
        )                                              AS first_approved_day
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE decsn_year       = 2026
      AND tpa_ita          = 'TPA'
      AND treatmt_start_dt >= DATE '2026-01-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    GROUP BY
        clnt_no,
        CAST(
            CAST(YEAR(treatmt_start_dt) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(treatmt_start_dt) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        ),
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'                     THEN 'champion'
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG')      THEN 'challenger'
        END
),

-- daily event counts: clients whose first_approved_day = this vintage_day
daily_counts AS (
    SELECT
        cohort_month,
        CAST(arm AS VARCHAR(20))        AS arm,
        first_approved_day              AS vintage_day,
        COUNT(DISTINCT clnt_no)         AS n_events
    FROM client_base
    WHERE arm IS NOT NULL
      AND first_approved_day IS NOT NULL
      AND first_approved_day BETWEEN 0 AND 90
    GROUP BY cohort_month, arm, first_approved_day
),

-- dense grid: cohort_month × arm × vintage_day
dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM vt_pcq_monthly_cells c
    CROSS JOIN vt_pcq_monthly_spine d
)

SELECT
    CAST('PCQ' AS VARCHAR(10))                          AS campaign,
    g.cohort_month,
    g.arm,
    CAST('approved' AS VARCHAR(20))                     AS metric,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                   AS cum_events
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm          = g.arm
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day;

DROP TABLE vt_pcq_monthly_cells;
DROP TABLE vt_pcq_monthly_spine;
