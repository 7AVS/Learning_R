-- crv_vintage_monthly.sql
-- Campaign : CRV (Credit Card Installment Plan)
-- Source   : DL_MR_PROD.cards_crv_install_decis_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : responder = 1 (first installment activation); event date = first_response_date
-- Anchor   : offer_start_date; cohort_month = offer_start_date truncated to first-of-month
-- Grain    : account (acct_no); clnt_no not confirmed on this table in current schema
-- Arm field: action_control — raw values 'Action' / 'Control' (confirmed from queries)
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_monthly_cells;
--   DROP TABLE vt_crv_monthly_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size per (cohort_month, arm)
-- One row per account per (cohort_month, arm): an account may appear in
-- multiple waves; each (offer_start_date, action_control) pair is a separate
-- tactic record. We count distinct acct_no within each cell.
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_monthly_cells AS (
    WITH client_base AS (
        SELECT
            acct_no,
            CAST(
                CAST(YEAR(offer_start_date) AS CHAR(4))
                || '-'
                || TRIM(CAST(MONTH(offer_start_date) AS CHAR(2)) (FORMAT 'Z9'))
                || '-01'
                AS DATE FORMAT 'YYYY-MM-DD'
            )                              AS cohort_month,
            TRIM(action_control)           AS arm
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        GROUP BY
            acct_no,
            CAST(
                CAST(YEAR(offer_start_date) AS CHAR(4))
                || '-'
                || TRIM(CAST(MONTH(offer_start_date) AS CHAR(2)) (FORMAT 'Z9'))
                || '-01'
                AS DATE FORMAT 'YYYY-MM-DD'
            ),
            TRIM(action_control)
    )
    SELECT
        cohort_month,
        CAST(arm AS VARCHAR(10))         AS arm,
        COUNT(DISTINCT acct_no)          AS cohort_size
    FROM client_base
    GROUP BY cohort_month, arm
) WITH DATA PRIMARY INDEX (cohort_month, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_monthly_cells COLUMN (cohort_month, arm);

-- ============================================================================
-- STEP 2: days spine 0–90
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_monthly_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_monthly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve — responder metric
-- vintage_day = first_response_date - offer_start_date
-- An account with multiple waves may convert on multiple; we take the first
-- response within each (cohort_month, arm) cell.
-- ============================================================================
WITH
client_base AS (
    SELECT
        acct_no,
        CAST(
            CAST(YEAR(offer_start_date) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(offer_start_date) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        )                              AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10)) AS arm,
        -- earliest converting wave for this account in this cohort_month × arm cell
        MIN(
            CASE WHEN responder = 1
                 THEN CAST(first_response_date - offer_start_date AS INTEGER)
            END
        )                              AS first_response_day
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
    GROUP BY
        acct_no,
        CAST(
            CAST(YEAR(offer_start_date) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(offer_start_date) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        ),
        CAST(TRIM(action_control) AS VARCHAR(10))
),

-- daily event counts: accounts whose first_response_day = this vintage_day
daily_counts AS (
    SELECT
        cohort_month,
        arm,
        first_response_day              AS vintage_day,
        COUNT(DISTINCT acct_no)         AS n_events
    FROM client_base
    WHERE first_response_day IS NOT NULL
      AND first_response_day BETWEEN 0 AND 90
    GROUP BY cohort_month, arm, first_response_day
),

-- dense grid: cohort_month × arm × vintage_day
dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM vt_crv_monthly_cells c
    CROSS JOIN vt_crv_monthly_spine d
)

SELECT
    CAST('CRV' AS VARCHAR(10))                           AS campaign,
    g.cohort_month,
    g.arm,
    CAST('responder' AS VARCHAR(20))                     AS metric,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                    AS cum_events
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm          = g.arm
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day;

DROP TABLE vt_crv_monthly_cells;
DROP TABLE vt_crv_monthly_spine;
