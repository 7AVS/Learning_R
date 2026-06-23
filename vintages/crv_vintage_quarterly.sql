-- crv_vintage_quarterly.sql
-- Campaign : CRV (Credit Card Installment Plan)
-- Source   : DL_MR_PROD.cards_crv_install_decis_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : responder = 1 (first installment activation); event date = first_response_date
-- Anchor   : offer_start_date; cohort_quarter = offer_start_date truncated to first-of-(CALENDAR)-quarter
-- Grain    : account (acct_no); clnt_no not confirmed on this table in current schema
-- Arm field: action_control — raw values 'Action' / 'Control' (confirmed from queries)
-- NOTE     : CALENDAR quarter (Jan-Mar = Q1). For RBC FISCAL quarter (FY starts Nov),
--            shift the anchor by +2 months before truncating — one-line change, ask if needed.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_quarterly_cells;
--   DROP TABLE vt_crv_quarterly_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size per (cohort_quarter, arm)
-- first-of-quarter = first-of-month, backed up by (month-1) MOD 3 months.
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_quarterly_cells AS (
    WITH client_base AS (
        SELECT
            acct_no,
            ADD_MONTHS(
                (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)),
                -((EXTRACT(MONTH FROM offer_start_date) - 1) MOD 3)
            )                              AS cohort_quarter,
            TRIM(action_control)           AS arm
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        GROUP BY
            acct_no,
            ADD_MONTHS(
                (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)),
                -((EXTRACT(MONTH FROM offer_start_date) - 1) MOD 3)
            ),
            TRIM(action_control)
    )
    SELECT
        cohort_quarter,
        CAST(arm AS VARCHAR(10))         AS arm,
        COUNT(DISTINCT acct_no)          AS cohort_size
    FROM client_base
    GROUP BY cohort_quarter, arm
) WITH DATA PRIMARY INDEX (cohort_quarter, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_quarterly_cells COLUMN (cohort_quarter, arm);

-- ============================================================================
-- STEP 2: days spine 0–90
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_quarterly_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_quarterly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve — responder metric
-- vintage_day = first_response_date - offer_start_date; first response per account per cell.
-- ============================================================================
WITH
client_base AS (
    SELECT
        acct_no,
        ADD_MONTHS(
            (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)),
            -((EXTRACT(MONTH FROM offer_start_date) - 1) MOD 3)
        )                              AS cohort_quarter,
        CAST(TRIM(action_control) AS VARCHAR(10)) AS arm,
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
        ADD_MONTHS(
            (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)),
            -((EXTRACT(MONTH FROM offer_start_date) - 1) MOD 3)
        ),
        CAST(TRIM(action_control) AS VARCHAR(10))
),

daily_counts AS (
    SELECT
        cohort_quarter,
        arm,
        first_response_day              AS vintage_day,
        COUNT(DISTINCT acct_no)         AS n_events
    FROM client_base
    WHERE first_response_day IS NOT NULL
      AND first_response_day BETWEEN 0 AND 90
    GROUP BY cohort_quarter, arm, first_response_day
),

dense_grid AS (
    SELECT c.cohort_quarter, c.arm, c.cohort_size, d.vintage_day
    FROM vt_crv_quarterly_cells c
    CROSS JOIN vt_crv_quarterly_spine d
)

SELECT
    CAST('CRV' AS VARCHAR(10))                           AS campaign,
    g.cohort_quarter,
    g.arm,
    CAST('responder' AS VARCHAR(20))                     AS metric,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_quarter, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                    AS cum_events
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_quarter = g.cohort_quarter
    AND dc.arm            = g.arm
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_quarter, g.arm, g.vintage_day;

DROP TABLE vt_crv_quarterly_cells;
DROP TABLE vt_crv_quarterly_spine;
