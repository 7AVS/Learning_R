-- crv_vintage_monthly_raw.sql
-- Campaign : CRV (Credit Card Installment Plan)
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Population + arm : cards_crv_install_decis_resp  (still curated — no raw arm source confirmed)
-- Success (RAW)    : cards_crv_install_details, one row per activated plan
--                    Event date = instl_txn_dt; join key = acct_no + offer_start_date
--                    vintage_day = MIN(instl_txn_dt - offer_start_date) per acct × cohort cell
--                    Replaces responder=1 from decis_resp — plan-level fact, not a curated flag
-- Anchor   : offer_start_date; cohort_month = offer_start_date truncated to first-of-month
-- Grain    : account (acct_no)
-- TODO     : install_type_ind values not confirmed. Query runs over ALL plan types.
--            If only specific install types should count, add:
--              AND d.install_type_ind IN (<confirmed values>)
--            in the raw_conversions CTE below. Ask Andre / verify via HELP TABLE.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_raw_cells;
--   DROP TABLE vt_crv_raw_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size per (cohort_month, arm)
-- Source: curated decis_resp (population + arm — no raw alternative confirmed)
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_raw_cells AS (
    WITH client_base AS (
        SELECT
            acct_no,
            (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
            TRIM(action_control)           AS arm
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
        GROUP BY
            acct_no,
            (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)),
            TRIM(action_control)
    )
    SELECT
        cohort_month,
        CAST(arm AS VARCHAR(10))         AS arm,
        COUNT(DISTINCT acct_no)          AS cohort_size
    FROM client_base
    GROUP BY cohort_month, arm
) WITH DATA PRIMARY INDEX (cohort_month, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_raw_cells COLUMN (cohort_month, arm);

-- ============================================================================
-- STEP 2: days spine 0–90
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_raw_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_raw_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve — raw plan-activation metric
-- Join decis_resp (population/arm) → install_details (raw activation event)
-- vintage_day = first instl_txn_dt - offer_start_date within the 90-day window
-- An account with multiple waves: each (offer_start_date, arm) pair is its own
-- cohort cell; we take the earliest activation date within each cell.
-- ============================================================================
WITH
client_base AS (
    SELECT
        r.acct_no,
        r.offer_start_date,
        (r.offer_start_date - (EXTRACT(DAY FROM r.offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(r.action_control) AS VARCHAR(10)) AS arm
    FROM DL_MR_PROD.cards_crv_install_decis_resp r
    WHERE r.offer_start_date >= DATE '2026-01-01'
      AND TRIM(r.action_control) IN ('Action', 'Control')
),

-- Raw conversions: first plan activation per account × offer_start_date
-- instl_txn_dt is the event date (plan activation, not a curated flag)
-- TODO: add install_type_ind filter here once values are confirmed
raw_conversions AS (
    SELECT
        d.acct_no,
        d.offer_start_date,
        MIN(CAST(d.instl_txn_dt - d.offer_start_date AS INTEGER)) AS first_activation_day
    FROM DL_MR_PROD.cards_crv_install_details d
    WHERE d.offer_start_date >= DATE '2026-01-01'
      AND d.instl_txn_dt >= d.offer_start_date
      AND CAST(d.instl_txn_dt - d.offer_start_date AS INTEGER) BETWEEN 0 AND 90
    GROUP BY d.acct_no, d.offer_start_date
),

client_with_activation AS (
    SELECT
        cb.cohort_month,
        cb.arm,
        cb.acct_no,
        MIN(rc.first_activation_day) AS first_response_day
    FROM client_base cb
    LEFT JOIN raw_conversions rc
        ON  rc.acct_no          = cb.acct_no
        AND rc.offer_start_date = cb.offer_start_date
    GROUP BY cb.cohort_month, cb.arm, cb.acct_no
),

daily_counts AS (
    SELECT
        cohort_month,
        arm,
        first_response_day              AS vintage_day,
        COUNT(DISTINCT acct_no)         AS n_events
    FROM client_with_activation
    WHERE first_response_day IS NOT NULL
    GROUP BY cohort_month, arm, first_response_day
),

dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM vt_crv_raw_cells c
    CROSS JOIN vt_crv_raw_spine d
)

SELECT
    CAST('CRV' AS VARCHAR(10))                           AS campaign,
    g.cohort_month,
    g.arm,
    CAST('plan_activation_raw' AS VARCHAR(30))           AS metric,
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

DROP TABLE vt_crv_raw_cells;
DROP TABLE vt_crv_raw_spine;
