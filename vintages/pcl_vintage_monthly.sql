-- pcl_vintage_monthly.sql
-- Campaign : PCL (Pre-Approved Credit Limit Increase)
-- Source   : DL_MR_PROD.cards_pli_decision_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : responder_cli = 1 (CLI response flag); event date = dt_cl_change
-- Anchor   : treatmt_end_dt (PCL design: strt_dt is unreliable; vintage_day counts forward from end_dt)
-- Grain    : account (acct_no); clnt_no present but arm split on tst_grp_cd (raw codes, unconfirmed)
--
-- ARM NOTE: tst_grp_cd Action/Control codes for PCL are unconfirmed (no lookup available).
--   Raw tst_grp_cd values are carried through. Run the discovery query in
--   campaigns/CRV/crv_pcl_overlap_summary.sql §C4 to profile codes before labelling arms.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcl_monthly_cells;
--   DROP TABLE vt_pcl_monthly_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size per (cohort_month, arm)
-- arm = raw tst_grp_cd (unconfirmed Action/Control mapping — carry raw codes)
-- cohort_month = treatmt_end_dt truncated to first-of-month (PCL anchor)
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_monthly_cells AS (
    WITH client_base AS (
        SELECT
            acct_no,
            CAST(
                CAST(YEAR(treatmt_end_dt) AS CHAR(4))
                || '-'
                || TRIM(CAST(MONTH(treatmt_end_dt) AS CHAR(2)) (FORMAT 'Z9'))
                || '-01'
                AS DATE FORMAT 'YYYY-MM-DD'
            )                              AS cohort_month,
            TRIM(tst_grp_cd)              AS arm
        FROM DL_MR_PROD.cards_pli_decision_resp
        WHERE treatmt_end_dt >= DATE '2026-01-01'
        GROUP BY
            acct_no,
            CAST(
                CAST(YEAR(treatmt_end_dt) AS CHAR(4))
                || '-'
                || TRIM(CAST(MONTH(treatmt_end_dt) AS CHAR(2)) (FORMAT 'Z9'))
                || '-01'
                AS DATE FORMAT 'YYYY-MM-DD'
            ),
            TRIM(tst_grp_cd)
    )
    SELECT
        cohort_month,
        CAST(arm AS VARCHAR(20))         AS arm,
        COUNT(DISTINCT acct_no)          AS cohort_size
    FROM client_base
    WHERE arm IS NOT NULL
      AND arm <> ''
    GROUP BY cohort_month, arm
) WITH DATA PRIMARY INDEX (cohort_month, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_monthly_cells COLUMN (cohort_month, arm);

-- ============================================================================
-- STEP 2: days spine 0–90
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_monthly_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_monthly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve — responder_cli metric
-- vintage_day = dt_cl_change - treatmt_end_dt (forward-counting from anchor)
-- ============================================================================
WITH
client_base AS (
    SELECT
        acct_no,
        CAST(
            CAST(YEAR(treatmt_end_dt) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(treatmt_end_dt) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        )                              AS cohort_month,
        CAST(TRIM(tst_grp_cd) AS VARCHAR(20)) AS arm,
        -- responder_cli = 1 when CLI accepted; event date is dt_cl_change
        CASE WHEN responder_cli = 1
             THEN CAST(dt_cl_change - treatmt_end_dt AS INTEGER)
        END                            AS first_response_day
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE treatmt_end_dt >= DATE '2026-01-01'
      AND TRIM(tst_grp_cd) IS NOT NULL
      AND TRIM(tst_grp_cd) <> ''
    GROUP BY
        acct_no,
        CAST(
            CAST(YEAR(treatmt_end_dt) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(treatmt_end_dt) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        ),
        CAST(TRIM(tst_grp_cd) AS VARCHAR(20)),
        CASE WHEN responder_cli = 1
             THEN CAST(dt_cl_change - treatmt_end_dt AS INTEGER)
        END
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
    FROM vt_pcl_monthly_cells c
    CROSS JOIN vt_pcl_monthly_spine d
)

SELECT
    CAST('PCL' AS VARCHAR(10))                           AS campaign,
    g.cohort_month,
    g.arm,
    CAST('responder_cli' AS VARCHAR(20))                 AS metric,
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

DROP TABLE vt_pcl_monthly_cells;
DROP TABLE vt_pcl_monthly_spine;
