-- crv_vintage_v1_datalab.sql
-- Campaign : CRV (Credit Card Installment Plan) — SOURCE RECONCILIATION, DATALAB SIDE
-- Source   : DL_MR_PROD.cards_crv_install_decis_resp (curated, acct-grain, responder flag)
-- Bridge   : D3CV12A.CR_CRD_RPTS_ACCT is a NEUTRAL acct->clnt identity crosswalk ONLY —
--            it carries no success signal, it just exists because the curated table has
--            no clnt_no. This file is fully self-contained: only these two Teradata tables,
--            no shared tables with crv_vintage_v2_production.sql, no cross-referencing.
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Grain    : client (clnt_no) — collapsed from acct_no via the identity bridge
-- Success  : responder = 1 (first installment activation); vintage_day = first_response_days
-- Arm      : action_control — 'Action' / 'Control'
--
-- Grain caveat: CRV randomization is per-wave, not sticky. Collapsing acct_no -> clnt_no
-- and then to (cohort_month, arm) can put the same client in both arms across different
-- waves. Flagged for a downstream multi-arm prevalence check, not solved here.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_v1_cells;
--   DROP TABLE vt_crv_v1_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size per (cohort_month, arm), client grain
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_v1_cells AS (
    WITH bridge AS (
        SELECT
            c.acct_no,
            r.clnt_no
        FROM DL_MR_PROD.cards_crv_install_decis_resp c
        JOIN D3CV12A.CR_CRD_RPTS_ACCT r
            ON r.acct_no = CAST(c.acct_no AS DECIMAL(13,0))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.ME_DT DESC) = 1
    ),
    acct_cohort AS (
        SELECT DISTINCT
            acct_no,
            (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
            TRIM(action_control)          AS arm
        FROM DL_MR_PROD.cards_crv_install_decis_resp
        WHERE offer_start_date >= DATE '2026-01-01'
          AND TRIM(action_control) IN ('Action', 'Control')
    )
    SELECT
        a.cohort_month,
        CAST(a.arm AS VARCHAR(10))       AS arm,
        COUNT(DISTINCT b.clnt_no)        AS cohort_size
    FROM acct_cohort a
    JOIN bridge b ON b.acct_no = a.acct_no
    GROUP BY a.cohort_month, a.arm
) WITH DATA PRIMARY INDEX (cohort_month, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_v1_cells COLUMN (cohort_month, arm);

-- ============================================================================
-- STEP 2: days spine 0–90
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_v1_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_v1_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- client_vintage_day = MIN(first_response_days) across responder=1 rows in the
-- (clnt_no, cohort_month, arm) cell (a client can have multiple acct_no rows
-- once bridged, e.g. joint accounts or multiple waves).
-- ============================================================================
WITH
bridge AS (
    SELECT
        c.acct_no,
        r.clnt_no
    FROM DL_MR_PROD.cards_crv_install_decis_resp c
    JOIN D3CV12A.CR_CRD_RPTS_ACCT r
        ON r.acct_no = CAST(c.acct_no AS DECIMAL(13,0))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.ME_DT DESC) = 1
),
acct_base AS (
    SELECT
        acct_no,
        (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10)) AS arm,
        responder,
        first_response_days
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
),
client_base AS (
    SELECT
        b.clnt_no,
        a.cohort_month,
        a.arm,
        MAX(a.responder)                                             AS is_responder,
        MIN(CASE WHEN a.responder = 1 THEN a.first_response_days END) AS client_vintage_day
    FROM acct_base a
    JOIN bridge b ON b.acct_no = a.acct_no
    GROUP BY b.clnt_no, a.cohort_month, a.arm
),

-- clients whose first in-cell conversion lands on this vintage_day
daily_counts AS (
    SELECT
        cohort_month,
        arm,
        client_vintage_day               AS vintage_day,
        COUNT(DISTINCT clnt_no)          AS n_events
    FROM client_base
    WHERE is_responder = 1
      AND client_vintage_day BETWEEN 0 AND 90
    GROUP BY cohort_month, arm, client_vintage_day
),

-- dense grid: cohort_month × arm × vintage_day
dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM vt_crv_v1_cells c
    CROSS JOIN vt_crv_v1_spine d
)

SELECT
    CAST('CRV' AS VARCHAR(10))                           AS campaign,
    g.cohort_month,
    g.arm,
    CAST('crv_install_conversion' AS VARCHAR(30))        AS metric,
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

DROP TABLE vt_crv_v1_cells;
DROP TABLE vt_crv_v1_spine;
