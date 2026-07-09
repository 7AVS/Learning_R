-- crv_vintage_v1_datalab.sql
-- Campaign : CRV (Credit Card Installment Plan) — SOURCE RECONCILIATION, DATALAB SIDE
-- Source   : DL_MR_PROD.cards_crv_install_decis_resp (curated, acct-grain, responder flag)
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
--
-- REBUILD: this is the validated per-cohort summary (crv_cohort_summary_v1_datalab.sql)
-- plus a daily cumulative day-axis. Population/cohort/arm/success are IDENTICAL to the
-- summary. Two defects from the prior version of this file were removed:
--   (a) the D3CV12A.CR_CRD_RPTS_ACCT acct->clnt bridge is GONE. This file stays at
--       acct_no grain throughout, exactly like the summary. No clnt_no anywhere.
--   (b) the fixed 0..365 day spine is GONE. A CRV campaign only runs from
--       offer_start_date to offer_end_date (a short window, ~9 days) and success is
--       measured ONLY inside that window. The day axis is now bounded by the actual
--       campaign window, derived per cohort from offer_end_date - offer_start_date --
--       not a fixed horizon. Each cohort's curve terminates exactly at its own offer
--       window end (its cohort_max_day); there is no flat tail past the campaign.
--
-- Grain    : account (acct_no) — matches the summary, no bridge.
-- Success  : responder = 1 (first installment activation); vintage_day = first_response_days
-- Arm      : action_control — 'Action' / 'Control'
--
-- RECONCILIATION NOTE: at the max vintage_day (= that cell's cohort_max_day, i.e. its
-- offer window end) per (cohort_month, arm), cum_responders should equal the summary's
-- responders EXCEPT for responder=1 accounts whose first_response_days is NULL. Those
-- accounts count toward the summary's responder=1 total but have no day to land on in
-- this day-axis, so they are NOT in cum_responders here. If terminal cum_responders <
-- summary responders, that gap = responder=1 accounts with NULL first_response_days --
-- do not assume it away, use the reconciliation check query below to quantify it.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_crv_v1_cells;
--   DROP TABLE vt_crv_v1_spine;

-- ============================================================================
-- STEP 1: cohort cells — cohort_size + cohort_max_day per (cohort_month, arm),
-- ACCOUNT grain, no bridge. cohort_max_day = MAX(offer_end_date - offer_start_date)
-- across the accounts in the cell. If a cohort_month aggregates more than one offer,
-- this is the longest offer window in that cell, and it is what bounds the day axis
-- for that cell in STEP 3.
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_v1_cells AS (
    SELECT
        (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10))                     AS arm,
        COUNT(DISTINCT acct_no)                                       AS cohort_size,
        MAX(offer_end_date - offer_start_date)                        AS cohort_max_day
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
    GROUP BY cohort_month, arm
) WITH DATA PRIMARY INDEX (cohort_month, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_v1_cells COLUMN (cohort_month, arm);

-- ============================================================================
-- STEP 2: days spine 0..GLOBAL_MAX, where GLOBAL_MAX is the longest campaign window
-- across the whole filtered population -- data-derived, not hardcoded. vt_crv_v1_cells
-- already holds the per-cell max (cohort_max_day) for every (cohort_month, arm) that
-- partitions the filtered population, so GLOBAL_MAX = MAX(cohort_max_day) over that
-- small materialized table is exactly the population-wide max; no second pass over the
-- base table needed. The window is small (~9 days), so this spine is tiny.
-- TDWM: unconstrained CROSS JOIN against SYS_CALENDAR.CALENDAR blocks ("F-uncnstrm PJ");
-- materializing the spine in a VOLATILE TABLE with COLLECT STATISTICS clears it.
-- ============================================================================
CREATE VOLATILE TABLE vt_crv_v1_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '1900-01-01')
          BETWEEN 0 AND (SELECT MAX(cohort_max_day) FROM vt_crv_v1_cells)
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_crv_v1_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve — account grain throughout, no bridge
-- vintage_day = first_response_days directly off the curated table (no MIN/collapse
-- across accounts needed — one row per acct_no, no client-level fan-out from a bridge).
-- Dense grid is capped per-cell at that cell's own cohort_max_day (WHERE clause below),
-- so each cohort's curve ends exactly at its own offer_end -- no tail past the campaign.
-- ============================================================================
WITH
acct_base AS (
    SELECT
        acct_no,
        (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10))                     AS arm,
        responder,
        first_response_days
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
),

-- accounts whose first response lands on this vintage_day
daily_counts AS (
    SELECT
        cohort_month,
        arm,
        first_response_days              AS vintage_day,
        COUNT(DISTINCT acct_no)          AS n_events
    FROM acct_base
    WHERE responder = 1
      AND first_response_days IS NOT NULL
    GROUP BY cohort_month, arm, first_response_days
),

-- dense grid: cohort_month x arm x vintage_day, capped at each cell's own campaign window
dense_grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, s.vintage_day
    FROM vt_crv_v1_cells c
    CROSS JOIN vt_crv_v1_spine s
    WHERE s.vintage_day <= c.cohort_max_day
)

SELECT
    CAST('CRV' AS VARCHAR(10))                           AS campaign,
    g.cohort_month,
    g.arm,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                    AS cum_responders
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm          = g.arm
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day;

DROP TABLE vt_crv_v1_cells;
DROP TABLE vt_crv_v1_spine;

-- ============================================================================
-- OPTIONAL RECONCILIATION CHECK (diagnostic only — not part of the vintage output)
-- Per (cohort_month, arm): the summary's responder=1 count, split by whether
-- first_response_days is NULL or not. Account grain, no bridge. Use this to see
-- whether NULL-day responders explain any gap between this file's terminal
-- cum_responders and crv_cohort_summary_v1_datalab.sql's responders.
-- ============================================================================
WITH
acct_base_chk AS (
    SELECT
        acct_no,
        (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10))                     AS arm,
        responder,
        first_response_days
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
)
SELECT
    cohort_month,
    arm,
    COUNT(DISTINCT CASE WHEN responder = 1 THEN acct_no END)                                  AS summary_responders,
    COUNT(DISTINCT CASE WHEN responder = 1 AND first_response_days IS NULL THEN acct_no END)     AS responders_null_day,
    COUNT(DISTINCT CASE WHEN responder = 1 AND first_response_days IS NOT NULL THEN acct_no END) AS responders_with_day
FROM acct_base_chk
GROUP BY cohort_month, arm
ORDER BY cohort_month, arm;
