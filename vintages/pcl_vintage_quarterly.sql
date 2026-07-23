-- pcl_vintage_quarterly.sql
-- Campaign : PCL (Pre-Approved Credit Limit Increase)
-- Source   : DL_MR_PROD.cards_pli_decision_resp
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : responder_cli = 1 (CLI response flag); event date = dt_cl_change
-- Anchor   : treatmt_strt_dt (CHANGED 2026-07-22 per Andre — was treatmt_end_dt). See [VERIFY]
--          block below and run it first: treatmt_strt_dt was found unreliable in
--          DG6V01.tactic_evnt_ip_ar_hist (why end_dt anchoring existed historically);
--          unconfirmed whether the same problem exists on THIS curated table.
--          If strt_dt is unreliable here too, fall back to treatmt_end_dt minus the offer
--          window (window length unconfirmed, ~60d).
-- Grain    : account (acct_no)
-- Arm      : tst_grp_cd raw, Action/Control mapping UNCONFIRMED — arm_raw = arm (pass-through,
--          no translation applied). Run campaigns/CRV/crv_pcl_overlap_summary.sql §C4 to profile
--          codes before labelling arms. [VERIFY] arm codes unconfirmed.
-- Cohort bin: CALENDAR quarter 'YYYYQn' (Jan-Mar=Q1) of a deployment's own treatmt_strt_dt
-- Day window: 0-90
-- Denominator: one row per (acct_no, bin) = first in-bin deployment (MIN treatmt_strt_dt within
--          the bin). Arm = that deployment's arm; first-anchor wins on conflict. Quarterly
--          cohort_size <= sum of the 3 monthly cohort_sizes — gap = clients contacted in more
--          than one month of the quarter (PCL runs monthly waves, so this gap is expected).
-- Numerator: NOT deduped — every deployment gets its own success lookup. dt_cl_change already
--          lives on that same row (curated table = one row per deployment), so no cross-
--          deployment / last-touch attribution is needed here (unlike AUH/VBA/VBU, which pull
--          success from a shared raw event table). Rolls up under the client's bin arm.
--          cum_responses = cumulative SUCCESS EVENTS (one per deployment window), NOT clients —
--          this sums cleanly: quarterly cum_responses = sum of the 3 monthly files' cum_responses.
-- Sourced from: vintages/pcl_vintage_monthly.sql (prior version, treatmt_end_dt anchor)
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcl_quarterly_cells;
--   DROP TABLE vt_pcl_quarterly_spine;

-- ============================================================================
-- [VERIFY] BEFORE USE — treatmt_strt_dt reliability check on cards_pli_decision_resp. Run first.
-- ============================================================================
SELECT
    COUNT(*)                                                    AS total_rows,
    COUNT(CASE WHEN treatmt_strt_dt IS NULL THEN 1 END)          AS null_strt_dt,
    COUNT(CASE WHEN treatmt_strt_dt > treatmt_end_dt THEN 1 END) AS strt_after_end,
    MIN(treatmt_end_dt - treatmt_strt_dt)                        AS min_window_days,
    MAX(treatmt_end_dt - treatmt_strt_dt)                        AS max_window_days
FROM DL_MR_PROD.cards_pli_decision_resp
WHERE treatmt_end_dt >= DATE '2026-01-01';

-- ============================================================================
-- STEP 1: denominator cells
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_quarterly_cells AS (
    WITH bin_arm_lookup AS (
        SELECT
            acct_no,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) ||
                CASE
                    WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (1,2,3)    THEN 'Q1'
                    WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (4,5,6)    THEN 'Q2'
                    WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (7,8,9)    THEN 'Q3'
                    WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (10,11,12) THEN 'Q4'
                END
            AS VARCHAR(10))                          AS cohort,
            CAST(TRIM(tst_grp_cd) AS VARCHAR(30))     AS arm_raw,
            CAST(TRIM(tst_grp_cd) AS VARCHAR(30))     AS arm
        FROM DL_MR_PROD.cards_pli_decision_resp
        WHERE treatmt_strt_dt >= DATE '2026-01-01'
          AND TRIM(tst_grp_cd) IS NOT NULL
          AND TRIM(tst_grp_cd) <> ''
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY acct_no, cohort
            ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort, arm_raw, arm, COUNT(DISTINCT acct_no) AS cohort_size
    FROM bin_arm_lookup
    GROUP BY cohort, arm_raw, arm
) WITH DATA PRIMARY INDEX (cohort, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_quarterly_cells COLUMN (cohort, arm);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_quarterly_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_quarterly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
bin_arm_lookup AS (
    SELECT
        acct_no,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                          AS cohort,
        CAST(TRIM(tst_grp_cd) AS VARCHAR(30))     AS arm_raw,
        CAST(TRIM(tst_grp_cd) AS VARCHAR(30))     AS arm
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND TRIM(tst_grp_cd) IS NOT NULL
      AND TRIM(tst_grp_cd) <> ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY acct_no, cohort
        ORDER BY treatmt_strt_dt ASC
    ) = 1
),

-- every deployment (NOT deduped); dt_cl_change already lives on this same row
all_deployments AS (
    SELECT
        acct_no,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                          AS cohort,
        CASE WHEN responder_cli = 1
             THEN CAST(dt_cl_change - treatmt_strt_dt AS INTEGER)
        END                                      AS vintage_day_raw
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND TRIM(tst_grp_cd) IS NOT NULL
      AND TRIM(tst_grp_cd) <> ''
),

deployment_success AS (
    SELECT acct_no, cohort, vintage_day_raw AS vintage_day
    FROM all_deployments
    WHERE vintage_day_raw IS NOT NULL
),

-- roll up under the client's BIN arm (first-in-bin deployment), not this deployment's own arm
numerator_binned AS (
    SELECT bl.cohort, bl.arm_raw, bl.arm, ds.vintage_day
    FROM deployment_success ds
    INNER JOIN bin_arm_lookup bl
        ON bl.acct_no = ds.acct_no AND bl.cohort = ds.cohort
),

daily_counts AS (
    SELECT cohort, arm_raw, arm, vintage_day, COUNT(*) AS n_events
    FROM numerator_binned
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort, arm_raw, arm, vintage_day
),

dense_grid AS (
    SELECT c.cohort, c.arm_raw, c.arm, c.cohort_size, s.vintage_day
    FROM vt_pcl_quarterly_cells c
    CROSS JOIN vt_pcl_quarterly_spine s
)

SELECT
    CAST('PCL' AS VARCHAR(10)) AS campaign,
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

DROP TABLE vt_pcl_quarterly_cells;
DROP TABLE vt_pcl_quarterly_spine;
