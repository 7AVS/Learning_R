-- auh_vintage_quarterly.sql
-- Campaign : AUH (Authorized Users)
-- Source   : DG6V01.tactic_evnt_ip_ar_hist (population) + D3CV12A.CR_CRD_ACCT_EVNT_DLY joined
--          to D3CV12A.DLY_FULL_PORTFOLIO (RAW success event table — no deployment key on the
--          event table itself)
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance)
-- Success  : dtl_evnt_typ_cd=191 AND ADD_RELTN_CD=3 (authorized user actually added); event date
--          = evnt_dt; first add to ANY product within a deployment's own treatment window
-- Anchor   : treatmt_strt_dt (treatment start), per deployment
-- Grain    : account. Matches auh_vintage_reconstructed.sql's OWN `acct_no` alias exactly —
--          that file's cohort CTE defines `CAST(TACTIC_EVNT_ID AS BIGINT) AS acct_no`, and its
--          pop_d/pop_o CTEs count `COUNT(DISTINCT acct_no)`. This "acct_no" is the tactic event's
--          own surrogate id (TACTIC_EVNT_ID), not a portfolio account number — the canon file
--          renames it acct_no itself; this file does the same, for consistency, not invention.
--          Type alignment (2026-07-22 review): the last-touch join between this BIGINT-cast
--          acct_no and the raw event table's native acct_no (CR_CRD_ACCT_EVNT_DLY) now casts
--          the raw side explicitly (`CAST(e.acct_no AS BIGINT)`) — auh_vintage_reconstructed.sql
--          leaves the equivalent comparison implicit and lets Teradata coerce it; made explicit
--          here, same net comparison.
-- Arm      : tst_grp_cd — RIGHT(TRIM(tst_grp_cd),2)='_C' -> Control, ELSE -> Action.
--          [VERIFY] the `_C` suffix = Control convention is an UNCONFIRMED WORKING ASSUMPTION,
--          not settled fact, for BOTH AUH phases (2026042AUH and 2026119AUH). Daniel Chin's
--          Phase 1 tracking doc uses `_C` this way, and Phase 2 codes seen in the wild
--          (NRW_C, RORMC2_C) appear to follow the same pattern, but Robin Ji's Phase 2 email
--          (2026-05-14) confirmed the TST_GRP_CD prefix-to-arm mapping WITHOUT explicitly
--          confirming `_C` = Control. Treat as a TEMP label until confirmed — same caveat
--          class as PCL/VBA/VBU's unconfirmed arm derivations.
-- Population filter: tactic_id IN ('2026042AUH','2026119AUH')
-- Cohort bin: CALENDAR quarter 'YYYYQn' (Jan-Mar=Q1) of a deployment's own treatmt_strt_dt
-- Day window: 0-30 (canon window, per auh_vintage_reconstructed.sql's vintage_days spine.
--          REVERTED 2026-07-22 review — was extended to 90 in the first pass; cross-campaign
--          comparability was not requested, canon windows stand as-is)
-- Denominator: one row per (acct_no, bin) = first in-bin deployment (MIN treatmt_strt_dt within
--          the bin). Arm = that deployment's arm; first-anchor wins on conflict. Strategy_arm /
--          model_arm slicers dropped per the simple-version spec (single metric, no slicers).
--          The 2 AUH tactics are one-time deployments each, but if a client falls in BOTH within
--          the same quarter, quarterly cohort_size <= sum of the 3 monthly cohort_sizes applies.
-- Numerator: NOT deduped — every deployment gets its own success lookup, one success max per
--          deployment window. The event table (CR_CRD_ACCT_EVNT_DLY) carries no deployment key,
--          so an add event that falls inside TWO overlapping deployment windows for the same
--          account is attributed via LAST-TOUCH: the most recent deployment start on/before the
--          event date wins (touch_rank=1 below). At most one success counted per deployment.
--          Rolls up under the client's bin arm. cum_responses = cumulative SUCCESS EVENTS (one
--          per deployment window), NOT clients — sums cleanly: quarterly cum_responses = sum of
--          the 3 monthly files' cum_responses.
-- Sourced from: campaigns/AUH/auh_vintage_reconstructed.sql
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_auh_quarterly_cells;
--   DROP TABLE vt_auh_quarterly_spine;

-- ============================================================================
-- STEP 1: denominator cells
-- ============================================================================
CREATE VOLATILE TABLE vt_auh_quarterly_cells AS (
    WITH bin_arm_lookup AS (
        SELECT
            CAST(tactic_evnt_id AS BIGINT) AS acct_no,
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
            CASE WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(30))
                 ELSE CAST('Action' AS VARCHAR(30)) END AS arm
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE tactic_id IN ('2026042AUH', '2026119AUH')
          AND treatmt_strt_dt >= DATE '2026-01-01'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(tactic_evnt_id AS BIGINT), cohort
            ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort, arm_raw, arm, COUNT(DISTINCT acct_no) AS cohort_size
    FROM bin_arm_lookup
    GROUP BY cohort, arm_raw, arm
) WITH DATA PRIMARY INDEX (cohort, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_auh_quarterly_cells COLUMN (cohort, arm);

-- ============================================================================
-- STEP 2: day spine 0-30
-- ============================================================================
CREATE VOLATILE TABLE vt_auh_quarterly_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 30
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_auh_quarterly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
bin_arm_lookup AS (
    SELECT
        CAST(tactic_evnt_id AS BIGINT) AS acct_no,
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
        CASE WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(30))
             ELSE CAST('Action' AS VARCHAR(30)) END AS arm
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH', '2026119AUH')
      AND treatmt_strt_dt >= DATE '2026-01-01'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(tactic_evnt_id AS BIGINT), cohort
        ORDER BY treatmt_strt_dt ASC
    ) = 1
),

-- every deployment (NOT deduped)
all_deployments AS (
    SELECT
        CAST(tactic_evnt_id AS BIGINT) AS acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) ||
            CASE
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (1,2,3)    THEN 'Q1'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (4,5,6)    THEN 'Q2'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (7,8,9)    THEN 'Q3'
                WHEN EXTRACT(MONTH FROM treatmt_strt_dt) IN (10,11,12) THEN 'Q4'
            END
        AS VARCHAR(10))                          AS cohort
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH', '2026119AUH')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

-- raw success events (no deployment key on the event table itself)
events AS (
    SELECT DISTINCT a.acct_no, a.evnt_dt AS event_date
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
        ON  a.clnt_no  = c.clnt_no
        AND a.evnt_dt  = c.DT_RECORD_EXT
        AND a.acct_no  = c.acct_no
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD    = 3
      AND a.evnt_dt         >= DATE '2026-01-01'
),

-- last-touch: an event matching >1 deployment window for the same account is attributed
-- to the most-recently-started deployment (prevents double-counting under overlapping windows)
event_attribution AS (
    SELECT
        e.acct_no, e.event_date, d.treatmt_strt_dt, d.cohort,
        ROW_NUMBER() OVER (
            PARTITION BY e.acct_no, e.event_date
            ORDER BY d.treatmt_strt_dt DESC
        ) AS touch_rank
    FROM events e
    INNER JOIN all_deployments d
        -- explicit cast on the raw side: d.acct_no is CAST(tactic_evnt_id AS BIGINT); e.acct_no
        -- is CR_CRD_ACCT_EVNT_DLY's native (unBIGINT) acct_no. auh_vintage_reconstructed.sql's
        -- own join (`n.acct_no=c.acct_no` in success_events) leaves this comparison implicit and
        -- relies on Teradata's numeric type coercion (a Trino restriction, not a Teradata one) —
        -- made explicit here for clarity, same net comparison.
        ON  d.acct_no = CAST(e.acct_no AS BIGINT)
        AND e.event_date BETWEEN d.treatmt_strt_dt AND d.treatmt_end_dt
),

event_claimed AS (
    SELECT acct_no, event_date, treatmt_strt_dt, cohort
    FROM event_attribution
    WHERE touch_rank = 1
),

-- at most one success per deployment window
deployment_success AS (
    SELECT acct_no, treatmt_strt_dt, cohort, MIN(event_date) AS first_event_date
    FROM event_claimed
    GROUP BY acct_no, treatmt_strt_dt, cohort
),

deployment_vintage AS (
    SELECT acct_no, cohort,
           CAST(first_event_date - treatmt_strt_dt AS INTEGER) AS vintage_day
    FROM deployment_success
),

-- roll up under the client's BIN arm (first-in-bin deployment), not this deployment's own arm
numerator_binned AS (
    SELECT bl.cohort, bl.arm_raw, bl.arm, dv.vintage_day
    FROM deployment_vintage dv
    INNER JOIN bin_arm_lookup bl
        ON bl.acct_no = dv.acct_no AND bl.cohort = dv.cohort
),

daily_counts AS (
    SELECT cohort, arm_raw, arm, vintage_day, COUNT(*) AS n_events
    FROM numerator_binned
    WHERE vintage_day BETWEEN 0 AND 30
    GROUP BY cohort, arm_raw, arm, vintage_day
),

dense_grid AS (
    SELECT c.cohort, c.arm_raw, c.arm, c.cohort_size, s.vintage_day
    FROM vt_auh_quarterly_cells c
    CROSS JOIN vt_auh_quarterly_spine s
)

SELECT
    CAST('AUH' AS VARCHAR(10)) AS campaign,
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

DROP TABLE vt_auh_quarterly_cells;
DROP TABLE vt_auh_quarterly_spine;
