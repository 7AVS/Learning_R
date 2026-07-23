-- vba_vintage_quarterly.sql
-- Campaign : VBA (Visa Benefit Add)
-- Source   : DG6V01.tactic_evnt_ip_ar_hist (population) + p3c.appl_fact_dly (Casper) +
--          edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot (SCOT) —
--          RAW success event tables, no deployment key on either
-- Engine   : Teradata-direct (SYS_CALENDAR spine, volatile tables for TDWM cross-join clearance).
--          SCOT (tsz_00222) is an EDL-catalog table but is reachable Teradata-direct per the
--          older campaigns/VBA_VBU/vba_vintage_curves.sql ("Run in: Teradata (uses SYS_CALENDAR)").
-- [VERIFY] ENGINE: SCOT source uses EDL catalog path (edl0_im...) — confirm reachable from
--          Teradata-direct (QueryGrid/foreign server); if NOT, run these two campaigns via
--          Starburst federation instead (then Trino syntax rules apply).
-- Success  : RESTORED 2026-07-22 review — Casper + SCOT UNIONED AND DEDUPED to one
--          earliest-approval-per-client-per-deployment event; single metric (approval), two
--          systems. Casper: Status='A', PROD_APPRVD IN ('B','E'), CR_LMT_CHG_IND='N',
--          visa_prod_cd NOT IN ('CCL','BXX'), Cell_Code NOT IN ('PATACT','GV0320'), event date =
--          app_rcv_dt. SCOT: productcategory='CREDIT_CARD', statuscode='FULFILLED', event date =
--          creditapplication_createddatetime (collapsed to one row per client — SCOT's own
--          snapshot structure supports only one signal ever per client, not one per deployment;
--          see scot_events below). Union/dedup pattern per
--          campaigns/VBA_VBU/vba_vintage_curves_trino.sql (all_responses/success CTEs). Union
--          happens BEFORE last-touch deployment attribution: the physical event is deduped
--          first (plain UNION, not UNION ALL, on (clnt_no,event_date)), then attributed.
-- Anchor   : treatmt_strt_dt (treatment start), per deployment
-- Grain    : client (clnt_no)
-- Arm      : tst_grp_cd — LEFT(tst_grp_cd,1)='C' -> Control, ='T' -> Action, ELSE -> Other.
--          [VERIFY] this C/T-prefix split is NOT documented in campaigns/VBA_VBU/
--          vba_vintage_curves.sql (the canon source named for this file, which carries
--          tst_grp_cd raw with no split); it is confirmed instead in the sibling harness
--          campaigns/VBA_VBU/vba_summary_vintage_cell.py (`tc()`, line ~68) as VBA/VBU's
--          real Test/Control rollup rule. Used here to satisfy the standard Action/Control
--          output contract, but confirm before trusting the 'Other' bucket.
-- Population filter: SUBSTR(tactic_id,8,3)='VBA', SUBSTR(tactic_id,8,1)<>'J'
-- Cohort bin: CALENDAR quarter 'YYYYQn' (Jan-Mar=Q1) of a deployment's own treatmt_strt_dt
-- Day window: 0-90
-- Denominator: one row per (clnt_no, bin) = first in-bin deployment (MIN treatmt_strt_dt within
--          the bin). Arm = that deployment's arm; first-anchor wins on conflict. Quarterly
--          cohort_size <= sum of the 3 monthly cohort_sizes — gap = clients contacted in more
--          than one month of the quarter.
-- Numerator: NOT deduped — every deployment gets its own success lookup, one success max per
--          deployment window. Neither Casper nor SCOT carries a deployment key, so an approved
--          application (from either source) inside TWO overlapping deployment windows for the
--          same client is attributed via LAST-TOUCH: the most recent deployment start on/before
--          the event date wins (touch_rank=1 below). Rolls up under the client's bin arm.
--          cum_responses = cumulative SUCCESS EVENTS (one per deployment window), NOT clients —
--          sums cleanly: quarterly cum_responses = sum of the 3 monthly files' cum_responses.
-- Sourced from: campaigns/VBA_VBU/vba_vintage_curves.sql Query 2 (Casper+SCOT union path) +
--          campaigns/VBA_VBU/vba_vintage_curves_trino.sql (union/dedup pattern)
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_vba_quarterly_cells;
--   DROP TABLE vt_vba_quarterly_spine;

-- ============================================================================
-- STEP 1: denominator cells
-- ============================================================================
CREATE VOLATILE TABLE vt_vba_quarterly_cells AS (
    WITH bin_arm_lookup AS (
        SELECT
            clnt_no,
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
            CASE
                WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(30))
                WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Action'  AS VARCHAR(30))
                ELSE CAST('Other' AS VARCHAR(30))
            END                                        AS arm
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE treatmt_strt_dt >= DATE '2026-01-01'
          AND SUBSTR(tactic_id, 8, 3) = 'VBA'
          AND SUBSTR(tactic_id, 8, 1) <> 'J'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort
            ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort, arm_raw, arm, COUNT(DISTINCT clnt_no) AS cohort_size
    FROM bin_arm_lookup
    GROUP BY cohort, arm_raw, arm
) WITH DATA PRIMARY INDEX (cohort, arm) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_quarterly_cells COLUMN (cohort, arm);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_vba_quarterly_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vba_quarterly_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
bin_arm_lookup AS (
    SELECT
        clnt_no,
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
        CASE
            WHEN LEFT(TRIM(tst_grp_cd), 1) = 'C' THEN CAST('Control' AS VARCHAR(30))
            WHEN LEFT(TRIM(tst_grp_cd), 1) = 'T' THEN CAST('Action'  AS VARCHAR(30))
            ELSE CAST('Other' AS VARCHAR(30))
        END                                        AS arm
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND SUBSTR(tactic_id, 8, 3) = 'VBA'
      AND SUBSTR(tactic_id, 8, 1) <> 'J'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort
        ORDER BY treatmt_strt_dt ASC
    ) = 1
),

-- every deployment (NOT deduped)
all_deployments AS (
    SELECT DISTINCT
        clnt_no,
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
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND SUBSTR(tactic_id, 8, 3) = 'VBA'
      AND SUBSTR(tactic_id, 8, 1) <> 'J'
),

-- raw candidate success events, Casper (PRIMARY) — no deployment key on the event table itself
casper_events AS (
    SELECT DISTINCT app.bus_clnt_no AS clnt_no, app.app_rcv_dt AS event_date
    FROM p3c.appl_fact_dly app
    WHERE app.Status IN ('A')
      AND app.PROD_APPRVD IN ('B', 'E')
      AND app.CR_LMT_CHG_IND = 'N'
      AND app.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND (app.Cell_Code IS NULL OR app.Cell_Code NOT IN ('PATACT', 'GV0320'))
),

-- raw candidate success events, SCOT (SECONDARY) — one row per client (SCOT's snapshot
-- structure has no per-deployment key; MIN date + fulfilled flag is the most it supports)
scot_events_raw AS (
    SELECT
        CAST(creditapplication_borrowers_borrowersrfnumber AS INTEGER) AS clnt_no,
        MIN(CAST(creditapplication_createddatetime AS DATE))           AS event_date,
        MAX(CASE
            WHEN creditapplication_creditapplicationstatuscode IN ('FULFILLED') THEN 1 ELSE 0
        END)                                                           AS approved
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00222_data_credit_application_snapshot
    WHERE creditapplication_borrowers_facilities_facilityborroweroptions_products_productcategory
          IN ('CREDIT_CARD')
    GROUP BY 1
),
scot_events AS (
    SELECT clnt_no, event_date FROM scot_events_raw WHERE approved = 1
),

-- union FIRST, dedupe the physical event (clnt_no, event_date) — plain UNION, not UNION ALL —
-- THEN attribute to a deployment (last-touch, below)
events AS (
    SELECT clnt_no, event_date FROM casper_events
    UNION
    SELECT clnt_no, event_date FROM scot_events
),

-- last-touch: an event matching >1 deployment window for the same client is attributed to the
-- most-recently-started deployment (prevents double-counting under overlapping windows)
event_attribution AS (
    SELECT
        e.clnt_no, e.event_date, d.treatmt_strt_dt, d.cohort,
        ROW_NUMBER() OVER (
            PARTITION BY e.clnt_no, e.event_date
            ORDER BY d.treatmt_strt_dt DESC
        ) AS touch_rank
    FROM events e
    INNER JOIN all_deployments d
        ON  d.clnt_no = e.clnt_no
        AND e.event_date BETWEEN d.treatmt_strt_dt AND d.treatmt_end_dt
),

event_claimed AS (
    SELECT clnt_no, event_date, treatmt_strt_dt, cohort
    FROM event_attribution
    WHERE touch_rank = 1
),

-- at most one success per deployment window
deployment_success AS (
    SELECT clnt_no, treatmt_strt_dt, cohort, MIN(event_date) AS first_event_date
    FROM event_claimed
    GROUP BY clnt_no, treatmt_strt_dt, cohort
),

deployment_vintage AS (
    SELECT clnt_no, cohort,
           CAST(first_event_date - treatmt_strt_dt AS INTEGER) AS vintage_day
    FROM deployment_success
),

-- roll up under the client's BIN arm (first-in-bin deployment), not this deployment's own arm
numerator_binned AS (
    SELECT bl.cohort, bl.arm_raw, bl.arm, dv.vintage_day
    FROM deployment_vintage dv
    INNER JOIN bin_arm_lookup bl
        ON bl.clnt_no = dv.clnt_no AND bl.cohort = dv.cohort
),

daily_counts AS (
    SELECT cohort, arm_raw, arm, vintage_day, COUNT(*) AS n_events
    FROM numerator_binned
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort, arm_raw, arm, vintage_day
),

dense_grid AS (
    SELECT c.cohort, c.arm_raw, c.arm, c.cohort_size, s.vintage_day
    FROM vt_vba_quarterly_cells c
    CROSS JOIN vt_vba_quarterly_spine s
)

SELECT
    CAST('VBA' AS VARCHAR(10)) AS campaign,
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

DROP TABLE vt_vba_quarterly_cells;
DROP TABLE vt_vba_quarterly_spine;
