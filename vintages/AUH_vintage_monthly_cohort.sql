-- Campaign : AUH (Authorized Users)
-- Source   : DG6V01.tactic_evnt_ip_ar_hist (population)
--            + D3CV12A.CR_CRD_ACCT_EVNT_DLY (AU-add events: dtl_evnt_typ_cd=191, ADD_RELTN_CD=3)
-- Success  : First authorized-user ADD event per account, within the treatment window.
--            Anchor = treatmt_strt_dt. Success event: dtl_evnt_typ_cd=191, ADD_RELTN_CD=3.
--            (Primary metric = any-product add; target-product add excluded per simplified contract.)
--
-- PROVENANCE NOTE: event-table approach (CR_CRD_ACCT_EVNT_DLY + dtl_evnt_typ_cd=191 +
--   ADD_RELTN_CD=3) is the AUTHORITATIVE definition per auh_vintage_reconstructed.sql
--   (2026-06-11). This supersedes auh_interim_measurement.sql which uses the ownership
--   snapshot ACCT_CRD_OWN_DLY_DELTA — that table's CAPTR_DT is a refresh date, not an
--   event date, and inflates results by counting long-time AU holders. Never revert to
--   the snapshot approach for new queries.
--
-- Anchor   : treatmt_strt_dt
-- Arm      : tst_grp_cd raw — Control = RIGHT(TRIM(tst_grp_cd),2)='_C', else Test
--            (IMPORTANT: strategy_arm / model_arm subdivisions from source script are dropped;
--            monthly-cohort contract uses test_group only.)
-- Engine   : Starburst/Trino (federation)
-- Range    : treatmt_strt_dt >= 2026-01-01
-- Tactic IDs: '2026042AUH', '2026119AUH' (both waves included via mnemonic filter)

WITH pop AS (
    SELECT DISTINCT
        clnt_no,
        CAST(TACTIC_EVNT_ID AS DECIMAL(38,0))                        AS acct_no,
        tactic_id,
        treatmt_strt_dt,
        treatmt_end_dt,
        tst_grp_cd,
        CASE
            WHEN TRIM(tst_grp_cd) LIKE '%\_C' ESCAPE '\' THEN 'CONTROL'
            ELSE 'TEST'
        END                                                           AS arm
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH', '2026119AUH')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

cohort_months AS (
    SELECT
        clnt_no,
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        arm,
        date_trunc('month', treatmt_strt_dt)                          AS cohort_month
    FROM pop
),

cohort_size AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT acct_no)                                       AS cohort_size
    FROM cohort_months
    GROUP BY 1, 2
),

-- AU-add events from event table (NOT snapshot — snapshot captr_dt is a refresh date)
au_events AS (
    SELECT
        a.acct_no,
        a.evnt_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD    = 3
      AND a.evnt_dt        >= DATE '2026-01-01'
),

first_success AS (
    SELECT
        cm.acct_no,
        cm.cohort_month,
        cm.arm,
        cm.treatmt_strt_dt,
        MIN(ae.evnt_dt)                                               AS first_success_dt
    FROM cohort_months cm
    INNER JOIN au_events ae
        ON  ae.acct_no  = cm.acct_no
        AND ae.evnt_dt >= cm.treatmt_strt_dt
        AND ae.evnt_dt <= cm.treatmt_end_dt
    GROUP BY cm.acct_no, cm.cohort_month, cm.arm, cm.treatmt_strt_dt
),

vintage_days_raw AS (
    SELECT
        cohort_month,
        arm,
        date_diff('day', treatmt_strt_dt, first_success_dt)          AS vintage_day,
        COUNT(DISTINCT acct_no)                                       AS new_events
    FROM first_success
    WHERE date_diff('day', treatmt_strt_dt, first_success_dt) BETWEEN 0 AND 180
    GROUP BY 1, 2, 3
),

spine AS (
    SELECT
        cs.cohort_month,
        cs.arm,
        cs.cohort_size,
        t.vintage_day
    FROM cohort_size cs
    CROSS JOIN UNNEST(sequence(0, 180)) AS t(vintage_day)
),

joined AS (
    SELECT
        s.cohort_month,
        s.arm,
        s.vintage_day,
        s.cohort_size,
        COALESCE(r.new_events, 0)                                     AS new_events
    FROM spine s
    LEFT JOIN vintage_days_raw r
        ON  r.cohort_month = s.cohort_month
        AND r.arm          = s.arm
        AND r.vintage_day  = s.vintage_day
)

SELECT
    CAST('AUH' AS VARCHAR(10))                                        AS campaign,
    cohort_month,
    arm,
    vintage_day,
    cohort_size,
    SUM(new_events) OVER (
        PARTITION BY cohort_month, arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                 AS cum_events
FROM joined
ORDER BY cohort_month, arm, vintage_day
;
