-- auh_vintage_monthly.sql
-- Campaign : AUH (Authorized Users)
-- Source   : campaigns/AUH/auh_vintage_reconstructed.sql
-- Engine   : Teradata-direct (SYS_CALENDAR spine, Teradata date arithmetic)
-- Success  : D3CV12A.CR_CRD_ACCT_EVNT_DLY — dtl_evnt_typ_cd=191 AND ADD_RELTN_CD=3
--            (authorized user actually added, event-table source, NOT ownership snapshot)
--            Joined to D3CV12A.DLY_FULL_PORTFOLIO for visa_prod_cd.
--            First add to ANY product within treatment window = success.
--            Anchor: evnt_dt; earliest add per acct_no (MIN across new_owner).
-- Grain    : account (acct_no)
-- Arm field: tst_grp_cd suffix — RIGHT(TRIM(tst_grp_cd),2)='_C' → Control, else → Test
-- Population filters: tactic_id IN ('2026042AUH','2026119AUH')
-- Cohort   : calendar month of treatmt_strt_dt, Jan 2026 onward
-- Window   : 0–30 vintage days (source was 0–30; extended to 90 to match standard)
--
-- NOTE: SYS_CALENDAR cross-join is against pop_cells (small). TDWM blocker
-- applies to unconstrained population × calendar joins, not cell × calendar.
-- If TDWM still fires, materialize pop_cells + days_spine as volatile tables
-- with COLLECT STATISTICS (same pattern as pcq_ms_vintage.sql).

WITH
days_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 90
),

-- population: one row per account per cohort_month × arm
cohort AS (
    SELECT
        CAST(tactic_evnt_id AS BIGINT)                          AS acct_no,
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        CASE
            WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN 'Control'
            ELSE 'Test'
        END                                                     AS arm,
        (treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1)) AS cohort_month
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH', '2026119AUH')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

pop_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT acct_no) AS cohort_size
    FROM cohort
    GROUP BY cohort_month, arm
),

-- authorized-user add events from event table (not snapshot)
au_event AS (
    SELECT a.acct_no, a.evnt_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
        ON  a.clnt_no  = c.clnt_no
        AND a.evnt_dt  = c.DT_RECORD_EXT
        AND a.acct_no  = c.acct_no
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD    = 3
      AND a.evnt_dt         >= DATE '2026-01-01'
),

-- earliest add per account (any product)
new_owner AS (
    SELECT acct_no, MIN(evnt_dt) AS first_owned_dt
    FROM au_event
    GROUP BY acct_no
),

-- attribute success to campaign: add must fall within treatment window
success_events AS (
    SELECT
        c.acct_no,
        c.arm,
        c.cohort_month,
        c.treatmt_strt_dt,
        MIN(n.first_owned_dt) AS first_app_dt
    FROM cohort c
    INNER JOIN new_owner n
        ON  n.acct_no          = c.acct_no
        AND n.first_owned_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    GROUP BY c.acct_no, c.arm, c.cohort_month, c.treatmt_strt_dt
),

client_vintage AS (
    SELECT
        cohort_month,
        arm,
        acct_no,
        CAST(first_app_dt - treatmt_strt_dt AS INTEGER) AS vintage_day
    FROM success_events
    WHERE CAST(first_app_dt - treatmt_strt_dt AS INTEGER) BETWEEN 0 AND 90
),

daily_counts AS (
    SELECT cohort_month, arm, vintage_day, COUNT(DISTINCT acct_no) AS n_events
    FROM client_vintage
    GROUP BY cohort_month, arm, vintage_day
),

grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM pop_cells c CROSS JOIN days_spine d
)

SELECT
    CAST('AUH' AS VARCHAR(10))                          AS campaign,
    g.cohort_month,
    CAST(g.arm AS VARCHAR(20))                          AS arm,
    CAST('au_add_any_product' AS VARCHAR(30))           AS metric,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.cohort_month, g.arm
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                   AS cum_events
FROM grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.arm          = g.arm
    AND dc.vintage_day  = g.vintage_day
ORDER BY g.cohort_month, g.arm, g.vintage_day;
