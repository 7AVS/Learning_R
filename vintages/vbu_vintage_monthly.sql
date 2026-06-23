-- vbu_vintage_monthly.sql
-- Campaign : VBU (Visa Benefit Upgrade)
-- Source   : DG6V01.TACTIC_EVNT_IP_AR_HIST (population) + d3cv12a.cr_crd_rpts_acct (ME snapshot)
--            + D3CV12A.dly_full_portfolio (product changes + prior AIB check)
-- Engine   : Teradata-direct (SYS_CALENDAR spine; TDWM cross-join guard: pop_cells is small)
-- Success  : PRIMARY only — first change to visa_prod_cd='AIB', excluding clients who already
--            held AIB before treatmt_strt_dt; no no-ops, no flip-backs; event date = DT_record_ext
-- Arm      : tst_grp_cd raw (from TACTIC_EVNT_IP_AR_HIST)
-- Cohort   : MONTH(treatmt_strt_dt), Jan 2026 onward; vintage_day 0–90
--
-- NOTE: Curated table cards_bizups_vbu_descresp_clnt was not used here because arm field
--       values (test_group / control columns) are not documented in the schema. Raw source
--       approach matches the methodology in vbu_vintage_original.sql (Daniel Chin) and
--       VBU_vintage_monthly_cohort.sql (Trino reference).
-- NOTE: If TDWM blocks the cross-join, materialize pop_cells and days_spine as volatile
--       tables with COLLECT STATISTICS before the final SELECT.

WITH
days_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 90
),

-- Population: VBU tactic events from Jan 2026 onward
pop AS (
    SELECT DISTINCT
        clnt_no,
        CAST(tactic_id AS VARCHAR(50))                  AS tactic_id,
        treatmt_strt_dt,
        treatmt_end_dt,
        addnl_data_dt,
        tst_grp_cd,
        CAST(
            CAST(YEAR(treatmt_strt_dt) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(treatmt_strt_dt) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        )                                               AS cohort_month,
        SUBSTR(tst_grp_cd, 6, 3)                        AS from_product_code
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(CAST(tactic_id AS VARCHAR(50)), 8, 3) = 'VBU'
      AND SUBSTR(CAST(tactic_id AS VARCHAR(50)), 8, 1) <> 'J'
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

-- ME snapshot: client's product at month-end before launch (Teradata LAST_DAY + ADD_MONTHS)
elig AS (
    SELECT
        p.clnt_no,
        p.tactic_id,
        p.tst_grp_cd,
        p.treatmt_strt_dt,
        p.treatmt_end_dt,
        p.cohort_month,
        p.from_product_code,
        a.acct_no,
        a.prod_cd_current                               AS prod_me_before_launch
    FROM pop p
    JOIN d3cv12a.cr_crd_rpts_acct a
        ON  a.clnt_no  = p.clnt_no
        AND a.ME_dt    = LAST_DAY(ADD_MONTHS(p.addnl_data_dt, -1))
        AND a.status   = 'OPEN'
        AND (
            (a.prod_cd_current = p.from_product_code AND p.tst_grp_cd <> 'XX')
            OR (a.prod_cd_current IN ('C00', 'C01', 'C02') AND p.tst_grp_cd = 'XX')
        )
),

-- In-window product changes; exclude no-ops (= prod_me_before_launch) and flip-backs (= from_product_code)
acct_changes AS (
    SELECT
        e.clnt_no,
        e.tactic_id,
        e.tst_grp_cd,
        e.cohort_month,
        e.treatmt_strt_dt,
        e.treatmt_end_dt,
        e.acct_no,
        e.prod_me_before_launch,
        e.from_product_code,
        d.visa_prod_cd                                  AS new_product,
        d.DT_record_ext                                 AS dt_prod_change
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON  d.acct_no      = e.acct_no
        AND d.DT_record_ext BETWEEN (e.treatmt_strt_dt - INTERVAL '1' DAY)
                                AND (e.treatmt_end_dt   + INTERVAL '5' DAY)
        AND d.visa_prod_cd <> e.prod_me_before_launch
        AND d.visa_prod_cd <> e.from_product_code
),

-- Clients already holding AIB before treatment start (exclude from primary)
prior_aib AS (
    SELECT DISTINCT
        e.clnt_no,
        e.tactic_id,
        e.acct_no
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON  d.acct_no       = e.acct_no
        AND d.visa_prod_cd  = 'AIB'
        AND d.dt_record_ext < e.treatmt_strt_dt
),

-- First AIB change per client per cohort_month × arm, excluding prior AIB holders
first_success AS (
    SELECT
        a.clnt_no,
        a.cohort_month,
        a.tst_grp_cd                                    AS arm,
        a.treatmt_strt_dt,
        MIN(a.dt_prod_change)                           AS first_event_dt
    FROM acct_changes a
    LEFT JOIN prior_aib p
        ON  p.clnt_no   = a.clnt_no
        AND p.tactic_id = a.tactic_id
        AND p.acct_no   = a.acct_no
    WHERE a.new_product = 'AIB'
      AND p.acct_no IS NULL
    GROUP BY a.clnt_no, a.cohort_month, a.tst_grp_cd, a.treatmt_strt_dt
),

pop_cells AS (
    SELECT
        cohort_month,
        tst_grp_cd                                      AS arm,
        COUNT(DISTINCT clnt_no)                         AS cohort_size
    FROM pop
    GROUP BY cohort_month, tst_grp_cd
),

client_vintage AS (
    SELECT
        cohort_month,
        arm,
        clnt_no,
        CAST(first_event_dt - treatmt_strt_dt AS INTEGER) AS vintage_day
    FROM first_success
    WHERE CAST(first_event_dt - treatmt_strt_dt AS INTEGER) BETWEEN 0 AND 90
),

daily_counts AS (
    SELECT cohort_month, arm, vintage_day, COUNT(DISTINCT clnt_no) AS n_events
    FROM client_vintage
    GROUP BY cohort_month, arm, vintage_day
),

grid AS (
    SELECT c.cohort_month, c.arm, c.cohort_size, d.vintage_day
    FROM pop_cells c CROSS JOIN days_spine d
)

SELECT
    CAST('VBU' AS VARCHAR(10))                          AS campaign,
    g.cohort_month,
    CAST(g.arm AS VARCHAR(20))                          AS arm,
    CAST('primary_aib_upgrade' AS VARCHAR(30))          AS metric,
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
ORDER BY g.cohort_month, g.arm, g.vintage_day
;
