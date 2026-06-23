-- vba_vintage_monthly.sql
-- Campaign : VBA (Visa Benefit Add)
-- Source   : campaigns/VBA_VBU/vba_vintage_curves.sql (Query 2 — PRIMARY Casper path)
-- Engine   : Teradata-direct (SYS_CALENDAR spine, Teradata date arithmetic)
-- Success  : p3c.appl_fact_dly — Status='A', PROD_APPRVD IN ('B','E'),
--            CR_LMT_CHG_IND='N', visa_prod_cd NOT IN ('CCL','BXX'),
--            Cell_Code NOT IN ('PATACT','GV0320'), app_rcv_dt within treatment window
-- Grain    : client (clnt_no)
-- Arm field: tst_grp_cd (raw code from DG6V01.tactic_evnt_ip_ar_hist)
-- Cohort   : calendar month of treatmt_strt_dt, Jan 2026 onward
-- Window   : 0–90 vintage days
--
-- NOTE: SYS_CALENDAR CROSS JOIN is on the cohort_month × arm cell (small set),
-- not on the full population — TDWM cross-join blocker does not apply here.
-- If TDWM complains anyway, materialize pop_cells and days_spine as volatile tables
-- with COLLECT STATISTICS before the cross-join (same pattern as pcq_ms_vintage.sql).

WITH
-- population: one row per client per cohort_month × arm
pop AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        tst_grp_cd                             AS arm,
        CAST(
            CAST(YEAR(treatmt_strt_dt) AS CHAR(4))
            || '-'
            || TRIM(CAST(MONTH(treatmt_strt_dt) AS CHAR(2)) (FORMAT 'Z9'))
            || '-01'
            AS DATE FORMAT 'YYYY-MM-DD'
        )                                      AS cohort_month
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE treatmt_strt_dt >= DATE '2026-01-01'
      AND SUBSTR(tactic_id, 8, 3) = 'VBA'
      AND SUBSTR(tactic_id, 8, 1) <> 'J'
),

-- de-duplicate: one row per distinct clnt_no × cohort_month × arm
-- (a client may appear in multiple waves within a month; keep their earliest treatmt_strt_dt)
pop_dedup AS (
    SELECT
        clnt_no,
        MIN(treatmt_strt_dt)   AS treatmt_strt_dt,
        MAX(treatmt_end_dt)    AS treatmt_end_dt,
        arm,
        cohort_month
    FROM pop
    GROUP BY clnt_no, arm, cohort_month
),

-- cohort size: distinct clients per cohort_month × arm
pop_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no) AS cohort_size
    FROM pop_dedup
    GROUP BY cohort_month, arm
),

-- Casper applications: all status rows filtered by campaign rules
casper_raw AS (
    SELECT
        p.clnt_no,
        p.arm,
        p.cohort_month,
        p.treatmt_strt_dt,
        app.app_rcv_dt         AS response_dt
    FROM pop_dedup p
    INNER JOIN p3c.appl_fact_dly app
        ON  app.bus_clnt_no = p.clnt_no
        AND app.app_rcv_dt BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
    WHERE app.Status IN ('A')                         -- approved only (primary success)
      AND app.PROD_APPRVD IN ('B', 'E')
      AND app.CR_LMT_CHG_IND = 'N'
      AND app.visa_prod_cd NOT IN ('CCL', 'BXX')
      AND (app.Cell_Code IS NULL OR app.Cell_Code NOT IN ('PATACT', 'GV0320'))
),

-- first approved response per client per cohort_month × arm
first_response AS (
    SELECT
        clnt_no,
        arm,
        cohort_month,
        MIN(response_dt) AS first_response_dt
    FROM casper_raw
    GROUP BY clnt_no, arm, cohort_month
),

-- vintage days 0–90 (anchor date is arbitrary; only the integer offset matters)
days_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 90
),

-- dense grid: one row per cohort_month × arm × vintage_day
grid AS (
    SELECT
        c.cohort_month,
        c.arm,
        c.cohort_size,
        d.vintage_day
    FROM pop_cells c
    CROSS JOIN days_spine d
),

-- vintage_day per client = first_response_dt - their own treatmt_strt_dt
client_vintage AS (
    SELECT
        r.cohort_month,
        r.arm,
        r.clnt_no,
        CAST(r.first_response_dt - p.treatmt_strt_dt AS INTEGER) AS vintage_day
    FROM first_response r
    INNER JOIN pop_dedup p
        ON  p.clnt_no      = r.clnt_no
        AND p.arm          = r.arm
        AND p.cohort_month = r.cohort_month
    WHERE CAST(r.first_response_dt - p.treatmt_strt_dt AS INTEGER) BETWEEN 0 AND 90
),

-- daily event counts per cohort_month × arm × vintage_day
daily_counts AS (
    SELECT
        cohort_month,
        arm,
        vintage_day,
        COUNT(DISTINCT clnt_no) AS n_events
    FROM client_vintage
    GROUP BY cohort_month, arm, vintage_day
)

SELECT
    CAST('VBA' AS VARCHAR(10))                          AS campaign,
    g.cohort_month,
    CAST(g.arm AS VARCHAR(50))                          AS arm,
    CAST('approved_casper' AS VARCHAR(30))              AS metric,
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
