-- Campaign : VBA (Visa Benefit Add)
-- Source   : DG6V01.TACTIC_EVNT_IP_AR_HIST (population) + p3c.appl_fact_dly (Casper, primary success)
-- Success  : Casper credit-card application approved — Status='A', PROD_APPRVD IN ('B','E'),
--            CR_LMT_CHG_IND='N', visa_prod_cd NOT IN ('CCL','BXX'), Cell_Code NOT IN ('PATACT','GV0320')
--            Earliest approved app per client within the treatment window.
-- Anchor   : treatmt_strt_dt
-- Arm      : tst_grp_cd (raw; no transformation)
-- Engine   : Starburst/Trino (federation: Teradata tables via Starburst)
-- Range    : treatmt_strt_dt >= 2026-01-01

WITH pop AS (
    SELECT DISTINCT
        clnt_no,
        CAST(tactic_id AS VARCHAR(50))                                AS tactic_id,
        treatmt_strt_dt,
        tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE SUBSTR(CAST(tactic_id AS VARCHAR(50)), 8, 3) = 'VBA'
      AND SUBSTR(CAST(tactic_id AS VARCHAR(50)), 8, 1) <> 'J'
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

cohort_months AS (
    SELECT
        clnt_no,
        tst_grp_cd,
        treatmt_strt_dt,
        date_trunc('month', treatmt_strt_dt)                          AS cohort_month
    FROM pop
),

cohort_size AS (
    SELECT
        cohort_month,
        tst_grp_cd                                                    AS arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size
    FROM cohort_months
    GROUP BY 1, 2
),

first_success AS (
    -- Earliest approved Casper application per client, within treatment window
    SELECT
        cm.clnt_no,
        cm.cohort_month,
        cm.tst_grp_cd                                                 AS arm,
        MIN(p3c.app_rcv_dt)                                           AS first_success_dt
    FROM cohort_months cm
    INNER JOIN p3c.appl_fact_dly p3c
        ON  p3c.bus_clnt_no    = cm.clnt_no
        AND p3c.app_rcv_dt    >= cm.treatmt_strt_dt
        AND p3c.app_rcv_dt    >= DATE '2026-01-01'
    WHERE p3c.Status          = 'A'
      AND p3c.PROD_APPRVD    IN ('B', 'E')
      AND p3c.CR_LMT_CHG_IND = 'N'
      AND p3c.visa_prod_cd   NOT IN ('CCL', 'BXX')
      AND (p3c.Cell_Code IS NULL OR p3c.Cell_Code NOT IN ('PATACT', 'GV0320'))
    GROUP BY cm.clnt_no, cm.cohort_month, cm.tst_grp_cd
),

vintage_days_raw AS (
    -- Vintage day = days from client's own anchor to first success
    SELECT
        cohort_month,
        arm,
        date_diff('day', cm.treatmt_strt_dt, fs.first_success_dt)    AS vintage_day,
        COUNT(DISTINCT fs.clnt_no)                                    AS new_events
    FROM first_success fs
    INNER JOIN cohort_months cm
        ON  cm.clnt_no      = fs.clnt_no
        AND cm.cohort_month = fs.cohort_month
        AND cm.tst_grp_cd   = fs.arm
    WHERE date_diff('day', cm.treatmt_strt_dt, fs.first_success_dt) BETWEEN 0 AND 180
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
    CAST('VBA' AS VARCHAR(10))                                        AS campaign,
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
