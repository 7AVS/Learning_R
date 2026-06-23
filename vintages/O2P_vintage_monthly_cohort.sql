-- Campaign : O2P (Online-to-Product)
-- Source   : DG6V01.TACTIC_EVNT_IP_AR_HIST (population)
--            + DDWV01.CR_APP_CLNT_RELTN_DLY / OVRL_CR_APP_DLY / CR_APP_CLNT_PROD_RELTN_DLY / CR_APP_PROD_DLY
--              (raw CR_APP chain — O2P not in curated table)
-- Success  : Primary = first application for prod_typ='43' (primary O2P target) with
--            prod_app_sts_cd IN (32,37,45,47,51,56,62) AND app_typ='P'
-- Anchor   : treatmt_strt_dt
-- Arm      : tst_grp_cd — TG4=TEST, TG7=CONTROL (raw codes mapped per source script)
-- Engine   : Starburst/Trino (federation)
-- Range    : treatmt_strt_dt >= 2026-01-01
--
-- NOTE: O2P has multiple tactic_ids ('2026099O2P', '2026126O2P', '2026132O2P').
-- The population filter below covers all via SUBSTR mnemonic match.
-- Volatile-table materialization from source script is NOT replicated here —
-- the simplified monthly-cohort contract is a single CTE query; CR_APP tables
-- are scanned once. If TDWM kills the job, materialize converters_raw as a
-- volatile table in a separate step and replace the CTE reference.

WITH pop AS (
    SELECT DISTINCT
        clnt_no,
        CAST(tactic_id AS VARCHAR(50))                                AS tactic_id,
        treatmt_strt_dt,
        tst_grp_cd
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(CAST(tactic_id AS VARCHAR(50)), 8, 3) = 'O2P'
      AND treatmt_strt_dt >= DATE '2026-01-01'
      AND TRIM(tst_grp_cd) IN ('TG4', 'TG7')
),

cohort_months AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        date_trunc('month', treatmt_strt_dt)                          AS cohort_month,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END                                                           AS arm
    FROM pop
),

cohort_size AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size
    FROM cohort_months
    GROUP BY 1, 2
),

-- CR_APP converters: primary product only (appl_for_prod_typ='43')
converters_raw AS (
    SELECT
        a.clnt_no,
        d.prod_app_dt                                                 AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN_DLY      AS a
    INNER JOIN DDWV01.OVRL_CR_APP_DLY      AS b
        ON  b.cr_app_id   = a.cr_app_id
        AND b.sys_src_id  = a.sys_src_id
    INNER JOIN DDWV01.CR_APP_CLNT_PROD_RELTN_DLY AS c
        ON  c.cr_app_id          = a.cr_app_id
        AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id         = a.sys_src_id
    INNER JOIN DDWV01.CR_APP_PROD_DLY      AS d
        ON  d.cr_app_id        = c.cr_app_id
        AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id       = c.sys_src_id
    WHERE b.app_typ              = 'P'
      AND d.appl_for_prod_typ    = '43'
      AND d.prod_app_sts_cd     IN (32, 37, 45, 47, 51, 56, 62)
      AND d.prod_app_compl_dt   IS NOT NULL
      AND d.prod_app_dt         >= DATE '2026-01-01'
),

first_success AS (
    SELECT
        cm.clnt_no,
        cm.cohort_month,
        cm.arm,
        cm.treatmt_strt_dt,
        MIN(cv.app_dt)                                                AS first_success_dt
    FROM cohort_months cm
    INNER JOIN converters_raw cv
        ON  cv.clnt_no  = cm.clnt_no
        AND cv.app_dt  >= cm.treatmt_strt_dt
        AND cv.app_dt  >= DATE '2026-01-01'
    GROUP BY cm.clnt_no, cm.cohort_month, cm.arm, cm.treatmt_strt_dt
),

vintage_days_raw AS (
    SELECT
        cohort_month,
        arm,
        date_diff('day', treatmt_strt_dt, first_success_dt)          AS vintage_day,
        COUNT(DISTINCT clnt_no)                                       AS new_events
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
    CAST('O2P' AS VARCHAR(10))                                        AS campaign,
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
