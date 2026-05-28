-- async_banner_vintage_success.sql
-- Engine: Teradata NATIVE. No federation, no GA4, no Trino syntax.
-- Purpose: Success-only vintage curves (responders) for PCD, CTU, O2P.
--   No engagement columns. Vintage day 0-60, cumulative window functions
--   partitioned by (cohort, segment, segment_level, test_control_flag, cohort_arm).
-- Sources:
--   PCD  — DG6V01.TACTIC_EVNT_IP_AR_HIST + D3CV12A.DLY_FULL_PORTFOLIO
--   CTU  — DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.CLNT_AR_RELTN_DLY/AR_STATIC_DLY/
--           DEPOSIT_ACCOUNT_DLY/DEP_ACCT_SW_DLY/PBA_ACCT_LKUP
--   O2P  — DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.CR_APP_CLNT_RELTN/OVRL_CR_APP/
--           CR_APP_CLNT_PROD_RELTN/CR_APP_PROD
-- Sibling: async_banner_vintage_engagement.sql (Trino, engagement only)


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD (using curated dl_mr_prod.cards_pcd_ongoing_decis_resp)      ║
-- ║ Replaces tactic-event parsing + DLY_FULL_PORTFOLIO join with the curated   ║
-- ║ table that already carries product_at_decision, target_product,            ║
-- ║ new_product, dt_prod_change, channel_deploy_mb, and pre-computed responder ║
-- ║ flags (responder_anyproduct, responder_targetproduct).                     ║
-- ║                                                                            ║
-- ║ Output adds responders_target (= responder_targetproduct, the primary      ║
-- ║ success = converted to the targeted product) alongside responders          ║
-- ║ (= responder_anyproduct, the secondary success = any product change).      ║
-- ║                                                                            ║
-- ║ test_control_flag derived from act_ctl_seg in the curated table. If those  ║
-- ║ values look wrong, swap for a join back to TACTIC_EVNT_IP_AR_HIST.tst_grp_cd.║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT (calendar_date - DATE '2026-04-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-01' + 60
),

cohort AS (
    SELECT
        acct_no,
        clnt_no,
        tactic_id_parent,
        response_start,
        response_end,
        CAST(response_start - (EXTRACT(DAY FROM response_start) - 1) AS DATE) AS cohort_month,
        product_at_decision,
        target_product,
        new_product,
        dt_prod_change,
        responder_anyproduct,
        responder_targetproduct,
        responder_upgrade_path,
        -- cohort_arm from the campaign code (same allowlist as position-3 in tactic event)
        CASE
            WHEN strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
            THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm,
        -- test_control_flag overrides the curated team's act_ctl_seg derivation;
        -- our rule = suffix on the raw test group code, same logic we use on tst_grp_cd.
        CASE
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
        END AS test_control_flag
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent IN ('2026111PCD','2026125PCD')
      AND response_start >= DATE '2026-04-01'
),

population AS (
    SELECT cohort_month, product_at_decision, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    WHERE test_control_flag IS NOT NULL
    GROUP BY 1,2,3,4
),

success_daily AS (
    SELECT cohort_month, product_at_decision, test_control_flag, cohort_arm,
           (dt_prod_change - response_start) AS vintage_day,
           COUNT(DISTINCT CASE WHEN responder_anyproduct    = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target,
           COUNT(DISTINCT CASE WHEN responder_upgrade_path  = 1 THEN clnt_no END) AS responders_upgrade
    FROM cohort
    WHERE test_control_flag IS NOT NULL
      AND dt_prod_change IS NOT NULL
      AND (dt_prod_change - response_start) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),

spine AS (
    SELECT p.cohort_month, p.product_at_decision, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.product_at_decision, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r.responders,         0) AS responders,
        COALESCE(r.responders_target,  0) AS responders_target,
        COALESCE(r.responders_upgrade, 0) AS responders_upgrade
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.cohort_month         = s.cohort_month
        AND r.product_at_decision  = s.product_at_decision
        AND r.test_control_flag    = s.test_control_flag
        AND r.cohort_arm           = s.cohort_arm
        AND r.vintage_day          = s.vintage_day
),

final_grain AS (
    SELECT
        cohort_month                          AS cohort,
        CAST('ALL'     AS VARCHAR(50))        AS segment,
        CAST('OVERALL' AS VARCHAR(50))        AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        SUM(total_population)                 AS total_population,
        SUM(responders)                       AS responders,
        SUM(responders_target)                AS responders_target,
        SUM(responders_upgrade)               AS responders_upgrade
    FROM base
    GROUP BY cohort_month, test_control_flag, cohort_arm, vintage_day

    UNION ALL

    SELECT
        cohort_month                          AS cohort,
        CAST('PRODUCT' AS VARCHAR(50))        AS segment,
        product_at_decision                   AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        total_population,
        responders, responders_target, responders_upgrade
    FROM base
)

SELECT
    CAST('PCD' AS VARCHAR(50)) AS campaign,
    cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders, responders_target, responders_upgrade,
    SUM(responders) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum,
    SUM(responders_target) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_target_cum,
    SUM(responders_upgrade) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_upgrade_cum
FROM final_grain
ORDER BY 2, 3, 4, 5, 6, 7
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU (using curated dl_mr_prod.nbo_pba_upgrade)                   ║
-- ║ tactic_id filter: SUBSTR(tactic_id,8,3)='CTU'. No test/control split →     ║
-- ║ test_control_flag='ALL'. cohort_arm: ASYNC if chnl_mb=1.                   ║
-- ║ responders = success=1; responders_target = primary_success=1.             ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT (calendar_date - DATE '2026-04-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-01' + 60
),

cohort AS (
    SELECT
        clnt_no,
        tactic_id,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month,
        current_product,
        target_product,
        primary_success,
        secondary_success,
        success,
        response_dt,
        CASE WHEN chnl_mb = 1 THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,
        CAST('ALL' AS VARCHAR(50)) AS test_control_flag
    FROM dl_mr_prod.nbo_pba_upgrade
    WHERE SUBSTR(tactic_id, 8, 3) = 'CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

population AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3
),

success_daily AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           (response_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT CASE WHEN success          = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN primary_success  = 1 THEN clnt_no END) AS responders_target
    FROM cohort
    WHERE response_dt IS NOT NULL
      AND (response_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4
),

spine AS (
    SELECT p.cohort_month, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.cohort_month      = s.cohort_month
        AND r.test_control_flag = s.test_control_flag
        AND r.cohort_arm        = s.cohort_arm
        AND r.vintage_day       = s.vintage_day
)

SELECT
    CAST('CTU'     AS VARCHAR(50)) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders, responders_target,
    SUM(responders) OVER (
        PARTITION BY cohort_month, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum,
    SUM(responders_target) OVER (
        PARTITION BY cohort_month, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_target_cum
FROM base
ORDER BY 2, 5, 6, 7
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P (using curated dl_mr_prod.nbo_pba_upgrade)                   ║
-- ║ tactic_id filter: SUBSTR(tactic_id,8,3)='O2P'. TG4=TEST, TG7=CONTROL.     ║
-- ║ cohort_arm: ASYNC if chnl_mb=1. ALL grain only (no rpt_grp_cd breakdown).  ║
-- ║ responders = success=1; responders_target = primary_success=1.             ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT (calendar_date - DATE '2026-04-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-01' + 60
),

cohort AS (
    SELECT
        clnt_no,
        tactic_id,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month,
        current_product,
        target_product,
        primary_success,
        secondary_success,
        success,
        response_dt,
        CASE WHEN chnl_mb = 1 THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag
    FROM dl_mr_prod.nbo_pba_upgrade
    WHERE SUBSTR(tactic_id, 8, 3) = 'O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

population AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3
),

success_daily AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           (response_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT CASE WHEN success          = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN primary_success  = 1 THEN clnt_no END) AS responders_target
    FROM cohort
    WHERE response_dt IS NOT NULL
      AND (response_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4
),

spine AS (
    SELECT p.cohort_month, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.cohort_month      = s.cohort_month
        AND r.test_control_flag = s.test_control_flag
        AND r.cohort_arm        = s.cohort_arm
        AND r.vintage_day       = s.vintage_day
)

SELECT
    CAST('O2P'     AS VARCHAR(50)) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders, responders_target,
    SUM(responders) OVER (
        PARTITION BY cohort_month, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum,
    SUM(responders_target) OVER (
        PARTITION BY cohort_month, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_target_cum
FROM base
ORDER BY 2, 5, 6, 7
;
