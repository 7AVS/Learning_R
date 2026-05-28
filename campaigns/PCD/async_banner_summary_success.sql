-- async_banner_summary_success.sql
-- Engine: Teradata NATIVE. No federation, no GA4, no Trino syntax.
-- Purpose: Success totals (no vintage_day breakdown) for PCD, CTU, O2P.
--   No engagement columns. No cumulative window functions — just totals.
-- Sources:
--   PCD  — DG6V01.TACTIC_EVNT_IP_AR_HIST + D3CV12A.DLY_FULL_PORTFOLIO
--   CTU  — DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.CLNT_AR_RELTN_DLY/AR_STATIC_DLY/
--           DEPOSIT_ACCOUNT_DLY/DEP_ACCT_SW_DLY/PBA_ACCT_LKUP
--   O2P  — DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.CR_APP_CLNT_RELTN/OVRL_CR_APP/
--           CR_APP_CLNT_PROD_RELTN/CR_APP_PROD
-- Sibling: async_banner_summary_engagement.sql (Trino, engagement totals)


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD (using curated dl_mr_prod.cards_pcd_ongoing_decis_resp)      ║
-- ║ Same swap as the vintage success file. Adds responders_target alongside    ║
-- ║ responders. cohort_arm from channel_deploy_mb; test_control_flag from      ║
-- ║ act_ctl_seg (verify against tst_grp_cd if values look off).                ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
cohort AS (
    SELECT
        acct_no, clnt_no, tactic_id_parent,
        response_start, response_end,
        CAST(response_start - (EXTRACT(DAY FROM response_start) - 1) AS DATE) AS cohort_month,
        product_at_decision,
        target_product,
        new_product,
        dt_prod_change,
        responder_anyproduct,
        responder_targetproduct,
        responder_upgrade_path,
        CASE
            WHEN strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
            THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm,
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

success_total AS (
    SELECT cohort_month, product_at_decision, test_control_flag, cohort_arm,
           COUNT(DISTINCT CASE WHEN responder_anyproduct    = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target,
           COUNT(DISTINCT CASE WHEN responder_upgrade_path  = 1 THEN clnt_no END) AS responders_upgrade
    FROM cohort
    WHERE test_control_flag IS NOT NULL
    GROUP BY 1,2,3,4
),

base AS (
    SELECT
        p.cohort_month, p.product_at_decision, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(r.responders,         0) AS responders,
        COALESCE(r.responders_target,  0) AS responders_target,
        COALESCE(r.responders_upgrade, 0) AS responders_upgrade
    FROM population p
    LEFT JOIN success_total r
        ON  r.cohort_month        = p.cohort_month
        AND r.product_at_decision = p.product_at_decision
        AND r.test_control_flag   = p.test_control_flag
        AND r.cohort_arm          = p.cohort_arm
)

SELECT
    CAST('PCD' AS VARCHAR(50))     AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm,
    SUM(total_population)   AS total_population,
    SUM(responders)         AS responders,
    SUM(responders_target)  AS responders_target,
    SUM(responders_upgrade) AS responders_upgrade
FROM base
GROUP BY cohort_month, test_control_flag, cohort_arm

UNION ALL

SELECT
    CAST('PCD' AS VARCHAR(50))     AS campaign,
    cohort_month                   AS cohort,
    CAST('PRODUCT' AS VARCHAR(50)) AS segment,
    product_at_decision            AS segment_level,
    test_control_flag, cohort_arm,
    total_population,
    responders, responders_target, responders_upgrade
FROM base
ORDER BY 2, 3, 4, 5, 6
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU (using curated dl_mr_prod.nbo_pba_upgrade)                   ║
-- ║ tactic_id filter: SUBSTR(tactic_id,8,3)='CTU'. No test/control split →     ║
-- ║ test_control_flag='ALL'. cohort_arm: ASYNC if chnl_mb=1.                   ║
-- ║ responders = success=1; responders_target = primary_success=1.             ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
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
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

population AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3
),

success_total AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           COUNT(DISTINCT CASE WHEN success         = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN primary_success = 1 THEN clnt_no END) AS responders_target
    FROM cohort
    GROUP BY 1,2,3
),

base AS (
    SELECT
        p.cohort_month, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
    FROM population p
    LEFT JOIN success_total r
        ON  r.cohort_month      = p.cohort_month
        AND r.test_control_flag = p.test_control_flag
        AND r.cohort_arm        = p.cohort_arm
)

SELECT
    CAST('CTU'     AS VARCHAR(50)) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm,
    total_population,
    responders, responders_target
FROM base
ORDER BY 2, 5, 6
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P (using curated dl_mr_prod.nbo_pba_upgrade)                   ║
-- ║ tactic_id filter: SUBSTR(tactic_id,8,3)='O2P'. TG4=TEST, TG7=CONTROL.     ║
-- ║ cohort_arm: ASYNC if chnl_mb=1. ALL grain only (no rpt_grp_cd breakdown).  ║
-- ║ responders = success=1; responders_target = primary_success=1.             ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
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
    WHERE tactic_id IN ('2026099O2P','2026126O2P','2026132O2P')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

population AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3
),

success_total AS (
    SELECT cohort_month, test_control_flag, cohort_arm,
           COUNT(DISTINCT CASE WHEN success         = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN primary_success = 1 THEN clnt_no END) AS responders_target
    FROM cohort
    GROUP BY 1,2,3
),

base AS (
    SELECT
        p.cohort_month, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
    FROM population p
    LEFT JOIN success_total r
        ON  r.cohort_month      = p.cohort_month
        AND r.test_control_flag = p.test_control_flag
        AND r.cohort_arm        = p.cohort_arm
)

SELECT
    CAST('O2P'     AS VARCHAR(50)) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm,
    total_population,
    responders, responders_target
FROM base
ORDER BY 2, 5, 6
;
