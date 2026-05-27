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
           COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target
    FROM cohort
    WHERE test_control_flag IS NOT NULL
    GROUP BY 1,2,3,4
),

base AS (
    SELECT
        p.cohort_month, p.product_at_decision, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
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
    SUM(total_population)  AS total_population,
    SUM(responders)        AS responders,
    SUM(responders_target) AS responders_target
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
    responders, responders_target
FROM base
ORDER BY 2, 3, 4, 5, 6
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU                                                              ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month,
        CASE WHEN SUBSTR(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'
             THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

cohort AS (
    SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_end_dt, cohort_month, cohort_arm
    FROM cohort_raw
),

population AS (
    SELECT cohort_month, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2
),

cohort_snap_dts AS (
    SELECT DISTINCT (treatmt_strt_dt - 1) AS snap_dt FROM cohort
),

cohort_window AS (
    SELECT MIN(treatmt_strt_dt) AS min_dt, MAX(treatmt_end_dt) AS max_dt FROM cohort
),

pba_lkup_curr AS (
    SELECT acct_typ_cd, acct_clss_cd, srvc_fee_opt_cd, prod_en_nm
    FROM (
        SELECT acct_typ_cd, acct_clss_cd, srvc_fee_opt_cd, prod_en_nm, snap_dt,
               MAX(snap_dt) OVER () AS max_snap_dt
        FROM DDWV01.PBA_ACCT_LKUP
        WHERE pda_typ_cd = 'C'
          AND snap_dt BETWEEN (SELECT min_dt FROM cohort_window)
                          AND (SELECT max_dt FROM cohort_window)
    ) sub
    WHERE snap_dt = max_snap_dt
),

precamp_product AS (
    SELECT
        c.clnt_no, c.cohort_month, c.cohort_arm, c.treatmt_strt_dt, c.treatmt_end_dt,
        b.ar_id,
        CASE
            WHEN s.acct_typ = 13 AND s.acct_cls = 10     AND d.flt_pr_tm_trnsctn = 3 THEN 'RBC Student Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls = 10     AND d.flt_pr_tm_trnsctn = 4 THEN 'RBC No Limit Banking for Students'
            WHEN s.acct_typ = 13 AND s.acct_cls = 0      AND d.flt_pr_tm_trnsctn = 2 THEN 'RBC Day to Day Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls = 0      AND d.flt_pr_tm_trnsctn = 4 THEN 'RBC No Limit Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls IN (8,9)  AND d.flt_pr_tm_trnsctn = 0 THEN 'RBC Signature No Limit Banking'
        END AS from_product
    FROM cohort c
    INNER JOIN DDWV01.CLNT_AR_RELTN_DLY b
        ON  b.clnt_no    = c.clnt_no
        AND b.dw_srvc_id = 1
        AND b.snap_dt    = c.treatmt_strt_dt - 1
    INNER JOIN DDWV01.AR_STATIC_DLY s
        ON  s.ar_id          = b.ar_id
        AND s.snap_dt        = b.snap_dt
        AND s.srvc_id        = 1
        AND s.open_cls_sts   = 'O'
        AND s.acct_typ       = 13
        AND s.acct_cls IN (0,8,9,10)
    INNER JOIN DDWV01.DEPOSIT_ACCOUNT_DLY d
        ON  d.ar_id      = b.ar_id
        AND d.snap_dt    = b.snap_dt
        AND d.dw_srvc_id = 1
    WHERE b.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
      AND s.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
      AND d.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
),

success_total AS (
    SELECT
        p.cohort_month, p.cohort_arm,
        COUNT(DISTINCT p.clnt_no) AS responders
    FROM precamp_product p
    INNER JOIN DDWV01.DEP_ACCT_SW_DLY sw
        ON  sw.ar_id            = p.ar_id
        AND sw.acct_sw_proc_dt BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
    INNER JOIN pba_lkup_curr tl
        ON  tl.acct_typ_cd     = sw.to_acct_typ
        AND tl.acct_clss_cd    = sw.to_acct_clss
        AND tl.srvc_fee_opt_cd = sw.to_fee_opt
    WHERE sw.acct_sw_proc_dt BETWEEN (SELECT min_dt FROM cohort_window)
                                 AND (SELECT max_dt FROM cohort_window)
      AND (
        (p.from_product = 'RBC Student Banking'
         AND tl.prod_en_nm IN ('RBC No Limit Banking for Students','RBC Day to Day Banking','RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking'))
        OR (p.from_product = 'RBC No Limit Banking for Students'
         AND tl.prod_en_nm IN ('RBC Student Banking','RBC Day to Day Banking','RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking'))
        OR (p.from_product = 'RBC Day to Day Banking'
         AND tl.prod_en_nm IN ('RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking'))
        OR (p.from_product = 'RBC No Limit Banking'
         AND tl.prod_en_nm IN ('RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking'))
        OR (p.from_product = 'RBC Signature No Limit Banking'
         AND tl.prod_en_nm = 'RBC VIP Banking')
      )
    GROUP BY 1,2
)

SELECT
    CAST('CTU'     AS VARCHAR(50)) AS campaign,
    p.cohort_month                 AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    CAST('ALL'     AS VARCHAR(50)) AS test_control_flag,
    p.cohort_arm,
    p.total_population,
    COALESCE(r.responders, 0) AS responders
FROM population p
LEFT JOIN success_total r
    ON  r.cohort_month = p.cohort_month
    AND r.cohort_arm   = p.cohort_arm
ORDER BY 2, 6
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P                                                              ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month,
        TRIM(rpt_grp_cd) AS rpt_grp_cd,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag,
        CASE
            WHEN TRIM(rpt_grp_cd) IN (
                'PO2PNL01','PO2PNL03','PO2PNL07',
                'PO2POT01','PO2POT03','PO2POT07',
                'PO2PPR01','PO2PPR03','PO2PPR07'
            ) THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm,
        CASE WHEN TRIM(tactic_cell_cd) LIKE '%MB%' THEN 1 ELSE 0 END AS is_mobile
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026099O2P','2026126O2P','2026132O2P')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

cohort AS (
    SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt,
           cohort_month, rpt_grp_cd, test_control_flag, cohort_arm,
           MAX(is_mobile) AS is_mobile
    FROM cohort_raw
    GROUP BY 1,2,3,4,5,6,7
),

population AS (
    SELECT cohort_month, rpt_grp_cd, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no)                                  AS total_population,
           COUNT(DISTINCT CASE WHEN is_mobile = 1 THEN clnt_no END) AS mobile_population
    FROM cohort
    GROUP BY 1,2,3,4
),

applications AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN     AS a
    JOIN DDWV01.OVRL_CR_APP            AS b
        ON  b.cr_app_id  = a.cr_app_id
        AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN AS c
        ON  c.cr_app_id          = a.cr_app_id
        AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id         = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD            AS d
        ON  d.cr_app_id          = c.cr_app_id
        AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id         = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN (32,37,45,47,51,56,62)
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),

success_total AS (
    SELECT
        c.cohort_month, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm,
        COUNT(DISTINCT c.clnt_no) AS responders
    FROM cohort c
    INNER JOIN applications a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    GROUP BY 1,2,3,4
),

base AS (
    SELECT
        p.cohort_month, p.rpt_grp_cd, p.test_control_flag, p.cohort_arm,
        p.total_population, p.mobile_population,
        COALESCE(r.responders, 0) AS responders
    FROM population p
    LEFT JOIN success_total r
        ON  r.cohort_month      = p.cohort_month
        AND r.rpt_grp_cd        = p.rpt_grp_cd
        AND r.test_control_flag = p.test_control_flag
        AND r.cohort_arm        = p.cohort_arm
)

SELECT
    CAST('O2P' AS VARCHAR(50))     AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm,
    SUM(total_population)  AS total_population,
    SUM(mobile_population) AS mobile_population,
    SUM(responders)        AS responders
FROM base
GROUP BY cohort_month, test_control_flag, cohort_arm

UNION ALL

SELECT
    CAST('O2P' AS VARCHAR(50))          AS campaign,
    cohort_month                        AS cohort,
    CAST('REPORT_GROUP' AS VARCHAR(50)) AS segment,
    rpt_grp_cd                          AS segment_level,
    test_control_flag, cohort_arm,
    total_population, mobile_population,
    responders
FROM base
ORDER BY 2, 3, 4, 5, 6
;
