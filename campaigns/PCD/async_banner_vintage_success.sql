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
        CASE WHEN channel_deploy_mb = 'Y' THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,
        CASE
            WHEN TRIM(act_ctl_seg) IN ('Control','C','CONTROL') THEN 'CONTROL'
            WHEN TRIM(act_ctl_seg) IN ('Action','A','ACTION','Test','TEST') THEN 'TEST'
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
           COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target
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
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
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
        SUM(responders_target)                AS responders_target
    FROM base
    GROUP BY cohort_month, test_control_flag, cohort_arm, vintage_day

    UNION ALL

    SELECT
        cohort_month                          AS cohort,
        CAST('PRODUCT' AS VARCHAR(50))        AS segment,
        product_at_decision                   AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        total_population,
        responders, responders_target
    FROM base
)

SELECT
    CAST('PCD' AS VARCHAR(50)) AS campaign,
    cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders, responders_target,
    SUM(responders) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum,
    SUM(responders_target) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_target_cum
FROM final_grain
ORDER BY 2, 3, 4, 5, 6, 7
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU                                                              ║
-- ║ tactic_id: 2026098CTU. No test/control design → test_control_flag='ALL'.   ║
-- ║ cohort_arm: ASYNC if SUBSTR(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'.  ║
-- ║ Both arms get a responder count (precamp_product chain).                   ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT (calendar_date - DATE '2026-04-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-01' + 60
),

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

switches_with_product AS (
    SELECT
        p.clnt_no, p.cohort_month, p.cohort_arm, p.treatmt_strt_dt, p.from_product,
        sw.acct_sw_proc_dt AS switch_dt,
        tl.prod_en_nm      AS latest_to_product
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
),

success_events AS (
    SELECT clnt_no, cohort_month, cohort_arm, treatmt_strt_dt,
           MIN(switch_dt) AS first_switch_dt
    FROM switches_with_product
    WHERE (
        (from_product = 'RBC Student Banking'
         AND latest_to_product IN ('RBC No Limit Banking for Students','RBC Day to Day Banking','RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking'))
        OR (from_product = 'RBC No Limit Banking for Students'
         AND latest_to_product IN ('RBC Student Banking','RBC Day to Day Banking','RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking'))
        OR (from_product = 'RBC Day to Day Banking'
         AND latest_to_product IN ('RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking'))
        OR (from_product = 'RBC No Limit Banking'
         AND latest_to_product IN ('RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking'))
        OR (from_product = 'RBC Signature No Limit Banking'
         AND latest_to_product = 'RBC VIP Banking')
    )
    GROUP BY 1,2,3,4
),

success_daily AS (
    SELECT cohort_month, cohort_arm,
           (first_switch_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM success_events
    WHERE (first_switch_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3
),

spine AS (
    SELECT p.cohort_month, p.cohort_arm, v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r.responders, 0) AS responders
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.cohort_month = s.cohort_month
        AND r.cohort_arm   = s.cohort_arm
        AND r.vintage_day  = s.vintage_day
)

SELECT
    CAST('CTU'     AS VARCHAR(50)) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    CAST('ALL'     AS VARCHAR(50)) AS test_control_flag,
    cohort_arm, vintage_day,
    total_population,
    responders,
    SUM(responders) OVER (
        PARTITION BY cohort_month, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM base
ORDER BY cohort_month, cohort_arm, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P                                                              ║
-- ║ tactic_ids: 2026099O2P, 2026126O2P, 2026132O2P (suffix is letter O).       ║
-- ║ TG4=TEST, TG7=CONTROL. cohort_arm: ASYNC if RPT_GRP_CD IN (9 PO2P codes).  ║
-- ║ Both arms get a responder count (CR_APP chain).                            ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT (calendar_date - DATE '2026-04-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2026-04-01' AND DATE '2026-04-01' + 60
),

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

success_events AS (
    SELECT c.cohort_month, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm,
           c.clnt_no, c.treatmt_strt_dt,
           MIN(a.app_dt) AS first_app_dt
    FROM cohort c
    INNER JOIN applications a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    GROUP BY 1,2,3,4,5,6
),

success_daily AS (
    SELECT cohort_month, rpt_grp_cd, test_control_flag, cohort_arm,
           (first_app_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM success_events
    WHERE (first_app_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),

spine AS (
    SELECT p.cohort_month, p.rpt_grp_cd, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population, p.mobile_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.rpt_grp_cd, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population, s.mobile_population,
        COALESCE(r.responders, 0) AS responders
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.cohort_month      = s.cohort_month
        AND r.rpt_grp_cd        = s.rpt_grp_cd
        AND r.test_control_flag = s.test_control_flag
        AND r.cohort_arm        = s.cohort_arm
        AND r.vintage_day       = s.vintage_day
),

final_grain AS (
    SELECT
        cohort_month                          AS cohort,
        CAST('ALL'     AS VARCHAR(50))        AS segment,
        CAST('OVERALL' AS VARCHAR(50))        AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        SUM(total_population)                 AS total_population,
        SUM(mobile_population)                AS mobile_population,
        SUM(responders)                       AS responders
    FROM base
    GROUP BY cohort_month, test_control_flag, cohort_arm, vintage_day

    UNION ALL

    SELECT
        cohort_month                          AS cohort,
        CAST('REPORT_GROUP' AS VARCHAR(50))   AS segment,
        rpt_grp_cd                            AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        total_population, mobile_population,
        responders
    FROM base
)

SELECT
    CAST('O2P' AS VARCHAR(50)) AS campaign,
    cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day,
    total_population, mobile_population,
    responders,
    SUM(responders) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM final_grain
ORDER BY cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day
;
