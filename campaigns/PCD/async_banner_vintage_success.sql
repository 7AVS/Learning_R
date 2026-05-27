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
-- ║ BLOCK 1 — PCD                                                              ║
-- ║ tactic_ids: 2026111PCD, 2026125PCD. tst_grp_cd ends in T/C.                ║
-- ║ cohort_arm: ASYNC if position-3 of tactic_decisn_vrb_info IN (allowlist).   ║
-- ║ ASYNC responders only — NON_ASYNC responders are NULL.                     ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH RECURSIVE
vintage_days (vintage_day) AS (
    SELECT 0 AS vintage_day
    UNION ALL
    SELECT vintage_day + 1 FROM vintage_days WHERE vintage_day < 60
),

cohort_raw AS (
    SELECT
        clnt_no,
        visa_acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month,
        STRTOK(REGEXP_REPLACE(TRIM(tactic_decisn_vrb_info), ' +', ' '), ' ', 4) AS product_mnemonic,
        CASE
            WHEN TRIM(COALESCE(tst_grp_cd, '')) LIKE '%C' THEN 'CONTROL'
            WHEN TRIM(COALESCE(tst_grp_cd, '')) LIKE '%T' THEN 'TEST'
        END AS test_control_flag,
        CASE
            WHEN STRTOK(REGEXP_REPLACE(TRIM(tactic_decisn_vrb_info), ' +', ' '), ' ', 3)
                IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
            THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm,
        SUBSTR(tactic_decisn_vrb_info, 42, 3) AS from_product_code
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD','2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND (TRIM(COALESCE(tst_grp_cd, '')) LIKE '%T'
           OR TRIM(COALESCE(tst_grp_cd, '')) LIKE '%C')
),

cohort AS (
    SELECT DISTINCT
        clnt_no, visa_acct_no, treatmt_strt_dt, treatmt_end_dt,
        cohort_month, product_mnemonic, test_control_flag, cohort_arm, from_product_code
    FROM cohort_raw
),

population AS (
    SELECT cohort_month, product_mnemonic, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3,4
),

success_events AS (
    SELECT
        c.cohort_month, c.product_mnemonic, c.test_control_flag, c.clnt_no, c.treatmt_strt_dt,
        MIN(dfp.dt_record_ext) AS first_change_dt
    FROM cohort c
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO dfp
        ON  dfp.acct_no = c.visa_acct_no
        AND dfp.dt_record_ext BETWEEN c.treatmt_strt_dt - 1 AND c.treatmt_end_dt
        AND dfp.visa_prod_cd <> c.from_product_code
    WHERE c.cohort_arm = 'ASYNC'
      AND dfp.dt_record_ext >= DATE '2026-04-01'
      AND dfp.dt_record_ext <= DATE '2026-07-31'
    GROUP BY 1,2,3,4,5
),

success_daily AS (
    SELECT cohort_month, product_mnemonic, test_control_flag,
           (first_change_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM success_events
    WHERE (first_change_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4
),

spine AS (
    SELECT p.cohort_month, p.product_mnemonic, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.product_mnemonic, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        CASE WHEN s.cohort_arm = 'ASYNC' THEN COALESCE(r.responders, 0)
             ELSE CAST(NULL AS BIGINT) END AS responders
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.cohort_month      = s.cohort_month
        AND r.product_mnemonic  = s.product_mnemonic
        AND r.test_control_flag = s.test_control_flag
        AND r.vintage_day       = s.vintage_day
),

final_grain AS (
    SELECT
        cohort_month                          AS cohort,
        CAST('ALL'     AS VARCHAR(50))        AS segment,
        CAST('OVERALL' AS VARCHAR(50))        AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        SUM(total_population)                 AS total_population,
        SUM(responders)                       AS responders
    FROM base
    GROUP BY cohort_month, test_control_flag, cohort_arm, vintage_day

    UNION ALL

    SELECT
        cohort_month                          AS cohort,
        CAST('PRODUCT' AS VARCHAR(50))        AS segment,
        product_mnemonic                      AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        total_population,
        responders
    FROM base
)

SELECT
    CAST('PCD' AS VARCHAR(50)) AS campaign,
    cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders,
    SUM(responders) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum
FROM final_grain
ORDER BY cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU                                                              ║
-- ║ tactic_id: 2026098CTU. No test/control design → test_control_flag='ALL'.   ║
-- ║ cohort_arm: ASYNC if SUBSTR(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'.  ║
-- ║ Both arms get a responder count (precamp_product chain).                   ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH RECURSIVE
vintage_days (vintage_day) AS (
    SELECT 0 AS vintage_day
    UNION ALL
    SELECT vintage_day + 1 FROM vintage_days WHERE vintage_day < 60
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

WITH RECURSIVE
vintage_days (vintage_day) AS (
    SELECT 0 AS vintage_day
    UNION ALL
    SELECT vintage_day + 1 FROM vintage_days WHERE vintage_day < 60
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
