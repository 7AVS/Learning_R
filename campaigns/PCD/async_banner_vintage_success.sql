-- async_banner_vintage_success.sql
-- Engine: Teradata NATIVE. No federation, no GA4, no Trino syntax.
-- Purpose: Success-only vintage curves (responders) for PCD, CTU, O2P.
--   No engagement columns. Vintage day 0-60, cumulative window functions
--   partitioned by (cohort, segment, segment_level, test_control_flag, cohort_arm).
-- Sources:
--   PCD  — dl_mr_prod.cards_pcd_ongoing_decis_resp (curated)
--   CTU  — dl_mr_prod.nbo_pba_upgrade (curated)
--   O2P  — DG6V01.TACTIC_EVNT_IP_AR_HIST + DDWV01.CR_APP_CLNT_RELTN/OVRL_CR_APP/
--           CR_APP_CLNT_PROD_RELTN/CR_APP_PROD (raw CR_APP chain — O2P not in curated)
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
-- ║ Also emits nibt_value_target (SUM nibt_expected_value for targetproduct     ║
-- ║ responders) and nibt_value_upgrade (SUM nibt_expec_value_upgradepath for   ║
-- ║ upgrade-path responders), both raw and cumulative.                         ║
-- ║                                                                            ║
-- ║ test_control_flag derived from act_ctl_seg in the curated table. If those  ║
-- ║ values look wrong, swap for a join back to TACTIC_EVNT_IP_AR_HIST.tst_grp_cd.║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    -- 0..60 integer series; date anchor is arbitrary, NOT a campaign launch date
    -- (vintage_day is anchored per cohort downstream via the JOIN condition).
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 60
),

cohort AS (
    SELECT
        acct_no,
        clnt_no,
        tactic_id_parent,
        response_start,
        response_end,
        response_start AS wave_dt,
        product_at_decision,
        target_product,
        new_product,
        dt_prod_change,
        responder_anyproduct,
        responder_targetproduct,
        responder_upgrade_path,
        nibt_expected_value,
        nibt_expec_value_upgradepath,
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
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start >= DATE '2026-04-01'
),

population AS (
    SELECT wave_dt, product_at_decision, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    WHERE test_control_flag IS NOT NULL
    GROUP BY 1,2,3,4
),

success_daily AS (
    SELECT wave_dt, product_at_decision, test_control_flag, cohort_arm,
           (dt_prod_change - response_start) AS vintage_day,
           COUNT(DISTINCT CASE WHEN responder_anyproduct    = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target,
           COUNT(DISTINCT CASE WHEN responder_upgrade_path  = 1 THEN clnt_no END) AS responders_upgrade,
           SUM(CASE WHEN responder_targetproduct = 1 THEN nibt_expected_value      END) AS nibt_value_target,
           SUM(CASE WHEN responder_upgrade_path = 1 THEN nibt_expec_value_upgradepath END) AS nibt_value_upgrade
    FROM cohort
    WHERE test_control_flag IS NOT NULL
      AND dt_prod_change IS NOT NULL
      AND (dt_prod_change - response_start) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),

spine AS (
    SELECT p.wave_dt, p.product_at_decision, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.wave_dt, s.product_at_decision, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r.responders,         0) AS responders,
        COALESCE(r.responders_target,  0) AS responders_target,
        COALESCE(r.responders_upgrade, 0) AS responders_upgrade,
        COALESCE(r.nibt_value_target,   0) AS nibt_value_target,
        COALESCE(r.nibt_value_upgrade,  0) AS nibt_value_upgrade
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.wave_dt              = s.wave_dt
        AND r.product_at_decision  = s.product_at_decision
        AND r.test_control_flag    = s.test_control_flag
        AND r.cohort_arm           = s.cohort_arm
        AND r.vintage_day          = s.vintage_day
),

final_grain AS (
    SELECT
        wave_dt                               AS cohort,
        CAST('ALL'     AS VARCHAR(50))        AS segment,
        CAST('OVERALL' AS VARCHAR(50))        AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        SUM(total_population)                 AS total_population,
        SUM(responders)                       AS responders,
        SUM(responders_target)                AS responders_target,
        SUM(responders_upgrade)               AS responders_upgrade,
        SUM(nibt_value_target)                AS nibt_value_target,
        SUM(nibt_value_upgrade)               AS nibt_value_upgrade
    FROM base
    GROUP BY wave_dt, test_control_flag, cohort_arm, vintage_day

    UNION ALL

    SELECT
        wave_dt                               AS cohort,
        CAST('PRODUCT' AS VARCHAR(50))        AS segment,
        product_at_decision                   AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        total_population,
        responders, responders_target, responders_upgrade,
        nibt_value_target, nibt_value_upgrade
    FROM base
)

SELECT
    CAST('PCD' AS VARCHAR(50)) AS campaign,
    cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders, responders_target, responders_upgrade,
    nibt_value_target, nibt_value_upgrade,
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
    ) AS responders_upgrade_cum,
    SUM(nibt_value_target) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS nibt_value_target_cum,
    SUM(nibt_value_upgrade) OVER (
        PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS nibt_value_upgrade_cum
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
    -- 0..60 integer series; date anchor is arbitrary, NOT a campaign launch date
    -- (vintage_day is anchored per cohort downstream via the JOIN condition).
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 60
),

cohort AS (
    SELECT
        clnt_no,
        tactic_id,
        treatmt_strt_dt,
        treatmt_end_dt,
        treatmt_strt_dt AS wave_dt,
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
    SELECT wave_dt, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3
),

success_daily AS (
    SELECT wave_dt, test_control_flag, cohort_arm,
           (response_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT CASE WHEN success          = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN primary_success  = 1 THEN clnt_no END) AS responders_target
    FROM cohort
    WHERE response_dt IS NOT NULL
      AND (response_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4
),

spine AS (
    SELECT p.wave_dt, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.wave_dt, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r.responders,        0) AS responders,
        COALESCE(r.responders_target, 0) AS responders_target
    FROM spine s
    LEFT JOIN success_daily r
        ON  r.wave_dt           = s.wave_dt
        AND r.test_control_flag = s.test_control_flag
        AND r.cohort_arm        = s.cohort_arm
        AND r.vintage_day       = s.vintage_day
)

SELECT
    CAST('CTU'     AS VARCHAR(50)) AS campaign,
    wave_dt                        AS cohort,
    CAST('ALL'     AS VARCHAR(50)) AS segment,
    CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
    test_control_flag, cohort_arm, vintage_day,
    total_population,
    responders, responders_target,
    SUM(responders) OVER (
        PARTITION BY wave_dt, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_cum,
    SUM(responders_target) OVER (
        PARTITION BY wave_dt, test_control_flag, cohort_arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS responders_target_cum
FROM base
ORDER BY 2, 5, 6, 7
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P                                                              ║
-- ║ tactic_ids: 2026099O2P, 2026126O2P, 2026132O2P (suffix is letter O).       ║
-- ║ TG4=TEST, TG7=CONTROL. cohort_arm: ASYNC if RPT_GRP_CD IN (9 PO2P codes).  ║
-- ║ Both arms get a responder count (CR_APP chain).                            ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    -- 0..60 integer series; date anchor is arbitrary, NOT a campaign launch date
    -- (vintage_day is anchored per cohort downstream via the JOIN condition).
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 60
),

cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        treatmt_strt_dt AS wave_dt,
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
        END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

cohort AS (
    SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_end_dt,
           wave_dt, rpt_grp_cd, test_control_flag, cohort_arm
    FROM cohort_raw
),

population AS (
    SELECT wave_dt, rpt_grp_cd, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3,4
),

-- O2P conversion = ONE fresh daily snapshot (captr_dt is cumulative; latest captr_dt holds full history). Pinned to latest captr_dt; no accumulation. Base tables lag ~4 weeks.
applications AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt, d.appl_for_prod_typ
    FROM DDWV01.CR_APP_CLNT_RELTN_DLY      AS a
    JOIN DDWV01.OVRL_CR_APP_DLY            AS b
        ON  b.cr_app_id = a.cr_app_id AND b.sys_src_id = a.sys_src_id AND b.captr_dt = a.captr_dt
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN_DLY AS c
        ON  c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id = a.sys_src_id AND c.captr_dt = a.captr_dt
    JOIN DDWV01.CR_APP_PROD_DLY            AS d
        ON  d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id = c.sys_src_id AND d.captr_dt = c.captr_dt
    WHERE a.captr_dt = (SELECT MAX(captr_dt) FROM DDWV01.CR_APP_PROD_DLY WHERE captr_dt >= DATE '2026-06-01')
      AND b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN (32,37,45,47,51,56,62)
      AND d.prod_app_compl_dt IS NOT NULL
),

success_events AS (
    SELECT c.wave_dt, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm,
           c.clnt_no, c.treatmt_strt_dt,
           MIN(a.app_dt)                                              AS first_app_dt,
           MIN(CASE WHEN a.appl_for_prod_typ = '43' THEN a.app_dt END) AS first_app_dt_target
    FROM cohort c
    INNER JOIN applications a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_strt_dt + 60
    GROUP BY 1,2,3,4,5,6
),

responders_daily AS (
    SELECT wave_dt, rpt_grp_cd, test_control_flag, cohort_arm,
           (first_app_dt - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM success_events
    WHERE (first_app_dt - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),

responders_target_daily AS (
    SELECT wave_dt, rpt_grp_cd, test_control_flag, cohort_arm,
           (first_app_dt_target - treatmt_strt_dt) AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders_target
    FROM success_events
    WHERE first_app_dt_target IS NOT NULL
      AND (first_app_dt_target - treatmt_strt_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),

spine AS (
    SELECT p.wave_dt, p.rpt_grp_cd, p.test_control_flag, p.cohort_arm,
           v.vintage_day, p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.wave_dt, s.rpt_grp_cd, s.test_control_flag, s.cohort_arm, s.vintage_day,
        s.total_population,
        COALESCE(r1.responders,        0) AS responders,
        COALESCE(r2.responders_target, 0) AS responders_target
    FROM spine s
    LEFT JOIN responders_daily r1
        ON  r1.wave_dt           = s.wave_dt
        AND r1.rpt_grp_cd        = s.rpt_grp_cd
        AND r1.test_control_flag = s.test_control_flag
        AND r1.cohort_arm        = s.cohort_arm
        AND r1.vintage_day       = s.vintage_day
    LEFT JOIN responders_target_daily r2
        ON  r2.wave_dt           = s.wave_dt
        AND r2.rpt_grp_cd        = s.rpt_grp_cd
        AND r2.test_control_flag = s.test_control_flag
        AND r2.cohort_arm        = s.cohort_arm
        AND r2.vintage_day       = s.vintage_day
),

final_grain AS (
    SELECT
        wave_dt                               AS cohort,
        CAST('ALL'     AS VARCHAR(50))        AS segment,
        CAST('OVERALL' AS VARCHAR(50))        AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        SUM(total_population)                 AS total_population,
        SUM(responders)                       AS responders,
        SUM(responders_target)                AS responders_target
    FROM base
    GROUP BY wave_dt, test_control_flag, cohort_arm, vintage_day

    UNION ALL

    SELECT
        wave_dt                               AS cohort,
        CAST('REPORT_GROUP' AS VARCHAR(50))   AS segment,
        rpt_grp_cd                            AS segment_level,
        test_control_flag, cohort_arm, vintage_day,
        total_population,
        responders, responders_target
    FROM base
)

SELECT
    CAST('O2P' AS VARCHAR(50)) AS campaign,
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
ORDER BY cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day
;
