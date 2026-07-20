-- =============================================================================
-- ASYNC BANNER VINTAGE SQL -- CTU / O2P / PCD (A/B holdout logic)
-- Transcribed 2026-07-20 from 10 phone photos (pics/PXL_20260720_2005*-2007*).
-- Teradata-direct vintage queries with NEW A/B holdout logic for CTU
-- (deployment 2) and O2P (new deployment).
--
-- STATUS: DRAFT TRANSCRIPTION ONLY. NOT yet verified against source or run.
-- Every [VERIFY] / [UNCLEAR] / [OCR-FIX] inline tag marks something Andre
-- must confirm against the real source before this is run anywhere.
--
-- Three independent statements below, each a standalone SQL statement
-- ending in ';': BLOCK CTU, BLOCK O2P, BLOCK PCD.
-- =============================================================================


-- =============================================================================
-- BLOCK 2 -- CTU vintage with A/B experiment support
-- =============================================================================
-- DEPLOYMENT 1 (20260908CTU): curated table, ASYNC + NON_ASYNC, no control   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
-- Deployment 2 (20261610CTU): ASYNC arm via tactic event (_A=TEST/_B=CTRL)   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
--                NON_ASYNC arm via curated (chnl_mb=0), no A/B
-- Success sourced from dl_mr_prod.nbo_pba_upgrade
WITH
vintage_days AS (
  SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
  FROM sys_calendar.calendar
  WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 60
),
cohort AS (
  -- DEPLOYMENT 1 (20260908CTU): curated table, no A/B control
  -- Arms: ASYNC (chnl_mb=1) vs NON_ASYNC (chnl_mb=0), test_control_flag='ALL'
  SELECT a.clnt_no, a.tactic_id, a.treatmt_strt_dt, a.treatmt_end_dt,
    a.treatmt_strt_dt AS wave_dt,
    CASE WHEN a.chnl_mb = 1 THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,
    CAST('ALL' AS VARCHAR(50)) AS test_control_flag,
    a.success, a.primary_success, a.response_dt
  FROM dl_mr_prod.nbo_pba_upgrade a
  WHERE a.tactic_id = '20260908CTU'   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
  UNION ALL
  -- DEPLOYMENT 2 (20261610CTU) ASYNC ARM: A/B experiment from tactic event
  -- Population from tactic_event_table; success from curated via LEFT JOIN
  -- _A = TEST (received async banner), _B = CONTROL (held out from async)
  SELECT t.clnt_no, t.tactic_id, t.treatmt_strt_dt, t.treatmt_end_dt,
    t.treatmt_strt_dt AS wave_dt,
    CAST('ASYNC' AS VARCHAR(50)) AS cohort_arm,
    CASE
      WHEN TRIM(t.tst_grp_cd) IN ('T_EDA0_A','T_ANA3_A','T_ONS1_A','T_ADAS_A','T_EDA1_A','T_ONS6_A','T_ANA6_A','T_EDA3_A','T_ADAG_A','T_ANA1_A','T_ONS3_A','TADWM1_A','TADWMS_A') THEN 'TEST'   -- [VERIFY: tst_grp_cd IN-list OCR uncertain]
      WHEN TRIM(t.tst_grp_cd) IN ('T_EDA6_B','T_ANA3_B','T_ONS1_B','T_ADAS_B','T_EDA1_B','T_ONS6_B','T_ANA6_B','T_EDA3_B','T_ADAG_B','T_ANA1_B','T_ONS3_B','TADWMJ_B','TADWMS_B') THEN 'CONTROL'   -- [VERIFY: tst_grp_cd IN-list OCR uncertain]
    END AS test_control_flag,
    u.success, u.primary_success
    -- [VERIFY: dep2 ASYNC join ON-clause and response_dt source not visible in photos]
  FROM [UNCLEAR: source table for alias 't' not visible in photos - header comment says "tactic event" table] t
    LEFT JOIN dl_mr_prod.nbo_pba_upgrade u
  WHERE t.tactic_id = '20261610CTU'   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
    AND TRIM(t.tst_grp_cd) IN ('T_EDA0_A','T_ANA3_A','T_ONS1_A','T_ADAS_A','T_EDA1_A','T_ONS6_A','T_ANA6_A','T_EDA3_A','T_ADAG_A','T_ANA1_A','T_ONS3_A','TADWM1_A','TADWMS_A','T_EDA6_B','T_ANA3_B','T_ONS1_B','T_ADAS_B','T_EDA1_B','T_ONS6_B','T_ANA6_B','T_EDA3_B','T_ADAG_B','T_ANA1_B','T_ONS3_B','TADWMJ_B','TADWMS_B')   -- [VERIFY: tst_grp_cd IN-list OCR uncertain]
  UNION ALL
  -- DEPLOYMENT 2 (20261610CTU) NON_ASYNC ARM: from curated, no A/B split
  SELECT a.clnt_no, a.tactic_id, a.treatmt_strt_dt, a.treatmt_end_dt,
    a.treatmt_strt_dt AS wave_dt,
    CAST('NON_ASYNC' AS VARCHAR(50)) AS cohort_arm,
    CAST('ALL' AS VARCHAR(50)) AS test_control_flag,
    a.success, a.primary_success, a.response_dt
  FROM dl_mr_prod.nbo_pba_upgrade a
  WHERE a.tactic_id = '20261610CTU' AND a.chnl_mb = 0   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
),
population AS (
  SELECT wave_dt, test_control_flag, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
  FROM cohort GROUP BY 1,2,3
),
success_daily AS (
  SELECT wave_dt, test_control_flag, cohort_arm,
    (response_dt - treatmt_strt_dt) AS vintage_day,
    COUNT(DISTINCT CASE WHEN success = 1 THEN clnt_no END) AS responders,
    COUNT(DISTINCT CASE WHEN primary_success = 1 THEN clnt_no END) AS responders_target
  FROM cohort
  WHERE response_dt IS NOT NULL AND (response_dt - treatmt_strt_dt) BETWEEN 0 AND 60
  GROUP BY 1,2,3,4
),
spine AS (
  SELECT p.wave_dt, p.test_control_flag, p.cohort_arm, v.vintage_day, p.total_population
  FROM population p CROSS JOIN vintage_days v
),
base AS (
  SELECT s.wave_dt, s.test_control_flag, s.cohort_arm, s.vintage_day, s.total_population,
    COALESCE(r.responders, 0) AS responders,
    COALESCE(r.responders_target, 0) AS responders_target
  FROM spine s
  LEFT JOIN success_daily r
    ON r.wave_dt = s.wave_dt AND r.test_control_flag = s.test_control_flag
    AND r.cohort_arm = s.cohort_arm AND r.vintage_day = s.vintage_day
)
SELECT CAST('CTU' AS VARCHAR(50)) AS campaign, wave_dt AS cohort,
  CAST('ALL' AS VARCHAR(50)) AS segment, CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
  test_control_flag, cohort_arm, vintage_day, total_population, responders, responders_target,
  SUM(responders) OVER (PARTITION BY wave_dt, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
  SUM(responders_target) OVER (PARTITION BY wave_dt, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum
FROM base
ORDER BY 2, 5, 6, 7
;


-- =============================================================================
-- BLOCK 3 - O2P
-- =============================================================================
-- OLD (20260902P, 20261302P): TG4=TEST, TG7=CONTROL   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
-- NEW (20261602P): TG4 split 47.5% MB_CHAMPION / 47.5% NCM_MB_CHALLENGER / TG7 5% HOLDOUT.   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
--   Uses TACTIC_CELL_CD LIKE '4985', cohort_arm: ASYNC if RPT_GRP_CD IN (9 PO2P codes).   -- [VERIFY: has_mb / TACTIC_CELL_CD pattern unclear across photos]
--   Both arms get a responder_count (CB_APP_CHAIN).   -- [VERIFY: "CB_APP_CHAIN" reference not reconciled with rest of query - meaning/usage unclear]
DROP TABLE o2p_conv_vt;
-- Step A: materialize O2P converters ONCE into a volatile table.
CREATE VOLATILE TABLE o2p_conv_vt AS (
  SELECT a.clnt_no, d.prod_app_dt AS app_dt, d.appl_for_prod_typ
  FROM DDWV01.CR_APP_CLNT_RELTN_DLY AS a
  JOIN DDWV01.OVRL_CR_APP_DLY AS b ON b.cr_app_id = a.cr_app_id AND b.sys_src_id = a.sys_src_id
  JOIN DDWV01.CR_APP_PROD_RELTN_DLY AS c ON c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no AND c.sys_src_id = a.sys_src_id
  JOIN DDWV01.CR_APP_PROD_DLY AS d ON d.cr_app_id = c.cr_app_id AND d.cr_prod_seq_no = c.cr_prod_seq_no AND d.sys_src_id = c.sys_src_id
  WHERE b.app_typ = 'P' AND d.appl_for_prod_typ IN ('40','41','43')
    AND d.prod_app_sts_cd IN (32,37,45,47,51,56,62)
    AND d.prod_app_compl_dt IS NOT NULL AND d.prod_app_compl_dt >= DATE '2026-04-01'
) WITH DATA PRIMARY INDEX (clnt_no) ON COMMIT PRESERVE ROWS;
-- Step B: the vintage
WITH
vintage_days AS (
  SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
  FROM sys_calendar.calendar
  WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 60
),
cohort AS (
  -- OLD DEPLOYMENTS (20260902P, 20261302P): TG4=TEST, TG7=CONTROL   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
  SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_end_dt, treatmt_strt_dt AS wave_dt,
    CASE WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST' WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL' END AS test_control_flag,
    CASE WHEN TRIM(rpt_grp_cd) IN ('PO2PMLO1','PO2PMLO3','PO2PMLO7','PO2PMTO1','PO2PMTO3','PO2PMTO7','PO2PMPO1','PO2PMPO3','PO2PMPO7') THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm   -- [VERIFY: rpt_grp_cd allowlist may be cut off at screen edge]
  FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
  WHERE tactic_id IN ('20260990Z0P','20261320Z0P')   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
  UNION ALL
  -- NEW DEPLOYMENT (20261680Z0P): 3-arm - MB_CHAMPION / NON_MB_CHALLENGER / HOLDOUT   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
  SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt, treatmt_strt_dt AS wave_dt,
    CASE
      WHEN tst_grp_cd = 'TG7' THEN 'HOLDOUT'
      WHEN tst_grp_cd = 'TG4' AND has_mb = 1 THEN 'MB_CHAMPION'
      WHEN tst_grp_cd = 'TG4' AND has_mb = 0 THEN 'NON_MB_CHALLENGER'
    END AS test_control_flag,
    'ASYNC' AS cohort_arm
  FROM (
    SELECT clnt_no, MIN(treatmt_strt_dt) AS treatmt_strt_dt, MIN(treatmt_end_dt) AS treatmt_end_dt,
      TRIM(tst_grp_cd) AS tst_grp_cd,
      MAX(CASE WHEN TACTIC_CELL_CD LIKE 'XMB' THEN 1 ELSE 0 END) AS has_mb   -- [VERIFY: has_mb / TACTIC_CELL_CD pattern unclear across photos]
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '20261680Z0P'   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
      AND TRIM(rpt_grp_cd) IN ('PO2PHL01','PO2PHL03','PO2PHL07','PO2P0101','PO2P0103','PO2P0107','PO2PPR01','PO2PPR03','PO2PPR07')   -- [VERIFY: rpt_grp_cd allowlist may be cut off at screen edge]
    GROUP BY clnt_no, TRIM(tst_grp_cd)
  ) sub
),
population AS (
  SELECT wave_dt, test_control_flag, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
  FROM cohort GROUP BY 1,2,3
),
success_events AS (
  SELECT c.wave_dt, c.test_control_flag, c.cohort_arm, c.clnt_no, c.treatmt_strt_dt,
    MIN(a.app_dt) AS first_app_dt,
    MIN(CASE WHEN a.appl_for_prod_typ = '43' THEN a.app_dt END) AS first_app_dt_target
  FROM cohort c
  INNER JOIN o2p_conv_vt a
    ON a.clnt_no = c.clnt_no AND a.app_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_strt_dt + 60
  GROUP BY 1,2,3,4,5
),
responders_daily AS (
  SELECT wave_dt, test_control_flag, cohort_arm, (first_app_dt - treatmt_strt_dt) AS vintage_day,
    COUNT(DISTINCT clnt_no) AS responders
  FROM success_events
  WHERE (first_app_dt - treatmt_strt_dt) BETWEEN 0 AND 60
  GROUP BY 1,2,3,4
),
responders_target_daily AS (
  SELECT wave_dt, test_control_flag, cohort_arm, (first_app_dt_target - treatmt_strt_dt) AS vintage_day,
    COUNT(DISTINCT clnt_no) AS responders_target
  FROM success_events
  WHERE first_app_dt_target IS NOT NULL AND (first_app_dt_target - treatmt_strt_dt) BETWEEN 0 AND 60
  GROUP BY 1,2,3,4
),
spine AS (
  SELECT p.wave_dt, p.test_control_flag, p.cohort_arm, v.vintage_day, p.total_population
  FROM population p CROSS JOIN vintage_days v
),
base AS (
  SELECT s.wave_dt, s.test_control_flag, s.cohort_arm, s.vintage_day, s.total_population,
    COALESCE(r1.responders, 0) AS responders,
    COALESCE(r2.responders_target, 0) AS responders_target
  FROM spine s
  LEFT JOIN responders_daily r1 ON r1.wave_dt = s.wave_dt AND r1.test_control_flag = s.test_control_flag AND r1.cohort_arm = s.cohort_arm AND r1.vintage_day = s.vintage_day
  LEFT JOIN responders_target_daily r2 ON r2.wave_dt = s.wave_dt AND r2.test_control_flag = s.test_control_flag AND r2.cohort_arm = s.cohort_arm AND r2.vintage_day = s.vintage_day
)
SELECT CAST('O2P' AS VARCHAR(50)) AS campaign, wave_dt AS cohort,
  test_control_flag, cohort_arm, vintage_day, total_population, responders, responders_target,
  SUM(responders) OVER (PARTITION BY wave_dt, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
  SUM(responders_target) OVER (PARTITION BY wave_dt, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum
FROM base
ORDER BY cohort, test_control_flag, cohort_arm, vintage_day
;


-- =============================================================================
-- BLOCK: PCD vintage (curated table cards_pcd_ongoing_decis_resp)
-- =============================================================================
WITH
vintage_days AS (
  -- 0 to N integer series; date_anchor is arbitrary, NOT a campaign launch date
  -- (vintage_day is anchored per cohort downstream via the JOIN condition).
  SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
  FROM sys_calendar.calendar
  WHERE calendar_date BETWEEN DATE '2000-01-01' AND DATE '2000-01-01' + 60
),
cohort AS (
  SELECT acct_no, clnt_no, tactic_id_parent, response_start, response_end,
    response_start AS wave_dt, product_at_decision, target_product, new_product, dt_prod_change,
    responder_anyproduct, responder_targetproduct, responder_upgrade_path,
    nibt_expected_value, nibt_exec_value_upgradepath,   -- [VERIFY: repo elsewhere spells this "nibt_expec_value_upgradepath" - field name uncertain, do not assume either spelling is correct]
    -- cohort_arm from the campaign code (same allowlist as position-3 in tactic_event)
    CASE WHEN strategy_seg_cd IN ('MISCDVUS3','MAO28CJ5','MAO2E061','MF88LGX6','MF88U3PY') THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,   -- [VERIFY: strategy_seg_cd allowlist may be cut off at screen edge]
    -- test_control_flag overrides the curated team's act_ctl_seg derivation;
    -- our rule: suffix on the raw test_group_code, same logic we use on tst_grp_cd.
    CASE WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL' WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST' END AS test_control_flag
  FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
  WHERE response_start >= DATE '2026-04-01'
),
population AS (
  SELECT wave_dt, product_at_decision, test_control_flag, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
  FROM cohort WHERE test_control_flag IS NOT NULL GROUP BY 1,2,3,4
),
success_daily AS (
  SELECT wave_dt, product_at_decision, test_control_flag, cohort_arm,
    (dt_prod_change - response_start) AS vintage_day,
    COUNT(DISTINCT CASE WHEN responder_anyproduct = 1 THEN clnt_no END) AS responders,
    COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target,
    COUNT(DISTINCT CASE WHEN responder_upgrade_path = 1 THEN clnt_no END) AS responders_upgrade
  FROM cohort
  WHERE test_control_flag IS NOT NULL AND dt_prod_change IS NOT NULL
    AND (dt_prod_change - response_start) BETWEEN 0 AND 60
  GROUP BY 1,2,3,4,5
),
spine AS (
  SELECT p.wave_dt, p.product_at_decision, p.test_control_flag, p.cohort_arm, v.vintage_day, p.total_population
  FROM population p CROSS JOIN vintage_days v
),
base AS (
  SELECT s.wave_dt, s.product_at_decision, s.test_control_flag, s.cohort_arm, s.vintage_day, s.total_population,
    COALESCE(r.responders, 0) AS responders,
    COALESCE(r.responders_target, 0) AS responders_target,
    COALESCE(r.responders_upgrade, 0) AS responders_upgrade
  FROM spine s
  LEFT JOIN success_daily r ON r.wave_dt = s.wave_dt AND r.product_at_decision = s.product_at_decision AND r.test_control_flag = s.test_control_flag AND r.cohort_arm = s.cohort_arm AND r.vintage_day = s.vintage_day
),
-- [VERIFY: final_grain UNION ALL column mapping (cohort/segment/segment_level) is uncertain from photos - reconcile against source]
final_grain AS (
  SELECT wave_dt, CAST('ALL' AS VARCHAR(50)) AS cohort, CAST('OVERALL' AS VARCHAR(50)) AS segment,
    CAST(NULL AS VARCHAR(50)) AS segment_level,   -- [UNCLEAR: OCR gap - segment_level expression not visible for ALL branch]
    test_control_flag, cohort_arm, vintage_day,
    SUM(total_population) AS total_population, SUM(responders) AS responders,
    SUM(responders_target) AS responders_target, SUM(responders_upgrade) AS responders_upgrade
  FROM base
  GROUP BY wave_dt, test_control_flag, cohort_arm, vintage_day
  UNION ALL
  SELECT wave_dt, CAST('PRODUCT' AS VARCHAR(50)) AS cohort, product_at_decision AS segment,
    product_at_decision AS segment_level,   -- [UNCLEAR: OCR - PRODUCT branch column alignment uncertain]
    test_control_flag, cohort_arm, vintage_day,
    total_population, responders, responders_target, responders_upgrade
  FROM base
)
SELECT CAST('PCD' AS VARCHAR(50)) AS campaign,
  cohort, segment, segment_level, test_control_flag, cohort_arm, vintage_day,
  total_population, responders, responders_target, responders_upgrade,
  SUM(responders) OVER (PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
  SUM(responders_target) OVER (PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum,
  SUM(responders_upgrade) OVER (PARTITION BY cohort, segment, segment_level, test_control_flag, cohort_arm ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_upgrade_cum
FROM final_grain
ORDER BY 2, 3, 4, 5, 6, 7
;
