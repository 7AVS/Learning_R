-- value_capture/value_capture_report_v3.sql
-- Teradata-direct rebuild of value_capture_report.sql (v1). Partner-template mapping (DESC/Type/
-- Reference Document/Notes) stays Andre's job in Excel (REDESIGN_SPEC.md).
-- Output: one row per contrast, labelled columns -- experiment | mne | arm_test | arm_ctrl |
--   action_population | control_population | action_response | control_response |
--   action_response_rate | control_response_rate | lift | p_value | n_unmapped | n_arm_conflict.
-- Engine: TERADATA-DIRECT, bare table names, no catalog prefix -- do NOT run through Starburst.
-- Grain: QUARTERLY, client-deduped -- treatmt_end_dt BETWEEN '2026-05-01' AND '2026-07-31',
--   first-touch collapse per clnt_no, ever-success over all in-window rows (v1 logic, unchanged).
-- all_rows contract (9 cols, teammate hook-up point): mne | arm_test | arm_ctrl | n_test | x_test
--   | n_ctrl | x_ctrl | n_unmapped | n_arm_conflict. arm_test/arm_ctrl = raw source codes, never
--   invented prose. n_unmapped = distinct in-window clients whose arm couldn't be resolved
--   (excluded from measurement). n_arm_conflict = distinct clients seen in both arms across waves,
--   resolved to first-touch (still counted once in n_test/n_ctrl -- this is informational).
-- PCQ stratifies by decile INTERNALLY (Simpson's-paradox guard): one row per decile into all_rows,
--   pooled by the stats layer into one output row -- decile itself never appears; emitted counts are
--   simple sums while lift is MH-weighted, so the two won't exactly reconcile for PCQ (expected).
--   n_unmapped/n_arm_conflict are computed ONCE per campaign (not per decile), then CROSS JOINed
--   onto every decile row identically; the pooling step aggregates them with MAX, not SUM, so the
--   decile fan-out never inflates them.
-- Stats: MH-weighted two-proportion z-test; z converted to a two-sided p_value via the Zelen-Severo/
--   Abramowitz-Stegun normal-CDF approximation (Teradata has no NORM.S.DIST). z is internal only.
--
-- v3 ADDS three async blocks (PCD, O2P, CTU), summary-converted from
-- campaigns/PCD/async_banner_vintage_ab.sql -- a transcribed DRAFT (10 phone photos, not yet
-- verified against source or run). Every literal lifted from that draft that carried a [VERIFY]
-- tag keeps the tag here; reconcile in-env before this ships. Only RANDOMIZED arms emit contract
-- rows: PCD ASYNC TEST/CONTROL only (NON_ASYNC is not a randomized comparison, excluded); O2P NEW
-- deployment 3-arm split only (OLD 2-arm TG4/TG7 deployment has no holdout, excluded); CTU
-- deployment-2 ASYNC TEST/CONTROL only (deployment 1 and the NON_ASYNC arm have no control,
-- excluded). arm_test/arm_ctrl for all three are DERIVED LABELS, not raw codes -- async has no
-- single raw code per arm, it's a suffix pattern over many codes (see note at each *_rows CTE).
-- O2P emits TWO contract rows sharing one HOLDOUT control (MB_CHAMPION vs HOLDOUT,
-- NON_MB_CHALLENGER vs HOLDOUT); the stats layer below is unchanged and treats each row as an
-- independent single-stratum test, which is correct since they don't share a GROUP BY key.
--
-- SPOOL + RERUN: the O2P converter (CR_APP 4-table daily join) is materialized into a VOLATILE table
-- BEFORE the main query -- as a plain CTE hit by the EXISTS correlation it re-runs per client and spools
-- (validated source async_banner_summary_success.sql does the same). Volatile tables live only for the
-- session, so a FRESH session needs no DROP -- and a DROP of a non-existent table errors 3807 and can
-- abort the batch, so it is deliberately NOT here.
-- >> RERUNNING IN THE SAME SESSION? Run  DROP TABLE o2p_conv_vt;  by itself FIRST, then this. <<
-- Run this CREATE (+ COLLECT STATS), then the WITH query below, in the SAME session.

CREATE VOLATILE TABLE o2p_conv_vt AS (
  SELECT a.clnt_no, d.prod_app_dt AS app_dt
  FROM DDWV01.CR_APP_CLNT_RELTN_DLY AS a
  JOIN DDWV01.OVRL_CR_APP_DLY AS b ON b.cr_app_id = a.cr_app_id AND b.sys_src_id = a.sys_src_id
  JOIN DDWV01.CR_APP_CLNT_PROD_RELTN_DLY AS c ON c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no AND c.sys_src_id = a.sys_src_id
  JOIN DDWV01.CR_APP_PROD_DLY AS d ON d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no AND d.sys_src_id = c.sys_src_id
  WHERE b.app_typ = 'P' AND d.appl_for_prod_typ IN ('40','41','43')
    AND d.prod_app_sts_cd IN (32,37,45,47,51,56,62)
    AND d.prod_app_compl_dt IS NOT NULL AND d.prod_app_compl_dt >= DATE '2026-04-01'
) WITH DATA PRIMARY INDEX (clnt_no) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS COLUMN (clnt_no) ON o2p_conv_vt;

WITH

-- ---- PCL: population/arm/success flags verbatim from v1 -------------------------------------------
pcl_win AS (
  SELECT clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'test'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'control' END AS arm_role,
    decile,                                     -- first-touch tie-break only; PCL is single-stratum
    responder_cli, treatmt_strt_dt, treatmt_end_dt
  FROM DL_MR_PROD.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
),
pcl_ft AS (
  SELECT clnt_no, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC, decile ASC) AS rn
  FROM pcl_win
),
pcl_succ AS (
  SELECT clnt_no, MAX(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END) AS ever_responder_cli,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcl_win GROUP BY clnt_no
),
pcl_client AS (
  SELECT f.clnt_no, f.arm_role, s.ever_responder_cli, s.n_arms
  FROM pcl_ft f JOIN pcl_succ s ON s.clnt_no = f.clnt_no WHERE f.rn = 1
),
pcl_cells AS (
  SELECT
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                           AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_responder_cli = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                           AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_responder_cli = 1 THEN clnt_no END) AS control_successes
  FROM pcl_client
),
pcl_conflict AS (
  -- clients seen in both arms, resolved to first-touch; PCL is already one row per client here
  SELECT COUNT(*) AS n_arm_conflict FROM pcl_client WHERE n_arms > 1
),
pcl_rows AS (
  -- UNION-width guard: first branch of all_rows below, VARCHAR widths here fix every later branch.
  SELECT CAST('PCL' AS VARCHAR(20)) AS mne,
    CAST('R____WMS' AS VARCHAR(30)) AS arm_test, CAST('R____NMS' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(0 AS BIGINT) AS n_unmapped,                        -- no unmapped-code concept for PCL
    CAST(conf.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcl_cells cells CROSS JOIN pcl_conflict conf
),

-- ---- PCQ: population/arm/success flags verbatim from v1. Arm map is an INLINE CASE -- never a
-- FROM-less SELECT/UNION arm_map CTE; that pattern aborted Teradata previously. --------------------
pcq_win AS (
  SELECT r.clnt_no, TRIM(r.test_group_latest) AS test_group_latest,
    CASE WHEN TRIM(r.test_group_latest) = 'NG3_CHMP'               THEN 'control'
         WHEN TRIM(r.test_group_latest) IN ('NG3_CHLN','NG3_CHLG') THEN 'test' END AS arm_role,
    CAST(r.model_score_decile AS VARCHAR(10)) AS decile,
    r.treatmt_start_dt, r.treatmt_end_dt, r.app_approved, r.asc_on_app_source
  FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
  WHERE r.decsn_year = 2026 AND r.tpa_ita = 'TPA'
    AND r.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
),
pcq_ft AS (
  SELECT clnt_no, arm_role, decile,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_start_dt ASC, decile ASC) AS rn
  FROM pcq_win WHERE arm_role IS NOT NULL
),
pcq_succ AS (
  SELECT clnt_no,
    MAX(CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN 1 ELSE 0 END) AS ever_approved_asc,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcq_win GROUP BY clnt_no
),
pcq_client AS (
  SELECT f.clnt_no, f.arm_role, f.decile, s.ever_approved_asc, s.n_arms
  FROM pcq_ft f JOIN pcq_succ s ON s.clnt_no = f.clnt_no WHERE f.rn = 1
),
pcq_cells AS (
  -- one row per decile -- internal stratification only, decile never reaches the output
  SELECT decile,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                          AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_approved_asc = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                          AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_approved_asc = 1 THEN clnt_no END) AS control_successes
  FROM pcq_client GROUP BY decile
),
pcq_unmapped AS (
  -- campaign-level, NOT per decile: distinct clients whose test_group_latest never resolved to an
  -- arm (NG3_CHLD and anything else unrecognised). Computed once from pcq_win before stratification.
  SELECT COUNT(DISTINCT clnt_no) AS n_unmapped FROM pcq_win WHERE arm_role IS NULL
),
pcq_conflict AS (
  -- campaign-level, NOT per decile: clients seen in both arms across waves, resolved to first-touch
  SELECT COUNT(*) AS n_arm_conflict FROM pcq_client WHERE n_arms > 1
),
pcq_rows AS (
  -- success = app_approved gated on Period-ASC (canon: numerator only, denominator = all targeted)
  SELECT CAST('PCQ' AS VARCHAR(20)) AS mne,
    CAST('NG3_CHLN+NG3_CHLG' AS VARCHAR(30)) AS arm_test, CAST('NG3_CHMP' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(u.n_unmapped AS BIGINT) AS n_unmapped, CAST(c.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcq_cells cells CROSS JOIN pcq_unmapped u CROSS JOIN pcq_conflict c
  -- NOTE: this CROSS JOIN repeats the campaign-level n_unmapped/n_arm_conflict onto every decile
  -- row identically (fan-out is intentional here) -- the pooled CTE below MUST aggregate these two
  -- columns with MAX, never SUM, or the decile count would inflate them.
),

-- ---- PCD: ASYNC cohort only (curated table), summary-converted from async_banner_vintage_ab.sql --
-- Source draft had vintage_day spine + cumulative windows; deleted here, replaced by ONE row per
-- arm with a 0-60d success window. NON_ASYNC is not a randomized comparison and does not emit rows.
pcd_win AS (
  SELECT clnt_no, response_start, dt_prod_change, responder_anyproduct,
    CASE WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
         WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST' END AS arm_role
  FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
  WHERE strategy_seg_cd IN ('MISCDVUS3','MAO28CJ5','MAO2E061','MF88LGX6','MF88U3PY')   -- [VERIFY: strategy_seg_cd allowlist may be cut off at screen edge]
    -- above IN-list restricts to cohort_arm='ASYNC' (the only randomized slice on this table)
    AND response_end BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window
    -- tactic_id_parent also scopes a single deployment on this table; source draft doesn't filter
    -- it, so it's left open here -- the strategy_seg_cd allowlist is the deployment/cohort scope.
),
pcd_ft AS (
  SELECT clnt_no, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY response_start ASC) AS rn
  FROM pcd_win
),
pcd_succ AS (
  SELECT clnt_no,
    MAX(CASE WHEN responder_anyproduct = 1 AND dt_prod_change IS NOT NULL
      AND (dt_prod_change - response_start) BETWEEN 0 AND 60 THEN 1 ELSE 0 END) AS ever_responder,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcd_win GROUP BY clnt_no
),
pcd_client AS (
  SELECT f.clnt_no, f.arm_role, s.ever_responder, s.n_arms
  FROM pcd_ft f JOIN pcd_succ s ON s.clnt_no = f.clnt_no WHERE f.rn = 1
),
pcd_cells AS (
  SELECT
    COUNT(DISTINCT CASE WHEN arm_role = 'TEST'    THEN clnt_no END)                          AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'TEST'    AND ever_responder = 1 THEN clnt_no END)    AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'CONTROL' THEN clnt_no END)                          AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'CONTROL' AND ever_responder = 1 THEN clnt_no END)    AS control_successes
  FROM pcd_client
),
pcd_unmapped AS (
  -- clients in the ASYNC allowlist whose test_groups_period didn't end in %C or %T
  SELECT COUNT(DISTINCT clnt_no) AS n_unmapped FROM pcd_win WHERE arm_role IS NULL
),
pcd_conflict AS (
  SELECT COUNT(*) AS n_arm_conflict FROM pcd_client WHERE n_arms > 1
),
pcd_rows AS (
  -- arm_test/arm_ctrl are DERIVED LABELS, not raw source codes -- async has no single raw code per
  -- arm, it's a suffix pattern over test_groups_period (see header note).
  SELECT CAST('PCD' AS VARCHAR(20)) AS mne,
    CAST('TEST' AS VARCHAR(30)) AS arm_test, CAST('CONTROL' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(u.n_unmapped AS BIGINT) AS n_unmapped, CAST(c.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcd_cells cells CROSS JOIN pcd_unmapped u CROSS JOIN pcd_conflict c
),

-- ---- O2P: NEW deployment only (3-arm: HOLDOUT / MB_CHAMPION / NON_MB_CHALLENGER), summary-
-- converted from async_banner_vintage_ab.sql. OLD deployments (TG4=TEST/TG7=CONTROL, no holdout)
-- are excluded -- not the randomized contrast this block measures. Emits TWO contract rows below,
-- both sharing the same HOLDOUT n/x. -------------------------------------------------------------
o2p_raw AS (
  SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt, TRIM(tst_grp_cd) AS tst_grp_cd, TACTIC_CELL_CD
  FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
  WHERE tactic_id = '20261680Z0P'   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos -- header comment in source says "20261602P", code used "20261680Z0P"; kept the operative code-branch literal]
    AND TRIM(rpt_grp_cd) IN ('PO2PHL01','PO2PHL03','PO2PHL07','PO2P0101','PO2P0103','PO2P0107','PO2PPR01','PO2PPR03','PO2PPR07')   -- [VERIFY: rpt_grp_cd allowlist may be cut off at screen edge]
    AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window (tactic_id already scopes this to the single new deployment)
),
o2p_grp AS (
  -- group-detection kept verbatim from source: collapse multiple tactic events per client+test-group
  -- into one row (MIN treatment dates, MAX has_mb) before arm classification.
  SELECT clnt_no, MIN(treatmt_strt_dt) AS treatmt_strt_dt, tst_grp_cd,
    MAX(CASE WHEN TACTIC_CELL_CD LIKE '%MB%' THEN 1 ELSE 0 END) AS has_mb   -- [VERIFY: token is MB per Andre 2026-07-20; wildcard %MB% assumed - confirm exact cell-code form in-env]
  FROM o2p_raw
  GROUP BY clnt_no, tst_grp_cd
),
o2p_win AS (
  SELECT clnt_no, treatmt_strt_dt,
    CASE
      WHEN tst_grp_cd = 'TG7' THEN 'HOLDOUT'
      WHEN tst_grp_cd = 'TG4' AND has_mb = 1 THEN 'MB_CHAMPION'
      WHEN tst_grp_cd = 'TG4' AND has_mb = 0 THEN 'NON_MB_CHALLENGER'
    END AS arm_role
  FROM o2p_grp
),
o2p_ft AS (
  SELECT clnt_no, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC) AS rn
  FROM o2p_win
),
o2p_arms AS (
  SELECT clnt_no, COUNT(DISTINCT arm_role) AS n_arms FROM o2p_win GROUP BY clnt_no
),
o2p_succ_flag AS (
  -- EXISTS against the pre-materialized VOLATILE converter (o2p_conv_vt, created above) -- Teradata
  -- gotcha: no EXISTS inside CASE WHEN, so it's here in WHERE, LEFT JOIN back below.
  SELECT DISTINCT w.clnt_no
  FROM o2p_win w
  WHERE EXISTS (
    SELECT 1 FROM o2p_conv_vt oc
    WHERE oc.clnt_no = w.clnt_no AND oc.app_dt BETWEEN w.treatmt_strt_dt AND w.treatmt_strt_dt + 60
  )
),
o2p_client AS (
  SELECT f.clnt_no, f.arm_role, a.n_arms,
    CASE WHEN sf.clnt_no IS NOT NULL THEN 1 ELSE 0 END AS ever_success
  FROM o2p_ft f
  JOIN o2p_arms a ON a.clnt_no = f.clnt_no
  LEFT JOIN o2p_succ_flag sf ON sf.clnt_no = f.clnt_no
  WHERE f.rn = 1
),
o2p_cells AS (
  SELECT
    COUNT(DISTINCT CASE WHEN arm_role = 'MB_CHAMPION'       THEN clnt_no END)                        AS mb_champion_n,
    COUNT(DISTINCT CASE WHEN arm_role = 'MB_CHAMPION'       AND ever_success = 1 THEN clnt_no END)    AS mb_champion_x,
    COUNT(DISTINCT CASE WHEN arm_role = 'NON_MB_CHALLENGER' THEN clnt_no END)                        AS challenger_n,
    COUNT(DISTINCT CASE WHEN arm_role = 'NON_MB_CHALLENGER' AND ever_success = 1 THEN clnt_no END)    AS challenger_x,
    COUNT(DISTINCT CASE WHEN arm_role = 'HOLDOUT'            THEN clnt_no END)                        AS holdout_n,
    COUNT(DISTINCT CASE WHEN arm_role = 'HOLDOUT'            AND ever_success = 1 THEN clnt_no END)    AS holdout_x
  FROM o2p_client
),
o2p_unmapped AS (
  -- tst_grp_cd values outside TG4/TG7 (e.g. other test groups on the same tactic) resolve to NULL
  SELECT COUNT(DISTINCT clnt_no) AS n_unmapped FROM o2p_win WHERE arm_role IS NULL
),
o2p_conflict AS (
  SELECT COUNT(*) AS n_arm_conflict FROM o2p_client WHERE n_arms > 1
),
o2p_rows AS (
  -- arm_test/arm_ctrl are DERIVED LABELS, not raw source codes -- async has no single raw code per
  -- arm (see header note). TWO rows, same mne, sharing the HOLDOUT n/x -- they don't share a
  -- (mne, arm_test, arm_ctrl) key so the pooling GROUP BY downstream keeps them as separate strata.
  SELECT CAST('O2P' AS VARCHAR(20)) AS mne,
    CAST('MB_CHAMPION' AS VARCHAR(30)) AS arm_test, CAST('HOLDOUT' AS VARCHAR(30)) AS arm_ctrl,
    cells.mb_champion_n AS n_test, cells.mb_champion_x AS x_test,
    cells.holdout_n AS n_ctrl, cells.holdout_x AS x_ctrl,
    CAST(u.n_unmapped AS BIGINT) AS n_unmapped, CAST(c.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM o2p_cells cells CROSS JOIN o2p_unmapped u CROSS JOIN o2p_conflict c
  UNION ALL
  SELECT CAST('O2P' AS VARCHAR(20)) AS mne,
    CAST('NON_MB_CHALLENGER' AS VARCHAR(30)) AS arm_test, CAST('HOLDOUT' AS VARCHAR(30)) AS arm_ctrl,
    cells.challenger_n AS n_test, cells.challenger_x AS x_test,
    cells.holdout_n AS n_ctrl, cells.holdout_x AS x_ctrl,
    CAST(u.n_unmapped AS BIGINT) AS n_unmapped, CAST(c.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM o2p_cells cells CROSS JOIN o2p_unmapped u CROSS JOIN o2p_conflict c
),

-- ---- CTU: deployment-2 ASYNC arm only (TEST vs CONTROL), summary-converted from
-- async_banner_vintage_ab.sql. Deployment 1 (no A/B control) and the NON_ASYNC arm (no A/B split)
-- are excluded -- neither is a randomized contrast. ------------------------------------------------
ctu_raw AS (
  SELECT t.clnt_no, t.tactic_id, t.treatmt_strt_dt, TRIM(t.tst_grp_cd) AS tst_grp_cd
  FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t   -- [VERIFY: FROM table inferred - photos omitted it]
  WHERE t.tactic_id = '20261610CTU'   -- [VERIFY tactic_id: OCR uncertain, formats conflict across photos]
    AND TRIM(t.tst_grp_cd) IN ('T_EDA0_A','T_ANA3_A','T_ONS1_A','T_ADAS_A','T_EDA1_A','T_ONS6_A','T_ANA6_A','T_EDA3_A','T_ADAG_A','T_ANA1_A','T_ONS3_A','TADWM1_A','TADWMS_A','T_EDA6_B','T_ANA3_B','T_ONS1_B','T_ADAS_B','T_EDA1_B','T_ONS6_B','T_ANA6_B','T_EDA3_B','T_ADAG_B','T_ANA1_B','T_ONS3_B','TADWMJ_B','TADWMS_B')   -- [VERIFY: tst_grp_cd IN-list OCR uncertain]
    AND t.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window (tactic_id already scopes this to deployment 2)
),
ctu_win AS (
  SELECT clnt_no, tactic_id, treatmt_strt_dt,
    CASE
      WHEN tst_grp_cd IN ('T_EDA0_A','T_ANA3_A','T_ONS1_A','T_ADAS_A','T_EDA1_A','T_ONS6_A','T_ANA6_A','T_EDA3_A','T_ADAG_A','T_ANA1_A','T_ONS3_A','TADWM1_A','TADWMS_A') THEN 'TEST'      -- [VERIFY: tst_grp_cd IN-list OCR uncertain]
      WHEN tst_grp_cd IN ('T_EDA6_B','T_ANA3_B','T_ONS1_B','T_ADAS_B','T_EDA1_B','T_ONS6_B','T_ANA6_B','T_EDA3_B','T_ADAG_B','T_ANA1_B','T_ONS3_B','TADWMJ_B','TADWMS_B') THEN 'CONTROL'    -- [VERIFY: tst_grp_cd IN-list OCR uncertain]
    END AS arm_role
  FROM ctu_raw
),
ctu_ft AS (
  SELECT clnt_no, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC) AS rn
  FROM ctu_win
),
ctu_arms AS (
  SELECT clnt_no, COUNT(DISTINCT arm_role) AS n_arms FROM ctu_win GROUP BY clnt_no
),
ctu_succ AS (
  -- success sourced from DL_MR_PROD.nbo_pba_upgrade -- [VERIFY: dep2 ASYNC join ON-clause and
  -- response_dt source not fully visible in photos; using ON u.clnt_no = t.clnt_no AND
  -- u.tactic_id = t.tactic_id as the reconciled join key]
  SELECT t.clnt_no,
    MAX(CASE WHEN u.success = 1 AND u.response_dt IS NOT NULL
      AND (u.response_dt - t.treatmt_strt_dt) BETWEEN 0 AND 60 THEN 1 ELSE 0 END) AS ever_success
  FROM ctu_win t
  LEFT JOIN DL_MR_PROD.nbo_pba_upgrade u
    ON u.clnt_no = t.clnt_no AND u.tactic_id = t.tactic_id
  GROUP BY t.clnt_no
),
ctu_client AS (
  SELECT f.clnt_no, f.arm_role, a.n_arms, s.ever_success
  FROM ctu_ft f
  JOIN ctu_arms a ON a.clnt_no = f.clnt_no
  LEFT JOIN ctu_succ s ON s.clnt_no = f.clnt_no
  WHERE f.rn = 1
),
ctu_cells AS (
  SELECT
    COUNT(DISTINCT CASE WHEN arm_role = 'TEST'    THEN clnt_no END)                          AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'TEST'    AND ever_success = 1 THEN clnt_no END)      AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'CONTROL' THEN clnt_no END)                          AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'CONTROL' AND ever_success = 1 THEN clnt_no END)      AS control_successes
  FROM ctu_client
),
ctu_conflict AS (
  SELECT COUNT(*) AS n_arm_conflict FROM ctu_client WHERE n_arms > 1
),
ctu_rows AS (
  -- arm_test/arm_ctrl are DERIVED LABELS, not raw source codes -- async has no single raw code per
  -- arm (see header note). n_unmapped hardcoded 0: ctu_raw's WHERE already restricts tst_grp_cd to
  -- the exact union of the TEST/CONTROL IN-lists used in the CASE, so arm_role can't resolve NULL
  -- here (same structural reasoning as PCL above).
  SELECT CAST('CTU' AS VARCHAR(20)) AS mne,
    CAST('TEST' AS VARCHAR(30)) AS arm_test, CAST('CONTROL' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(0 AS BIGINT) AS n_unmapped,
    CAST(conf.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM ctu_cells cells CROSS JOIN ctu_conflict conf
),

-- ==== TEAMMATE HOOK-UP POINT: add a new campaign as one more UNION ALL branch, same 9 columns ====
all_rows AS (
  SELECT * FROM pcl_rows
  UNION ALL SELECT * FROM pcq_rows
  UNION ALL SELECT * FROM pcd_rows
  UNION ALL SELECT * FROM o2p_rows
  UNION ALL SELECT * FROM ctu_rows
  -- UNION ALL SELECT * FROM <next_campaign>_rows
),

-- ---- stats layer: campaign-agnostic, sees only the contract. MH-weighted two-proportion z-test ----
strata_base AS (
  SELECT mne, arm_test, arm_ctrl, n_test AS n1, x_test AS x1, n_ctrl AS n0, x_ctrl AS x0,
    n_unmapped, n_arm_conflict,
    CAST(x_test AS FLOAT)/NULLIF(CAST(n_test AS FLOAT),0) - CAST(x_ctrl AS FLOAT)/NULLIF(CAST(n_ctrl AS FLOAT),0) AS d,
    (CAST(n_test AS FLOAT)*CAST(n_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS w,
    (CAST(x_test AS FLOAT)+CAST(x_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS pbar
  FROM all_rows
),
strata_v AS (
  SELECT mne, arm_test, arm_ctrl, n1, x1, n0, x0, n_unmapped, n_arm_conflict, d, w,
    pbar * (1 - pbar) * (
      (CASE WHEN n1 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n1 END) +
      (CASE WHEN n0 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n0 END)
    ) AS v
  FROM strata_base
),
pooled AS (
  -- GROUP BY collapses PCQ's per-decile strata into one MH-weighted row; PCL has one stratum already.
  -- n_unmapped/n_arm_conflict are identical across every decile row for a given campaign (see
  -- pcq_rows note above), so MAX returns the correct campaign scalar without decile-count inflation.
  SELECT mne, arm_test, arm_ctrl,
    SUM(n1) AS n_test, SUM(x1) AS x_test, SUM(n0) AS n_ctrl, SUM(x0) AS x_ctrl,
    SUM(w * d) / NULLIF(SUM(w), 0) AS lift, SQRT(SUM(w * w * v)) / NULLIF(SUM(w), 0) AS se,
    MAX(n_unmapped) AS n_unmapped, MAX(n_arm_conflict) AS n_arm_conflict
  FROM strata_v GROUP BY mne, arm_test, arm_ctrl
),
stats AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    CASE WHEN se IS NULL OR se = 0 THEN NULL ELSE lift / se END AS z
  FROM pooled
),

-- ---- CDF / p-value: Zelen & Severo / Abramowitz & Stegun 26.2.17 rational approximation to the
-- standard-normal CDF (normal_cdf does not exist in Teradata). Ported verbatim from
-- value_capture_report.sql (v1), which validated this polynomial against scipy. Max abs error on the
-- CDF itself < 7.5e-8; p_value doubles that (~1.5e-7), far below anything that could flip a
-- significance call. z stays internal to this chain -- it does NOT appear in the final output.
zs_base AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    z, ABS(z) AS az
  FROM stats
),
zs_t AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    z, az,
    CAST(1 AS FLOAT) / (1 + 0.2316419 * az) AS t
  FROM zs_base
),
zs_phi AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    z, az, t,
    CAST(0.3989422804014327 AS FLOAT) * EXP(-az * az / 2) AS phi
  FROM zs_t
),
zs_cdf AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict, z,
    1 - phi * ( 0.319381530 * t
              - 0.356563782 * t * t
              + 1.781477937 * t * t * t
              - 1.821255978 * t * t * t * t
              + 1.330274429 * t * t * t * t * t )               AS cdf
  FROM zs_phi
),
stats_p AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift, n_unmapped, n_arm_conflict,
    CASE WHEN z IS NULL THEN NULL ELSE 2 * (1 - cdf) END AS p_value
  FROM zs_cdf
)

-- ---- final output: one row per campaign/arm-contrast, partner-required column layout ---------------
-- experiment groups mne into the partner's experiment buckets: PCL and PCQ are both Sales Modal
-- experiments run on different campaigns; PCD/O2P/CTU are the Async Banner experiment. AUH would be
-- 'Authorized Users' when its block is added.
-- rate columns are CRUDE (pooled counts). lift for a stratified campaign (PCQ) is MH-WEIGHTED across
-- deciles, so action_response_rate - control_response_rate will NOT equal lift there. Expected.
-- Single SELECT, no UNION ALL at this level -- ORDER BY column names is safe here (the "positional
-- only" rule applies to a query with UNION ALL at the outer level, which this does not have).
SELECT
  CASE WHEN mne IN ('PCL','PCQ') THEN 'Sales Modal'
       WHEN mne IN ('PCD','O2P','CTU') THEN 'Async Banner'
       ELSE mne END AS experiment,
  mne, arm_test, arm_ctrl,
  n_test AS action_population, n_ctrl AS control_population,
  x_test AS action_response, x_ctrl AS control_response,
  100.0 * CAST(x_test AS FLOAT) / NULLIF(CAST(n_test AS FLOAT), 0) AS action_response_rate,
  100.0 * CAST(x_ctrl AS FLOAT) / NULLIF(CAST(n_ctrl AS FLOAT), 0) AS control_response_rate,
  lift * 100 AS lift,
  CAST(p_value AS FLOAT) AS p_value,
  n_unmapped, n_arm_conflict
FROM stats_p
ORDER BY mne, arm_test;
