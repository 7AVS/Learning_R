-- value_capture/value_capture_report_v2.sql
-- Teradata-direct rebuild of value_capture_report.sql (v1). SQL counts clients only -- display
-- formatting, p-value, and partner-template mapping are Andre's job in Excel (REDESIGN_SPEC.md).
-- Engine: TERADATA-DIRECT, bare table names, no catalog prefix -- do NOT run through Starburst.
-- Grain: QUARTERLY, client-deduped -- treatmt_end_dt BETWEEN '2026-05-01' AND '2026-07-31',
--   first-touch collapse per clnt_no, ever-success over all in-window rows (v1 logic, unchanged).
-- all_rows contract (8 cols, teammate hook-up point): row_type | mne | arm_test | arm_ctrl | n_test
--   | x_test | n_ctrl | x_ctrl. arm_test/arm_ctrl = raw source codes, never invented prose.
-- PCQ stratifies by decile INTERNALLY (Simpson's-paradox guard): one row per decile into all_rows,
--   pooled by the stats layer into one output row -- decile itself never appears; emitted counts are
--   simple sums while lift is MH-weighted, so the two won't exactly reconcile for PCQ (expected).
-- Stats: MH-weighted two-proportion z-test. sig flags = z-threshold checks (80/90/95%), no CDF/p-value
--   (Andre gets p via Excel NORM.S.DIST). row_type='diag' rows skip the stats path.

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
pcl_rows AS (
  -- UNION-width guard: first branch of all_rows below, VARCHAR widths here fix every later branch.
  SELECT CAST('total' AS VARCHAR(10)) AS row_type, CAST('PCL' AS VARCHAR(20)) AS mne,
    CAST('R____WMS' AS VARCHAR(30)) AS arm_test, CAST('R____NMS' AS VARCHAR(30)) AS arm_ctrl,
    test_clients AS n_test, test_successes AS x_test, control_clients AS n_ctrl, control_successes AS x_ctrl
  FROM pcl_cells
),
pcl_conflict_row AS (
  -- clients seen in both arms, resolved to first-touch; surfaces only if any exist, count in n_test.
  SELECT CAST('diag' AS VARCHAR(10)) AS row_type, CAST('PCL' AS VARCHAR(20)) AS mne,
    CAST('ARM_CONFLICT' AS VARCHAR(30)) AS arm_test, CAST(NULL AS VARCHAR(30)) AS arm_ctrl,
    COUNT(*) AS n_test, CAST(NULL AS BIGINT) AS x_test, CAST(NULL AS BIGINT) AS n_ctrl, CAST(NULL AS BIGINT) AS x_ctrl
  FROM pcl_client WHERE n_arms > 1 HAVING COUNT(*) > 0
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
pcq_unmapped AS (
  SELECT DISTINCT test_group_latest FROM pcq_win WHERE arm_role IS NULL
),
pcq_unmapped_row AS (
  -- codes not in the CASE above; surfaces only if any exist, count in n_test (never silently dropped).
  SELECT CAST('diag' AS VARCHAR(10)) AS row_type, CAST('PCQ' AS VARCHAR(20)) AS mne,
    CAST('UNMAPPED_CODES' AS VARCHAR(30)) AS arm_test, CAST(NULL AS VARCHAR(30)) AS arm_ctrl,
    COUNT(*) AS n_test, CAST(NULL AS BIGINT) AS x_test, CAST(NULL AS BIGINT) AS n_ctrl, CAST(NULL AS BIGINT) AS x_ctrl
  FROM pcq_unmapped HAVING COUNT(*) > 0
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
pcq_rows AS (
  -- success = app_approved gated on Period-ASC (canon: numerator only, denominator = all targeted)
  SELECT CAST('total' AS VARCHAR(10)) AS row_type, CAST('PCQ' AS VARCHAR(20)) AS mne,
    CAST('NG3_CHLN+NG3_CHLG' AS VARCHAR(30)) AS arm_test, CAST('NG3_CHMP' AS VARCHAR(30)) AS arm_ctrl,
    test_clients AS n_test, test_successes AS x_test, control_clients AS n_ctrl, control_successes AS x_ctrl
  FROM pcq_cells
),
pcq_conflict_row AS (
  SELECT CAST('diag' AS VARCHAR(10)) AS row_type, CAST('PCQ' AS VARCHAR(20)) AS mne,
    CAST('ARM_CONFLICT' AS VARCHAR(30)) AS arm_test, CAST(NULL AS VARCHAR(30)) AS arm_ctrl,
    COUNT(*) AS n_test, CAST(NULL AS BIGINT) AS x_test, CAST(NULL AS BIGINT) AS n_ctrl, CAST(NULL AS BIGINT) AS x_ctrl
  FROM pcq_client WHERE n_arms > 1 HAVING COUNT(*) > 0
),

-- ==== TEAMMATE HOOK-UP POINT: add a new campaign as one more UNION ALL branch, same 8 columns ====
all_rows AS (
  SELECT * FROM pcl_rows
  UNION ALL SELECT * FROM pcq_rows
  UNION ALL SELECT * FROM pcl_conflict_row
  UNION ALL SELECT * FROM pcq_unmapped_row
  UNION ALL SELECT * FROM pcq_conflict_row
  -- UNION ALL SELECT * FROM <next_campaign>_rows
),

-- ---- stats layer: campaign-agnostic, sees only the contract. MH-weighted two-proportion z-test ----
strata_base AS (
  SELECT mne, arm_test, arm_ctrl, n_test AS n1, x_test AS x1, n_ctrl AS n0, x_ctrl AS x0,
    CAST(x_test AS FLOAT)/NULLIF(CAST(n_test AS FLOAT),0) - CAST(x_ctrl AS FLOAT)/NULLIF(CAST(n_ctrl AS FLOAT),0) AS d,
    (CAST(n_test AS FLOAT)*CAST(n_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS w,
    (CAST(x_test AS FLOAT)+CAST(x_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS pbar
  FROM all_rows WHERE row_type = 'total'
),
strata_v AS (
  SELECT mne, arm_test, arm_ctrl, n1, x1, n0, x0, d, w,
    pbar * (1 - pbar) * (
      (CASE WHEN n1 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n1 END) +
      (CASE WHEN n0 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n0 END)
    ) AS v
  FROM strata_base
),
pooled AS (
  -- GROUP BY collapses PCQ's per-decile strata into one MH-weighted row; PCL has one stratum already
  SELECT mne, arm_test, arm_ctrl,
    SUM(n1) AS n_test, SUM(x1) AS x_test, SUM(n0) AS n_ctrl, SUM(x0) AS x_ctrl,
    SUM(w * d) / NULLIF(SUM(w), 0) AS lift, SQRT(SUM(w * w * v)) / NULLIF(SUM(w), 0) AS se
  FROM strata_v GROUP BY mne, arm_test, arm_ctrl
),
stats AS (
  SELECT mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl, lift,
    CASE WHEN se IS NULL OR se = 0 THEN NULL ELSE lift / se END AS z
  FROM pooled
)

-- ---- final output: 'total' rows carry stats; 'diag' rows pass through with stats columns NULL ----
-- rate_test/rate_ctrl are CRUDE (pooled counts). lift_pp for a stratified campaign (PCQ) is
-- MH-WEIGHTED across deciles, so rate_test - rate_ctrl will NOT equal lift_pp there. Expected.
SELECT
  CAST('total' AS VARCHAR(10)) AS row_type, mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl,
  100.0 * CAST(x_test AS FLOAT) / NULLIF(CAST(n_test AS FLOAT), 0) AS rate_test_pct,
  100.0 * CAST(x_ctrl AS FLOAT) / NULLIF(CAST(n_ctrl AS FLOAT), 0) AS rate_ctrl_pct,
  lift * 100 AS lift_pp, z,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.2816 THEN 'Y' ELSE 'N' END AS sig80,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.6449 THEN 'Y' ELSE 'N' END AS sig90,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.9600 THEN 'Y' ELSE 'N' END AS sig95
FROM stats
UNION ALL
SELECT row_type, mne, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl,
  CAST(NULL AS FLOAT) AS rate_test_pct, CAST(NULL AS FLOAT) AS rate_ctrl_pct,
  CAST(NULL AS FLOAT) AS lift_pp, CAST(NULL AS FLOAT) AS z,
  CAST(NULL AS VARCHAR(1)) AS sig80, CAST(NULL AS VARCHAR(1)) AS sig90, CAST(NULL AS VARCHAR(1)) AS sig95
FROM all_rows WHERE row_type = 'diag'
ORDER BY 1, 2;  -- Teradata: ORDER BY over a UNION must use ordinal positions, not column names
