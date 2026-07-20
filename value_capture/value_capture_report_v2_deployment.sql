-- value_capture/value_capture_report_v2_deployment.sql
-- Teradata-direct sibling of value_capture_report_v2.sql. Same measurement logic, emitted at TWO
-- grains in one result set instead of one quarterly rollup.
-- Engine: TERADATA-DIRECT, bare table names, no catalog prefix -- do NOT run through Starburst.
--
-- GRAIN A 'deployment': one row per (mne, treatmt_strt_dt, treatmt_end_dt) -- a deployment/wave.
--   NO cross-deployment dedup, NO first-touch collapse across waves. A client hit by three
--   deployments in the quarter is counted in all three. Within ONE deployment the client is still
--   deduped to a single row (COUNT DISTINCT clnt_no) and success = MAX(success flag) over that
--   client's rows within that deployment only.
-- GRAIN B 'quarter': one row per mne, the v2 rollup -- computed INDEPENDENTLY from the raw
--   population with the full first-touch collapse (ROW_NUMBER per clnt_no ordered by treatment
--   start, keep rn=1) and ever-success = MAX over ALL the client's in-window rows. Each client
--   counted exactly once for the quarter.
--   *** DO NOT reproduce the quarter row by summing the deployment rows -- that double-counts any
--   multi-deployment client. The two grains are two separate queries against pcl_win/pcq_win that
--   happen to share a UNION ALL and a stats layer; they are not derived from each other. ***
--
-- Deployment scope = same boundary as v2's quarter window: a deployment is in-scope if its
-- treatmt_end_dt falls in DATE '2026-05-01' to DATE '2026-07-31' (RBC fiscal Q3 FY2026). Teradata-
-- direct won't allow a FROM-less CTE to hold that pair as a shared scalar (canon gotcha -- that
-- pattern is what aborted the arm_map CTE previously), so the literal is duplicated once per
-- campaign's *_win CTE, each marked "EDIT POINT" below -- update both together.
--
-- all_rows contract (12 cols, teammate hook-up point): grain | mne | trt_start_dt | trt_end_dt |
--   arm_test | arm_ctrl | n_test | x_test | n_ctrl | x_ctrl | n_unmapped | n_arm_conflict.
--   arm_test/arm_ctrl = raw source codes, never invented prose. n_unmapped = distinct in-scope
--   clients whose arm couldn't be resolved, scoped to the row's own grain (per-deployment for
--   grain A, per-quarter for grain B). n_arm_conflict = distinct clients seen in both arms within
--   the row's own scope, resolved to first-touch (still counted once in n_test/n_ctrl -- informational).
-- PCQ stratifies by decile INTERNALLY at BOTH grains (Simpson's-paradox guard): one row per decile
--   into all_rows, pooled by the stats layer into one output row per (grain, mne, trt_start_dt,
--   trt_end_dt, arm_test, arm_ctrl) -- decile itself never appears; emitted counts are simple sums
--   while lift is MH-weighted, so the two won't exactly reconcile for PCQ (expected, same as v2).
--   n_unmapped/n_arm_conflict are computed once per (campaign, grain-scope) -- not per decile --
--   then LEFT/CROSS JOINed onto every decile row identically; the pooling step aggregates them
--   with MAX, not SUM, so the decile fan-out never inflates them.
-- Stats: MH-weighted two-proportion z-test. sig flags = z-threshold checks (80/90/95%), no CDF/p-value
--   (Andre gets p via Excel NORM.S.DIST).

WITH

-- ==================================================================================================
-- PCL -- population/arm/success flags verbatim from v2. Two independent aggregations follow:
--   *_dep_* = deployment grain (no collapse across waves), *_ft/*_succ/*_client/*_cells = quarter
--   grain (unchanged from v2, first-touch collapsed).
-- ==================================================================================================
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

-- ---- PCL grain A: deployment (per-wave, no cross-wave collapse) --------------------------------
pcl_dep_ft AS (
  SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no, treatmt_strt_dt, treatmt_end_dt ORDER BY decile ASC) AS rn
  FROM pcl_win
),
pcl_dep_succ AS (
  -- success + arm-count scoped to THIS deployment only (not the whole quarter)
  SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt,
    MAX(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END) AS ever_responder_cli,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcl_win GROUP BY clnt_no, treatmt_strt_dt, treatmt_end_dt
),
pcl_dep_client AS (
  SELECT f.clnt_no, f.treatmt_strt_dt, f.treatmt_end_dt, f.arm_role,
    s.ever_responder_cli, s.n_arms
  FROM pcl_dep_ft f
  JOIN pcl_dep_succ s
    ON s.clnt_no = f.clnt_no AND s.treatmt_strt_dt = f.treatmt_strt_dt AND s.treatmt_end_dt = f.treatmt_end_dt
  WHERE f.rn = 1
),
pcl_dep_cells AS (
  SELECT treatmt_strt_dt, treatmt_end_dt,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                           AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_responder_cli = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                           AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_responder_cli = 1 THEN clnt_no END) AS control_successes
  FROM pcl_dep_client GROUP BY treatmt_strt_dt, treatmt_end_dt
),
pcl_dep_conflict AS (
  SELECT treatmt_strt_dt, treatmt_end_dt, COUNT(*) AS n_arm_conflict
  FROM pcl_dep_client WHERE n_arms > 1 GROUP BY treatmt_strt_dt, treatmt_end_dt
),
pcl_dep_rows AS (
  -- UNION-width guard: first branch of all_rows below, VARCHAR widths here fix every later branch.
  SELECT CAST('deployment' AS VARCHAR(10)) AS grain, CAST('PCL' AS VARCHAR(20)) AS mne,
    cells.treatmt_strt_dt AS trt_start_dt, cells.treatmt_end_dt AS trt_end_dt,
    CAST('R____WMS' AS VARCHAR(30)) AS arm_test, CAST('R____NMS' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(0 AS BIGINT) AS n_unmapped,                        -- no unmapped-code concept for PCL
    CAST(COALESCE(conf.n_arm_conflict, 0) AS BIGINT) AS n_arm_conflict
  FROM pcl_dep_cells cells
  LEFT JOIN pcl_dep_conflict conf
    ON conf.treatmt_strt_dt = cells.treatmt_strt_dt AND conf.treatmt_end_dt = cells.treatmt_end_dt
),

-- ---- PCL grain B: quarter (first-touch collapsed across all in-window waves -- v2 logic, unchanged) --
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
pcl_q_window AS (
  SELECT MIN(treatmt_strt_dt) AS trt_start_dt, MAX(treatmt_end_dt) AS trt_end_dt FROM pcl_win
),
pcl_q_rows AS (
  SELECT CAST('quarter' AS VARCHAR(10)) AS grain, CAST('PCL' AS VARCHAR(20)) AS mne,
    qwin.trt_start_dt, qwin.trt_end_dt,
    CAST('R____WMS' AS VARCHAR(30)) AS arm_test, CAST('R____NMS' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(0 AS BIGINT) AS n_unmapped,
    CAST(conf.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcl_cells cells CROSS JOIN pcl_conflict conf CROSS JOIN pcl_q_window qwin
),

-- ==================================================================================================
-- PCQ -- population/arm/success flags verbatim from v2. Arm map is an INLINE CASE -- never a
-- FROM-less SELECT/UNION arm_map CTE; that pattern aborted Teradata previously. Same split as PCL:
-- *_dep_* = deployment grain, *_ft/*_succ/*_client/*_cells = quarter grain (unchanged from v2).
-- ==================================================================================================
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

-- ---- PCQ grain A: deployment (per-wave, no cross-wave collapse); decile stratified internally ----
pcq_dep_ft AS (
  SELECT clnt_no, treatmt_start_dt, treatmt_end_dt, arm_role, decile,
    ROW_NUMBER() OVER (PARTITION BY clnt_no, treatmt_start_dt, treatmt_end_dt ORDER BY decile ASC) AS rn
  FROM pcq_win WHERE arm_role IS NOT NULL
),
pcq_dep_succ AS (
  -- success + arm-count scoped to THIS deployment only (not the whole quarter)
  SELECT clnt_no, treatmt_start_dt, treatmt_end_dt,
    MAX(CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN 1 ELSE 0 END) AS ever_approved_asc,
    COUNT(DISTINCT arm_role) AS n_arms
  FROM pcq_win GROUP BY clnt_no, treatmt_start_dt, treatmt_end_dt
),
pcq_dep_client AS (
  SELECT f.clnt_no, f.treatmt_start_dt, f.treatmt_end_dt, f.arm_role, f.decile,
    s.ever_approved_asc, s.n_arms
  FROM pcq_dep_ft f
  JOIN pcq_dep_succ s
    ON s.clnt_no = f.clnt_no AND s.treatmt_start_dt = f.treatmt_start_dt AND s.treatmt_end_dt = f.treatmt_end_dt
  WHERE f.rn = 1
),
pcq_dep_cells AS (
  -- one row per (deployment, decile) -- internal stratification only, decile never reaches the output
  SELECT treatmt_start_dt, treatmt_end_dt, decile,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                          AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_approved_asc = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                          AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_approved_asc = 1 THEN clnt_no END) AS control_successes
  FROM pcq_dep_client GROUP BY treatmt_start_dt, treatmt_end_dt, decile
),
pcq_dep_unmapped AS (
  -- per-deployment, NOT per decile: distinct clients whose test_group_latest never resolved to an
  -- arm within this deployment. Computed once per deployment before stratification.
  SELECT treatmt_start_dt, treatmt_end_dt, COUNT(DISTINCT clnt_no) AS n_unmapped
  FROM pcq_win WHERE arm_role IS NULL GROUP BY treatmt_start_dt, treatmt_end_dt
),
pcq_dep_conflict AS (
  -- per-deployment, NOT per decile: clients seen in both arms within this single deployment
  SELECT treatmt_start_dt, treatmt_end_dt, COUNT(*) AS n_arm_conflict
  FROM pcq_dep_client WHERE n_arms > 1 GROUP BY treatmt_start_dt, treatmt_end_dt
),
pcq_dep_rows AS (
  -- success = app_approved gated on Period-ASC (canon: numerator only, denominator = all targeted)
  SELECT CAST('deployment' AS VARCHAR(10)) AS grain, CAST('PCQ' AS VARCHAR(20)) AS mne,
    cells.treatmt_start_dt AS trt_start_dt, cells.treatmt_end_dt AS trt_end_dt,
    CAST('NG3_CHLN+NG3_CHLG' AS VARCHAR(30)) AS arm_test, CAST('NG3_CHMP' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(COALESCE(u.n_unmapped, 0) AS BIGINT) AS n_unmapped,
    CAST(COALESCE(c.n_arm_conflict, 0) AS BIGINT) AS n_arm_conflict
  FROM pcq_dep_cells cells
  LEFT JOIN pcq_dep_unmapped u
    ON u.treatmt_start_dt = cells.treatmt_start_dt AND u.treatmt_end_dt = cells.treatmt_end_dt
  LEFT JOIN pcq_dep_conflict c
    ON c.treatmt_start_dt = cells.treatmt_start_dt AND c.treatmt_end_dt = cells.treatmt_end_dt
  -- NOTE: this LEFT JOIN repeats the per-deployment n_unmapped/n_arm_conflict onto every decile row
  -- for that deployment identically (fan-out is intentional here) -- the pooled CTE below MUST
  -- aggregate these two columns with MAX, never SUM, or the decile count would inflate them.
),

-- ---- PCQ grain B: quarter (first-touch collapsed across all in-window waves -- v2 logic, unchanged) --
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
pcq_q_window AS (
  SELECT MIN(treatmt_start_dt) AS trt_start_dt, MAX(treatmt_end_dt) AS trt_end_dt FROM pcq_win
),
pcq_q_rows AS (
  SELECT CAST('quarter' AS VARCHAR(10)) AS grain, CAST('PCQ' AS VARCHAR(20)) AS mne,
    qwin.trt_start_dt, qwin.trt_end_dt,
    CAST('NG3_CHLN+NG3_CHLG' AS VARCHAR(30)) AS arm_test, CAST('NG3_CHMP' AS VARCHAR(30)) AS arm_ctrl,
    cells.test_clients AS n_test, cells.test_successes AS x_test,
    cells.control_clients AS n_ctrl, cells.control_successes AS x_ctrl,
    CAST(u.n_unmapped AS BIGINT) AS n_unmapped, CAST(c.n_arm_conflict AS BIGINT) AS n_arm_conflict
  FROM pcq_cells cells CROSS JOIN pcq_unmapped u CROSS JOIN pcq_conflict c CROSS JOIN pcq_q_window qwin
  -- NOTE: same intentional fan-out as pcq_dep_rows -- pooled CTE MUST use MAX on these two columns.
),

-- ==== TEAMMATE HOOK-UP POINT: add a new campaign as two more UNION ALL branches (dep + quarter),
-- same 12 columns ================================================================================
all_rows AS (
  SELECT * FROM pcl_dep_rows
  UNION ALL SELECT * FROM pcl_q_rows
  UNION ALL SELECT * FROM pcq_dep_rows
  UNION ALL SELECT * FROM pcq_q_rows
  -- UNION ALL SELECT * FROM <next_campaign>_dep_rows
  -- UNION ALL SELECT * FROM <next_campaign>_q_rows
),

-- ---- stats layer: campaign-and-grain-agnostic, sees only the contract. MH-weighted two-proportion
-- z-test, unchanged math from v2. GROUP BY carries grain + trt_start_dt/trt_end_dt so a campaign's
-- deployment rows and its quarter row never pool into each other. ------------------------------------
strata_base AS (
  SELECT grain, mne, trt_start_dt, trt_end_dt, arm_test, arm_ctrl,
    n_test AS n1, x_test AS x1, n_ctrl AS n0, x_ctrl AS x0, n_unmapped, n_arm_conflict,
    CAST(x_test AS FLOAT)/NULLIF(CAST(n_test AS FLOAT),0) - CAST(x_ctrl AS FLOAT)/NULLIF(CAST(n_ctrl AS FLOAT),0) AS d,
    (CAST(n_test AS FLOAT)*CAST(n_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS w,
    (CAST(x_test AS FLOAT)+CAST(x_ctrl AS FLOAT))/NULLIF(CAST(n_test AS FLOAT)+CAST(n_ctrl AS FLOAT),0)           AS pbar
  FROM all_rows
),
strata_v AS (
  SELECT grain, mne, trt_start_dt, trt_end_dt, arm_test, arm_ctrl, n1, x1, n0, x0, n_unmapped, n_arm_conflict, d, w,
    pbar * (1 - pbar) * (
      (CASE WHEN n1 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n1 END) +
      (CASE WHEN n0 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n0 END)
    ) AS v
  FROM strata_base
),
pooled AS (
  -- GROUP BY collapses PCQ's per-decile strata into one MH-weighted row per (grain, deployment/
  -- quarter); PCL has one stratum already at each grain. n_unmapped/n_arm_conflict are identical
  -- across every decile row for a given (campaign, grain-scope) (see pcq_*_rows notes above), so
  -- MAX returns the correct scalar without decile-count inflation.
  SELECT grain, mne, trt_start_dt, trt_end_dt, arm_test, arm_ctrl,
    SUM(n1) AS n_test, SUM(x1) AS x_test, SUM(n0) AS n_ctrl, SUM(x0) AS x_ctrl,
    SUM(w * d) / NULLIF(SUM(w), 0) AS lift, SQRT(SUM(w * w * v)) / NULLIF(SUM(w), 0) AS se,
    MAX(n_unmapped) AS n_unmapped, MAX(n_arm_conflict) AS n_arm_conflict
  FROM strata_v GROUP BY grain, mne, trt_start_dt, trt_end_dt, arm_test, arm_ctrl
),
stats AS (
  SELECT grain, mne, trt_start_dt, trt_end_dt, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl,
    lift, n_unmapped, n_arm_conflict,
    CASE WHEN se IS NULL OR se = 0 THEN NULL ELSE lift / se END AS z
  FROM pooled
)

-- ---- final output: deployment rows + quarter row per campaign, stats + diagnostic columns together --
-- rate_test/rate_ctrl are CRUDE (pooled counts). lift_pp for a stratified campaign (PCQ) is
-- MH-WEIGHTED across deciles, so rate_test - rate_ctrl will NOT equal lift_pp there. Expected.
-- *** Reminder: grain='deployment' rows must never be summed to reproduce the grain='quarter' row --
-- a multi-deployment client is counted once per deployment but only once, first-touch, in quarter. ***
-- Single SELECT, no UNION ALL at this level -- ORDER BY column names is safe here (the "positional
-- only" rule applies to a query with UNION ALL at the outer level, which this no longer has).
-- Order choice: deployment rows in date order, quarter row last per campaign (deterministic via the
-- CASE on grain, then trt_start_dt).
SELECT
  grain, mne, trt_start_dt, trt_end_dt, arm_test, arm_ctrl, n_test, x_test, n_ctrl, x_ctrl,
  100.0 * CAST(x_test AS FLOAT) / NULLIF(CAST(n_test AS FLOAT), 0) AS rate_test_pct,
  100.0 * CAST(x_ctrl AS FLOAT) / NULLIF(CAST(n_ctrl AS FLOAT), 0) AS rate_ctrl_pct,
  lift * 100 AS lift_pp, z,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.2816 THEN 'Y' ELSE 'N' END AS sig80,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.6449 THEN 'Y' ELSE 'N' END AS sig90,
  CASE WHEN z IS NULL THEN NULL WHEN ABS(z) >= 1.9600 THEN 'Y' ELSE 'N' END AS sig95,
  n_unmapped, n_arm_conflict
FROM stats
ORDER BY mne, CASE WHEN grain = 'quarter' THEN 1 ELSE 0 END, trt_start_dt;
