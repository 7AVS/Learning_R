-- value_capture/value_capture_report.sql
-- SINGLE Teradata-direct query producing the value-capture FINAL REPORT rows: a QUARTERLY,
-- ONE-LINE-PER-CAMPAIGN rollup (one row per test contrast, stratified lift + significance computed
-- in SQL -- no workbook, no Python).
-- Engine: TERADATA-DIRECT throughout (bare table names, NO catalog prefix -- do NOT run through
--   Starburst federation). Both curated tables are native Teradata objects here:
--   DL_MR_PROD.cards_pli_decision_resp, DL_MR_PROD.cards_tpa_pcq_decision_resp. This query does NOT
--   touch the tactic-event table (DG6V01.TACTIC_EVNT_IP_AR_HIST) -- it's the ASSIGNMENT contrast on
--   test_group_latest only, so it needs no delivery-flag scan.
-- No volatile tables needed: both campaign chains prune on decsn_year/treatmt-date filters; PCQ's
--   arm mapping is an inline CASE (no join, no separate arm_map CTE), and the only product joins are
--   CROSS JOINs against one-row aggregate CTEs (pcl_window/pcq_window), which are bounded and do not
--   trip the TDWM unconstrained-product-join blocker. If spool becomes an issue in practice, pcl_win /
--   pcq_win are the two CTEs to materialize as VOLATILE TABLEs with COLLECT STATISTICS -- default
--   here is plain CTEs.
--
-- ============================================================================
-- WHY THIS IS A CLIENT-DEDUPED REBUILD, NOT A SUM OVER THE PER-COHORT BLOCK FILES
-- ============================================================================
-- blocks/pcl_sales_modal_block.sql and blocks/pcq_ms_block.sql are UNCHANGED and stay at cohort_month
--   grain, START-date windowed, for per-cohort presentation. Summing THEIR output across cohort_month
--   would DOUBLE-COUNT any client who appears in more than one cohort (a real risk for PCQ, which
--   carries every account row, and a confirmed risk for PCL -- see
--   campaigns/sales_modal/pcl/p2c_deployment_structure.sql's D2 diagnostic, "clients_in_both_arms").
--   So this query does NOT read from the block files or sum their output. It rebuilds the same
--   population directly from the curated tables with THREE rules the quarterly report requires that
--   the per-cohort blocks do not use:
--   1. INCLUSION WINDOW = TREATMENT END DATE IN THE QUARTER (not start date): rows/cohorts where the
--      treatment window's END date falls in [2026-05-01, 2026-07-31] (inclusive). Applies to BOTH
--      campaigns -- this replaces the block files' treatmt_strt_dt start-window for THIS query only.
--   2. FIRST-TOUCH CLIENT COLLAPSE: per clnt_no, the in-window row with the EARLIEST treatment start
--      date defines that client's single arm_role and stratum (decile for PCQ; PCL has no
--      stratification here so this is moot beyond determinism) -- ROW_NUMBER() OVER (PARTITION BY
--      clnt_no ORDER BY <treatment start col> ASC, decile ASC), keep rn=1. Each client contributes to
--      exactly ONE (stratum, arm) cell -- see the worked walkthrough below.
--   3. SUCCESS ON THE DEDUPED CLIENT = EVER-CONVERTED: MAX(success flag) over ALL of that client's
--      in-window rows (any cohort), not just their first-touch row. PCL: ever_responder_cli. PCQ:
--      ever_approved_asc (Period-ASC gated) and ever_approved_raw (ungated), computed separately.
--
-- COLUMN VERIFICATION (treatment END-date column -- do not guess, both confirmed against live repo
--   queries on the SAME curated tables used here):
--   PCL  treatmt_end_dt  on dw00_im.dl_mr_prod.cards_pli_decision_resp -- confirmed in
--        campaigns/sales_modal/pcl/p2c_deployment_structure.sql (lines 15-17, 22: selected alongside
--        treatmt_strt_dt from that exact table; D2 in that same file is literally the arm-conflict
--        diagnostic this query now implements as pcl_conflict_row).
--   PCQ  treatmt_end_dt  on cards_tpa_pcq_decision_resp -- confirmed in
--        campaigns/PCQ/next_best_card/pcq_q1_26_strategy_trend.sql (line 38, `DATABASE DL_MR_PROD;`
--        then `FROM cards_tpa_pcq_decision_resp`, selected alongside treatmt_start_dt/test_group_latest
--        /model_score_decile/app_approved/asc_on_app_source -- same columns already in use here); also
--        appears in pcq_q1_26_monthly_balance.sql, pcq_q1_26_period_asc.sql, _q2_v2_halo.sql.
--
-- ARM-CONFLICT DIAGNOSTIC (both campaigns): a client whose arm_role differs across their in-window
--   rows (e.g. challenger in one deployment/wave, champion in another) resolves to their FIRST-TOUCH
--   arm per rule 2 above -- but their ever-success flag (rule 3) is computed across ALL rows, so for a
--   conflicted client that ever-success may have actually happened under the OTHER arm. This is
--   surfaced, not hidden: pcl_conflict_row / pcq_conflict_row emit a count of such clients per
--   campaign (counts NULL otherwise, same "diagnostic row" pattern as pcq_unmapped_row below).
--
-- RECONCILIATION: this query intentionally does NOT reconcile to a simple re-aggregation of
--   p9_vcl_full_measurement.sql or pcq_ms_summary.sql QUERY 2 the way the per-cohort blocks do --
--   the end-date window and first-touch collapse are deliberate corrections that change which rows
--   count and how clients are grained. Population/arm/success FLAGS themselves (report_groups_period
--   pattern, responder_cli, test_group_latest, app_approved, asc_on_app_source) are copied verbatim
--   from the same source files as before.
--
-- STATS (spelled out; all in FLOAT arithmetic on POST-AGGREGATION counts -- 9881-safe, since no
--   ROUND/division ever touches a raw Teradata-sourced column, only locally-computed SUM() results;
--   UNCHANGED from the prior version -- only the inputs feeding `paired` changed, not the math):
--   per (mne, test_desc, stratum) -- already one row per stratum out of each campaign's *_cells CTE,
--     so `paired`'s SUM is a harmless passthrough (kept so a future teammate block that legitimately
--     delivers multiple rows per stratum still aggregates correctly): n1=SUM(test_clients),
--     x1=SUM(test_successes), n0=SUM(control_clients), x0=SUM(control_successes)
--   d = x1/n1 - x0/n0          (arm rate difference, NULLIF-guarded)
--   w = n1*n0/(n1+n0)          (stratum weight)
--   pbar = (x1+x0)/(n1+n0)     (pooled rate)
--   v = pbar*(1-pbar)*(1/n1+1/n0)
--   per (mne, test_desc), pooling stratum: leads=SUM(n1); lift=SUM(w*d)/SUM(w);
--     se=SQRT(SUM(w*w*v))/SUM(w); z=lift/se; p_value=2*(1-normal_cdf_approx(ABS(z)));
--     significance = IF p_value<0.05 THEN 'Y' ELSE 'N'.
--   Single-stratum tests (PCL: stratum='overall' only) collapse this to the standard two-proportion
--   z-test automatically -- w/(sum of one w) = 1, so lift=d, se=SQRT(v).
--   normal_cdf DOES NOT EXIST IN TERADATA: the standard-normal CDF is approximated below via the
--   Zelen & Severo / Abramowitz & Stegun 26.2.17 rational approximation (max abs error on the CDF
--   itself < 7.5e-8; since p_value doubles that, the observed error on p_value is up to ~1.5e-7 --
--   still many orders of magnitude below anything that could flip a significance call at p<0.05).
--
-- OPEN DECISIONS inherited from campaigns/sales_modal/README.md (NOT resolved by this query):
--   #1 Period-ASC gating (PCQ) -- both variants shipped as separate test_desc rows below, Andre
--      picks which goes to the partner sheet.
--   #2 population split (PCQ) -- this query uses ASSIGNMENT (test_group_latest), not delivery
--      (ms_targeted), per the block file's header note (only ITT is valid for a lift-flavored read).
--
-- Output is decision-sized (<=~10 rows): 1 PCL row + 1 PCQ-gated row + 1 PCQ-ungated row, plus
--   validation rows IF (and only if) unmapped test_group_latest codes or arm-conflicted clients exist.

WITH

-- ============================================================================
-- PCL: population/arm/success FLAGS copied verbatim from p9_vcl_full_measurement.sql; window rule
-- and client-dedup collapse are NEW for this quarterly query (see header).
-- ============================================================================
pcl_win AS (
  SELECT
    clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'test'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'control' END AS arm_role,
    decile,                     -- carried ONLY as a deterministic first-touch tie-break (rule 2);
                                -- PCL's own contract stratum stays 'overall' (no decile stratification)
    responder_cli,
    treatmt_strt_dt,
    treatmt_end_dt
  FROM DL_MR_PROD.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'   -- EDIT POINT: quarter window (TREATMENT END date)
),
pcl_ft AS (
  -- first-touch: earliest treatmt_strt_dt per client defines their arm_role for the quarter
  SELECT
    clnt_no, arm_role,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt ASC, decile ASC) AS rn
  FROM pcl_win
),
pcl_succ AS (
  -- ever-success + arm-conflict flag, over ALL of the client's in-window rows (any cohort)
  SELECT
    clnt_no,
    MAX(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END) AS ever_responder_cli,
    COUNT(DISTINCT arm_role)                            AS n_arms
  FROM pcl_win
  GROUP BY clnt_no
),
pcl_client AS (
  -- ONE row per client: first-touch arm + ever-success + conflict flag
  SELECT f.clnt_no, f.arm_role, s.ever_responder_cli, s.n_arms
  FROM pcl_ft f
  JOIN pcl_succ s ON s.clnt_no = f.clnt_no
  WHERE f.rn = 1
),
pcl_cells AS (
  -- one row (stratum='overall'): each client counted exactly once, in exactly one arm
  SELECT
    CAST('overall' AS VARCHAR(20)) AS stratum,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                        AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_responder_cli = 1 THEN clnt_no END) AS test_successes,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                        AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_responder_cli = 1 THEN clnt_no END) AS control_successes
  FROM pcl_client
),
pcl_window AS (
  -- one-row aggregate, CROSS JOINed below (Teradata is unreliable with scalar subselects in a
  -- CTE's SELECT list)
  SELECT MIN(treatmt_strt_dt) AS trt_start_dt, MAX(treatmt_end_dt) AS trt_end_dt
  FROM pcl_win
),
pcl_rows AS (
  -- UNION-truncation guard: this is the FIRST branch of all_rows below, so its VARCHAR widths fix
  -- the output column widths for every branch (Teradata rule -- see CLAUDE.md Teradata Quirks #3).
  SELECT
    CAST('PCL' AS VARCHAR(10))                                     AS mne,
    CAST('Sales Modal (served) vs BAU (not served)' AS VARCHAR(300)) AS test_desc,
    w.trt_start_dt                                                  AS trt_start_dt,
    w.trt_end_dt                                                    AS trt_end_dt,
    CAST('Credit limit increase accepted' AS VARCHAR(100))          AS success_name,
    c.stratum                                                       AS stratum,
    test_clients,
    test_successes,
    control_clients,
    control_successes
  FROM pcl_cells c
  CROSS JOIN pcl_window w
),
pcl_conflict_row AS (
  -- Diagnostic: clients seen in BOTH arms across in-window rows, resolved to first-touch (rule 2).
  -- Surfaces only if such clients exist -- counts NULL, same pattern as pcq_unmapped_row below.
  SELECT
    CAST('PCL' AS VARCHAR(10)) AS mne,
    CAST('ARM-CONFLICT clients (first-touch arm used): ' || CAST(COUNT(*) AS VARCHAR(10)) AS VARCHAR(300)) AS test_desc,
    CAST(NULL AS DATE)          AS trt_start_dt,
    CAST(NULL AS DATE)          AS trt_end_dt,
    CAST(NULL AS VARCHAR(100))  AS success_name,
    CAST('overall' AS VARCHAR(20)) AS stratum,
    CAST(NULL AS BIGINT)        AS test_clients,
    CAST(NULL AS BIGINT)        AS test_successes,
    CAST(NULL AS BIGINT)        AS control_clients,
    CAST(NULL AS BIGINT)        AS control_successes
  FROM pcl_client
  WHERE n_arms > 1
  HAVING COUNT(*) > 0
),

-- ============================================================================
-- PCQ: population/arm/success FLAGS copied verbatim from pcq_ms_summary.sql / pcq_ms_vintage.sql;
-- window rule and client-dedup collapse are NEW for this quarterly query (see header).
-- arm_role mapped INLINE via CASE -- a prior version used a separate arm_map CTE built from a bare
-- SELECT-of-constants UNION ALL (no FROM clause); Teradata rejects a FROM-less SELECT outright and
-- the query aborted. Folding the map into a CASE removes both the FROM-less SELECT and the join.
-- ============================================================================
pcq_win AS (
  -- EDIT POINT / VERIFY BEFORE RUNNING: codes drift across sources (NG3_CHMP champion; NG3_CHLN/
  -- NG3_CHLG challengers confirmed in pcq_ms_vintage.sql 2026-06-19 note; CHLD reported seen
  -- elsewhere but NOT confirmed in this repo -- deliberately left unmapped so it surfaces in
  -- pcq_unmapped_row rather than being guessed). Re-pull DISTINCT test_group_latest from a live
  -- query before trusting this map; unmapped codes surface below, they do not silently drop.
  SELECT
    r.clnt_no,
    TRIM(r.test_group_latest)                                                 AS test_group_latest,
    CASE WHEN TRIM(r.test_group_latest) = 'NG3_CHMP'               THEN 'control'
         WHEN TRIM(r.test_group_latest) IN ('NG3_CHLN','NG3_CHLG') THEN 'test' END AS arm_role,
    CAST(r.model_score_decile AS VARCHAR(10)) AS decile,
    r.treatmt_start_dt,
    r.treatmt_end_dt,
    r.app_approved,
    r.asc_on_app_source
  FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
  WHERE r.decsn_year       = 2026
    AND r.tpa_ita          = 'TPA'
    AND r.treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-07-31'  -- EDIT POINT: quarter window (TREATMENT END date)
),
pcq_unmapped AS (
  SELECT DISTINCT test_group_latest FROM pcq_win WHERE arm_role IS NULL
),
pcq_unmapped_row AS (
  -- Surfaces as a real output row (not a silent drop) if any test_group_latest code fails arm_map.
  -- No array_agg/array_join in Teradata -- message built from COUNT/MIN/MAX instead of a full list.
  SELECT
    CAST('PCQ' AS VARCHAR(10)) AS mne,
    CAST('UNMAPPED test_group codes: ' || CAST(COUNT(*) AS VARCHAR(5)) || ' code(s), e.g. '
         || MIN(test_group_latest) || ' .. ' || MAX(test_group_latest)
         || ' -- fix arm_map and rerun' AS VARCHAR(300))            AS test_desc,
    CAST(NULL AS DATE)          AS trt_start_dt,
    CAST(NULL AS DATE)          AS trt_end_dt,
    CAST(NULL AS VARCHAR(100))  AS success_name,
    CAST('overall' AS VARCHAR(20)) AS stratum,
    CAST(NULL AS BIGINT)        AS test_clients,
    CAST(NULL AS BIGINT)        AS test_successes,
    CAST(NULL AS BIGINT)        AS control_clients,
    CAST(NULL AS BIGINT)        AS control_successes
  FROM pcq_unmapped
  HAVING COUNT(*) > 0
),
pcq_ft AS (
  -- first-touch: earliest treatmt_start_dt (decile as deterministic tie-break) per client, among
  -- rows with a RESOLVED arm_role only -- unmapped-code rows can't anchor a test/control cell.
  SELECT
    clnt_no, arm_role, decile,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_start_dt ASC, decile ASC) AS rn
  FROM pcq_win
  WHERE arm_role IS NOT NULL
),
pcq_succ AS (
  -- ever-success (both gate variants) + arm-conflict flag, over ALL in-window rows (any cohort).
  -- COUNT(DISTINCT arm_role) ignores NULL automatically, so unmapped rows never count as an "arm".
  SELECT
    clnt_no,
    MAX(CASE WHEN app_approved = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN 1 ELSE 0 END) AS ever_approved_asc,
    MAX(CASE WHEN app_approved = 1 THEN 1 ELSE 0 END)                                            AS ever_approved_raw,
    COUNT(DISTINCT arm_role)                                                                       AS n_arms
  FROM pcq_win
  GROUP BY clnt_no
),
pcq_client AS (
  -- ONE row per (arm-resolved) client: first-touch arm/decile + ever-success + conflict flag
  SELECT f.clnt_no, f.arm_role, f.decile, s.ever_approved_asc, s.ever_approved_raw, s.n_arms
  FROM pcq_ft f
  JOIN pcq_succ s ON s.clnt_no = f.clnt_no
  WHERE f.rn = 1
),
pcq_cells AS (
  -- one row per decile: each client counted exactly once, in exactly one arm, within their
  -- first-touch decile
  SELECT
    decile,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END)                              AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_approved_asc = 1 THEN clnt_no END)     AS test_successes_asc,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    AND ever_approved_raw = 1 THEN clnt_no END)     AS test_successes_raw,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END)                              AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_approved_asc = 1 THEN clnt_no END)     AS control_successes_asc,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' AND ever_approved_raw = 1 THEN clnt_no END)     AS control_successes_raw
  FROM pcq_client
  GROUP BY decile
),
pcq_window AS (
  -- one-row aggregate, CROSS JOINed below (Teradata is unreliable with scalar subselects in a
  -- CTE's SELECT list). Uses ALL in-window rows (mapped or not) -- the observed window is a fact
  -- about the data, independent of arm-mapping success.
  SELECT MIN(treatmt_start_dt) AS trt_start_dt, MAX(treatmt_end_dt) AS trt_end_dt
  FROM pcq_win
),
-- success metric = approved only (Open Decision #1 stays visible as two rows: gated vs ungated).
-- completed_asc/completed_raw are NOT carried into this rollup -- Andre picked "approved" as the
-- reported success metric here; both metrics remain available at cohort grain in pcq_ms_block.sql.
pcq_rows_gated AS (
  SELECT
    CAST('PCQ' AS VARCHAR(10)) AS mne,
    CAST('Modal Sales assignment (challenger) vs champion -- approved (Period-ASC gated)' AS VARCHAR(300)) AS test_desc,
    w.trt_start_dt                                AS trt_start_dt,
    w.trt_end_dt                                  AS trt_end_dt,
    CAST('App approved' AS VARCHAR(100))          AS success_name,
    CAST('D' || c.decile AS VARCHAR(20))          AS stratum,
    test_clients,
    test_successes_asc                            AS test_successes,
    control_clients,
    control_successes_asc                         AS control_successes
  FROM pcq_cells c
  CROSS JOIN pcq_window w
),
pcq_rows_ungated AS (
  SELECT
    CAST('PCQ' AS VARCHAR(10)) AS mne,
    CAST('Modal Sales assignment (challenger) vs champion -- approved (ungated)' AS VARCHAR(300)) AS test_desc,
    w.trt_start_dt                                AS trt_start_dt,
    w.trt_end_dt                                  AS trt_end_dt,
    CAST('App approved' AS VARCHAR(100))          AS success_name,
    CAST('D' || c.decile AS VARCHAR(20))          AS stratum,
    test_clients,
    test_successes_raw                            AS test_successes,
    control_clients,
    control_successes_raw                         AS control_successes
  FROM pcq_cells c
  CROSS JOIN pcq_window w
),
pcq_conflict_row AS (
  -- Diagnostic: clients seen in BOTH arms across in-window rows, resolved to first-touch (rule 2).
  -- Surfaces only if such clients exist -- counts NULL, same pattern as pcq_unmapped_row above.
  SELECT
    CAST('PCQ' AS VARCHAR(10)) AS mne,
    CAST('ARM-CONFLICT clients (first-touch arm used): ' || CAST(COUNT(*) AS VARCHAR(10)) AS VARCHAR(300)) AS test_desc,
    CAST(NULL AS DATE)          AS trt_start_dt,
    CAST(NULL AS DATE)          AS trt_end_dt,
    CAST(NULL AS VARCHAR(100))  AS success_name,
    CAST('overall' AS VARCHAR(20)) AS stratum,
    CAST(NULL AS BIGINT)        AS test_clients,
    CAST(NULL AS BIGINT)        AS test_successes,
    CAST(NULL AS BIGINT)        AS control_clients,
    CAST(NULL AS BIGINT)        AS control_successes
  FROM pcq_client
  WHERE n_arms > 1
  HAVING COUNT(*) > 0
),

-- ============================================================================
-- all_rows -- TEAMMATE HOOK-UP POINT. Each new campaign block gets added as a CTE emitting the
-- contract shape (mne, test_desc, trt_start_dt, trt_end_dt, success_name, stratum, test_clients,
-- test_successes, control_clients, control_successes -- already client-deduped to the quarter,
-- NO cohort_month) and UNION'd here.
-- ============================================================================
all_rows AS (
  SELECT * FROM pcl_rows
  UNION ALL
  SELECT * FROM pcq_rows_gated
  UNION ALL
  SELECT * FROM pcq_rows_ungated
  UNION ALL
  SELECT * FROM pcq_unmapped_row
  UNION ALL
  SELECT * FROM pcl_conflict_row
  UNION ALL
  SELECT * FROM pcq_conflict_row
  -- UNION ALL SELECT * FROM <next_campaign>_rows   -- add future teammate blocks here
),

-- ============================================================================
-- paired -- (mne, test_desc, stratum) is already one row per stratum out of each campaign's *_cells
-- CTE (client-deduped above); this SUM is a harmless passthrough that keeps future teammate blocks
-- safe if they legitimately deliver more than one row per stratum.
-- ============================================================================
paired AS (
  SELECT
    mne, test_desc, stratum,
    SUM(test_clients)     AS n1,
    SUM(test_successes)   AS x1,
    SUM(control_clients)  AS n0,
    SUM(control_successes) AS x0
  FROM all_rows
  GROUP BY mne, test_desc, stratum
),

-- ============================================================================
-- strata_stats -- per-stratum building blocks for the stratified two-proportion test.
-- All arithmetic on aggregated (post-SUM) counts, cast to FLOAT, NULLIF-guarded. UNCHANGED.
-- ============================================================================
strata_base AS (
  SELECT
    mne, test_desc, stratum, n1, x1, n0, x0,
    CAST(x1 AS FLOAT) / NULLIF(CAST(n1 AS FLOAT), 0)
      - CAST(x0 AS FLOAT) / NULLIF(CAST(n0 AS FLOAT), 0)                          AS d,
    (CAST(n1 AS FLOAT) * CAST(n0 AS FLOAT))
      / NULLIF(CAST(n1 AS FLOAT) + CAST(n0 AS FLOAT), 0)                          AS w,
    (CAST(x1 AS FLOAT) + CAST(x0 AS FLOAT))
      / NULLIF(CAST(n1 AS FLOAT) + CAST(n0 AS FLOAT), 0)                          AS pbar
  FROM paired
),
strata_stats AS (
  SELECT
    mne, test_desc, stratum, n1, x1, n0, x0, d, w, pbar,
    pbar * (1 - pbar) * (
      (CASE WHEN n1 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n1 END) +
      (CASE WHEN n0 = 0 THEN CAST(0 AS FLOAT) ELSE CAST(1 AS FLOAT) / n0 END)
    )                                                                                AS v
  FROM strata_base
),

-- ============================================================================
-- test_stats -- pool strata per (mne, test_desc): weighted lift, SE, z. p_value follows below via
-- the Zelen-Severo CDF approximation. UNCHANGED. Single-stratum tests (PCL) reduce to the plain
-- two-proportion z-test automatically, since a single stratum's w cancels out of SUM(w*d)/SUM(w)
-- leaving d, and SUM(w*w*v)/SUM(w) leaves w*v = w*pbar(1-pbar)(1/n1+1/n0); dividing by SUM(w)=w
-- again leaves SQRT(v) = the plain two-prop SE.
-- ============================================================================
test_stats AS (
  SELECT
    mne, test_desc,
    SUM(n1)     AS leads,
    SUM(w * d)  AS sum_wd,
    SUM(w)      AS sum_w,
    SUM(w * w * v) AS sum_w2v
  FROM strata_stats
  GROUP BY mne, test_desc
),
test_stats2 AS (
  SELECT
    mne, test_desc, leads,
    sum_wd / NULLIF(sum_w, 0)              AS lift,
    SQRT(sum_w2v) / NULLIF(sum_w, 0)        AS se
  FROM test_stats
),
test_stats3 AS (
  SELECT
    mne, test_desc, leads, lift, se,
    CASE WHEN se IS NULL OR se = 0 THEN NULL ELSE lift / se END AS z
  FROM test_stats2
),

-- ============================================================================
-- Zelen & Severo / Abramowitz & Stegun 26.2.17 standard-normal CDF approximation (no normal_cdf in
-- Teradata). t computed once and carried through the chain; powers done as repeated multiplication
-- (Teradata-safe, avoids **). Max abs error on the CDF itself < 7.5e-8. UNCHANGED.
-- ============================================================================
zs_base AS (
  SELECT mne, test_desc, leads, lift, se, z, ABS(z) AS az
  FROM test_stats3
),
zs_t AS (
  SELECT mne, test_desc, leads, lift, se, z, az,
         CAST(1 AS FLOAT) / (1 + 0.2316419 * az) AS t
  FROM zs_base
),
zs_phi AS (
  SELECT mne, test_desc, leads, lift, se, z, az, t,
         CAST(0.3989422804014327 AS FLOAT) * EXP(-az * az / 2) AS phi
  FROM zs_t
),
zs_cdf AS (
  SELECT mne, test_desc, leads, lift, se, z,
         1 - phi * ( 0.319381530 * t
                   - 0.356563782 * t * t
                   + 1.781477937 * t * t * t
                   - 1.821255978 * t * t * t * t
                   + 1.330274429 * t * t * t * t * t )               AS cdf
  FROM zs_phi
),
test_stats4 AS (
  SELECT
    mne, test_desc, leads, lift, se, z,
    CASE WHEN z IS NULL THEN NULL ELSE 2 * (1 - cdf) END AS p_value
  FROM zs_cdf
),

-- ============================================================================
-- test_window -- trt_start_dt/trt_end_dt/success_name carried per (mne, test_desc), from all_rows.
-- ============================================================================
test_window AS (
  SELECT
    mne, test_desc,
    MIN(trt_start_dt) AS trt_start_dt,
    MAX(trt_end_dt)   AS trt_end_dt,
    MIN(success_name) AS success_name
  FROM all_rows
  GROUP BY mne, test_desc
)

-- ============================================================================
-- FINAL SELECT -- partner-template left-to-right column layout. DESC/Type/Reference Document/Notes
-- are entered manually on the partner sheet -- emitted here as NULL placeholders in their correct
-- template positions so the column order lines up when pasted.
-- ============================================================================
SELECT
  w.mne,
  CAST(NULL AS VARCHAR(300)) AS desc_manual,          -- partner sheet: DESC (manual)
  CAST(NULL AS VARCHAR(300)) AS type_manual,          -- partner sheet: Type (manual)
  w.test_desc,                                        -- partner sheet: Test Desc
  w.trt_start_dt                             AS trt_start_dt,   -- partner sheet: Treatment Start Date
  w.trt_end_dt                                AS trt_end_dt,     -- partner sheet: Treatment End Date
  w.success_name,                                      -- partner sheet: Success
  s.leads                                    AS leads_unique_clients,   -- partner sheet: Leads/Unique Clients
  ROUND(s.lift * 100, 2)                     AS lift_pp,                -- partner sheet: Lift (percentage points)
  s.z,                                                                   -- supporting stat (audit)
  s.p_value,                                                            -- supporting stat (audit; Zelen-Severo approx)
  CASE WHEN s.p_value IS NULL THEN NULL
       WHEN s.p_value < 0.05 THEN 'Y' ELSE 'N' END AS significance,     -- partner sheet: P-value/Significance
  CAST(NULL AS VARCHAR(300)) AS reference_document,   -- partner sheet: Reference Document (manual)
  CAST(NULL AS VARCHAR(300)) AS notes                 -- partner sheet: Notes (manual)
FROM test_stats4 s
JOIN test_window w ON w.mne = s.mne AND w.test_desc = s.test_desc
ORDER BY w.mne, w.test_desc;
