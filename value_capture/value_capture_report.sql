-- value_capture/value_capture_report.sql
-- SINGLE Teradata-direct query producing the value-capture FINAL REPORT rows (one row per test
-- contrast, stratified lift + significance computed in SQL -- no workbook, no Python).
-- Engine: TERADATA-DIRECT throughout (bare table names, NO catalog prefix -- do NOT run through
--   Starburst federation). Both curated tables are native Teradata objects here:
--   DL_MR_PROD.cards_pli_decision_resp, DL_MR_PROD.cards_tpa_pcq_decision_resp. This query does NOT
--   touch the tactic-event table (DG6V01.TACTIC_EVNT_IP_AR_HIST) -- it's the ASSIGNMENT contrast on
--   test_group_latest only, so it needs no delivery-flag scan.
-- No volatile tables needed: both campaign CTEs prune on decsn_year/treatmt-date filters, and the
--   only join beyond that is against a 3-row literal arm_map (not a cross join against
--   sys_calendar.calendar, so the TDWM unconstrained-product-join blocker does not apply here). If
--   spool becomes an issue in practice, pcl_pop / pcq_base are the two CTEs to materialize as
--   VOLATILE TABLEs with COLLECT STATISTICS -- default here is plain CTEs.
--
-- For PER-COHORT presentation (cohort_month grain, not pooled), query blocks/pcl_sales_modal_block.sql
--   and blocks/pcq_ms_block.sql directly -- they are UNCHANGED (pcl block also Teradata-direct now;
--   pcq block already was) and stay at that grain. This query pools their same population/arm/success
--   logic across cohort_month and decile to the final per-test-contrast numbers the partner sheet needs.
--
-- RECONCILIATION (carried over from the two block files):
--   pcl_rows  -- reconciles to campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql: p9's clients/
--     converted_clients, summed over every dim except cohort_month/arm, equal test_clients/
--     test_successes (arm=challenger) and control_clients/control_successes (arm=champion) here.
--   pcq_rows_* -- reconciles to campaigns/sales_modal/pcq/pcq_ms_summary.sql QUERY 2: same base
--     table/filters; QUERY 2's clients/approved, summed over its other grouping columns for a fixed
--     (test_group_latest, model_score_decile) bucketed by cohort_month, equal this query's
--     clients/approved_asc/approved_raw before arm-mapping and stratum pooling.
--
-- STATS (spelled out; all in FLOAT arithmetic on POST-AGGREGATION counts -- 9881-safe, since no
--   ROUND/division ever touches a raw Teradata-sourced column, only locally-computed SUM() results):
--   per (mne, test_desc, stratum), pooling cohort_month: n1=SUM(test_clients), x1=SUM(test_successes),
--     n0=SUM(control_clients), x0=SUM(control_successes)
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
-- Output is decision-sized (<=~10 rows): 1 PCL row + 1 PCQ-gated row + 1 PCQ-ungated row, plus a
--   validation row IF (and only if) unmapped test_group_latest codes are found in the data.

WITH

-- ============================================================================
-- PCL: ported verbatim from blocks/pcl_sales_modal_block.sql (pop/pop1/by_cohort logic unchanged)
-- ============================================================================
pcl_pop AS (
  SELECT
    clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    responder_cli,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn
  FROM DL_MR_PROD.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'   -- EDIT POINT: window
),
pcl_pop1 AS (
  SELECT clnt_no, arm, responder_cli,
         CAST(CAST(treatmt_strt_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)) AS cohort_month
  FROM pcl_pop WHERE rn = 1
),
pcl_by_cohort AS (
  SELECT
    cohort_month,
    COUNT(DISTINCT CASE WHEN arm = 'challenger' THEN clnt_no END)                       AS test_clients,
    COUNT(DISTINCT CASE WHEN arm = 'challenger' AND responder_cli = 1 THEN clnt_no END)  AS test_successes,
    COUNT(DISTINCT CASE WHEN arm = 'champion'   THEN clnt_no END)                        AS control_clients,
    COUNT(DISTINCT CASE WHEN arm = 'champion'   AND responder_cli = 1 THEN clnt_no END)  AS control_successes
  FROM pcl_pop1
  GROUP BY cohort_month
),
pcl_window AS (
  -- one-row aggregate, CROSS JOINed below (Teradata is unreliable with scalar subselects in a
  -- CTE's SELECT list)
  SELECT MIN(treatmt_strt_dt) AS trt_start_dt, MAX(treatmt_strt_dt) AS trt_end_dt
  FROM pcl_pop
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
    CAST('overall' AS VARCHAR(20))                                  AS stratum,
    CAST(bc.cohort_month AS VARCHAR(7))                             AS cohort_month,
    test_clients,
    test_successes,
    control_clients,
    control_successes
  FROM pcl_by_cohort bc
  CROSS JOIN pcl_window w
),

-- ============================================================================
-- PCQ: ported from blocks/pcq_ms_block.sql, arm-mapped in SQL. Base filters/table copied verbatim
-- from pcq_ms_summary.sql QUERY 2's base CTE.
-- ============================================================================
arm_map AS (
  -- EDIT POINT / VERIFY BEFORE RUNNING: codes drift across sources (CHLN/CHLG confirmed in
  -- pcq_ms_vintage.sql 2026-06-19 header note; CHLD reported seen elsewhere but NOT confirmed in
  -- this repo -- NOT added here per the no-guessing-codes rule). Re-pull DISTINCT test_group_latest
  -- from a live query before trusting this map; unmapped codes surface in pcq_unmapped_row below,
  -- they do not silently drop.
  -- Rewritten as literal UNION ALL (Teradata doesn't reliably support a VALUES row-constructor CTE);
  -- first branch CAST per the UNION-truncation rule (CLAUDE.md Teradata Quirks #3).
  SELECT CAST('NG3_CHMP' AS VARCHAR(20)) AS test_group_latest, CAST('control' AS VARCHAR(10)) AS arm_role
  UNION ALL
  SELECT 'NG3_CHLN', 'test'
  UNION ALL
  SELECT 'NG3_CHLG', 'test'
),
pcq_base AS (
  SELECT
    clnt_no,
    TRIM(test_group_latest)                                            AS test_group_latest,
    CAST(model_score_decile AS VARCHAR(10))                            AS decile,
    CAST(CAST(treatmt_start_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)) AS cohort_month,
    treatmt_start_dt,
    app_approved,
    asc_on_app_source
  FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
  WHERE decsn_year       = 2026
    AND tpa_ita          = 'TPA'
    AND treatmt_start_dt >= DATE '2026-06-01'            -- EDIT POINT: window
),
pcq_window AS (
  -- one-row aggregate, CROSS JOINed below (Teradata is unreliable with scalar subselects in a
  -- CTE's SELECT list)
  SELECT MIN(treatmt_start_dt) AS trt_start_dt, MAX(treatmt_start_dt) AS trt_end_dt
  FROM pcq_base
),
pcq_mapped AS (
  SELECT b.*, m.arm_role
  FROM pcq_base b
  LEFT JOIN arm_map m ON m.test_group_latest = b.test_group_latest
),
pcq_unmapped AS (
  SELECT DISTINCT test_group_latest FROM pcq_mapped WHERE arm_role IS NULL
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
    CAST(NULL AS VARCHAR(7))    AS cohort_month,
    CAST(NULL AS BIGINT)        AS test_clients,
    CAST(NULL AS BIGINT)        AS test_successes,
    CAST(NULL AS BIGINT)        AS control_clients,
    CAST(NULL AS BIGINT)        AS control_successes
  FROM pcq_unmapped
  HAVING COUNT(*) > 0
),
pcq_agg AS (
  SELECT
    decile,
    cohort_month,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'    THEN clnt_no END) AS test_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'
                          AND app_approved = 1
                          AND TRIM(asc_on_app_source) = 'Period-ASC' THEN clnt_no END) AS test_successes_asc,
    COUNT(DISTINCT CASE WHEN arm_role = 'test'
                          AND app_approved = 1 THEN clnt_no END)     AS test_successes_raw,
    COUNT(DISTINCT CASE WHEN arm_role = 'control' THEN clnt_no END) AS control_clients,
    COUNT(DISTINCT CASE WHEN arm_role = 'control'
                          AND app_approved = 1
                          AND TRIM(asc_on_app_source) = 'Period-ASC' THEN clnt_no END) AS control_successes_asc,
    COUNT(DISTINCT CASE WHEN arm_role = 'control'
                          AND app_approved = 1 THEN clnt_no END)     AS control_successes_raw
  FROM pcq_mapped
  WHERE arm_role IS NOT NULL
  GROUP BY decile, cohort_month
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
    CAST('D' || a.decile AS VARCHAR(20))          AS stratum,
    CAST(a.cohort_month AS VARCHAR(7))            AS cohort_month,
    test_clients,
    test_successes_asc                            AS test_successes,
    control_clients,
    control_successes_asc                         AS control_successes
  FROM pcq_agg a
  CROSS JOIN pcq_window w
),
pcq_rows_ungated AS (
  SELECT
    CAST('PCQ' AS VARCHAR(10)) AS mne,
    CAST('Modal Sales assignment (challenger) vs champion -- approved (ungated)' AS VARCHAR(300)) AS test_desc,
    w.trt_start_dt                                AS trt_start_dt,
    w.trt_end_dt                                  AS trt_end_dt,
    CAST('App approved' AS VARCHAR(100))          AS success_name,
    CAST('D' || a.decile AS VARCHAR(20))          AS stratum,
    CAST(a.cohort_month AS VARCHAR(7))            AS cohort_month,
    test_clients,
    test_successes_raw                            AS test_successes,
    control_clients,
    control_successes_raw                         AS control_successes
  FROM pcq_agg a
  CROSS JOIN pcq_window w
),

-- ============================================================================
-- all_rows -- TEAMMATE HOOK-UP POINT. Each new campaign block gets added as a CTE emitting the
-- contract shape (mne, test_desc, trt_start_dt, trt_end_dt, success_name, stratum, cohort_month,
-- test_clients, test_successes, control_clients, control_successes) and UNION'd here.
-- ============================================================================
all_rows AS (
  SELECT * FROM pcl_rows
  UNION ALL
  SELECT * FROM pcq_rows_gated
  UNION ALL
  SELECT * FROM pcq_rows_ungated
  UNION ALL
  SELECT * FROM pcq_unmapped_row
  -- UNION ALL SELECT * FROM <next_campaign>_rows   -- add future teammate blocks here
),

-- ============================================================================
-- paired -- pool cohort_months per (mne, test_desc, stratum)
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
-- All arithmetic on aggregated (post-SUM) counts, cast to FLOAT, NULLIF-guarded.
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
-- the Zelen-Severo CDF approximation. Single-stratum tests (PCL) reduce to the plain two-proportion
-- z-test automatically, since a single stratum's w cancels out of SUM(w*d)/SUM(w) leaving d, and
-- SUM(w*w*v)/SUM(w) leaves w*v = w*pbar(1-pbar)(1/n1+1/n0); dividing by SUM(w)=w again leaves
-- SQRT(v) = the plain two-prop SE.
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
-- (Teradata-safe, avoids **). Max abs error on the CDF itself < 7.5e-8.
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
  w.trt_end_dt                               AS trt_end_dt,     -- partner sheet: Treatment End Date
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
