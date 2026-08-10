-- probe_experiment_onset.sql
-- ============================================================================
-- PURPOSE: the existing per-mnemonic deployment probe shows every deployment,
--   but not WHICH ones carried an experiment marker (e.g. Andre can see all
--   PCQ deployments but not which ran Modal Sales). This file answers that:
--   per deployment, how many clients carry the experiment marker codes, so
--   Andre can scan down and find the FIRST deployment where the experiment
--   actually appears (= when it started).
--
-- NO Q3 <<WINDOW>> FILTER, ON PURPOSE. The pp_*.sql curve files all restrict
--   to deployments ENDING in Q3 FY2026 (2026-05-01..2026-07-31). This probe
--   does NOT apply that filter — the whole point is onset-finding across ALL
--   2026 deployments, including ones outside Q3. Floor is 2026-01-01 only
--   (contract rule 6 / CLAUDE.md 2024_data_floor, tightened to 2026 per the
--   pp_ files' own floor comments).
--
-- HOW TO READ IT: run BLOCK 1 for an experiment, scan down rows in strt_dt
--   order, find the first row where carries_experiment = 'EXPERIMENT'. Run
--   BLOCK 2 only if you need to see the literal code strings behind that flag.
--
-- ENGINE: Teradata-direct for all four blocks below — every pp_ source file
--   read for this probe (pp_pcq_sales_modal.sql, pp_pcd_async.sql,
--   pp_pcl_sales_modal.sql, pp_auh_campaign.sql) is Teradata-direct (SYS_CALENDAR
--   volatile-table spine pattern). No Trino/Starburst block needed here.
--
-- MARKER COLUMN PER EXPERIMENT (this file doubles as documentation of how
--   each experiment is identified):
--   - PCQ Modal Sales : test_group_latest  (pp_pcq_sales_modal.sql)
--   - PCD Async       : strategy_seg_cd    (pp_pcd_async.sql)
--   - PCL Sales Modal : report_groups_period (pp_pcl_sales_modal.sql)
--   - AUH             : tst_grp_cd         (pp_auh_campaign.sql)
--
-- NOT RUN. NOT COMMITTED as data output — probe only.
-- ============================================================================


-- ============================================================================
-- === PCQ MODAL SALES =========================================================
-- Source pp_ file : pp_pcq_sales_modal.sql
-- Table           : DL_MR_PROD.cards_tpa_pcq_decision_resp
-- Deployment key  : tactic_id  -- [CONFIRMED via file comment: "*** deployment
--                    DROPPED, 2026-08-10 *** — was tactic_id." The curve file's
--                    SELECT no longer emits it (cohort_month replaced it as the
--                    curve grain), but the source table still carries it and
--                    this is the literal name the file itself states. Not guessed.
-- Start col       : treatmt_start_dt
-- End col         : treatmt_end_dt
-- Client col      : clnt_no
-- Marker col      : test_group_latest
-- Marker condition: TRIM(test_group_latest) IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
-- Mandatory filter: tpa_ita = 'TPA'  (canon: reference_pcq_measurement_filters.md
--                    — PCQ has no ITA arm, this is not optional)
-- ============================================================================

-- BLOCK 1 — one row per deployment, ordered by start date ascending
-- Expected size: PCQ has run ~10-30 waves since 2026-01-01 -> ~10-30 rows.
SELECT
      tactic_id                                                              AS deployment
    , MIN(treatmt_start_dt)                                                  AS strt_dt
    , MAX(treatmt_end_dt)                                                    AS end_dt
    , COUNT(DISTINCT clnt_no)                                                AS clients_total
    , COUNT(DISTINCT CASE WHEN TRIM(test_group_latest)
                                IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
                           THEN clnt_no END)                                 AS clients_in_experiment
    , CASE WHEN COUNT(DISTINCT CASE WHEN TRIM(test_group_latest)
                                          IN ('NG3_CHMP','NG3_CHLN','NG3_CHLG')
                                     THEN clnt_no END) > 0
           THEN 'EXPERIMENT' ELSE 'no' END                                   AS carries_experiment
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-01-01'          -- floor only, NO Q3 <<WINDOW>>
GROUP BY 1
ORDER BY strt_dt;

-- BLOCK 2 — distinct marker codes actually present per deployment
-- Expected size: test_group_latest has a small confirmed code set (3 values seen
--   in pp_pcq_sales_modal.sql: NG3_CHMP/NG3_CHLN/NG3_CHLG) but this table may carry
--   other TPA test groups outside the NG3 modal experiment too -> likely a handful
--   of distinct codes per deployment. Rows ~ (deployments x <10) -- safe to run.
SELECT
      tactic_id             AS deployment
    , TRIM(test_group_latest) AS code
    , COUNT(DISTINCT clnt_no) AS clients
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- === PCD ASYNC ================================================================
-- Source pp_ file : pp_pcd_async.sql
-- Table           : dl_mr_prod.cards_pcd_ongoing_decis_resp
-- Deployment key  : tactic_id_parent  -- [CONFIRMED, actively used in the file:
--                    "tactic_id_parent AS deployment"]
-- Start col       : response_start
-- End col         : response_end
-- Client col      : clnt_no
-- Marker col      : strategy_seg_cd
-- Marker condition: strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
--                    'MFB8UJPY','MFB9BX97','MFB9HYQ7')
--   [FLAG carried over from pp_pcd_async.sql, NOT resolved here]: Andre's working
--   file lists 'MAO28C35' where this repo (pp_pcd_async.sql +
--   campaigns/PCD/async_banner_summary.sql) has always used 'MAO28CJ5'. Using the
--   repo value here for consistency with the curve file. Confirm before trusting
--   any deployment whose clients_in_experiment count hinges on that one code.
-- Mandatory filter: none beyond the floor -- pp_pcd_async.sql applies NO business
--   flag filter (strategy_seg_cd IS the only selector in that file); clients_total
--   below is therefore every PCD deployment in the curated table, not just async.
-- ============================================================================

-- BLOCK 1 — one row per deployment, ordered by start date ascending
-- Expected size: cards_pcd_ongoing_decis_resp spans ALL PCD strategy segments
--   (not just the 7 async ones), so this likely returns more rows than the
--   PCQ/PCL blocks -- estimate ~20-50 deployments since 2026-01-01.
SELECT
      tactic_id_parent                                                       AS deployment
    , MIN(response_start)                                                    AS strt_dt
    , MAX(response_end)                                                      AS end_dt
    , COUNT(DISTINCT clnt_no)                                                AS clients_total
    , COUNT(DISTINCT CASE WHEN strategy_seg_cd
                                IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                                    'MFB8UJPY','MFB9BX97','MFB9HYQ7')
                           THEN clnt_no END)                                 AS clients_in_experiment
    , CASE WHEN COUNT(DISTINCT CASE WHEN strategy_seg_cd
                                          IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                                              'MFB8UJPY','MFB9BX97','MFB9HYQ7')
                                     THEN clnt_no END) > 0
           THEN 'EXPERIMENT' ELSE 'no' END                                   AS carries_experiment
FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE response_start >= DATE '2026-01-01'            -- floor only, NO Q3 <<WINDOW>>
GROUP BY 1
ORDER BY strt_dt;

-- BLOCK 2 — distinct marker codes actually present per deployment
-- [CAUTION -- CARDINALITY]: strategy_seg_cd covers every PCD strategy segment
--   system-wide, not just the 7 async codes. This could return hundreds of
--   distinct codes x dozens of deployments. Run BLOCK 1 alone first; only run
--   this if you need the literal code strings, and consider adding
--   "AND strategy_seg_cd LIKE 'M%'" or similar narrowing before running unfiltered.
SELECT
      tactic_id_parent      AS deployment
    , TRIM(strategy_seg_cd) AS code
    , COUNT(DISTINCT clnt_no) AS clients
FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
WHERE response_start >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- === PCL SALES MODAL ==========================================================
-- Source pp_ file : pp_pcl_sales_modal.sql
-- Table           : DL_MR_PROD.cards_pli_decision_resp
-- Deployment key  : parent_tactic_id  -- [CONFIRMED via file comment: "***
--                    deployment DROPPED, 2026-08-10 *** — was parent_tactic_id."
--                    Same situation as PCQ: dropped from the curve SELECT,
--                    literal name stated in the file's own comment.]
-- Start col       : treatmt_strt_dt
-- End col         : treatmt_end_dt
-- Client col      : clnt_no
-- Marker col      : report_groups_period
-- Marker condition: report_groups_period LIKE '%R____WMS%'
--                    OR report_groups_period LIKE '%R____NMS%'
-- Mandatory filter: none beyond the floor -- pp_pcl_sales_modal.sql's ONLY
--   population filter besides the floor IS the marker condition itself, so
--   clients_total below (unfiltered on the marker) is the true PCL campaign
--   population per deployment, same source/grain the campaign-scope sibling uses.
-- ============================================================================

-- BLOCK 1 — one row per deployment, ordered by start date ascending
-- Expected size: ~10-30 PCL deployments since 2026-01-01.
SELECT
      parent_tactic_id                                                       AS deployment
    , MIN(treatmt_strt_dt)                                                   AS strt_dt
    , MAX(treatmt_end_dt)                                                    AS end_dt
    , COUNT(DISTINCT clnt_no)                                                AS clients_total
    , COUNT(DISTINCT CASE WHEN report_groups_period LIKE '%R____WMS%'
                             OR report_groups_period LIKE '%R____NMS%'
                           THEN clnt_no END)                                 AS clients_in_experiment
    , CASE WHEN COUNT(DISTINCT CASE WHEN report_groups_period LIKE '%R____WMS%'
                                       OR report_groups_period LIKE '%R____NMS%'
                                     THEN clnt_no END) > 0
           THEN 'EXPERIMENT' ELSE 'no' END                                   AS carries_experiment
FROM DL_MR_PROD.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2026-01-01'           -- floor only, NO Q3 <<WINDOW>>
GROUP BY 1
ORDER BY strt_dt;

-- BLOCK 2 — distinct marker codes actually present per deployment
-- [CAUTION -- CARDINALITY]: report_groups_period is matched with a wildcard LIKE
--   in pp_pcl_sales_modal.sql, which suggests it is a composite/descriptive string
--   (possibly multiple concatenated report-group tags per row), not a small fixed
--   code list. Distinct-value count is UNKNOWN and could be large. Run BLOCK 1
--   alone first; treat this block as exploratory and cap/inspect before trusting
--   row count.
SELECT
      parent_tactic_id            AS deployment
    , TRIM(report_groups_period)  AS code
    , COUNT(DISTINCT clnt_no)     AS clients
FROM DL_MR_PROD.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;


-- ============================================================================
-- === AUH ======================================================================
-- Source pp_ file : pp_auh_campaign.sql (file header inside reads
--                    "auh_experiment_vintage.sql" -- naming mismatch in the repo,
--                    not something this probe resolves; content used as-is)
-- Table           : DG6V01.tactic_evnt_ip_ar_hist
-- Deployment key  : tactic_id  -- [CONFIRMED, actively used in the file's WHERE]
-- Start col       : treatmt_strt_dt
-- End col         : treatmt_end_dt
-- Client col      : acct_no = CAST(tactic_evnt_id AS BIGINT)  -- [NOTE: AUH grain
--                    is ACCOUNT, not client -- pp_auh_campaign.sql's own dedup
--                    comment: "*** DEDUP IDENTIFIER: acct_no ***". Using acct_no
--                    here, not clnt_no, to match that file exactly.]
-- Marker col      : tst_grp_cd
-- Marker condition: SUBSTR(TRIM(tst_grp_cd),1,2) IN ('NR','RN','RO')
-- Mandatory filter: [SCOPE DIFFERENCE FROM pp_auh_campaign.sql -- FLAGGED, NOT
--   SILENTLY CHANGED] pp_auh_campaign.sql hardcodes
--   "tactic_id IN ('2026042AUH','2026119AUH')" -- only the 2 known AUH waves.
--   That defeats this probe's purpose (finding deployments the hardcoded list
--   doesn't yet know about). Below, the mandatory filter is instead
--   SUBSTR(tactic_id,8,3) = 'AUH', per CLAUDE.md's confirmed TACTIC_ID
--   convention ("positions 8-10 = MNE"). [VERIFY]: that convention is repo canon,
--   not something stated inside pp_auh_campaign.sql itself -- confirm the
--   SUBSTR pattern actually isolates AUH tactics on this table before trusting
--   BLOCK 1's deployment list as complete.
-- ============================================================================

-- BLOCK 1 — one row per deployment, ordered by start date ascending
-- Expected size: SMALL. Even widened past the 2-tactic hardcoded list, AUH is a
--   low-frequency campaign -- likely single digits to ~10 rows.
SELECT
      tactic_id                                                              AS deployment
    , MIN(treatmt_strt_dt)                                                   AS strt_dt
    , MAX(treatmt_end_dt)                                                    AS end_dt
    , COUNT(DISTINCT CAST(tactic_evnt_id AS BIGINT))                         AS clients_total
    , COUNT(DISTINCT CASE WHEN SUBSTR(TRIM(tst_grp_cd),1,2) IN ('NR','RN','RO')
                           THEN CAST(tactic_evnt_id AS BIGINT) END)          AS clients_in_experiment
    , CASE WHEN COUNT(DISTINCT CASE WHEN SUBSTR(TRIM(tst_grp_cd),1,2) IN ('NR','RN','RO')
                                     THEN CAST(tactic_evnt_id AS BIGINT) END) > 0
           THEN 'EXPERIMENT' ELSE 'no' END                                   AS carries_experiment
FROM DG6V01.tactic_evnt_ip_ar_hist
WHERE SUBSTR(tactic_id, 8, 3) = 'AUH'                -- [VERIFY] canon convention, not from pp_ file
  AND treatmt_strt_dt >= DATE '2026-01-01'           -- floor only, NO Q3 <<WINDOW>>
GROUP BY 1
ORDER BY strt_dt;

-- BLOCK 2 — distinct marker codes actually present per deployment
-- Expected size: SMALL. pp_auh_campaign.sql's own commented Diagnostic A block
--   states tst_grp_cd cardinality is "~20 rows" for the 2 known AUH tactics --
--   safe to run even widened to all AUH tactics.
SELECT
      tactic_id            AS deployment
    , TRIM(tst_grp_cd)     AS code
    , COUNT(DISTINCT CAST(tactic_evnt_id AS BIGINT)) AS clients
FROM DG6V01.tactic_evnt_ip_ar_hist
WHERE SUBSTR(tactic_id, 8, 3) = 'AUH'                -- [VERIFY] canon convention, not from pp_ file
  AND treatmt_strt_dt >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2;
