-- probe_experiment_onset.sql
-- ============================================================================
-- ONE query, ONE result set, all 7 mnemonics. This is probe_deployments_by_mne.sql
--   (one row per deployment) plus an experiment marker derived from the SAME
--   tactic table -- no joins to curated per-campaign tables. Every marker below
--   is a tactic-table-native expression already proven elsewhere in the repo
--   (file:line cited per mnemonic), not re-derived or guessed here.
--
-- HOW TO READ IT: filter/sort by mne, scan strt_dt ascending, find the first row
--   where carries_experiment = 'EXPERIMENT' -> that deployment is the onset.
--   in_q3 is informational only (flags end_dt landing in 2026-05-01..2026-07-31)
--   -- it is NOT filtered on, because onset can predate the quarter.
--
-- ENGINE: Trino / Starburst. Table form DG6V01.tactic_evnt_ip_ar_hist (bare, no
--   dw00 prefix) is the SAME table probe_deployments_by_mne.sql uses, just in its
--   Starburst-reachable bare form -- proven running on Trino in pp_auh_campaign.sql
--   header (:16-18), which cites pp_crv_campaign.sql, crv_vintage_v2_production.sql,
--   and campaigns/PCD/async_banner_summary.sql, all headed "Engine: Trino/Starburst".
--   No volatile tables, no QUALIFY, no SYS_CALENDAR.
--
-- MARKER PER MNEMONIC (this file doubles as documentation of how each experiment
--   is identified, all confirmed against tactic-table columns):
--   - PCQ Sales Modal : SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MS%'
--       Confirmed tactic-side, same table, campaigns/sales_modal/pcq/pcq_ms_vs_benchmark.sql:33
--       (CTE reads FROM DG6V01.TACTIC_EVNT_IP_AR_HIST, line 30).
--   - PCD Async       : element_at(split(regexp_replace(trim(TACTIC_DECISN_VRB_INFO),' +',' '),' '),3)
--                        IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
--       Confirmed tactic-side, campaigns/PCD/async_banner_summary.sql:36-38
--       (cohort_raw CTE reads FROM DG6V01.TACTIC_EVNT_IP_AR_HIST, line 41).
--   - AUH Rewards/NonReward : SUBSTR(TRIM(TST_GRP_CD),1,2) IN ('NR','RN','RO')
--       Tactic-side by construction -- vintages/power_pack's own prior AUH block
--       (this file's previous version, BLOCK AUH) read this off
--       DG6V01.tactic_evnt_ip_ar_hist directly; matches the expression this task
--       specified verbatim.
--   - PCL Sales Modal : *** NOT CONFIRMED ***. The only known marker,
--       report_groups_period LIKE '%R____WMS%' / '%R____NMS%', lives on the CURATED
--       table dw00_im.dl_mr_prod.cards_pli_decision_resp (pp_pcl_sales_modal.sql:37,
--       103-104 and every file under campaigns/sales_modal/pcl/). Grepped the whole
--       repo (schemas/, campaigns/sales_modal/pcl/, campaigns/PCL_PLI/) for a
--       tactic-table-side equivalent (RPT_GRP_CD or similar) and found none tied to
--       DG6V01.tactic_evnt_ip_ar_hist / DTZV01.TACTIC_EVNT_IP_AR_H60M. The closest hit,
--       schemas/hdfs_tactic_tables_schema.md:44 (`tsz_00150_data_dtzta_t_tactic_rpt_grp`
--       -> rpt_grp_cd), is a GA4/EDL lookup table in a different zone, not a column on
--       the EDW tactic event table -- not usable here. Per instruction: NOT invented.
--       clients_in_experiment is NULL for PCL rows and experiment says so explicitly.
--   - CRV / VBA / VBU : no experiment marker exists for these mnemonics.
--       clients_in_experiment = 0 (NULL never produced -- no marker to fail), experiment = 'n/a'.
--
-- FILTERS: SUBSTR(TACTIC_ID,8,3) IN 7 mnemonics; TREATMT_STRT_DT >= 2026-01-01.
--   NO Q3 window filter -- onset can predate the quarter; in_q3 is display-only.
--
-- NOT RUN. NOT COMMITTED as data output -- probe only.
-- Expected size: ~150-250 rows (all 2026 deployments x 7 mnemonics). Roster, not
--   a decision table.
-- ============================================================================

WITH agg AS (
    SELECT
          TACTIC_ID                                                        AS deployment
        , MIN(TREATMT_STRT_DT)                                             AS strt_dt
        , MAX(TREATMT_END_DT)                                              AS end_dt
        , COUNT(DISTINCT CLNT_NO)                                          AS clients
        , CASE WHEN SUBSTR(TACTIC_ID, 8, 3) = 'PCL' THEN NULL
               ELSE COUNT(DISTINCT CASE
                      WHEN SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
                           AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
                      THEN CLNT_NO
                      WHEN SUBSTR(TACTIC_ID, 8, 3) = 'PCD'
                           AND element_at(split(regexp_replace(trim(TACTIC_DECISN_VRB_INFO), ' +', ' '), ' '), 3)
                               IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
                      THEN CLNT_NO
                      WHEN SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
                           AND SUBSTR(TRIM(TST_GRP_CD), 1, 2) IN ('NR','RN','RO')
                      THEN CLNT_NO
                 END)
          END                                                              AS clients_in_experiment
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE SUBSTR(TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH','VBA','VBU')
      AND TREATMT_STRT_DT >= DATE '2026-01-01'          -- floor only, no Q3 window
    GROUP BY TACTIC_ID
)
SELECT
      SUBSTR(deployment, 8, 3)                                             AS mne
    , deployment
    , strt_dt
    , end_dt
    , clients
    , CASE SUBSTR(deployment, 8, 3)
           WHEN 'PCQ' THEN CAST('Sales Modal' AS VARCHAR(60))
           WHEN 'PCD' THEN CAST('Async' AS VARCHAR(60))
           WHEN 'AUH' THEN CAST('Rewards/NonReward arms' AS VARCHAR(60))
           WHEN 'PCL' THEN CAST('[VERIFY] PCL tactic-side marker not confirmed' AS VARCHAR(60))
           ELSE CAST('n/a' AS VARCHAR(60))
      END                                                                  AS experiment
    , clients_in_experiment
    , CASE WHEN clients_in_experiment > 0 THEN 'EXPERIMENT' ELSE '' END    AS carries_experiment
    , CASE WHEN end_dt >= DATE '2026-05-01' AND end_dt < DATE '2026-08-01'
           THEN 'Q3' ELSE '' END                                          AS in_q3
FROM agg
ORDER BY mne, strt_dt
;
