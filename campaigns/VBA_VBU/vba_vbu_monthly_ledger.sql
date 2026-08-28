-- ============================================================================
-- VBA/RBOL + VBU DEPLOYMENT LEDGERS — scorecard reconciliation  (Teradata-direct)  2026-08-28
-- Two queries, one file. Rolled up by COHORT MONTH (no tactic_id). Start month and end month side by side, so
-- May–Jul can be summed by start, then by end, to see which lands on the scorecard.
-- ============================================================================

-- ---------------------------------------------------------- VBA / RBOL ----
SELECT
   mnc
  ,CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS INTEGER)*100 + EXTRACT(MONTH FROM treatmt_strt_dt) AS start_yyyymm
  ,CAST(EXTRACT(YEAR FROM treatmt_end_dt)  AS INTEGER)*100 + EXTRACT(MONTH FROM treatmt_end_dt)  AS end_yyyymm
  ,comparison
  ,tpa_ita_indicator
  ,control                                   AS arm
  ,COUNT(*)                                  AS leads
  ,SUM(gross_response)                       AS gross_resp
  ,SUM(net_response)                         AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE treatmt_strt_dt >= DATE '2025-04-01'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6
;

-- ---------------------------------------------------------------- VBU ----
-- [VERIFY] response_end assumed DATE; if string: CAST(SUBSTR(response_end,1,4) AS INTEGER)*100 + CAST(SUBSTR(response_end,6,2) AS INTEGER)
SELECT
   year_mon_start
  ,CAST(EXTRACT(YEAR FROM response_end) AS INTEGER)*100 + EXTRACT(MONTH FROM response_end) AS resp_end_yyyymm
  ,control                                   AS arm
  ,from_product
  ,target_product
  ,COUNT(*)                                  AS leads
  ,SUM(CASE WHEN responder LIKE '1.%' THEN 1 ELSE 0 END)                          AS resp_target
  ,SUM(CASE WHEN responder LIKE '1.%' OR responder LIKE '2.%' THEN 1 ELSE 0 END)  AS resp_any
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start >= '2026-01'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
;
