-- ============================================================================
-- VBA + VBU MONTHLY LEDGERS — Q3 2026 scorecard reconciliation  (Teradata-direct)  2026-08-28
-- Two independent queries in one file. Each rolled up by COHORT MONTH (treatment start) × END MONTH × segment × arm.
-- No tactic_id — multiple tactics in a month collapse into the month. Sum May–Jul by start month, then by end month,
-- and see which lands on the scorecard (VBA 184,521 leads / 0.0% lift; VBU 58,382 / 1.4%).
-- ============================================================================

-- ---------------------------------------------------------------- VBA ----
-- comparison restricted to 'VBA Only: Action vs. Control' [VERIFY exact string — distinct list query at bottom].
SELECT
   CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS INTEGER)*100 + EXTRACT(MONTH FROM treatmt_strt_dt) AS start_yyyymm
  ,CAST(EXTRACT(YEAR FROM treatmt_end_dt)  AS INTEGER)*100 + EXTRACT(MONTH FROM treatmt_end_dt)  AS end_yyyymm
  ,tpa_ita_indicator
  ,visa_offer_prod
  ,control                                   AS arm
  ,COUNT(*)                                  AS leads
  ,SUM(gross_response)                       AS gross_resp
  ,SUM(net_response)                         AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE treatmt_strt_dt >= DATE '2026-01-01'
  AND mnc = 'VBA'
  AND comparison = 'VBA Only: Action vs. Control'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,3,4,5
;

-- ---------------------------------------------------------------- VBU ----
-- end month = response_end [VERIFY type DATE; if string use CAST(SUBSTR(response_end,1,4) AS INTEGER)*100 + CAST(SUBSTR(response_end,6,2) AS INTEGER)].
SELECT
   year_mon_start
  ,CAST(EXTRACT(YEAR FROM response_end) AS INTEGER)*100 + EXTRACT(MONTH FROM response_end) AS resp_end_yyyymm
  ,from_product
  ,target_product
  ,test_group
  ,control                                   AS arm
  ,COUNT(*)                                  AS leads
  ,SUM(CASE WHEN responder LIKE '1.%' THEN 1 ELSE 0 END)                          AS resp_target
  ,SUM(CASE WHEN responder LIKE '1.%' OR responder LIKE '2.%' THEN 1 ELSE 0 END)  AS resp_any
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start >= '2026-01'
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6
;

-- If the VBA block returns 0 rows — exact comparison strings:
-- SELECT comparison, COUNT(*) FROM dl_mr_prod.nbo_vba_rbol_combined WHERE treatmt_strt_dt >= DATE '2026-05-01' GROUP BY 1;
