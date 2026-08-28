-- ============================================================================
-- VBU DEPLOYMENT LEDGER — scorecard reconciliation (Q3 2026)  (Teradata-direct)  2026-08-28
-- One row per deployment (tactic_id) × from→target product × arm, with start month (year_mon_start) AND response
-- window start/end side by side. Sum May–Jul 2026 by start, then by response_end, and see which lands on the
-- scorecard (VBU Q3-26 leads 58,382; lift 1.4%). Also toggle MC6→MCB in/out.
-- [VERIFY] response_start / response_end type: schema lists them as "Response window start/end" — assumed DATE.
--          If they are strings, the yyyymm expressions will error: replace with SUBSTR(response_end,1,7).
-- ============================================================================
SELECT
   tactic_id
  ,year_mon_start
  ,response_start
  ,response_end
  ,CAST(EXTRACT(YEAR FROM response_end) AS INTEGER)*100 + EXTRACT(MONTH FROM response_end) AS resp_end_yyyymm
  ,test_group
  ,control                                   AS arm
  ,from_product
  ,target_product
  ,COUNT(*)                                  AS leads
  ,SUM(CASE WHEN responder LIKE '1.%' THEN 1 ELSE 0 END)                          AS resp_target
  ,SUM(CASE WHEN responder LIKE '1.%' OR responder LIKE '2.%' THEN 1 ELSE 0 END)  AS resp_any
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start >= '2026-01'
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY year_mon_start, tactic_id, from_product, target_product, arm
;
