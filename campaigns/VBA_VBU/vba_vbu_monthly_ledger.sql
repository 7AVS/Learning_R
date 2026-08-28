-- ============================================================================
-- VBA + VBU MONTHLY LEDGER — Q3 2026 scorecard reconciliation  (Teradata-direct)  2026-08-28
-- ONE result set, both campaigns. Rolled up by COHORT MONTH (treatment start) × END MONTH × segment × arm.
-- No tactic_id — multiple tactics in a month collapse into the month. Sum May–Jul by start_yyyymm, then by
-- end_yyyymm, and see which lands on the scorecard (VBA 184,521 leads / 0.0% lift; VBU 58,382 / 1.4%).
-- Common columns: campaign | start_yyyymm | end_yyyymm | segment | arm | leads | resp_1 | resp_2
--   VBA: segment = tpa_ita_indicator (TPA / ITA / NULL); resp_1 = gross_response; resp_2 = net_response.
--        comparison restricted to 'VBA Only: Action vs. Control' [VERIFY exact string — see distinct list below].
--   VBU: segment = from_product→target_product; resp_1 = target-product change; resp_2 = any change.
--        end month = response_end [VERIFY type DATE; if string use SUBSTR(response_end,1,7)].
-- ============================================================================
SELECT
   CAST('VBA' AS VARCHAR(50))                                                        AS campaign
  ,CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS INTEGER)*100 + EXTRACT(MONTH FROM treatmt_strt_dt) AS start_yyyymm
  ,CAST(EXTRACT(YEAR FROM treatmt_end_dt)  AS INTEGER)*100 + EXTRACT(MONTH FROM treatmt_end_dt)  AS end_yyyymm
  ,CAST(COALESCE(tpa_ita_indicator,'NULL') AS VARCHAR(50))                          AS segment
  ,CAST(control AS VARCHAR(50))                                                     AS arm
  ,COUNT(*)                                                                         AS leads
  ,SUM(gross_response)                                                              AS resp_1
  ,SUM(net_response)                                                                AS resp_2
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE treatmt_strt_dt >= DATE '2026-01-01'
  AND mnc = 'VBA'
  AND comparison = 'VBA Only: Action vs. Control'
GROUP BY 2,3,4,5

UNION ALL

SELECT
   CAST('VBU' AS VARCHAR(50))
  ,CAST(SUBSTR(year_mon_start,1,4) AS INTEGER)*100 + CAST(SUBSTR(year_mon_start,6,2) AS INTEGER)
  ,CAST(EXTRACT(YEAR FROM response_end) AS INTEGER)*100 + EXTRACT(MONTH FROM response_end)
  ,CAST(from_product || '->' || target_product AS VARCHAR(50))
  ,CAST(control AS VARCHAR(50))
  ,COUNT(*)
  ,SUM(CASE WHEN responder LIKE '1.%' THEN 1 ELSE 0 END)
  ,SUM(CASE WHEN responder LIKE '1.%' OR responder LIKE '2.%' THEN 1 ELSE 0 END)
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start >= '2026-01'
GROUP BY 2,3,4,5
ORDER BY 1,2,3,4,5
;

-- Run once if the VBA block returns 0 rows: exact comparison strings
-- SELECT comparison, COUNT(*) FROM dl_mr_prod.nbo_vba_rbol_combined WHERE treatmt_strt_dt >= DATE '2026-05-01' GROUP BY 1;
