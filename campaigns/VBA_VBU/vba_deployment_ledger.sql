-- ============================================================================
-- VBA / RBOL DEPLOYMENT LEDGER — scorecard reconciliation  (Teradata-direct)  2026-08-28
-- One row per deployment (tactic_id) × TPA/ITA × arm. Start month AND end month side by side, so you can
-- sum May–Jul by START and by END and see which lands on the scorecard (VBA Q3-26 action leads 184,521;
-- Q3-25 ≈ 125,500; VBU/RBOL analogous). Everything else (Excel filters/pivot) is done by eye on this output.
-- Columns: leads = COUNT(*) rows; gross/net = SUM of response flags. comparison kept so 'VBA Only' vs 'VBA Total' can be toggled.
-- ============================================================================
SELECT
   mnc
  ,tactic_id
  ,treatmt_strt_dt
  ,treatmt_end_dt
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
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY mnc, treatmt_strt_dt, tactic_id, comparison, tpa_ita_indicator, arm
;
