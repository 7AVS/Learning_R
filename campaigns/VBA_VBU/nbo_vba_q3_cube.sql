-- Q3 commentary cube (Andre, 2026-08-27). Teradata-direct. Source: Q3 Commentary email 2026-08-25 asks:
-- (1) VBA ITA segment impact on RR/lift, (2) MCB on TPA side, (3) VBU MC6->MCB (NOT in this table; see cards_bizups_vbu_descresp_clnt).
-- Q3 FY2026 = treatmt_strt_dt May-Jul 2026; Q3 FY2025 = May-Jul 2025. Floor 2025-05-01 covers both.
-- Export result to campaigns/VBA_VBU/nbo_vba_q3_cube_<date>.csv; pivot per ask: rows = tpa_ita_indicator / visa_offer_test, cols = leads, action RR, control RR, lift.
SELECT
   tactic_id
  ,treatmt_strt_dt
  ,treatmt_end_dt
  ,comparison
  ,segment
  ,mnc
  ,control
  ,tpa_ita_indicator
  ,visa_offer_prod
  ,visa_offer_test
  ,visa_fee
  ,SUM(gross_response) AS gross_response
  ,SUM(net_response)   AS net_response
  ,COUNT(*)            AS clnt_count
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE treatmt_strt_dt >= DATE '2025-05-01'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;
