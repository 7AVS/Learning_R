-- ============================================================================
-- VBA — full decisioning-field sweep (Teradata-direct)  2026-08-28
-- Purpose: find the field (or field combination) whose SUBSET reproduces the
--   NBO Monthly Summary dashboard for campaigns ending May-2026:
--     dashboard: Action 92,792 / NAC 2,158
--     ledger (comparison='VBA: Action vs. Control'): 96,913 / 5,275
--   One block per class-A (decisioning, categorical/binary) column from
--   nbo_vba_rbol_combined — see campaigns/VBA_VBU/vba_decisioning_fields.md
--   for the full A/B/C/D classification and reasoning per column.
-- `comparison` is deliberately NOT filtered in the WHERE clause here (unlike
--   vba_nbo_dashboard_filter_probe.sql) so its own values show up as one of
--   the blocks below, alongside every other decisioning column.
-- Columns marked [?] in the .md are genuinely unsure (schema says "Unknown —
--   ask") but are included anyway — better to show a field than hide it.
-- No UNION — each block is its own statement, run independently.
-- Column names are exactly as listed in schemas/nbo_vba_rbol_combined.md —
--   nothing invented. If a block errors "column does not exist", skip it and
--   note the name.
-- ============================================================================

-- Identity / treatment ---------------------------------------------------

SELECT 'comparison' AS fld, CAST(comparison AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'segment' AS fld, CAST(segment AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'mnc' AS fld, CAST(mnc AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'wave' AS fld, CAST(wave AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'tactic_cell_cd' AS fld, CAST(tactic_cell_cd AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'treatmt_mn' AS fld, CAST(treatmt_mn AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'control' AS fld, CAST(control AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'test_group' AS fld, CAST(test_group AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'tst_grp_cd' AS fld, CAST(tst_grp_cd AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- Targeting / model -------------------------------------------------------

SELECT 'nibt' AS fld, CAST(nibt AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'model' AS fld, CAST(model AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'lmt1' AS fld, CAST(lmt1 AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'lmt2' AS fld, CAST(lmt2 AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'rate' AS fld, CAST(rate AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'decile' AS fld, CAST(decile AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'email_creative_id' AS fld, CAST(email_creative_id AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- Channel deployment flags --------------------------------------------------

SELECT 'chnl_dm' AS fld, CAST(chnl_dm AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_em' AS fld, CAST(chnl_em AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_do' AS fld, CAST(chnl_do AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_im' AS fld, CAST(chnl_im AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_in' AS fld, CAST(chnl_in AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_iu' AS fld, CAST(chnl_iu AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_rd' AS fld, CAST(chnl_rd AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_iv' AS fld, CAST(chnl_iv AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_om' AS fld, CAST(chnl_om AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'chnl_zz' AS fld, CAST(chnl_zz AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'channel' AS fld, CAST(channel AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'gu' AS fld, CAST(gu AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- Email engagement (unsure counts) -----------------------------------------

SELECT 'num_descn' AS fld, CAST(num_descn AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'num_target' AS fld, CAST(num_target AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- VBA conversion track (decisioning fields only — outcomes excluded) --------

SELECT 'visa_offer_prod' AS fld, CAST(visa_offer_prod AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'visa_offer_test' AS fld, CAST(visa_offer_test AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'visa_fee' AS fld, CAST(visa_fee AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'visa_onoff' AS fld, CAST(visa_onoff AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- RBOL conversion track (decisioning fields only — outcomes excluded) -------

SELECT 'vbo' AS fld, CAST(vbo AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'rbol_fee' AS fld, CAST(rbol_fee AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'rbol_onoff' AS fld, CAST(rbol_onoff AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'rbol_offer_prod' AS fld, CAST(rbol_offer_prod AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'rbol_offer' AS fld, CAST(rbol_offer AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- Other flags ----------------------------------------------------------------

SELECT 'blip' AS fld, CAST(blip AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'cpc_chng' AS fld, CAST(cpc_chng AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'hsbc_ind' AS fld, CAST(hsbc_ind AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'vba_tpa_rank' AS fld, CAST(vba_tpa_rank AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'tpa_ita_indicator' AS fld, CAST(tpa_ita_indicator AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'hsbc_indicator' AS fld, CAST(hsbc_indicator AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'vba_ita_rank' AS fld, CAST(vba_ita_rank AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;
