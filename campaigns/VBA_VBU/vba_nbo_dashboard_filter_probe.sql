-- ============================================================================
-- VBA — which field makes the NBO Monthly Summary slice?  (Teradata-direct)  2026-08-28
-- Target (campaign END month May-2026): dashboard Action leads 92,792 / NAC leads 2,158. Ledger gives 96,913 / 5,275.
-- Each block: one candidate column × arm × counts. Run all; the column whose SUBSET lands on 92,792 / 2,158 is the filter.
-- Column names per schemas/nbo_vba_rbol_combined.md — if a block errors "column does not exist", skip it and note the name.
-- ============================================================================
SELECT 'tst_grp_cd' AS fld, CAST(tst_grp_cd AS VARCHAR(50)) AS val, control AS arm, COUNT(*) AS leads, SUM(net_response) AS net_resp
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'test_group', CAST(test_group AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'segment', CAST(segment AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'hsbc_indicator', CAST(hsbc_indicator AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'hsbc_ind', CAST(hsbc_ind AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'wave', CAST(wave AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'tactic_cell_cd', CAST(tactic_cell_cd AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'vbo', CAST(vbo AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'blip', CAST(blip AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

SELECT 'cpc_chng', CAST(cpc_chng AS VARCHAR(50)), control, COUNT(*), SUM(net_response)
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-05-01' AND DATE '2026-05-31'
GROUP BY 1,2,3 ORDER BY 3,4 DESC;

-- Also: exact end dates in the month (dashboard folds campaigns ending in the first 10 days into the PRIOR month)
SELECT treatmt_end_dt, control, COUNT(*) AS leads
FROM dl_mr_prod.nbo_vba_rbol_combined
WHERE mnc='VBA' AND comparison='VBA: Action vs. Control' AND treatmt_end_dt BETWEEN DATE '2026-04-25' AND DATE '2026-06-10'
GROUP BY 1,2 ORDER BY 1,2;
