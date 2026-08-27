-- ENGINE: Teradata-direct
-- Q3 Commentary ask (2026-08-27): does the new FY2026 MC6->MCB migration path explain VBU lift down 37% YoY (Q3-25 vs Q3-26)?
-- Source: DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT (schema: schemas/cards_bizups_vbu_descresp_clnt.md)
-- Arm = `control` (confirmed in VBU vintage build); `test_group` kept as a label. `responder*` are CHAR -> CASE.
-- [VERIFY] year_mon_start format: assumed 'YYYY-MM' string. If it is YYYYMM integer use >= 202505.
-- [VERIFY] responder value: assumed '1'; if 'Y', change the CASE literals.
-- Floor 2025-05 covers Q3 FY2025 (May-Jul 2025) and Q3 FY2026 (May-Jul 2026). Export -> campaigns/VBA_VBU/vbu_q3_cube_<date>.csv
SELECT
   tactic_id
  ,year_mon_start
  ,test_group
  ,control
  ,from_product
  ,target_product
  ,SUM(CASE WHEN responder               = '1' THEN 1 ELSE 0 END) AS responder
  ,SUM(CASE WHEN responder_anyproduct    = '1' THEN 1 ELSE 0 END) AS responder_anyproduct
  ,SUM(CASE WHEN responder_targetproduct = '1' THEN 1 ELSE 0 END) AS responder_targetproduct
  ,COUNT(*)                                                       AS clnt_count
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start >= '2025-05'
GROUP BY 1,2,3,4,5,6
;
