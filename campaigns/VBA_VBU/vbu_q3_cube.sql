-- ============================================================================
-- VBU Q3 COMMENTARY CUBE  (Teradata-direct)  — 2026-08-27
-- Source: DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT (schema: schemas/cards_bizups_vbu_descresp_clnt.md)
-- Context: Q3 scorecard shows VBU (SMB Cards Upgrade) lift 1.4%, down 37% relative YoY; leads -1%.
-- Campaign owner: CPX/GLX/CLX -> AIB offers unchanged Q3-25 vs Q3-26, EXCEPT Ultimate Banking version eliminated in 2026
-- and a NEW MC6 -> MCB migration path introduced. Ask: does the MC6->MCB segment explain the lift decline?
-- Fiscal quarters: Q3 FY2025 = year_mon_start May-Jul 2025; Q3 FY2026 = May-Jul 2026.
-- Arm = `control` (confirmed in VBU vintage build); `test_group` is a label. responder* are CHAR -> CASE.
-- [VERIFY] year_mon_start format: assumed 'YYYY-MM' string; if YYYYMM integer use >= 202505.
-- [VERIFY] responder value: assumed '1'; if 'Y', change the CASE literals.
--   Lift (abs) = responder_targetproduct/clnt_count [action] - same [control], in pp. Also report responder (overall).
--
-- QUESTIONS TO ANSWER FROM THE EXPORTED CSV (answer each with a table, then one sentence):
-- Q1. Q3-25 vs Q3-26, total VBU: leads, action RR, control RR, lift (pp and relative) on responder_targetproduct
--     AND on responder. Does it reproduce the scorecard (lift ~1.4%, -37% YoY, leads -1%)? State which metric matches.
-- Q2. Same split by from_product -> target_product for both quarters. Which cells exist only in Q3-26 (new MC6->MCB)?
--     Which exist only in Q3-25 (eliminated Ultimate Banking)? Confirm with the tactic_id / test_group labels.
-- Q3. Lift per cell, Q3-25 vs Q3-26, for the unchanged cells (CPX/GLX/CLX -> AIB). Did lift fall in cells whose
--     offer did NOT change? If yes, the MC6->MCB story is not the (only) explanation.
-- Q4. MC6->MCB cell in Q3-26: leads, action RR, control RR, lift, z. Is its lift below the portfolio average?
--     Re-compute Q3-26 total lift EXCLUDING MC6->MCB — how much of the -37% YoY does the new cell account for?
-- Q5. Mix: share of leads by from->target cell, Q3-25 vs Q3-26. Mix-adjusted Q3-26 lift at Q3-25 weights.
-- Q6. Control arm size per cell per quarter — flag any cell whose control is < 500 clients or whose control share
--     differs from the others (SRM check). Never cite a lift from a cell with an empty or tiny control.
-- Q7. Maturity caveat: which Q3-26 months are still inside their response window as of today?
-- Q8. One-paragraph reply to the campaign owner: does MC6->MCB explain the -37%, partially, or not; cite tables.
-- Statistical bar: 2-sided z-test on every lift cited; below 80% confidence = "not significant".
-- ============================================================================
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
