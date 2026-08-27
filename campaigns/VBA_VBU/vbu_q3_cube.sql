-- ============================================================================
-- VBU Q3 COMMENTARY CUBE  (Teradata-direct)  — 2026-08-27
-- Source: DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT (schema: schemas/cards_bizups_vbu_descresp_clnt.md)
-- Context: Q3 scorecard shows VBU (SMB Cards Upgrade) lift 1.4%, down 37% relative YoY; leads -1%.
-- Campaign owner: CPX/GLX/CLX -> AIB offers unchanged Q3-25 vs Q3-26, EXCEPT Ultimate Banking version eliminated in 2026
-- and a NEW MC6 -> MCB migration path introduced. Ask: does the MC6->MCB segment explain the lift decline?
-- Fiscal quarters: Q3 FY2025 = year_mon_start May-Jul 2025; Q3 FY2026 = May-Jul 2026.
-- VERIFIED 2026-08-27 (vbu_control_arm_check.sql): control IN ('Action','Control'), ~4.6% holdout; year_mon_start = 'YYYY-MM';
--   `responder` = 3-way label '0.No Change(s) from…' / '1.Change to Target P…' / '2.Change to NON-Targ…' (NOT a 0/1 flag);
--   responder_targetproduct = '1' works. Control organic target-product conversion ~0.1% (1-2 clients/month) -> lift ≈ action RR.
-- Q3 only: May-Jul both years. No August.
--   Lift (abs) = resp_target/clnt_count [Action] - same [Control], in pp. Also report resp_any. Scorecard 1.4% = whichever matches.
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
  ,SUM(CASE WHEN responder LIKE '1.%'                         THEN 1 ELSE 0 END) AS resp_target
  ,SUM(CASE WHEN responder LIKE '2.%'                         THEN 1 ELSE 0 END) AS resp_nontarget
  ,SUM(CASE WHEN responder LIKE '1.%' OR responder LIKE '2.%' THEN 1 ELSE 0 END) AS resp_any
  ,SUM(CASE WHEN responder_targetproduct = '1'                THEN 1 ELSE 0 END) AS responder_targetproduct
  ,COUNT(*)                                                       AS clnt_count
FROM DL_MR_PROD.CARDS_BIZUPS_VBU_DESCRESP_CLNT
WHERE year_mon_start IN ('2025-05','2025-06','2025-07','2026-05','2026-06','2026-07')
GROUP BY 1,2,3,4,5,6
;
