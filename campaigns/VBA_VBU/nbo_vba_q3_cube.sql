-- ============================================================================
-- VBA Q3 COMMENTARY CUBE  (Teradata-direct)  — 2026-08-27
-- Source: dl_mr_prod.nbo_vba_rbol_combined. One row per tactic / treatment window / comparison / arm / TPA-ITA / offer.
-- Context: Q3 scorecard shows VBA leads +47% YoY, lift over No-Action-Control 0.0% (-98% YoY). Campaign owner's
-- explanation: ITA (intention-to-apply) leads launched Q3 FY2026 (none in Q3 FY2025); MCB offers new on the TPA side in FY2026.
-- Fiscal quarters: Q3 FY2025 = treatmt_strt_dt in May-Jul 2025; Q3 FY2026 = May-Jul 2026. Fiscal year starts Nov 1.
-- Definitions: TPA = pre-approved; ITA = intention to apply; tpa_ita_indicator NULL = neither.
--   Use comparison = 'VBA Only: Action vs. Control' for lift (control = 'Control' is the no-action control).
--   Lift (abs) = net_response/clnt_count [Action] - net_response/clnt_count [Control], in pp. Leads = clnt_count.
--
-- QUESTIONS TO ANSWER FROM THE EXPORTED CSV (answer each with a table, then one sentence):
-- Q1. Q3-25 vs Q3-26, total VBA: leads, action net RR, control net RR, lift (pp and relative). Does it reproduce
--     the scorecard (~184,521 leads, ~0.0% lift)? If not, state which filter/period is closest.
-- Q2. Same table split by tpa_ita_indicator (TPA / ITA / NULL) for Q3-26. Does ITA have ANY control rows?
--     If ITA has no control, the pooled lift compares a mixed action arm vs a TPA-only control — quantify:
--     (a) TPA-only lift Q3-25 vs Q3-26; (b) pooled lift Q3-26; (c) the gap = ITA dilution.
-- Q3. Leads growth decomposition Q3-25 -> Q3-26: how much of the +47% is ITA (new), how much TPA, how much NULL?
-- Q4. TPA side by visa_offer_test for Q3-26: leads, action RR, control RR, lift per offer. Isolate the MCB offers
--     (visa_offer_prod = 'MCB': MCB 35k, MCB 45k). Is MCB lift above or below the non-MCB TPA lift?
--     Re-compute TPA lift EXCLUDING MCB — does removing MCB move the YoY comparison materially (>0.1pp)?
-- Q5. Mix check: share of TPA leads on MCB vs AIB vs CPX vs MC6, Q3-25 vs Q3-26. Did the offer mix shift toward
--     lower-lift offers? Report a mix-adjusted TPA lift (Q3-26 per-offer rates weighted by Q3-25 offer mix).
-- Q6. Maturity caveat: for Q3-26 cohorts, treatment_end_dt vs today — which months are still inside their
--     response window? Flag any month whose lift is not yet final.
-- Q7. One-paragraph reply to the campaign owner: (i) ITA impact on pooled lift, (ii) MCB impact on TPA lift,
--     (iii) what the scorecard number should be read as. Numbers cited with the table they came from.
-- Statistical bar: report a 2-sided z-test on the action-vs-control net RR difference for every lift you cite;
--   flag anything below 80% confidence as "not significant". Never present a lift where the control arm is empty.
-- ============================================================================
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
