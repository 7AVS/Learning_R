-- =============================================================================
-- Q17 -- CRV response by prior-PLI status   (mirror of Q15; tests Hypothesis B)
-- Hypothesis B: CRV converts better AFTER a PLI conversion -> sequence CRV after
--   PLI, don't discontinue. Classify each CRV OFFER by the client's PLI history
--   BEFORE that offer, then compare CRV response rate per group (counts only).
-- Grain: one row per (acct x CRV offer wave) = CRV-offer-centric (mirror of Q15's
--   PLI-lead grain). LEFT JOIN to PLI history is collapsed by GROUP BY -> no fanout.
--
-- KEY COMPARISON: prior_pli_converter vs prior_pli_nonconverter. BOTH were PLI-
--   targeted (controls for "engaged enough to get a PLI offer"); they differ only
--   in whether they took it. That isolates the PRIMING effect of the conversion.
--   no_prior_pli is the outer reference, but it mixes in a different population.
--
-- FLAGGED CHOICES (change if you disagree):
--   * CRV offers filtered to action_control='Action' (Control was never deployed,
--     so its "response" is not a meaningful offer-response measure).
--   * No channel filter on CRV offers (Hyp B is behavioral, channel-agnostic). Add
--     channels_deployed LIKE '%IM%' to restrict to the mobile banner battleground.
--   * No channel filter on PLI (a limit increase primes CRV regardless of which
--     channel delivered it). "prior_pli_converter" = converted ANY prior PLI.
--   * "Prior" = PLI ended strictly before the CRV offer started (no overlap).
-- Starburst-safe: no QUALIFY, no NULLIFZERO, no TOP. Counts only (rate in Excel).
-- =============================================================================
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, responder AS crv_resp
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Action'
),
pli_leads AS (
    SELECT acct_no, treatmt_end_dt, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
),
crv_prior_pli AS (
    SELECT c.acct_no, c.offer_start_date, c.crv_resp,
           COUNT(p.acct_no)     AS n_prior_pli,
           MAX(p.responder_cli) AS any_prior_pli_conv
    FROM crv_offers c
    LEFT JOIN pli_leads p
      ON p.acct_no = c.acct_no
     AND p.treatmt_end_dt < c.offer_start_date
    GROUP BY c.acct_no, c.offer_start_date, c.crv_resp
),
classified AS (
    SELECT crv_resp,
           CASE WHEN n_prior_pli = 0        THEN 'no_prior_pli'
                WHEN any_prior_pli_conv = 1 THEN 'prior_pli_converter'
                ELSE 'prior_pli_nonconverter' END AS pli_status
    FROM crv_prior_pli
)
SELECT
    pli_status,
    COUNT(*)      AS n_crv_offers,
    SUM(crv_resp) AS n_crv_responders
FROM classified
GROUP BY pli_status
ORDER BY pli_status
;
