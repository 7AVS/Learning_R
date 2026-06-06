-- #############################################################################
-- CRV x PLI -- Hypothesis test: does a prior PLI conversion predict CRV response?
-- (Q17 series)   [finding notes]
-- #############################################################################
--
-- FINDING (one line) -- a prior PLI conversion is a strong predictor of CRV response:
--   clients who had converted a PLI respond to CRV 2-8x more than same-decile clients
--   who had a PLI but didn't convert, and the lift tracks the CONVERSION, not the decile.
--
-- HYPOTHESIS TESTED -- "CRV response is higher among clients who previously converted a
--   PLI than among those who didn't." This is the SEQUENTIAL (after-PLI) direction --
--   distinct from the concurrent cannibalisation question (H1).
--
-- WHAT THE TEST LOOKS AT (scope) -- the ENTIRE CRV Action population: every CRV Action
--   offer from 2024-10-01 on (~23.4M). NOT the overlap slice. Each CRV offer is labelled
--   by the client's PRIOR PLI history, where "prior" = a PLI that ENDED BEFORE the CRV
--   offer started (sequential, not concurrent). Outcome = CRV response; PLI only labels:
--     no_prior_pli            = no PLI before this CRV offer
--     prior_pli_converter     = had a prior PLI and CONVERTED it
--     prior_pli_nonconverter  = had a prior PLI but did NOT convert
--
-- WHAT THE TEST SHOWS (SQL = counts; rates in Excel) --
--   Full CRV split:  no_prior_pli 12.59M = 0.89% | converter 4.56M = 2.73%
--                    | nonconverter 6.22M = 0.77%   -> converters ~3.5x non-converters.
--   Confound check (Q17d) -- converters vs non-converters WITHIN each decile: converters
--     higher in ALL 10 deciles (1.9x at decile 5, up to 7.9x at decile 1, 5.4x at 10).
--     Non-converters ~flat 0.45-0.91% across deciles; converters swing 1.7-3.9%
--     -> the difference is not explained by converters being higher-decile clients.
--   Timing (Q17c) -- CRV response is ~flat 0-180 days after the conversion, then drops
--     past 180 days.
--
-- WHAT IT DOES / DOESN'T ESTABLISH -- supports the hypothesis as an ASSOCIATION (and one
--   not explained by decile composition): a prior PLI conversion predicts higher CRV
--   response. It does NOT establish causation -- PLI was never randomised and converting
--   is itself a client choice within a decile, so "the limit increase causes/primes CRV
--   demand" is an untested explanation, not a result of this test.
--
-- CONTEXT (separate finding, for interpretation only -- not a recommendation) -- H1 tested
--   the concurrent direction: CRV running AT THE SAME TIME as PLI is associated with lower
--   PLI response (1.08pp Action-vs-Control gap, randomised; ~42K PLI conversions). H1
--   (concurrent) and this test (sequential) describe two different timing relationships
--   between the same two campaigns.
-- #############################################################################

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
-- Teradata (curated dl_mr_prod.* run via Teradata Studio): no QUALIFY (rank in CTE,
-- filter rn=1 outside), no date_diff (use DATE - DATE = int days). Counts only (rate in Excel).
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

-- =============================================================================
-- Q17c -- COMBINED: pli_status x decile x gap_bucket in one table (supersedes Q17a/b).
-- One row per CRV Action offer for prior-PLI clients. Pivot in Excel to read:
--   converter vs non-converter WITHIN decile (confound gate, collapse gap_bucket), AND
--   for converters the days-since-conversion timing -- all reconciled in one cut.
-- gap_bucket = 'n/a' for non-converters (no conversion to measure from).
-- Reference lead: converters = latest prior CONVERTED lead (its decile + date for gap);
--   non-converters = latest prior lead (its decile). new_decile dropped per request.
-- Teradata: DATE - DATE = int days; rank in CTE, filter outside (no QUALIFY).
-- Heavier than Q17 (fans each offer out to its prior PLI leads, then ranks) -- watch spool.
-- =============================================================================
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, responder AS crv_resp
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Action'
),
pli_leads AS (
    SELECT acct_no, treatmt_end_dt, responder_cli, decile
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
),
ranked AS (
    SELECT c.crv_resp,
           p.responder_cli,
           p.decile,
           (c.offer_start_date - p.treatmt_end_dt) AS gap_days,
           ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.offer_start_date ORDER BY p.treatmt_end_dt DESC) AS rn_any,
           ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.offer_start_date, p.responder_cli ORDER BY p.treatmt_end_dt DESC) AS rn_conv,
           MAX(p.responder_cli) OVER (PARTITION BY c.acct_no, c.offer_start_date) AS any_conv
    FROM crv_offers c
    JOIN pli_leads p
      ON p.acct_no = c.acct_no
     AND p.treatmt_end_dt < c.offer_start_date
),
ref AS (
    SELECT crv_resp, decile, gap_days, 1 AS is_conv
    FROM ranked
    WHERE any_conv = 1 AND responder_cli = 1 AND rn_conv = 1
    UNION ALL
    SELECT crv_resp, decile, CAST(NULL AS INTEGER) AS gap_days, 0 AS is_conv
    FROM ranked
    WHERE any_conv = 0 AND rn_any = 1
),
labeled AS (
    SELECT decile,
           crv_resp,
           CASE WHEN is_conv = 1 THEN 'prior_pli_converter' ELSE 'prior_pli_nonconverter' END AS pli_status,
           CASE WHEN is_conv = 0     THEN 'n/a'
                WHEN gap_days <= 30  THEN '000-030'
                WHEN gap_days <= 60  THEN '031-060'
                WHEN gap_days <= 90  THEN '061-090'
                WHEN gap_days <= 120 THEN '091-120'
                WHEN gap_days <= 150 THEN '121-150'
                WHEN gap_days <= 180 THEN '151-180'
                ELSE '180+' END AS gap_bucket
    FROM ref
)
SELECT
    decile,
    pli_status,
    gap_bucket,
    COUNT(*)      AS n_crv_offers,
    SUM(crv_resp) AS n_crv_responders
FROM labeled
GROUP BY decile, pli_status, gap_bucket
ORDER BY decile, pli_status, gap_bucket
;

-- =============================================================================
-- Q17d -- THE CONFOUND GATE, printed directly (no pivot needed). 10 rows.
-- One row per decile: converter vs non-converter CRV offers + responders side by side.
-- READ: per decile, conv rate = conv_responders/conv_offers vs
--       nonconv rate = nonconv_responders/nonconv_offers.
--   Converter still well above non-converter inside every decile -> more than decile
--   composition (Option A). Gap closes within decile -> it was just decile (Option B).
-- decile = the client's LATEST prior PLI lead's decile (same definition for both groups).
-- =============================================================================
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, responder AS crv_resp
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Action'
),
pli_leads AS (
    SELECT acct_no, treatmt_end_dt, responder_cli, decile
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
),
ranked AS (
    SELECT c.crv_resp,
           p.decile,
           ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.offer_start_date ORDER BY p.treatmt_end_dt DESC) AS rn_any,
           MAX(p.responder_cli) OVER (PARTITION BY c.acct_no, c.offer_start_date) AS any_conv
    FROM crv_offers c
    JOIN pli_leads p
      ON p.acct_no = c.acct_no
     AND p.treatmt_end_dt < c.offer_start_date
)
SELECT
    decile,
    SUM(CASE WHEN any_conv = 1 THEN 1 ELSE 0 END)        AS conv_offers,
    SUM(CASE WHEN any_conv = 1 THEN crv_resp ELSE 0 END) AS conv_responders,
    SUM(CASE WHEN any_conv = 0 THEN 1 ELSE 0 END)        AS nonconv_offers,
    SUM(CASE WHEN any_conv = 0 THEN crv_resp ELSE 0 END) AS nonconv_responders
FROM ranked
WHERE rn_any = 1
GROUP BY decile
ORDER BY decile
;

-- =============================================================================
-- Q17e -- Bring CRV CONTROL (held-out) back in: prior_pli_status x action_control.
-- Q17/a-d were Action-ONLY (gross CRV response). This keeps BOTH arms so you can read the
-- CRV banner's INCREMENTAL lift = (Action rate - Control rate) WITHIN each prior-PLI group
-- -- a randomisation-grounded read (CRV's Action/Control is the one randomised lever we have).
-- CAVEAT: only meaningful if held-out Control clients have a NON-ZERO organic CRV response.
--   If Control = never offered = responder ~ 0 by construction, Action - Control collapses
--   to the gross Action rate and this adds nothing. The output shows which case we're in.
-- =============================================================================
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, responder AS crv_resp, action_control
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
),
pli_leads AS (
    SELECT acct_no, treatmt_end_dt, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
),
crv_prior_pli AS (
    SELECT c.crv_resp,
           c.action_control,
           COUNT(p.acct_no)     AS n_prior_pli,
           MAX(p.responder_cli) AS any_prior_pli_conv
    FROM crv_offers c
    LEFT JOIN pli_leads p
      ON p.acct_no = c.acct_no
     AND p.treatmt_end_dt < c.offer_start_date
    GROUP BY c.acct_no, c.offer_start_date, c.crv_resp, c.action_control
),
classified AS (
    SELECT crv_resp,
           action_control,
           CASE WHEN n_prior_pli = 0        THEN 'no_prior_pli'
                WHEN any_prior_pli_conv = 1 THEN 'prior_pli_converter'
                ELSE 'prior_pli_nonconverter' END AS pli_status
    FROM crv_prior_pli
)
SELECT
    pli_status,
    action_control,
    COUNT(*)      AS n_crv_offers,
    SUM(crv_resp) AS n_crv_responders
FROM classified
GROUP BY pli_status, action_control
ORDER BY pli_status, action_control
;
