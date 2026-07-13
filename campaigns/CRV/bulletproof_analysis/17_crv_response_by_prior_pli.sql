-- #############################################################################
-- CRV x PLI -- Hypothesis test: does taking a PLI predict CRV take-up?  (Q17 series)
-- #############################################################################
--
-- ANSWER (one line) -- yes: clients who TOOK a PLI ("PLI takers") take up CRV ~2-8x more
--   than clients offered a PLI who DIDN'T ("PLI decliners") at the same decile, and they
--   do it even when held out of the CRV banner -- so it's largely organic, not banner-made.
--   This is an association (strong and decile-robust), not a proven cause.
--
-- WORDS (two products -- keep their conversions apart) --
--   PLI taker    = took the PLI limit increase     PLI decliner = offered PLI, declined
--   CRV take-up  = took a CRV installment plan  (the OUTCOME we count)
--   shown        = shown the CRV banner (Action)   held-out     = held out of it (Control)
--   In the columns: converter = PLI taker, nonconverter = PLI decliner, action = shown,
--                   control = held-out, responders = CRV take-up, offers = CRV offers.
--
-- HYPOTHESIS -- "CRV take-up is higher among PLI takers than PLI decliners." Sequential
--   (after-PLI) direction; the concurrent overlap question is H1, separate.
--
-- SCOPE -- the ENTIRE CRV population (every CRV offer from 2024-10-01, ~23.4M). NOT the
--   overlap slice. Each CRV offer is labelled by the client's PRIOR PLI (a PLI that ENDED
--   before the CRV offer started -- sequential, not concurrent).
--
-- WHAT WE FOUND (SQL = counts; rates in Excel) --
--   Overall (shown):  no-prior-PLI 0.89% | PLI takers 2.73% | PLI decliners 0.77%
--     -> PLI takers ~3.5x PLI decliners.
--   Within decile (Q17d): PLI takers > PLI decliners in ALL 10 deciles (1.9-7.9x)
--     -> not just that takers sit in better deciles.
--   Organic (held-out): even with NO banner, PLI takers take up CRV more than PLI
--     decliners in all 10 deciles (1.6-9x) -> the appetite is largely intrinsic.
--   Banner is a small add-on: for PLI takers, held-out take-up is close to shown
--     (~75-85% of their take-up is organic); the banner adds the rest.
--   Banner works harder on PLI takers: shown-minus-held-out lift ~+0.5-0.9pp for takers
--     vs ~+0.1-0.25pp for decliners (~3-5x). This piece is randomised -> clean.
--   Timing (Q17c): take-up ~flat 0-180 days after the PLI take, then drops past 180.
--
-- WHERE IT POINTS / WHAT IT DOESN'T SETTLE -- a PLI take strongly marks CRV appetite, and
--   the marker survives decile control AND shows up organically (no banner). It does NOT
--   prove the PLI take CAUSES the appetite: PLI was never randomised and taking it is a
--   client choice, so "the limit increase drives CRV demand" stays an untested read.
--   (The banner-works-harder-on-takers piece IS randomised, so that part is causal.)
--
-- CONTEXT (separate finding, not a recommendation) -- H1 tested the concurrent direction:
--   CRV running AT THE SAME TIME as PLI is associated with lower PLI response (1.08pp
--   shown-vs-held-out gap, randomised; ~42K PLI conversions). Concurrent (H1) and
--   sequential (this) are two different timing relationships between the same campaigns.
-- #############################################################################

-- =============================================================================
-- Q17 -- CRV response by prior-PLI status   (mirror of Q15; tests Hypothesis B)
-- Hypothesis: is CRV take-up higher AFTER a PLI take than after a PLI decline?
--   (sequential direction; concurrent overlap = H1.) Classify each CRV OFFER by the
--   client's PLI history BEFORE that offer, then compare CRV take-up per group (counts).
-- Grain: one row per (acct x CRV offer wave) = CRV-offer-centric (mirror of Q15's
--   PLI-lead grain). LEFT JOIN to PLI history is collapsed by GROUP BY -> no fanout.
--
-- KEY COMPARISON: prior_pli_converter vs prior_pli_nonconverter. BOTH were PLI-
--   targeted (controls for "engaged enough to get a PLI offer"); they differ only
--   in whether they took it -- isolates the PLI-take itself from merely being targeted.
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
-- Q17d -- ONE wide table: converter / non-converter  x  Action / Control, by decile.
-- One row per decile (clients WITH a prior PLI only; no_prior has no decile -> see Q17).
-- Answers everything in a single scan, per decile -- compute rates from the columns:
--   confound gate     -> converter_action rate   vs  nonconverter_action rate
--   banner lift       -> converter_action        vs  converter_control   (same for nonconverter)
--   organic affinity  -> converter_control       vs  nonconverter_control   (organic CRV uptake?)
-- decile = the client's LATEST prior PLI lead's decile. Counts only (rates in Excel).
-- NOTE: Control columns are only informative if held-out Control has a non-zero organic
--   CRV response; if Control responders ~ 0, there's no organic/lift signal to read.
-- =============================================================================
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, responder AS crv_resp, action_control
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
),
pli_leads AS (
    SELECT acct_no, treatmt_end_dt, responder_cli, decile
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
),
ranked AS (
    SELECT c.crv_resp,
           c.action_control,
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
    SUM(CASE WHEN any_conv = 1 AND action_control = 'Action'  THEN 1 ELSE 0 END)        AS converter_action_offers,
    SUM(CASE WHEN any_conv = 1 AND action_control = 'Action'  THEN crv_resp ELSE 0 END) AS converter_action_responders,
    SUM(CASE WHEN any_conv = 1 AND action_control = 'Control' THEN 1 ELSE 0 END)        AS converter_control_offers,
    SUM(CASE WHEN any_conv = 1 AND action_control = 'Control' THEN crv_resp ELSE 0 END) AS converter_control_responders,
    SUM(CASE WHEN any_conv = 0 AND action_control = 'Action'  THEN 1 ELSE 0 END)        AS nonconverter_action_offers,
    SUM(CASE WHEN any_conv = 0 AND action_control = 'Action'  THEN crv_resp ELSE 0 END) AS nonconverter_action_responders,
    SUM(CASE WHEN any_conv = 0 AND action_control = 'Control' THEN 1 ELSE 0 END)        AS nonconverter_control_offers,
    SUM(CASE WHEN any_conv = 0 AND action_control = 'Control' THEN crv_resp ELSE 0 END) AS nonconverter_control_responders
FROM ranked
WHERE rn_any = 1
GROUP BY decile
ORDER BY decile
;

-- =============================================================================
-- Q17b v2 -- cohort-month timeline, RECENT vs LEGACY takers (revised 2026-07-13)
-- Statuses: no_prior_pli / prior_pli_nonconverter / pli_converter_recent_90d
--   (took the PLI within 90 days before this CRV offer -- ANDRE's cutoff, editable)
--   / pli_converter_legacy (took it longer ago).
-- Converter lock = dt_cl_change (limit-change EVENT) < offer_start. v1's leakage
--   bucket (conv_unproven) RAN 2026-07-13 and was EMPTY -> lock airtight, bucket dropped.
-- v1 run results (pics PXL_20260713_2215*): taker/decliner ratio stable ~3.5-4x in all
--   19 cohorts, both arms; rates decline over time (taker 4.3%->2.7%) = pool aging.
-- Granular recency run (pics PXL_20260713_2256*, superseded Q17e, statements removed):
--   rate plateau ~3.5-3.6% through 180d since take, then decay 2.9% -> 2.2% -> 1.9% (365+);
--   365+ never drops below ~2.7x decliner baseline (trait persists); 365+ offer volume
--   balloons to ~174K/mo by 2026-02 (stale pool accumulation).
-- =============================================================================
WITH crv_offers AS (
    SELECT acct_no, offer_start_date, year_mth_offer_start, action_control,
           responder AS crv_resp
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
),
pli_leads AS (
    SELECT acct_no, treatmt_end_dt, responder_cli, dt_cl_change
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-01-01'
),
crv_prior_pli AS (
    SELECT c.acct_no, c.offer_start_date, c.year_mth_offer_start,
           c.action_control, c.crv_resp,
           COUNT(p.acct_no) AS n_prior_pli,
           MAX(CASE WHEN p.responder_cli = 1
                     AND p.dt_cl_change < c.offer_start_date
                    THEN p.dt_cl_change END) AS latest_take_dt
    FROM crv_offers c
    LEFT JOIN pli_leads p
      ON p.acct_no = c.acct_no
     AND p.treatmt_end_dt < c.offer_start_date
    GROUP BY c.acct_no, c.offer_start_date, c.year_mth_offer_start,
             c.action_control, c.crv_resp
),
classified AS (
    SELECT year_mth_offer_start, action_control, crv_resp,
           /* ANDRE: DECIDE -- recent/legacy cutoff, currently 90 days since the take */
           CASE WHEN n_prior_pli = 0          THEN 'no_prior_pli'
                WHEN latest_take_dt IS NULL   THEN 'prior_pli_nonconverter'
                WHEN offer_start_date - latest_take_dt <= 90
                                              THEN 'pli_converter_recent_90d'
                ELSE                               'pli_converter_legacy' END AS pli_status
    FROM crv_prior_pli
)
SELECT
    year_mth_offer_start AS cohort_month,
    pli_status,
    SUM(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END)        AS offers_action,
    SUM(CASE WHEN action_control = 'Action'  THEN crv_resp ELSE 0 END) AS responders_action,
    SUM(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END)        AS offers_control,
    SUM(CASE WHEN action_control = 'Control' THEN crv_resp ELSE 0 END) AS responders_control
FROM classified
GROUP BY 1, 2
ORDER BY 1, 2
;
