-- =============================================================================
-- Q17b -- CRV response by prior-PLI status, BY COHORT MONTH (the timeline view)
-- Fixes two Q17 scrutiny findings (2026-07-13):
--   1. LOCK TIGHTENED: converter = prior PLI lead with responder_cli=1 AND
--      dt_cl_change < offer_start_date (the actual limit-change EVENT date).
--      Q17 used treatmt_end_dt < offer_start only, but responder_cli outcomes can
--      land after treatment end -> a client converting PLI *after* the CRV offer
--      started could leak into "prior_pli_converter". dt_cl_change closes that.
--      (dt_cl_change NULL on a responder row -> falls back to treatmt_end_dt lock
--      alone and is flagged in its own status bucket so the leakage is COUNTED.)
--   2. TIMELINE: one row per CRV cohort month x pli_status (Q17 pooled 2024-10+
--      into one number; this shows whether the association holds across cohorts).
-- Grain: one row per (acct x CRV offer), collapsed by GROUP BY -> no fanout.
-- Population: ALL CRV offers 2024-10+, both arms (wide columns).
-- ENGINE: Teradata-direct (dl_mr_prod). Counts only (rates/curves in Excel:
--   plot rate per cohort_month per status -- Action solid, Control dashed).
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
           /* conversion EVENT locked before the CRV offer */
           MAX(CASE WHEN p.responder_cli = 1
                     AND p.dt_cl_change IS NOT NULL
                     AND p.dt_cl_change < c.offer_start_date
                    THEN 1 ELSE 0 END) AS conv_locked,
           /* responder rows that would have counted under Q17's lead-window-only
              lock but FAIL (or can't prove) the event-date lock -> leakage bucket */
           MAX(CASE WHEN p.responder_cli = 1
                     AND (p.dt_cl_change IS NULL
                          OR p.dt_cl_change >= c.offer_start_date)
                    THEN 1 ELSE 0 END) AS conv_unproven
    FROM crv_offers c
    LEFT JOIN pli_leads p
      ON p.acct_no = c.acct_no
     AND p.treatmt_end_dt < c.offer_start_date
    GROUP BY c.acct_no, c.offer_start_date, c.year_mth_offer_start,
             c.action_control, c.crv_resp
),
classified AS (
    SELECT year_mth_offer_start, action_control, crv_resp,
           CASE WHEN n_prior_pli = 0     THEN 'no_prior_pli'
                WHEN conv_locked = 1     THEN 'prior_pli_converter'
                WHEN conv_unproven = 1   THEN 'prior_pli_conv_unproven'  /* Q17 leakage, quantified */
                ELSE 'prior_pli_nonconverter' END AS pli_status
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
