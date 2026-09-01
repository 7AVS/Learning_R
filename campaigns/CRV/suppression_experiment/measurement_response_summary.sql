-- ============================================================================
-- MEASUREMENT — RESPONSE SUMMARY (CRV + PCL)                    [FINAL FILE]
-- The permanent scoreboard for the CRV live experiment (go-live 2026-08-14).
-- Iterate HERE; the e1-e9 files are the frozen audit trail that validated
-- every rule this file embodies. Two statements:
--   STMT 1 — CRV response (installment take-up) per cell        [runs weekly]
--   STMT 2 — PCL response (limit-increase conv on PCL leads)    [runs when a
--            post-go-live PCL wave is loaded; first = September 2026]
--
-- DESIGN (validated e1-e9, full detail: live_experiment_config_2026-08.md):
--   TG8 = 5% random do-not-contact holdout (drawn every deployment slice)
--   TG4 = passed >=1 of 2 rules -> CRV banner   | TG1 = failed both -> blocked
--   @132 of TACTIC_DECISN_VRB_INFO = pass flag (exists on TG8 too; absent on
--   old-format records, so no contamination possible)
-- CONTRASTS: TG8&Y vs TG4 (causal banner effect, passers)
--            TG8&N vs TG1 (falsification, both dark, expect ~0)
--            NEVER TG4 vs TG1 (rule-based populations, not causal)
-- HARD RULES BAKED IN:
--   * Arms/decisioning from the TACTIC table only; curated tables ONLY for
--     conversion outcomes (curated lags and TG1 has NULL action_control).
--   * Population floor treatmt_strt_dt >= 2026-08-14 (e9: razor-clean code
--     cutover; date floor also future-proofs new codes; e9 = monthly guard).
--   * Cell frozen at FIRST assignment (rules use treatment-sensitive inputs).
--   * CRV curated join = (acct_no, tactic_id), deployment-exact (e5_v3;
--     PMCS offer_start_date drifts up to 4 days — dates never join).
--   * Counts only. Rates and significance computed at read time.
-- CAVEATS TO CARRY INTO ANY SHIPPED NUMBER:
--   * In-flight until each cohort's ~90-day offer windows close (Aug cohort:
--     2026-11-27). November iOS banner-ranking fix lands mid-window -> split
--     reads pre/post once the fix date is known.
--   * TG8&Y passers are organic-only passers; TG4 includes banner-made
--     passers (rule 1 conditions on past response) — small composition
--     caveat; bulletproof variant = pre-period rule-2 stratum.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- STMT 1 — CRV RESPONSE per cell (weekly scoreboard; validated e5_v3)
-- ---------------------------------------------------------------------------
WITH expt AS (
    SELECT visa_acct_no, tactic_id, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
)
SELECT
    TRUNC(e.assign_dt, 'MON')          AS cohort_month,
    e.tst_grp_cd,
    e.pass_flag,
    c.action_control,                       -- check column only (TG1 = null expected)
    COUNT(*)                           AS offers,
    COUNT(DISTINCT c.acct_no)          AS accts,               -- must equal offers
    SUM(c.responder)                   AS crv_responders,
    MIN(c.offer_start_date)            AS min_offer_start,
    MAX(c.offer_end_date)              AS max_offer_end,       -- cohort maturity horizon
    -- maturity context: how far along the conversion window this read is
    CURRENT_DATE                       AS run_dt,
    MAX(CASE WHEN c.responder = 1
             THEN c.first_response_date END) AS last_response_dt,
    CURRENT_DATE - MIN(e.assign_dt)    AS days_in_mkt_oldest,
    CURRENT_DATE - MAX(e.assign_dt)    AS days_in_mkt_youngest
FROM expt e
JOIN dl_mr_prod.cards_crv_install_decis_resp c
  ON c.acct_no   = e.visa_acct_no
 AND c.tactic_id = e.tactic_id                  -- deployment-exact
 AND c.offer_start_date >= DATE '2026-08-01'    -- loose pushdown only
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

-- ---------------------------------------------------------------------------
-- STMT 2 — PCL RESPONSE per cell x wave x channel (validated e3/e3b/e3c/e4)
-- PCL response = responder_cli on a PCL lead born on/after the account's
-- assignment. Channel is a dimension (ruling: PCL = MB; confirm on first run).
-- Returns zero rows until a post-08-14 PCL wave loads (e3b: latest = 08-05).
-- ---------------------------------------------------------------------------
WITH expt AS (
    SELECT visa_acct_no, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
)
-- SCOPE (Andre 2026-09-01, Q04-style co-presence): count a PCL lead if its
-- window is STILL OPEN on/after the account's assignment — overlap, not
-- born-after. actual_strt_dt (e10: real in-market date) drives the timing
-- split: 'post_assign' = lead actually started after assignment (clean full
-- exposure); 'in_flight' = lead already in market at assignment (first days
-- pre-experiment; kept, labeled, read separately).
SELECT
    TRUNC(e.assign_dt, 'MON')          AS cohort_month,
    TRUNC(p.actual_strt_dt, 'MON')     AS pcl_wave_month,    -- ACTUAL in-market month
    e.tst_grp_cd,
    e.pass_flag,
    CASE WHEN p.actual_strt_dt >= e.assign_dt
         THEN 'post_assign' ELSE 'in_flight' END AS lead_timing,
    p.channel,
    COUNT(*)                           AS pcl_leads,
    COUNT(DISTINCT p.acct_no)          AS pcl_lead_accts,
    SUM(p.responder_cli)               AS pcl_responders,
    -- maturity context per wave (actual dates)
    CURRENT_DATE                       AS run_dt,
    MAX(CASE WHEN p.responder_cli = 1
             THEN p.dt_cl_change END)  AS last_response_dt,
    CURRENT_DATE - MIN(p.actual_strt_dt) AS days_in_mkt_oldest,
    CURRENT_DATE - MAX(p.actual_strt_dt) AS days_in_mkt_youngest
FROM expt e
JOIN dl_mr_prod.cards_pli_decision_resp p
  ON p.acct_no = e.visa_acct_no
 AND p.treatmt_strt_dt >= DATE '2026-05-01'     -- pushdown: labels lag actual by up to ~2mo
WHERE p.treatmt_end_dt >= e.assign_dt           -- lead window open on/after assignment = overlap
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;
