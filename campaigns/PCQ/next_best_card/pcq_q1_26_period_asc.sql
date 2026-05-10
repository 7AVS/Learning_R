-- PCQ Q1 26 quarterly NBC review -- Period-ASC focused output
-- Companion to pcq_q1_26_strategy_trend.sql (the wider Total + Period + Other version).
-- Cohort: treatmt_start_dt >= 2025-11-01, tpa_ita = 'TPA'
-- Rules:
--   deployed                   -> cohort-wide (no asc filter)  [denominator]
--   responded / approved       -> Period-ASC only
--   response channel pivots    -> Period-ASC only (per-response attribute)
--   portfolio + credit-limit-approved -> Period-ASC only
--   email funnel (sent/open/click/unsub) -> cohort-wide totals (tactic-config level)
--   contact channels (chnl_*)  -> cohort-wide totals (pre-deployment exposure)
--   offer credit limit         -> cohort-wide total (pre-response)
-- Volatile tables use pa_ prefix to coexist with the main query in the same session.

DATABASE DL_MR_PROD;


-- ============================================================
-- Drop volatiles from any prior run in this session.
-- First-time run will throw 3807 (object does not exist) -- harmless.
-- ============================================================
DROP TABLE pcq_q1_pa_acct_summary;
DROP TABLE pcq_q1_pa_approved;
DROP TABLE pcq_q1_pa_cohort;


-- ============================================================
-- pcq_q1_pa_cohort: all PCQ TPA deployments since Nov 2025
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_pa_cohort AS (
  SELECT
    clnt_no,
    acct_no,
    test_group_latest,
    strtgy_seg_typ,
    model_score_decile,
    offer_prod_latest,
    offer_prod_latest_name,
    offer_cr_lmt_latest,
    product_applied_name,
    cr_lmt_approved,
    asc_on_app_source,
    asc_on_app,
    treatmt_start_dt,
    treatmt_end_dt,
    response_dt,
    response_channel,
    response_channel_grp,
    app_approved,
    tactic_email,
    email_disposition,
    chnl_dm,
    chnl_do,
    chnl_ec,
    chnl_em,
    chnl_em_reminder,
    chnl_im,
    chnl_in,
    chnl_iu,
    chnl_iv,
    chnl_mb,
    chnl_md,
    chnl_rd
  FROM cards_tpa_pcq_decision_resp
  WHERE treatmt_start_dt >= DATE '2025-11-01'
    AND tpa_ita = 'TPA'
) WITH DATA
PRIMARY INDEX (clnt_no, acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_pa_approved: drives portfolio join (acct_no level)
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_pa_approved AS (
  SELECT
    acct_no,
    clnt_no,
    treatmt_start_dt,
    test_group_latest,
    strtgy_seg_typ,
    offer_prod_latest,
    asc_on_app_source
  FROM pcq_q1_pa_cohort
  WHERE app_approved = 1
    AND acct_no IS NOT NULL
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_pa_acct_summary: per-account portfolio rollup since treatmt_start_dt
--   (same logic as the main query -- portfolio data is asc-agnostic
--    at this stage; Period-ASC filter applies at the OUTPUT level)
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_pa_acct_summary AS (
  SELECT
    s.acct_no,
    s.sum_purchases,
    s.sum_purchases_d0_15,
    s.sum_purchases_d0_30,
    s.sum_purchases_d0_45,
    s.sum_purchases_d0_60,
    s.sum_purchases_d0_90,
    s.event_days,
    s.last_event_dt,
    (s.last_event_dt - a.treatmt_start_dt) AS days_observed,
    l.last_bal_current,
    l.last_mtd_avg_bal,
    l.last_status,
    l.acct_open_dt,
    l.acct_cls_dt,
    l.visa_prod_cd
  FROM (
    SELECT
      p.acct_no,
      SUM(p.net_prch_amt_dly)                                                                                AS sum_purchases,
      SUM(CASE WHEN (p.dt_record_ext - a.treatmt_start_dt) <= 15 THEN p.net_prch_amt_dly ELSE 0 END)         AS sum_purchases_d0_15,
      SUM(CASE WHEN (p.dt_record_ext - a.treatmt_start_dt) <= 30 THEN p.net_prch_amt_dly ELSE 0 END)         AS sum_purchases_d0_30,
      SUM(CASE WHEN (p.dt_record_ext - a.treatmt_start_dt) <= 45 THEN p.net_prch_amt_dly ELSE 0 END)         AS sum_purchases_d0_45,
      SUM(CASE WHEN (p.dt_record_ext - a.treatmt_start_dt) <= 60 THEN p.net_prch_amt_dly ELSE 0 END)         AS sum_purchases_d0_60,
      SUM(CASE WHEN (p.dt_record_ext - a.treatmt_start_dt) <= 90 THEN p.net_prch_amt_dly ELSE 0 END)         AS sum_purchases_d0_90,
      COUNT(*)                                                                                               AS event_days,
      MAX(p.dt_record_ext)                                                                                   AS last_event_dt
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN pcq_q1_pa_approved a
      ON p.acct_no = a.acct_no
    WHERE p.dt_record_ext >= a.treatmt_start_dt
    GROUP BY p.acct_no
  ) s
  INNER JOIN (
    SELECT
      p.acct_no,
      p.bal_current        AS last_bal_current,
      p.accum_dly_bal_mtd  AS last_mtd_avg_bal,
      p.status             AS last_status,
      p.acct_open_dt,
      p.acct_cls_dt,
      p.visa_prod_cd
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN pcq_q1_pa_approved a
      ON p.acct_no = a.acct_no
    WHERE p.dt_record_ext >= a.treatmt_start_dt
    QUALIFY ROW_NUMBER()
              OVER (PARTITION BY p.acct_no
                    ORDER BY p.dt_record_ext DESC) = 1
  ) l
    ON s.acct_no = l.acct_no
  INNER JOIN pcq_q1_pa_approved a
    ON s.acct_no = a.acct_no
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- OUTPUT: Period-ASC focused single summary
-- Grain: 8 dims (no asc, no tpa_ita)
-- ============================================================
SELECT
  c.treatmt_start_dt,
  c.test_group_latest,
  c.strtgy_seg_typ,
  c.offer_prod_latest,
  c.offer_prod_latest_name,
  c.response_channel_grp,
  c.product_applied_name,
  c.model_score_decile,

  -- COHORT-WIDE counts (no asc filter -- denominator)
  COUNT(*)                                                                                                                                                  AS deployed,

  -- PERIOD-ASC conversion (responded) + cumulative day buckets
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC'                                                  THEN 1 ELSE 0 END)        AS responded_period_asc,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 15   THEN 1 ELSE 0 END)        AS responded_period_asc_d0_15,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 30   THEN 1 ELSE 0 END)        AS responded_period_asc_d0_30,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 45   THEN 1 ELSE 0 END)        AS responded_period_asc_d0_45,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 60   THEN 1 ELSE 0 END)        AS responded_period_asc_d0_60,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 90   THEN 1 ELSE 0 END)        AS responded_period_asc_d0_90,

  -- PERIOD-ASC conversion (approved) + cumulative day buckets
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC'                                                  THEN 1 ELSE 0 END)               AS approved_period_asc,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 15   THEN 1 ELSE 0 END)               AS approved_period_asc_d0_15,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 30   THEN 1 ELSE 0 END)               AS approved_period_asc_d0_30,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 45   THEN 1 ELSE 0 END)               AS approved_period_asc_d0_45,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 60   THEN 1 ELSE 0 END)               AS approved_period_asc_d0_60,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND (c.response_dt - c.treatmt_start_dt) <= 90   THEN 1 ELSE 0 END)               AS approved_period_asc_d0_90,

  -- Response channel pivots (Period-ASC scoped -- per-response attribute)
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Online'            THEN 1 ELSE 0 END)        AS responded_via_online_period_asc,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Mobile'            THEN 1 ELSE 0 END)        AS responded_via_mobile_period_asc,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Branch/Advice Ctr' THEN 1 ELSE 0 END)        AS responded_via_branch_period_asc,
  SUM(CASE WHEN c.response_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Other'             THEN 1 ELSE 0 END)        AS responded_via_other_period_asc,

  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Online'            THEN 1 ELSE 0 END)               AS approved_via_online_period_asc,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Mobile'            THEN 1 ELSE 0 END)               AS approved_via_mobile_period_asc,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Branch/Advice Ctr' THEN 1 ELSE 0 END)               AS approved_via_branch_period_asc,
  SUM(CASE WHEN c.app_approved = 1 AND c.asc_on_app_source = 'Period-ASC' AND c.response_channel_grp = 'Other'             THEN 1 ELSE 0 END)               AS approved_via_other_period_asc,

  -- Portfolio (Period-ASC scoped)
  SUM(CASE WHEN s.acct_no IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END)                                                             AS approved_with_portfolio_data_period_asc,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.sum_purchases       ELSE 0 END)                                                                   AS sum_purchases_period_asc,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.sum_purchases_d0_15 ELSE 0 END)                                                                   AS sum_purchases_period_asc_d0_15,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.sum_purchases_d0_30 ELSE 0 END)                                                                   AS sum_purchases_period_asc_d0_30,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.sum_purchases_d0_45 ELSE 0 END)                                                                   AS sum_purchases_period_asc_d0_45,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.sum_purchases_d0_60 ELSE 0 END)                                                                   AS sum_purchases_period_asc_d0_60,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.sum_purchases_d0_90 ELSE 0 END)                                                                   AS sum_purchases_period_asc_d0_90,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.last_bal_current    ELSE 0 END)                                                                   AS sum_last_bal_current_period_asc,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.last_mtd_avg_bal    ELSE 0 END)                                                                   AS sum_last_mtd_avg_bal_period_asc,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN s.days_observed       ELSE 0 END)                                                                   AS sum_days_observed_period_asc,
  MAX(s.last_event_dt)                                                                                                                                      AS max_event_dt_in_cohort,
  SUM(CASE WHEN s.acct_cls_dt IS NOT NULL AND c.asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END)                                                         AS closed_accts_period_asc,

  -- Credit limit (offer cohort-wide; approved Period-ASC)
  SUM(c.offer_cr_lmt_latest)                                                                                                                                AS sum_offer_cr_lmt,
  SUM(CASE WHEN c.asc_on_app_source = 'Period-ASC' THEN c.cr_lmt_approved ELSE 0 END)                                                                       AS sum_cr_lmt_approved_period_asc,

  -- Email funnel (cohort-wide totals -- tactic-config level)
  SUM(CAST(c.tactic_email     AS INTEGER))                                                                                                                  AS clients_tactic_email,
  SUM(CASE WHEN c.email_disposition = 'eMail Sent'      THEN 1 ELSE 0 END)                                                                                  AS email_sent,
  SUM(CASE WHEN c.email_disposition = 'eMail Open'      THEN 1 ELSE 0 END)                                                                                  AS email_open,
  SUM(CASE WHEN c.email_disposition LIKE 'eMail Clic%'  THEN 1 ELSE 0 END)                                                                                  AS email_click,
  SUM(CASE WHEN c.email_disposition LIKE 'eMail Unsu%'  THEN 1 ELSE 0 END)                                                                                  AS email_unsub,

  -- Contact channels (cohort-wide totals -- pre-deployment exposure)
  SUM(CAST(c.chnl_dm          AS INTEGER))                                                                                                                  AS clients_chnl_dm,
  SUM(CAST(c.chnl_do          AS INTEGER))                                                                                                                  AS clients_chnl_do,
  SUM(CAST(c.chnl_ec          AS INTEGER))                                                                                                                  AS clients_chnl_ec,
  SUM(CAST(c.chnl_em          AS INTEGER))                                                                                                                  AS clients_chnl_em,
  SUM(CAST(c.chnl_em_reminder AS INTEGER))                                                                                                                  AS clients_chnl_em_reminder,
  SUM(CAST(c.chnl_im          AS INTEGER))                                                                                                                  AS clients_chnl_im,
  SUM(CAST(c.chnl_in          AS INTEGER))                                                                                                                  AS clients_chnl_in,
  SUM(CAST(c.chnl_iu          AS INTEGER))                                                                                                                  AS clients_chnl_iu,
  SUM(CAST(c.chnl_iv          AS INTEGER))                                                                                                                  AS clients_chnl_iv,
  SUM(CAST(c.chnl_mb          AS INTEGER))                                                                                                                  AS clients_chnl_mb,
  SUM(CAST(c.chnl_md          AS INTEGER))                                                                                                                  AS clients_chnl_md,
  SUM(CAST(c.chnl_rd          AS INTEGER))                                                                                                                  AS clients_chnl_rd

FROM pcq_q1_pa_cohort c
LEFT JOIN pcq_q1_pa_acct_summary s
  ON c.acct_no = s.acct_no
 AND c.app_approved = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8;
