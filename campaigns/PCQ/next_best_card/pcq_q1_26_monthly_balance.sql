-- PCQ Q1 26 -- Calendar-month vintage table for Period-ASC active accounts
-- Companion to pcq_q1_26_period_asc.sql.
-- Period-ASC + TPA filter applied at COHORT level here -- tighter scope
-- keeps the staged events volatile small enough to fit in spool.
-- Each row = cohort x strategy dims x me_dt (calendar month)
-- Volatile-table names use mb_ prefix to coexist in the same session.
--
-- Excel pivot recipe for Strategy x Month avg daily balance:
--   Rows    : strtgy_seg_typ (or offer_prod_latest)
--   Columns : me_dt
--   Values  : Calculated field = SUM(sum_bal_current_daily) / SUM(total_event_days) = true ADB

DATABASE DL_MR_PROD;


-- ============================================================
-- Drop volatiles from any prior run (first-run errors harmless)
-- ============================================================
DROP TABLE pcq_q1_mb_acct_month;
DROP TABLE pcq_q1_mb_events;
DROP TABLE pcq_q1_mb_approved;
DROP TABLE pcq_q1_mb_cohort;


-- ============================================================
-- pcq_q1_mb_cohort: PCQ TPA Period-ASC deployments since Nov 2025
-- Period-ASC filter at COHORT level -- tighter scope than the
-- companion period_asc file. Trades the ability to see Other-ASC
-- for a smaller dataset that fits in spool.
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_cohort AS (
  SELECT
    clnt_no,
    acct_no,
    strtgy_seg_typ,
    model_score_decile,
    offer_prod_latest,
    offer_prod_latest_name,
    product_applied_name,
    response_channel_grp,
    treatmt_start_dt,
    treatmt_end_dt,
    app_approved
  FROM cards_tpa_pcq_decision_resp
  WHERE treatmt_start_dt >= DATE '2025-11-01'
    AND tpa_ita = 'TPA'
    AND asc_on_app_source = 'Period-ASC'
) WITH DATA
PRIMARY INDEX (clnt_no, acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_mb_approved: approved Period-ASC accounts
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_approved AS (
  SELECT
    acct_no,
    clnt_no,
    treatmt_start_dt,
    treatmt_end_dt,
    strtgy_seg_typ,
    model_score_decile,
    offer_prod_latest,
    offer_prod_latest_name,
    product_applied_name,
    response_channel_grp
  FROM pcq_q1_mb_cohort
  WHERE app_approved = 1
    AND acct_no IS NOT NULL
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_mb_events: stage relevant portfolio events ONCE.
-- The only scan of DLY_FULL_PORTFOLIO. Period-ASC filter on
-- approved already cut the joined account pool, so the staged
-- volatile is much smaller than the full table.
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_events AS (
  SELECT
    p.acct_no,
    p.dt_record_ext,
    p.me_dt,
    p.net_prch_amt_dly,
    p.bal_current,
    p.cd_curr_pst_due
  FROM D3CV12A.DLY_FULL_PORTFOLIO p
  INNER JOIN pcq_q1_mb_approved a
    ON p.acct_no = a.acct_no
  WHERE p.dt_record_ext >= a.treatmt_start_dt
) WITH DATA
PRIMARY INDEX (acct_no, dt_record_ext)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_mb_acct_month: per-(account, me_dt) rollup from staged events.
-- Single pass with ROW_NUMBER for last-of-month balance, no
-- DLY_FULL_PORTFOLIO references.
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_acct_month AS (
  SELECT
    acct_no,
    me_dt,
    SUM(net_prch_amt_dly)                                       AS sum_purchases_in_month,
    SUM(bal_current)                                            AS sum_bal_current_in_month,
    COUNT(*)                                                    AS event_days_in_month,
    MAX(CASE WHEN rn = 1 THEN bal_current END)                  AS last_bal_current,
    MAX(CASE WHEN rn = 1 THEN cd_curr_pst_due END)              AS last_pst_due_cd,
    MAX(dt_record_ext)                                          AS last_event_dt_in_month
  FROM (
    SELECT
      e.acct_no,
      e.me_dt,
      e.dt_record_ext,
      e.net_prch_amt_dly,
      e.bal_current,
      e.cd_curr_pst_due,
      ROW_NUMBER() OVER (PARTITION BY e.acct_no, e.me_dt
                         ORDER BY e.dt_record_ext DESC)         AS rn
    FROM pcq_q1_mb_events e
  ) ranked
  GROUP BY acct_no, me_dt
) WITH DATA
PRIMARY INDEX (acct_no, me_dt)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- OUTPUT: cohort x strategy dims x me_dt
-- Sums + counts only; Andre divides in Excel pivot.
-- ============================================================
SELECT
  a.treatmt_start_dt,
  a.treatmt_end_dt,
  a.strtgy_seg_typ,
  a.offer_prod_latest,
  a.offer_prod_latest_name,
  a.product_applied_name,
  a.model_score_decile,
  a.response_channel_grp,
  am.me_dt,
  ((EXTRACT(YEAR FROM am.me_dt) - EXTRACT(YEAR FROM a.treatmt_start_dt)) * 12
   + (EXTRACT(MONTH FROM am.me_dt) - EXTRACT(MONTH FROM a.treatmt_start_dt))) AS months_since_treatment,
  COUNT(DISTINCT am.acct_no)            AS active_accts,
  SUM(am.sum_bal_current_in_month)      AS sum_bal_current_daily,
  SUM(am.last_bal_current)              AS sum_last_bal_current,

  -- Past-due balance buckets (per-month, last record in month). Sum of 13 buckets = sum_last_bal_current.
  -- Use sum_last_bal_current as denominator for "% of balance on past-due accounts" ratios.
  SUM(CASE WHEN am.last_pst_due_cd IS NULL THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_current,
  SUM(CASE WHEN am.last_pst_due_cd = '01'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d1_30,
  SUM(CASE WHEN am.last_pst_due_cd = '02'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d31_60,
  SUM(CASE WHEN am.last_pst_due_cd = '03'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d61_90,
  SUM(CASE WHEN am.last_pst_due_cd = '04'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d91_120,
  SUM(CASE WHEN am.last_pst_due_cd = '05'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d121_150,
  SUM(CASE WHEN am.last_pst_due_cd = '06'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d151_180,
  SUM(CASE WHEN am.last_pst_due_cd = '07'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d181_210,
  SUM(CASE WHEN am.last_pst_due_cd = '08'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d211_240,
  SUM(CASE WHEN am.last_pst_due_cd = '09'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d241_270,
  SUM(CASE WHEN am.last_pst_due_cd = '1A'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d271_300,
  SUM(CASE WHEN am.last_pst_due_cd = '1B'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d301_330,
  SUM(CASE WHEN am.last_pst_due_cd = '1C'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d331_plus,

  SUM(am.sum_purchases_in_month)        AS sum_purchases_in_month,
  SUM(am.event_days_in_month)           AS total_event_days
FROM pcq_q1_mb_acct_month am
INNER JOIN pcq_q1_mb_approved a
  ON am.acct_no = a.acct_no
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1, 9;
