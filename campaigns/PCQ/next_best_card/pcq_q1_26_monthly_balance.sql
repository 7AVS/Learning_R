-- PCQ Q1 26 -- Calendar-month vintage table for Period-ASC active accounts
-- Each row = cohort x strategy dims x me_dt (calendar month)
-- Use case: pivot Strategy x Month with avg balance per active account in the cell.
-- Period-ASC filter baked into the cohort step (vintage tracking is for attributable conversions).
-- Volatile-table names use mb_ prefix to coexist with other files in the same session.

DATABASE DL_MR_PROD;


-- ============================================================
-- Drop volatiles from any prior run (first run errors are harmless)
-- ============================================================
DROP TABLE pcq_q1_mb_acct_month;
DROP TABLE pcq_q1_mb_approved;
DROP TABLE pcq_q1_mb_cohort;


-- ============================================================
-- pcq_q1_mb_cohort: PCQ TPA deployments since Nov 2025
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_cohort AS (
  SELECT
    clnt_no,
    acct_no,
    test_group_latest,
    strtgy_seg_typ,
    model_score_decile,
    offer_prod_latest,
    offer_prod_latest_name,
    product_applied_name,
    asc_on_app_source,
    treatmt_start_dt,
    response_channel_grp,
    app_approved
  FROM cards_tpa_pcq_decision_resp
  WHERE treatmt_start_dt >= DATE '2025-11-01'
    AND tpa_ita = 'TPA'
) WITH DATA
PRIMARY INDEX (clnt_no, acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_mb_approved: Period-ASC approved accounts only
-- (vintage curves only make sense for attributable conversions)
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_approved AS (
  SELECT
    acct_no,
    clnt_no,
    treatmt_start_dt,
    test_group_latest,
    strtgy_seg_typ,
    model_score_decile,
    offer_prod_latest,
    offer_prod_latest_name,
    product_applied_name,
    response_channel_grp
  FROM pcq_q1_mb_cohort
  WHERE app_approved = 1
    AND acct_no IS NOT NULL
    AND asc_on_app_source = 'Period-ASC'
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_mb_acct_month: per-(account, me_dt) portfolio snapshot
--   last_mtd_avg_bal       = accum_dly_bal_mtd at last event of the month
--   last_bal_current       = bal_current at last event of the month
--   sum_purchases_in_month = SUM(net_prch_amt_dly) inside the month
-- Only events with dt_record_ext >= treatmt_start_dt are included.
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_acct_month AS (
  SELECT
    s.acct_no,
    s.me_dt,
    s.sum_purchases_in_month,
    s.event_days_in_month,
    s.last_event_dt_in_month,
    l.last_mtd_avg_bal,
    l.last_bal_current
  FROM (
    SELECT
      p.acct_no,
      p.me_dt,
      SUM(p.net_prch_amt_dly) AS sum_purchases_in_month,
      COUNT(*)                AS event_days_in_month,
      MAX(p.dt_record_ext)    AS last_event_dt_in_month
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN pcq_q1_mb_approved a
      ON p.acct_no = a.acct_no
    WHERE p.dt_record_ext >= a.treatmt_start_dt
    GROUP BY p.acct_no, p.me_dt
  ) s
  INNER JOIN (
    SELECT
      p.acct_no,
      p.me_dt,
      p.accum_dly_bal_mtd AS last_mtd_avg_bal,
      p.bal_current       AS last_bal_current
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN pcq_q1_mb_approved a
      ON p.acct_no = a.acct_no
    WHERE p.dt_record_ext >= a.treatmt_start_dt
    QUALIFY ROW_NUMBER()
              OVER (PARTITION BY p.acct_no, p.me_dt
                    ORDER BY p.dt_record_ext DESC) = 1
  ) l
    ON s.acct_no = l.acct_no
   AND s.me_dt   = l.me_dt
) WITH DATA
PRIMARY INDEX (acct_no, me_dt)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- OUTPUT: cohort x strategy dims x me_dt
-- Sums + counts only; Andre divides in Excel pivot.
--
-- Excel pivot recipe for Strategy x Month avg balance:
--   Rows    : test_group_latest (or strtgy_seg_typ / offer_prod_latest)
--   Columns : me_dt
--   Values  : Calculated field = SUM(sum_mtd_avg_bal) / SUM(active_accts)
--
-- months_since_treatment included so vintage analysis aligned on
-- cohort age (months-since-acquisition) is also a one-pivot away.
-- ============================================================
SELECT
  a.treatmt_start_dt,
  a.test_group_latest,
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
  SUM(am.last_mtd_avg_bal)              AS sum_mtd_avg_bal,
  SUM(am.last_bal_current)              AS sum_last_bal_current,
  SUM(am.sum_purchases_in_month)        AS sum_purchases_in_month,
  SUM(am.event_days_in_month)           AS total_event_days
FROM pcq_q1_mb_acct_month am
INNER JOIN pcq_q1_mb_approved a
  ON am.acct_no = a.acct_no
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1, 9;
