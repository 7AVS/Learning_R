-- PCQ Q1 26 quarterly NBC review: all strategies, single cohort summary
-- Cohort scope: treatmt_strt_dt >= 2025-11-01, no test_group filter
-- One output: counts + sums per cohort row -- averages computed downstream

DATABASE DL_MR_PROD;

-- ============================================================
-- pcq_q1_cohort: all PCQ deployments since Nov 2025
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_cohort AS (
  SELECT
    clnt_no,
    acct_no,
    test_group_latest,
    acq_strategy_code,
    offer_prod_latest,
    offer_prod_latest_name,
    product_applied,
    asc_on_app_source,
    asc_on_app,
    treatmt_strt_dt,
    treatmt_end_dt,
    response_dt,
    app_approved
  FROM cards_tpa_pcq_decision_resp
  WHERE treatmt_strt_dt >= DATE '2025-11-01'
) WITH DATA
PRIMARY INDEX (clnt_no, acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_approved: drives portfolio join (acct_no level)
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_approved AS (
  SELECT
    acct_no,
    clnt_no,
    treatmt_strt_dt,
    test_group_latest,
    acq_strategy_code,
    offer_prod_latest,
    asc_on_app_source
  FROM pcq_q1_cohort
  WHERE app_approved = 1
    AND acct_no IS NOT NULL
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- pcq_q1_acct_summary: per-account portfolio rollup since treatmt_strt_dt
--   sum_purchases     = SUM(net_prch_amt_dly)            [daily flow]
--   last_bal_current  = bal_current at last event        [snapshot]
--   last_mtd_avg_bal  = accum_dly_bal_mtd at last event  [MTD avg]
--   last_event_dt     = MAX(dt_record_ext)
--   days_observed     = last_event_dt - treatmt_strt_dt
-- ============================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_acct_summary AS (
  SELECT
    s.acct_no,
    s.sum_purchases,
    s.event_days,
    s.last_event_dt,
    (s.last_event_dt - a.treatmt_strt_dt) AS days_observed,
    l.last_bal_current,
    l.last_mtd_avg_bal,
    l.last_status,
    l.acct_open_dt,
    l.acct_cls_dt,
    l.visa_prod_cd
  FROM (
    SELECT
      p.acct_no,
      SUM(p.net_prch_amt_dly) AS sum_purchases,
      COUNT(*)                AS event_days,
      MAX(p.dt_record_ext)    AS last_event_dt
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN pcq_q1_approved a
      ON p.acct_no = a.acct_no
    WHERE p.dt_record_ext >= a.treatmt_strt_dt
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
    INNER JOIN pcq_q1_approved a
      ON p.acct_no = a.acct_no
    WHERE p.dt_record_ext >= a.treatmt_strt_dt
    QUALIFY ROW_NUMBER()
              OVER (PARTITION BY p.acct_no
                    ORDER BY p.dt_record_ext DESC) = 1
  ) l
    ON s.acct_no = l.acct_no
  INNER JOIN pcq_q1_approved a
    ON s.acct_no = a.acct_no
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================
-- OUTPUT: single cohort summary
-- Grain: treatmt_strt_dt x test_group_latest x acq_strategy_code
--        x offer_prod_latest x asc_on_app_source
-- Andre computes downstream:
--   approval_rate           = approved / deployed
--   avg_last_bal_per_acct   = sum_last_bal_current / approved_with_portfolio_data
--   avg_total_purch_per_acct = sum_purchases       / approved_with_portfolio_data
--   monthly_purch_per_acct  = sum_purchases / sum_days_observed * 30
-- ============================================================
SELECT
  c.treatmt_strt_dt,
  c.test_group_latest,
  c.acq_strategy_code,
  c.offer_prod_latest,
  c.offer_prod_latest_name,
  c.asc_on_app_source,
  COUNT(*)                                                   AS deployed,
  SUM(CASE WHEN c.response_dt IS NOT NULL THEN 1 ELSE 0 END) AS responded,
  SUM(CASE WHEN c.app_approved = 1        THEN 1 ELSE 0 END) AS approved,
  COUNT(s.acct_no)                                           AS approved_with_portfolio_data,
  SUM(s.sum_purchases)                                       AS sum_purchases,
  SUM(s.last_bal_current)                                    AS sum_last_bal_current,
  SUM(s.last_mtd_avg_bal)                                    AS sum_last_mtd_avg_bal,
  SUM(s.days_observed)                                       AS sum_days_observed,
  MAX(s.last_event_dt)                                       AS max_event_dt_in_cohort,
  SUM(CASE WHEN s.acct_cls_dt IS NOT NULL THEN 1 ELSE 0 END) AS closed_accts
FROM pcq_q1_cohort c
LEFT JOIN pcq_q1_acct_summary s
  ON c.acct_no = s.acct_no
 AND c.app_approved = 1
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6;
