-- ============================================================================
-- Q05 — CRV installment economics by COHORT x PRODUCT x MONTH  (single output)
--   Grain = crv_cohort x product_name_at_decision x slice.
--     product = 'ALL' (all products) or each product.
--     slice   = 'overall' (all months) or each CRV deployment month.
--   So every cohort carries: an ALL/overall row, ALL-per-month rows, product/overall rows,
--   and product-per-month rows. Converters only (INNER JOIN install_details).
--
--   4 cohorts: action/control x with/without PCL overlap.
--     CRV-Action: channels_deployed LIKE '%IM%'. CRV-Control: no channel filter (not deployed).
--   Product column = product_name_at_decision on cards_crv_install_decis_resp.
--   ALL/overall n_accounts = distinct accounts at that grain (an account can span products/months,
--   so it is NOT the sum of the finer rows).
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_decisions_in_window AS (
    SELECT
        acct_no, tactic_id, offer_start_date, offer_end_date, action_control, product_name_at_decision
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND ( (action_control = 'Action' AND channels_deployed LIKE '%IM%')
         OR  action_control = 'Control' )
),
crv_overlap_keys AS (
    SELECT c.acct_no, c.tactic_id, c.offer_start_date
    FROM crv_decisions_in_window c
    WHERE EXISTS (
        SELECT 1 FROM pcl_universe p
        WHERE p.acct_no          = c.acct_no
          AND c.offer_start_date <= p.treatmt_end_dt
          AND c.offer_end_date   >= p.treatmt_strt_dt )
),
crv_decisions_classified AS (
    SELECT
        c.acct_no,
        c.tactic_id,
        c.product_name_at_decision,
        CAST(c.offer_start_date - (EXTRACT(DAY FROM c.offer_start_date) - 1) AS VARCHAR(20)) AS crv_month,
        CASE
            WHEN c.action_control = 'Action'  AND ov.acct_no IS NOT NULL THEN 'action_with_pcl_overlap'
            WHEN c.action_control = 'Action'                              THEN 'action_no_pcl_overlap'
            WHEN c.action_control = 'Control' AND ov.acct_no IS NOT NULL THEN 'control_with_pcl_overlap'
            ELSE 'control_no_pcl_overlap'
        END AS crv_cohort
    FROM crv_decisions_in_window c
    LEFT JOIN crv_overlap_keys ov
      ON ov.acct_no = c.acct_no AND ov.tactic_id = c.tactic_id AND ov.offer_start_date = c.offer_start_date
),
crv_keys AS (
    SELECT DISTINCT crv_cohort, product_name_at_decision, crv_month, acct_no, tactic_id
    FROM crv_decisions_classified
),
details AS (
    SELECT
        w.crv_cohort, w.product_name_at_decision, w.crv_month, w.acct_no,
        d.instl_txn_ref_no, d.instl_apr, d.instl_txn_trm, d.instl_txn_prncpl_amt
    FROM crv_keys w
    INNER JOIN dl_mr_prod.cards_crv_install_details d
      ON d.acct_no = w.acct_no AND d.tactic_id = w.tactic_id
),

-- account aggregations at the four output grains
a_cpm AS (SELECT crv_cohort, product_name_at_decision, crv_month, acct_no,
              COUNT(DISTINCT instl_txn_ref_no) AS pl, SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS pr
          FROM details GROUP BY crv_cohort, product_name_at_decision, crv_month, acct_no),
a_cpo AS (SELECT crv_cohort, product_name_at_decision, acct_no,
              COUNT(DISTINCT instl_txn_ref_no) AS pl, SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS pr
          FROM details GROUP BY crv_cohort, product_name_at_decision, acct_no),
a_cm  AS (SELECT crv_cohort, crv_month, acct_no,
              COUNT(DISTINCT instl_txn_ref_no) AS pl, SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS pr
          FROM details GROUP BY crv_cohort, crv_month, acct_no),
a_co  AS (SELECT crv_cohort, acct_no,
              COUNT(DISTINCT instl_txn_ref_no) AS pl, SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS pr
          FROM details GROUP BY crv_cohort, acct_no),

-- wave counts at the four grains
w_cpm AS (SELECT crv_cohort, product_name_at_decision, crv_month, COUNT(DISTINCT tactic_id) AS n_waves
          FROM crv_keys GROUP BY crv_cohort, product_name_at_decision, crv_month),
w_cpo AS (SELECT crv_cohort, product_name_at_decision, COUNT(DISTINCT tactic_id) AS n_waves
          FROM crv_keys GROUP BY crv_cohort, product_name_at_decision),
w_cm  AS (SELECT crv_cohort, crv_month, COUNT(DISTINCT tactic_id) AS n_waves
          FROM crv_keys GROUP BY crv_cohort, crv_month),
w_co  AS (SELECT crv_cohort, COUNT(DISTINCT tactic_id) AS n_waves
          FROM crv_keys GROUP BY crv_cohort),

-- rollups
r_cpm AS (SELECT a.crv_cohort, a.product_name_at_decision, a.crv_month, w.n_waves,
              COUNT(*) AS n_accounts, SUM(a.pl) AS n_transactions, AVG(a.pr) AS mean_principal_per_acct
          FROM a_cpm a LEFT JOIN w_cpm w
            ON w.crv_cohort=a.crv_cohort AND w.product_name_at_decision=a.product_name_at_decision AND w.crv_month=a.crv_month
          GROUP BY a.crv_cohort, a.product_name_at_decision, a.crv_month, w.n_waves),
r_cpo AS (SELECT a.crv_cohort, a.product_name_at_decision, w.n_waves,
              COUNT(*) AS n_accounts, SUM(a.pl) AS n_transactions, AVG(a.pr) AS mean_principal_per_acct
          FROM a_cpo a LEFT JOIN w_cpo w
            ON w.crv_cohort=a.crv_cohort AND w.product_name_at_decision=a.product_name_at_decision
          GROUP BY a.crv_cohort, a.product_name_at_decision, w.n_waves),
r_cm  AS (SELECT a.crv_cohort, a.crv_month, w.n_waves,
              COUNT(*) AS n_accounts, SUM(a.pl) AS n_transactions, AVG(a.pr) AS mean_principal_per_acct
          FROM a_cm a LEFT JOIN w_cm w ON w.crv_cohort=a.crv_cohort AND w.crv_month=a.crv_month
          GROUP BY a.crv_cohort, a.crv_month, w.n_waves),
r_co  AS (SELECT a.crv_cohort, w.n_waves,
              COUNT(*) AS n_accounts, SUM(a.pl) AS n_transactions, AVG(a.pr) AS mean_principal_per_acct
          FROM a_co a CROSS JOIN w_co w   -- w_co is one row per cohort; cross then group keeps it 1:1
          WHERE w.crv_cohort = a.crv_cohort
          GROUP BY a.crv_cohort, w.n_waves)

-- B1: cohort x ALL x overall
SELECT r.crv_cohort, CAST('ALL' AS VARCHAR(60)) AS product_name_at_decision, CAST('overall' AS VARCHAR(20)) AS slice,
    r.n_waves, r.n_accounts, r.n_transactions,
    CAST(r.n_transactions AS FLOAT)/NULLIF(r.n_accounts,0) AS txns_per_acct, r.mean_principal_per_acct,
    AVG(CAST(d.instl_apr AS FLOAT)) AS mean_apr,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)) AS p50_apr,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)) AS p90_apr,
    AVG(CAST(d.instl_txn_trm AS FLOAT)) AS mean_term,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)) AS p50_term,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)) AS p90_term,
    AVG(CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS mean_txn_principal,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p50_txn_principal,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p90_txn_principal
FROM r_co r INNER JOIN details d ON d.crv_cohort=r.crv_cohort
GROUP BY r.crv_cohort, r.n_waves, r.n_accounts, r.n_transactions, r.mean_principal_per_acct

UNION ALL
-- B2: cohort x ALL x month
SELECT r.crv_cohort, CAST('ALL' AS VARCHAR(60)), CAST(r.crv_month AS VARCHAR(20)),
    r.n_waves, r.n_accounts, r.n_transactions,
    CAST(r.n_transactions AS FLOAT)/NULLIF(r.n_accounts,0), r.mean_principal_per_acct,
    AVG(CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    AVG(CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    AVG(CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT))
FROM r_cm r INNER JOIN details d ON d.crv_cohort=r.crv_cohort AND d.crv_month=r.crv_month
GROUP BY r.crv_cohort, r.crv_month, r.n_waves, r.n_accounts, r.n_transactions, r.mean_principal_per_acct

UNION ALL
-- B3: cohort x product x overall
SELECT r.crv_cohort, CAST(r.product_name_at_decision AS VARCHAR(60)), CAST('overall' AS VARCHAR(20)),
    r.n_waves, r.n_accounts, r.n_transactions,
    CAST(r.n_transactions AS FLOAT)/NULLIF(r.n_accounts,0), r.mean_principal_per_acct,
    AVG(CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    AVG(CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    AVG(CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT))
FROM r_cpo r INNER JOIN details d ON d.crv_cohort=r.crv_cohort AND d.product_name_at_decision=r.product_name_at_decision
GROUP BY r.crv_cohort, r.product_name_at_decision, r.n_waves, r.n_accounts, r.n_transactions, r.mean_principal_per_acct

UNION ALL
-- B4: cohort x product x month
SELECT r.crv_cohort, CAST(r.product_name_at_decision AS VARCHAR(60)), CAST(r.crv_month AS VARCHAR(20)),
    r.n_waves, r.n_accounts, r.n_transactions,
    CAST(r.n_transactions AS FLOAT)/NULLIF(r.n_accounts,0), r.mean_principal_per_acct,
    AVG(CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    AVG(CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    AVG(CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT))
FROM r_cpm r INNER JOIN details d
  ON d.crv_cohort=r.crv_cohort AND d.product_name_at_decision=r.product_name_at_decision AND d.crv_month=r.crv_month
GROUP BY r.crv_cohort, r.product_name_at_decision, r.crv_month, r.n_waves, r.n_accounts, r.n_transactions, r.mean_principal_per_acct

ORDER BY 1, 2, 3
;
