-- ============================================================================
-- Q05 — CRV installment economics by COHORT x PRODUCT  (single output)
--   Grain = crv_cohort x product_name_at_decision, with an 'ALL' (all products) row
--   per cohort. Overall window (no month split). Converters only (INNER JOIN details).
--
--   4 cohorts: action/control x with/without PCL overlap.
--     CRV-Action: channels_deployed LIKE '%IM%'. CRV-Control: no channel filter (not deployed).
--   Product column = product_name_at_decision on cards_crv_install_decis_resp.
--   Economics from cards_crv_install_details (acct_no + tactic_id; INNER JOIN = converters only).
--
--   Two jobs in one grid: (1) the ALL rows are the Action-vs-Control balance check (compare
--   action_with vs control_with); (2) the per-product rows show the product mix + whether
--   products differ on economics / transactional behaviour.
--   NOTE: n_transactions / txns_per_acct carry a ~1.74x tactic-fanout inflation from the legacy
--         join idiom — treat as indicative. n_accounts, principal, APR, term are clean.
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_decisions_in_window AS (
    SELECT
        acct_no,
        tactic_id,
        offer_start_date,
        offer_end_date,
        action_control,
        product_name_at_decision
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND (
            (action_control = 'Action'  AND channels_deployed LIKE '%IM%')
         OR  action_control = 'Control'
          )
),
crv_overlap_keys AS (
    SELECT c.acct_no, c.tactic_id, c.offer_start_date
    FROM crv_decisions_in_window c
    WHERE EXISTS (
        SELECT 1 FROM pcl_universe p
        WHERE p.acct_no          = c.acct_no
          AND c.offer_start_date <= p.treatmt_end_dt
          AND c.offer_end_date   >= p.treatmt_strt_dt
    )
),
crv_decisions_classified AS (
    SELECT
        c.acct_no,
        c.tactic_id,
        c.product_name_at_decision,
        CASE
            WHEN c.action_control = 'Action'  AND ov.acct_no IS NOT NULL THEN 'action_with_pcl_overlap'
            WHEN c.action_control = 'Action'                              THEN 'action_no_pcl_overlap'
            WHEN c.action_control = 'Control' AND ov.acct_no IS NOT NULL THEN 'control_with_pcl_overlap'
            ELSE 'control_no_pcl_overlap'
        END AS crv_cohort
    FROM crv_decisions_in_window c
    LEFT JOIN crv_overlap_keys ov
      ON ov.acct_no          = c.acct_no
     AND ov.tactic_id        = c.tactic_id
     AND ov.offer_start_date = c.offer_start_date
),
crv_keys AS (
    SELECT DISTINCT crv_cohort, product_name_at_decision, acct_no, tactic_id
    FROM crv_decisions_classified
),
details AS (
    SELECT
        w.crv_cohort,
        w.product_name_at_decision,
        w.acct_no,
        d.instl_txn_ref_no,
        d.instl_apr,
        d.instl_txn_trm,
        d.instl_txn_prncpl_amt
    FROM crv_keys w
    INNER JOIN dl_mr_prod.cards_crv_install_details d
      ON d.acct_no   = w.acct_no
     AND d.tactic_id = w.tactic_id
),
-- per-account aggregation: split by cohort x product, and pooled to cohort (for the ALL row)
acct_byprod AS (
    SELECT crv_cohort, product_name_at_decision, acct_no,
        COUNT(DISTINCT instl_txn_ref_no)         AS acct_plans,
        SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS acct_principal
    FROM details
    GROUP BY crv_cohort, product_name_at_decision, acct_no
),
acct_cohort AS (
    SELECT crv_cohort, acct_no,
        COUNT(DISTINCT instl_txn_ref_no)         AS acct_plans,
        SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS acct_principal
    FROM details
    GROUP BY crv_cohort, acct_no
),
waves_byprod AS (
    SELECT crv_cohort, product_name_at_decision, COUNT(DISTINCT tactic_id) AS n_waves
    FROM crv_keys GROUP BY crv_cohort, product_name_at_decision
),
waves_cohort AS (
    SELECT crv_cohort, COUNT(DISTINCT tactic_id) AS n_waves
    FROM crv_keys GROUP BY crv_cohort
),
roll_byprod AS (
    SELECT ag.crv_cohort, ag.product_name_at_decision, wv.n_waves,
        COUNT(*)               AS n_accounts,
        SUM(ag.acct_plans)     AS n_transactions,
        AVG(ag.acct_principal) AS mean_principal_per_acct
    FROM acct_byprod ag
    LEFT JOIN waves_byprod wv
      ON wv.crv_cohort = ag.crv_cohort AND wv.product_name_at_decision = ag.product_name_at_decision
    GROUP BY ag.crv_cohort, ag.product_name_at_decision, wv.n_waves
),
roll_cohort AS (
    SELECT ag.crv_cohort, wv.n_waves,
        COUNT(*)               AS n_accounts,
        SUM(ag.acct_plans)     AS n_transactions,
        AVG(ag.acct_principal) AS mean_principal_per_acct
    FROM acct_cohort ag
    LEFT JOIN waves_cohort wv ON wv.crv_cohort = ag.crv_cohort
    GROUP BY ag.crv_cohort, wv.n_waves
)
-- cohort ALL-products rows
SELECT
    r.crv_cohort,
    CAST('ALL' AS VARCHAR(60))                                  AS product_name_at_decision,
    r.n_waves,
    r.n_accounts,
    r.n_transactions,
    CAST(r.n_transactions AS FLOAT) / NULLIF(r.n_accounts, 0)   AS txns_per_acct,
    r.mean_principal_per_acct,
    AVG(CAST(d.instl_apr AS FLOAT))                             AS mean_apr,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT))   AS p50_apr,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT))   AS p90_apr,
    AVG(CAST(d.instl_txn_trm AS FLOAT))                         AS mean_term,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)) AS p50_term,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)) AS p90_term,
    AVG(CAST(d.instl_txn_prncpl_amt AS FLOAT))                  AS mean_txn_principal,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p50_txn_principal,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p90_txn_principal
FROM roll_cohort r
INNER JOIN details d
  ON d.crv_cohort = r.crv_cohort
GROUP BY r.crv_cohort, r.n_waves, r.n_accounts, r.n_transactions, r.mean_principal_per_acct

UNION ALL

-- cohort x product rows
SELECT
    ar.crv_cohort,
    CAST(ar.product_name_at_decision AS VARCHAR(60)),
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    CAST(ar.n_transactions AS FLOAT) / NULLIF(ar.n_accounts, 0),
    ar.mean_principal_per_acct,
    AVG(CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    AVG(CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    AVG(CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT))
FROM roll_byprod ar
INNER JOIN details d
  ON d.crv_cohort = ar.crv_cohort
 AND d.product_name_at_decision = ar.product_name_at_decision
GROUP BY ar.crv_cohort, ar.product_name_at_decision, ar.n_waves, ar.n_accounts, ar.n_transactions, ar.mean_principal_per_acct

ORDER BY 1, 2
;
