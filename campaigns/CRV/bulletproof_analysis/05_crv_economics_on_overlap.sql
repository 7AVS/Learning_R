-- CRV installment economics on the overlap cohort (CRV-Action leads that overlap a PCL-mobile deployment).
-- Join to cards_crv_install_details via (acct_no, tactic_id). Counts and percentile distributions only.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
),
crv_action_overlap AS (
    SELECT DISTINCT
        c.acct_no,
        c.tactic_id,
        c.offer_start_date,
        c.offer_start_date - (EXTRACT(DAY FROM c.offer_start_date) - 1) AS crv_month
    FROM dl_mr_prod.cards_crv_install_decis_resp c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    WHERE c.offer_start_date >= DATE '2024-10-01'
      AND c.channels_deployed LIKE '%IM%'
      AND c.action_control = 'Action'
),
details AS (
    SELECT
        o.crv_month,
        d.instl_txn_actvat_chnl,
        d.instl_apr,
        d.instl_txn_trm,
        d.instl_txn_prncp_amt,
        d.instl_txn_fee_amt,
        d.instl_fee_pct
    FROM crv_action_overlap o
    INNER JOIN dl_mr_prod.cards_crv_install_details d
      ON d.acct_no   = o.acct_no
     AND d.tactic_id = o.tactic_id
)
-- Overall distribution
SELECT
    'overall'                                                              AS crv_month,
    'ALL'                                                                  AS instl_txn_actvat_chnl,
    COUNT(*)                                                               AS n_transactions,
    AVG(CAST(instl_apr          AS DECIMAL(18,6)))                         AS mean_apr,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_apr)                AS p10_apr,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_apr)                AS p25_apr,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_apr)                AS median_apr,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_apr)                AS p75_apr,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_apr)                AS p90_apr,
    AVG(CAST(instl_txn_trm      AS DECIMAL(18,6)))                         AS mean_term,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_trm)            AS p10_term,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_trm)            AS median_term,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_trm)            AS p90_term,
    AVG(CAST(instl_txn_prncp_amt AS DECIMAL(18,6)))                        AS mean_principal,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p10_principal,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p25_principal,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS median_principal,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p75_principal,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p90_principal,
    AVG(CAST(instl_txn_fee_amt  AS DECIMAL(18,6)))                         AS mean_fee_amt,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_fee_amt)        AS median_fee_amt,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_fee_amt)        AS p90_fee_amt,
    AVG(CAST(instl_fee_pct      AS DECIMAL(18,6)))                         AS mean_fee_pct,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_fee_pct)            AS median_fee_pct,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_fee_pct)            AS p90_fee_pct
FROM details

UNION ALL

-- Per activation channel — overall
SELECT
    'overall'                                                              AS crv_month,
    instl_txn_actvat_chnl,
    COUNT(*)                                                               AS n_transactions,
    AVG(CAST(instl_apr          AS DECIMAL(18,6)))                         AS mean_apr,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_apr)                AS p10_apr,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_apr)                AS p25_apr,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_apr)                AS median_apr,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_apr)                AS p75_apr,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_apr)                AS p90_apr,
    AVG(CAST(instl_txn_trm      AS DECIMAL(18,6)))                         AS mean_term,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_trm)            AS p10_term,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_trm)            AS median_term,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_trm)            AS p90_term,
    AVG(CAST(instl_txn_prncp_amt AS DECIMAL(18,6)))                        AS mean_principal,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p10_principal,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p25_principal,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS median_principal,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p75_principal,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p90_principal,
    AVG(CAST(instl_txn_fee_amt  AS DECIMAL(18,6)))                         AS mean_fee_amt,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_fee_amt)        AS median_fee_amt,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_fee_amt)        AS p90_fee_amt,
    AVG(CAST(instl_fee_pct      AS DECIMAL(18,6)))                         AS mean_fee_pct,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_fee_pct)            AS median_fee_pct,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_fee_pct)            AS p90_fee_pct
FROM details
GROUP BY instl_txn_actvat_chnl

UNION ALL

-- Per CRV deployment month — all channels combined
SELECT
    CAST(crv_month AS VARCHAR(20))                                         AS crv_month,
    'ALL'                                                                  AS instl_txn_actvat_chnl,
    COUNT(*)                                                               AS n_transactions,
    AVG(CAST(instl_apr          AS DECIMAL(18,6)))                         AS mean_apr,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_apr)                AS p10_apr,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_apr)                AS p25_apr,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_apr)                AS median_apr,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_apr)                AS p75_apr,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_apr)                AS p90_apr,
    AVG(CAST(instl_txn_trm      AS DECIMAL(18,6)))                         AS mean_term,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_trm)            AS p10_term,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_trm)            AS median_term,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_trm)            AS p90_term,
    AVG(CAST(instl_txn_prncp_amt AS DECIMAL(18,6)))                        AS mean_principal,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p10_principal,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p25_principal,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS median_principal,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p75_principal,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_prncp_amt)      AS p90_principal,
    AVG(CAST(instl_txn_fee_amt  AS DECIMAL(18,6)))                         AS mean_fee_amt,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_fee_amt)        AS median_fee_amt,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_fee_amt)        AS p90_fee_amt,
    AVG(CAST(instl_fee_pct      AS DECIMAL(18,6)))                         AS mean_fee_pct,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_fee_pct)            AS median_fee_pct,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_fee_pct)            AS p90_fee_pct
FROM details
GROUP BY crv_month

ORDER BY 1, 2
;
