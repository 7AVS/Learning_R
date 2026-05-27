-- CRV installment economics on the overlap cohort (CRV-Action leads that overlap a PCL-mobile deployment).
-- Join to cards_crv_install_details via (acct_no, tactic_id). Counts and percentile distributions only.
-- Second SELECT: overlap days distribution among CRV-Action responders (parallel to Q02's all-leads view).

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
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
    CAST('overall' AS VARCHAR(20))                                         AS crv_month,
    CAST('ALL'     AS VARCHAR(20))                                         AS instl_txn_actvat_chnl,
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
    CAST('overall' AS VARCHAR(20))                                         AS crv_month,
    CAST(instl_txn_actvat_chnl AS VARCHAR(20))                             AS instl_txn_actvat_chnl,
    COUNT(*),
    AVG(CAST(instl_apr          AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_apr),
    AVG(CAST(instl_txn_trm      AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_trm),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_trm),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_trm),
    AVG(CAST(instl_txn_prncp_amt AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    AVG(CAST(instl_txn_fee_amt  AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_fee_amt),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_fee_amt),
    AVG(CAST(instl_fee_pct      AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_fee_pct),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_fee_pct)
FROM details
GROUP BY instl_txn_actvat_chnl

UNION ALL

-- Per CRV deployment month — all channels combined
SELECT
    CAST(crv_month AS VARCHAR(20)),
    CAST('ALL'     AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(instl_apr          AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_apr),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_apr),
    AVG(CAST(instl_txn_trm      AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_trm),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_trm),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_trm),
    AVG(CAST(instl_txn_prncp_amt AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_prncp_amt),
    AVG(CAST(instl_txn_fee_amt  AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_txn_fee_amt),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_txn_fee_amt),
    AVG(CAST(instl_fee_pct      AS DECIMAL(18,6))),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY instl_fee_pct),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY instl_fee_pct)
FROM details
GROUP BY crv_month

ORDER BY 1, 2
;


-- ============================================================
-- Second SELECT: overlap-days distribution among CRV responders
-- ============================================================

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action_resp AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month,
        responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
overlap_days_action AS (
    SELECT
        c.acct_no,
        c.crv_strt_dt,
        c.crv_month,
        c.responder,
        MAX(
            LEAST(c.crv_end_dt, p.pcl_end_dt)
            - GREATEST(c.crv_strt_dt, p.pcl_strt_dt)
            + 1
        ) AS overlap_days
    FROM crv_action_resp c
    INNER JOIN pcl_universe p
      ON p.acct_no       = c.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    GROUP BY c.acct_no, c.crv_strt_dt, c.crv_month, c.responder
)
SELECT
    CAST('crv_responders' AS VARCHAR(20))                              AS subset,
    CAST('overall'        AS VARCHAR(20))                              AS crv_month,
    COUNT(*)                                                           AS n,
    AVG(CAST(overlap_days AS DECIMAL(12,4)))                           AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days)         AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days)         AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days)         AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days)         AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days)         AS p90,
    MIN(overlap_days)                                                  AS min_days,
    MAX(overlap_days)                                                  AS max_days
FROM overlap_days_action
WHERE responder = 1

UNION ALL

SELECT
    CAST('crv_responders' AS VARCHAR(20)),
    CAST(crv_month        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_days_action
WHERE responder = 1
GROUP BY crv_month

ORDER BY 1, 2
;
