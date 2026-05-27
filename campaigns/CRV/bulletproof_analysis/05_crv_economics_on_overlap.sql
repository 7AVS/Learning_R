-- CRV installment economics split by PCL-mobile overlap cohort.
-- Cohort: action_with_pcl_overlap vs action_no_pcl_overlap.
-- Join to cards_crv_install_details via (acct_no, tactic_id) — converters only.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),

-- One row per CRV-Action wave; flag whether it overlaps a PCL-mobile window.
crv_action_classified AS (
    SELECT
        c.acct_no,
        c.tactic_id,
        c.offer_start_date,
        CAST(
            c.offer_start_date - (EXTRACT(DAY FROM c.offer_start_date) - 1) (FORMAT 'YYYY-MM-DD')
        AS VARCHAR(20))                                     AS crv_month,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM pcl_universe p
                WHERE p.acct_no          = c.acct_no
                  AND c.offer_start_date <= p.treatmt_end_dt
                  AND c.offer_end_date   >= p.treatmt_strt_dt
            ) THEN 'action_with_pcl_overlap'
            ELSE 'action_no_pcl_overlap'
        END                                                 AS cohort
    FROM dl_mr_prod.cards_crv_install_decis_resp c
    WHERE c.offer_start_date >= DATE '2024-10-01'
      AND c.channels_deployed LIKE '%IM%'
      AND c.action_control    = 'Action'
),

-- CRV wave joined to installment transactions; cohort + month carried through.
details AS (
    SELECT
        w.cohort,
        w.crv_month,
        w.acct_no,
        w.tactic_id,
        d.instl_apr,
        d.instl_txn_trm,
        d.instl_txn_prncpl_amt,
        d.instl_txn_fee_amt,
        d.instl_fee_pct
    FROM crv_action_classified w
    INNER JOIN dl_mr_prod.cards_crv_install_details d
      ON d.acct_no   = w.acct_no
     AND d.tactic_id = w.tactic_id
),

-- Per-account aggregation needed for mean_principal_per_acct.
-- Computed separately for overall and per-month slices, then stacked.
acct_agg_overall AS (
    SELECT
        cohort,
        CAST('overall' AS VARCHAR(20))  AS slice_val,
        acct_no,
        COUNT(*)                        AS acct_txn_cnt,
        SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS acct_total_principal
    FROM details
    GROUP BY cohort, acct_no
),

acct_agg_monthly AS (
    SELECT
        cohort,
        crv_month                       AS slice_val,
        acct_no,
        COUNT(*)                        AS acct_txn_cnt,
        SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS acct_total_principal
    FROM details
    GROUP BY cohort, crv_month, acct_no
),

-- Roll up to (cohort, slice) level for account-level metrics.
acct_rollup_overall AS (
    SELECT
        cohort,
        slice_val,
        COUNT(DISTINCT tactic_id)           AS n_waves,
        COUNT(DISTINCT acct_no)             AS n_accounts,
        SUM(acct_txn_cnt)                   AS n_transactions,
        AVG(acct_total_principal)           AS mean_principal_per_acct
    FROM (
        SELECT a.cohort, a.slice_val, a.acct_no, a.acct_txn_cnt, a.acct_total_principal,
               d2.tactic_id
        FROM acct_agg_overall a
        INNER JOIN (
            SELECT DISTINCT cohort, acct_no, tactic_id FROM details
        ) d2 ON d2.cohort = a.cohort AND d2.acct_no = a.acct_no
    ) x
    GROUP BY cohort, slice_val
),

acct_rollup_monthly AS (
    SELECT
        cohort,
        slice_val,
        COUNT(DISTINCT tactic_id)           AS n_waves,
        COUNT(DISTINCT acct_no)             AS n_accounts,
        SUM(acct_txn_cnt)                   AS n_transactions,
        AVG(acct_total_principal)           AS mean_principal_per_acct
    FROM (
        SELECT a.cohort, a.slice_val, a.acct_no, a.acct_txn_cnt, a.acct_total_principal,
               d2.tactic_id
        FROM acct_agg_monthly a
        INNER JOIN (
            SELECT DISTINCT cohort, crv_month, acct_no, tactic_id FROM details
        ) d2 ON d2.cohort   = a.cohort
             AND d2.crv_month = a.slice_val
             AND d2.acct_no   = a.acct_no
    ) x
    GROUP BY cohort, slice_val
)

-- Overall rows (one per cohort)
SELECT
    ar.cohort                                                                   AS cohort,
    ar.slice_val                                                                AS slice,
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    CAST(ar.n_transactions AS FLOAT) / NULLIF(ar.n_accounts, 0)                AS txns_per_acct,
    ar.mean_principal_per_acct,
    AVG(CAST(d.instl_apr              AS FLOAT))                                AS mean_apr,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT))   AS p50_apr,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT))   AS p90_apr,
    AVG(CAST(d.instl_txn_trm          AS FLOAT))                                AS mean_term,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)) AS p50_term,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)) AS p90_term,
    AVG(CAST(d.instl_txn_prncpl_amt   AS FLOAT))                                AS mean_txn_principal,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p50_txn_principal,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p90_txn_principal,
    AVG(CAST(d.instl_txn_fee_amt      AS FLOAT))                                AS mean_fee,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_fee_amt AS FLOAT)) AS p50_fee,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_fee_amt AS FLOAT)) AS p90_fee,
    AVG(CAST(d.instl_fee_pct          AS FLOAT))                                AS mean_fee_pct,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_fee_pct AS FLOAT)) AS p50_fee_pct,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_fee_pct AS FLOAT)) AS p90_fee_pct
FROM acct_rollup_overall ar
INNER JOIN details d
  ON d.cohort = ar.cohort
GROUP BY
    ar.cohort,
    ar.slice_val,
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    ar.mean_principal_per_acct

UNION ALL

-- Per-month rows (one per cohort × deployment month)
SELECT
    ar.cohort,
    ar.slice_val,
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    CAST(ar.n_transactions AS FLOAT) / NULLIF(ar.n_accounts, 0),
    ar.mean_principal_per_acct,
    AVG(CAST(d.instl_apr              AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_apr AS FLOAT)),
    AVG(CAST(d.instl_txn_trm          AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_trm AS FLOAT)),
    AVG(CAST(d.instl_txn_prncpl_amt   AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)),
    AVG(CAST(d.instl_txn_fee_amt      AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_txn_fee_amt AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_fee_amt AS FLOAT)),
    AVG(CAST(d.instl_fee_pct          AS FLOAT)),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY CAST(d.instl_fee_pct AS FLOAT)),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_fee_pct AS FLOAT))
FROM acct_rollup_monthly ar
INNER JOIN details d
  ON d.cohort    = ar.cohort
 AND d.crv_month = ar.slice_val
GROUP BY
    ar.cohort,
    ar.slice_val,
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    ar.mean_principal_per_acct

ORDER BY 1, 2
;
