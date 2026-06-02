-- CRV installment economics split into 4 cohorts:
--   action_with_pcl_overlap, action_no_pcl_overlap,
--   control_with_pcl_overlap, control_no_pcl_overlap.
-- Join to cards_crv_install_details via (acct_no, tactic_id) — converters only
-- (INNER JOIN filters naturally: only converted CRV decisions have install_details rows).
-- CRV-Action: channels_deployed LIKE '%IM%'.
-- CRV-Control: NO channel filter (Control is not deployed to any channel).
--
-- WHAT THIS ANSWERS
--   Characterises the CRV installment book among converters, split by PCL overlap.
--   Two jobs: (1) the VALUE side of the net (CRV $/conversion); (2) guards Q04 by
--   checking Action vs Control are balanced on product economics, not just headcount.
--
-- FINDINGS (2026-06-01)
--   * Per-unit economics FLAT across all 4 cohorts: ~$980/plan, ~6.6% APR, ~6.6mo term
--     -> no product-mix/risk confound; the Q04 cannibalization gap is not a selection artifact.
--   * Overlap propensity even: 40.3% of Action converters overlap PCL vs 39.2% of Control.
--   * mean_principal_per_acct = total installment $ per CLIENT across all plans (~$6.7k),
--     NOT per purchase. Clean per-account plan count ~= 6.85 (principal / txn_principal).
--   * CAVEAT: n_transactions and txns_per_acct are INFLATED ~1.74x by the tactic_id
--     fanout used to derive n_waves -- do NOT quote them. n_accounts, the means, APR,
--     and term are clean. Fix before reuse: count n_waves in a separate CTE.
--   * This query is the CRV-value multiplier only. It does NOT measure cannibalization
--     (Q04) or the net (needs Q06 incrementality x this value).

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),

-- CRV decisions in window (Action with IM channel filter, OR Control with no channel filter).
crv_decisions_in_window AS (
    SELECT
        acct_no,
        tactic_id,
        offer_start_date,
        offer_end_date,
        action_control
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND (
            (action_control = 'Action'  AND channels_deployed LIKE '%IM%')
         OR  action_control = 'Control'
          )
),

-- Subset of CRV decisions that overlap a PCL-mobile window (EXISTS in WHERE = allowed).
crv_overlap_keys AS (
    SELECT
        c.acct_no,
        c.tactic_id,
        c.offer_start_date
    FROM crv_decisions_in_window c
    WHERE EXISTS (
        SELECT 1 FROM pcl_universe p
        WHERE p.acct_no          = c.acct_no
          AND c.offer_start_date <= p.treatmt_end_dt
          AND c.offer_end_date   >= p.treatmt_strt_dt
    )
),

-- One row per CRV decision with overlap flag, then cohort assignment via CASE on IS NOT NULL.
crv_decisions_classified AS (
    SELECT
        c.acct_no,
        c.tactic_id,
        c.offer_start_date,
        CAST(c.offer_start_date - (EXTRACT(DAY FROM c.offer_start_date) - 1) AS VARCHAR(20)) AS crv_month,
        CASE
            WHEN c.action_control = 'Action'  AND ov.acct_no IS NOT NULL THEN 'action_with_pcl_overlap'
            WHEN c.action_control = 'Action'                              THEN 'action_no_pcl_overlap'
            WHEN c.action_control = 'Control' AND ov.acct_no IS NOT NULL THEN 'control_with_pcl_overlap'
            ELSE 'control_no_pcl_overlap'
        END                                                 AS crv_cohort
    FROM crv_decisions_in_window c
    LEFT JOIN crv_overlap_keys ov
      ON ov.acct_no          = c.acct_no
     AND ov.tactic_id        = c.tactic_id
     AND ov.offer_start_date = c.offer_start_date
),

-- PATCH 2026-06-02: dedup decision keys to DISTINCT (cohort, acct, tactic, month)
-- BEFORE joining install_details, so each installment plan is counted ONCE.
-- The old version joined raw decision rows -> details and fanned each plan out
-- by the number of decision waves, inflating n_transactions / txns_per_acct.
-- Plans now reconcile with Q13 (~3 per account).
crv_keys AS (
    SELECT DISTINCT crv_cohort, crv_month, acct_no, tactic_id
    FROM crv_decisions_classified
),

-- CRV wave joined to installment transactions; cohort + month carried through.
details AS (
    SELECT
        w.crv_cohort,
        w.crv_month,
        w.acct_no,
        w.tactic_id,
        d.instl_txn_ref_no,
        d.instl_apr,
        d.instl_txn_trm,
        d.instl_txn_prncpl_amt
    FROM crv_keys w
    INNER JOIN dl_mr_prod.cards_crv_install_details d
      ON d.acct_no   = w.acct_no
     AND d.tactic_id = w.tactic_id
),

-- Per-account aggregation needed for mean_principal_per_acct.
-- Computed separately for overall and per-month slices, then stacked.
acct_agg_overall AS (
    SELECT
        crv_cohort,
        CAST('overall' AS VARCHAR(20))  AS slice_val,
        acct_no,
        COUNT(DISTINCT instl_txn_ref_no) AS acct_txn_cnt,
        SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS acct_total_principal
    FROM details
    GROUP BY crv_cohort, acct_no
),

acct_agg_monthly AS (
    SELECT
        crv_cohort,
        crv_month                       AS slice_val,
        acct_no,
        COUNT(DISTINCT instl_txn_ref_no) AS acct_txn_cnt,
        SUM(CAST(instl_txn_prncpl_amt AS FLOAT)) AS acct_total_principal
    FROM details
    GROUP BY crv_cohort, crv_month, acct_no
),

-- Roll up to (crv_cohort, slice) level for account-level metrics.
-- PATCH 2026-06-02: account metrics come straight from acct_agg (one row per
-- cohort x acct) — NO join back to tactic keys. The old tactic-fanout join
-- re-multiplied acct_txn_cnt and acct_total_principal by waves-per-account.
-- n_waves is now a separate grouped count, joined on cohort only.
acct_rollup_overall AS (
    SELECT
        ag.crv_cohort,
        ag.slice_val,
        wv.n_waves,
        COUNT(*)                            AS n_accounts,
        SUM(ag.acct_txn_cnt)                AS n_transactions,
        AVG(ag.acct_total_principal)        AS mean_principal_per_acct
    FROM acct_agg_overall ag
    LEFT JOIN (
        SELECT crv_cohort, COUNT(DISTINCT tactic_id) AS n_waves
        FROM details GROUP BY crv_cohort
    ) wv ON wv.crv_cohort = ag.crv_cohort
    GROUP BY ag.crv_cohort, ag.slice_val, wv.n_waves
),

acct_rollup_monthly AS (
    SELECT
        ag.crv_cohort,
        ag.slice_val,
        wv.n_waves,
        COUNT(*)                            AS n_accounts,
        SUM(ag.acct_txn_cnt)                AS n_transactions,
        AVG(ag.acct_total_principal)        AS mean_principal_per_acct
    FROM acct_agg_monthly ag
    LEFT JOIN (
        SELECT crv_cohort, crv_month, COUNT(DISTINCT tactic_id) AS n_waves
        FROM details GROUP BY crv_cohort, crv_month
    ) wv ON wv.crv_cohort = ag.crv_cohort AND wv.crv_month = ag.slice_val
    GROUP BY ag.crv_cohort, ag.slice_val, wv.n_waves
)

-- Overall rows (one per crv_cohort)
SELECT
    ar.crv_cohort                                                               AS crv_cohort,
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
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT)) AS p90_txn_principal
FROM acct_rollup_overall ar
INNER JOIN details d
  ON d.crv_cohort = ar.crv_cohort
GROUP BY
    ar.crv_cohort,
    ar.slice_val,
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    ar.mean_principal_per_acct

UNION ALL

-- Per-month rows (one per crv_cohort × deployment month)
SELECT
    ar.crv_cohort,
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
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY CAST(d.instl_txn_prncpl_amt AS FLOAT))
FROM acct_rollup_monthly ar
INNER JOIN details d
  ON d.crv_cohort = ar.crv_cohort
 AND d.crv_month  = ar.slice_val
GROUP BY
    ar.crv_cohort,
    ar.slice_val,
    ar.n_waves,
    ar.n_accounts,
    ar.n_transactions,
    ar.mean_principal_per_acct

ORDER BY 1, 2
;
