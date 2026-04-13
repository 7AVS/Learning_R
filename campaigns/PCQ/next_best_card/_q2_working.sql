-- ==========================================================================
-- EMERGENCY WORKING Q2 — restored from the original Q11 (commit d6c03a1).
-- This is the query that ran successfully before the refactor.
-- Open THIS file in a fresh Studio tab and run it.
--
-- No enrichment columns yet. No fees, no loyalty, no booking_status flags.
-- Just the baseline analytical output so you are unblocked and have a table.
-- Enrichment will be bolted on in a separate pass once Q2 runs clean here.
-- ==========================================================================
SELECT
    r.test_group_latest,
    r.clnt_no,
    r.acct_no,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    r.asc_on_app_source,
    r.response_dt,
    pa.months_with_activity,
    pa.first_extract_dt,
    pa.last_extract_dt,
    pl.latest_balance,
    pl.latest_status,
    pa.total_net_purchases,
    pa.total_net_purchases / NULLIFZERO(pa.months_with_activity) AS avg_monthly_purchases,
    pa.st_bkpt,
    pa.st_coll,
    pa.st_frd,
    pa.st_inv,
    pa.st_open,
    pa.st_vol,
    pa.st_woff,
    pa.first_non_open_dt - r.treatmt_start_dt AS days_to_status_change
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
LEFT JOIN (
    -- Aggregates per account across all post-treatment rows
    SELECT
        r2.acct_no,
        MIN(p.dt_record_ext) AS first_extract_dt,
        MAX(p.dt_record_ext) AS last_extract_dt,
        COUNT(DISTINCT p.me_dt) AS months_with_activity,
        SUM(p.net_prch_amt_dly) AS total_net_purchases,
        MAX(CASE WHEN p.status = 'BKPT' THEN 1 ELSE 0 END) AS st_bkpt,
        MAX(CASE WHEN p.status = 'COLL' THEN 1 ELSE 0 END) AS st_coll,
        MAX(CASE WHEN p.status = 'FRD' THEN 1 ELSE 0 END) AS st_frd,
        MAX(CASE WHEN p.status = 'INV' THEN 1 ELSE 0 END) AS st_inv,
        MAX(CASE WHEN p.status = 'OPEN' THEN 1 ELSE 0 END) AS st_open,
        MAX(CASE WHEN p.status = 'VOL' THEN 1 ELSE 0 END) AS st_vol,
        MAX(CASE WHEN p.status = 'WOFF' THEN 1 ELSE 0 END) AS st_woff,
        MIN(CASE WHEN p.status <> 'OPEN' THEN p.dt_record_ext END) AS first_non_open_dt
    FROM (
        SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
          AND app_approved = 1
        GROUP BY acct_no
    ) r2
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON p.acct_no = r2.acct_no
        AND p.dt_record_ext >= r2.treatmt_start_dt
    GROUP BY r2.acct_no
) pa ON pa.acct_no = r.acct_no
LEFT JOIN (
    -- Latest row per account
    SELECT
        p.acct_no,
        p.bal_current AS latest_balance,
        p.status AS latest_status
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN (
        SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
          AND app_approved = 1
        GROUP BY acct_no
    ) r2 ON r2.acct_no = p.acct_no AND p.dt_record_ext >= r2.treatmt_start_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext DESC) = 1
) pl ON pl.acct_no = r.acct_no
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
ORDER BY
    r.test_group_latest,
    r.offer_prod_latest,
    pa.total_net_purchases DESC;
