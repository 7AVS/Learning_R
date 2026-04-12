-- PCQ Next Best Card Test — Exploratory Analysis
-- Source: DL_MR_PROD.cards_tpa_pcq_decision_resp
-- Test groups: NG3_1ST (control — 1st recommended card) vs NG3_2ND (test — 2nd recommended card)


-- ==========================================================================
-- Q1: How many deployment waves, and what is the treatment window?
-- Expected output: small — one row per wave per group with start/end dates.
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt,
    COUNT(*) AS clients
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt
ORDER BY
    treatmt_start_dt,
    test_group_latest;


-- ==========================================================================
-- Q2: Total deployment volume by test group (all waves combined).
-- Expected output: 2 rows — one per group.
-- ==========================================================================
SELECT
    test_group_latest,
    COUNT(*) AS total_clients
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest
ORDER BY
    test_group_latest;


-- ==========================================================================
-- Q3: Conversion summary — one row per test group.
-- All waves combined. ASC categories as columns, not rows.
-- Denominator = total population (including non-responders).
-- Expected output: 2 rows.
-- ==========================================================================
SELECT
    test_group_latest,
    COUNT(*) AS total_clients,
    SUM(CASE WHEN asc_on_app_source IS NOT NULL THEN 1 ELSE 0 END) AS total_responded,
    SUM(app_approved) AS total_approved,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) AS approved_no_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS rate_total_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_no_asc_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_other_asc_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_period_asc_pct
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest
ORDER BY
    test_group_latest;


-- ==========================================================================
-- Q4: Same as Q3 but split by wave (treatmt_start_dt).
-- Shows if conversion patterns differ between Jan and Feb deployments.
-- Expected output: 4 rows (2 groups × 2 waves).
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    COUNT(*) AS total_clients,
    SUM(CASE WHEN asc_on_app_source IS NOT NULL THEN 1 ELSE 0 END) AS total_responded,
    SUM(app_approved) AS total_approved,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) AS approved_no_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS rate_total_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_no_asc_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_other_asc_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_period_asc_pct
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt
ORDER BY
    test_group_latest,
    treatmt_start_dt;


-- ==========================================================================
-- Q5: Card product distribution by test group (all waves combined).
-- Approved broken out by ASC category. No percentages.
-- Expected output: ~14 rows (2 groups × 7 products).
-- ==========================================================================
SELECT
    test_group_latest,
    offer_prod_latest,
    offer_prod_latest_name,
    COUNT(*) AS total_clients,
    SUM(app_approved) AS total_approved,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) AS approved_no_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    offer_prod_latest,
    offer_prod_latest_name
ORDER BY
    test_group_latest,
    total_clients DESC;


-- ==========================================================================
-- Q6: Full detail dump — finest grain for Excel pivot.
-- Run this last. All dimensions included for ad-hoc pivoting.
-- Approved broken out by ASC category as columns.
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt,
    offer_prod_latest,
    offer_prod_latest_name,
    response_channel_grp,
    response_channel,
    COUNT(*) AS total_clients,
    SUM(app_approved) AS total_approved,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) AS approved_no_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS rate_total_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_period_asc_pct,
    AVG(days_to_respond * 1.0) AS avg_days_to_respond,
    MIN(days_to_respond) AS min_days_to_respond,
    MAX(days_to_respond) AS max_days_to_respond
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt,
    offer_prod_latest,
    offer_prod_latest_name,
    response_channel_grp,
    response_channel
ORDER BY
    test_group_latest,
    treatmt_start_dt,
    total_clients DESC;


-- ==========================================================================
-- Q7: Sanity check — client overlap across test groups.
-- Are any clients in BOTH NG3_1ST and NG3_2ND?
-- Expected output: 1 row. If overlap_count = 0, groups are clean.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest = 'NG3_1ST'
    GROUP BY clnt_no
) a
INNER JOIN (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest = 'NG3_2ND'
    GROUP BY clnt_no
) b
ON a.clnt_no = b.clnt_no;


-- ==========================================================================
-- Q8: Sanity check — approved clients across BOTH waves.
-- Same client approved in Jan AND Feb deployment?
-- Expected output: 1 row. If overlap_count = 0, no cross-wave approvals.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
      AND treatmt_start_dt = DATE '2025-01-09'
    GROUP BY clnt_no
) jan
INNER JOIN (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
      AND treatmt_start_dt = DATE '2025-02-06'
    GROUP BY clnt_no
) feb
ON jan.clnt_no = feb.clnt_no;


-- ==========================================================================
-- Q9: Sanity check — approved clients across multiple ASC categories.
-- Same client approved through more than one ASC source?
-- Expected output: count of clients + breakdown.
-- If overlap_count = 0, each client approved through only one ASC source.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
    GROUP BY clnt_no
    HAVING COUNT(DISTINCT asc_on_app_source) > 1
) multi_asc;


-- ==========================================================================
-- Q10: Sanity check — approved clients with multiple approval rows.
-- Any client approved more than once? Shows the detail so we can see
-- if it's the same card, different card, same wave, different wave.
-- Expected output: one row per multi-approved client.
-- If empty, every approved client has exactly one approval.
-- ==========================================================================
SELECT
    clnt_no,
    COUNT(*) AS total_approvals,
    COUNT(DISTINCT offer_prod_latest) AS distinct_products,
    COUNT(DISTINCT treatmt_start_dt) AS distinct_waves,
    COUNT(DISTINCT asc_on_app_source) AS distinct_asc
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND app_approved = 1
GROUP BY clnt_no
HAVING COUNT(*) > 1
ORDER BY total_approvals DESC;


-- ==========================================================================
-- Q11: Portfolio join — account-level detail for approved clients.
-- Pre-aggregates portfolio to account × me_dt to avoid card/transaction
-- level inflation. One row per approved account.
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
    COUNT(p.me_dt) AS months_with_data,
    MIN(p.me_dt) AS first_portfolio_dt,
    MAX(p.me_dt) AS last_portfolio_dt,
    AVG(p.bal_current) AS avg_balance,
    MAX(p.bal_current) AS max_balance,
    SUM(p.net_prch_amt_mtd) AS total_net_purchases,
    MAX(CASE WHEN p.status = 'BKPT' THEN 1 ELSE 0 END) AS st_bkpt,
    MAX(CASE WHEN p.status = 'COLL' THEN 1 ELSE 0 END) AS st_coll,
    MAX(CASE WHEN p.status = 'FRD' THEN 1 ELSE 0 END) AS st_frd,
    MAX(CASE WHEN p.status = 'INV' THEN 1 ELSE 0 END) AS st_inv,
    MAX(CASE WHEN p.status = 'OPEN' THEN 1 ELSE 0 END) AS st_open,
    MAX(CASE WHEN p.status = 'VOL' THEN 1 ELSE 0 END) AS st_vol,
    MAX(CASE WHEN p.status = 'WOFF' THEN 1 ELSE 0 END) AS st_woff,
    MIN(CASE WHEN p.status <> 'OPEN' THEN p.me_dt END) - r.treatmt_start_dt AS days_to_status_change
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
LEFT JOIN (
    SELECT
        acct_no,
        me_dt,
        SUM(bal_current) AS bal_current,
        SUM(net_prch_amt_mtd) AS net_prch_amt_mtd,
        MAX(status) AS status
    FROM D3CV12A.DLY_FULL_PORTFOLIO
    GROUP BY acct_no, me_dt
) p
    ON p.acct_no = r.acct_no
    AND p.me_dt >= r.treatmt_start_dt
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
GROUP BY
    r.test_group_latest,
    r.clnt_no,
    r.acct_no,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    r.asc_on_app_source,
    r.response_dt
ORDER BY
    r.test_group_latest,
    r.offer_prod_latest,
    total_net_purchases DESC;


-- ==========================================================================
-- Q12: Portfolio summary — test group × product for approved clients.
-- Rolls up Q11. Pre-aggregated portfolio.
-- ==========================================================================
SELECT
    r.test_group_latest,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    COUNT(DISTINCT r.acct_no) AS approved_accounts,
    COUNT(DISTINCT CASE WHEN p.acct_no IS NOT NULL THEN r.acct_no END) AS accounts_with_portfolio,
    AVG(p.bal_current) AS avg_balance,
    SUM(p.net_prch_amt_mtd) AS total_net_purchases,
    SUM(p.net_prch_amt_mtd) / NULLIFZERO(COUNT(DISTINCT CASE WHEN p.acct_no IS NOT NULL THEN r.acct_no END)) AS avg_purchases_per_account,
    COUNT(DISTINCT CASE WHEN p.status = 'BKPT' THEN r.acct_no END) AS accts_bkpt,
    COUNT(DISTINCT CASE WHEN p.status = 'COLL' THEN r.acct_no END) AS accts_coll,
    COUNT(DISTINCT CASE WHEN p.status = 'FRD' THEN r.acct_no END) AS accts_frd,
    COUNT(DISTINCT CASE WHEN p.status = 'INV' THEN r.acct_no END) AS accts_inv,
    COUNT(DISTINCT CASE WHEN p.status = 'OPEN' THEN r.acct_no END) AS accts_open,
    COUNT(DISTINCT CASE WHEN p.status = 'VOL' THEN r.acct_no END) AS accts_vol,
    COUNT(DISTINCT CASE WHEN p.status = 'WOFF' THEN r.acct_no END) AS accts_woff
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
LEFT JOIN (
    SELECT
        acct_no,
        me_dt,
        SUM(bal_current) AS bal_current,
        SUM(net_prch_amt_mtd) AS net_prch_amt_mtd,
        MAX(status) AS status
    FROM D3CV12A.DLY_FULL_PORTFOLIO
    GROUP BY acct_no, me_dt
) p
    ON p.acct_no = r.acct_no
    AND p.me_dt >= r.treatmt_start_dt
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
GROUP BY
    r.test_group_latest,
    r.offer_prod_latest,
    r.offer_prod_latest_name
ORDER BY
    r.test_group_latest,
    approved_accounts DESC;


-- ==========================================================================
-- Q13: Balance/spend curves — monthly metrics by me_dt.
-- Pre-aggregated portfolio to account × me_dt.
-- Pivot in Excel to see any combination of curves.
-- ==========================================================================
SELECT
    r.test_group_latest,
    r.treatmt_start_dt,
    r.asc_on_app_source,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.me_dt,
    COUNT(DISTINCT r.acct_no) AS accounts_reporting,
    AVG(p.bal_current) AS avg_balance,
    SUM(p.net_prch_amt_mtd) AS total_purchases_mtd
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
INNER JOIN (
    SELECT
        acct_no,
        me_dt,
        SUM(bal_current) AS bal_current,
        SUM(net_prch_amt_mtd) AS net_prch_amt_mtd
    FROM D3CV12A.DLY_FULL_PORTFOLIO
    GROUP BY acct_no, me_dt
) p
    ON p.acct_no = r.acct_no
    AND p.me_dt >= r.treatmt_start_dt
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
GROUP BY
    r.test_group_latest,
    r.treatmt_start_dt,
    r.asc_on_app_source,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.me_dt
ORDER BY
    r.test_group_latest,
    r.treatmt_start_dt,
    r.asc_on_app_source,
    r.offer_prod_latest,
    p.me_dt;


-- ==========================================================================
-- Q14: Sanity check — how many cards per account?
-- Shows distinct member_num (cards) per account for approved clients.
-- If most accounts have 1 card, the pre-aggregation is clean.
-- ==========================================================================
SELECT
    cards_per_account,
    COUNT(*) AS account_count
FROM (
    SELECT
        r.acct_no,
        COUNT(DISTINCT p.member_num) AS cards_per_account
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON p.acct_no = r.acct_no
        AND p.me_dt >= r.treatmt_start_dt
    WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND r.app_approved = 1
    GROUP BY r.acct_no
) card_check
GROUP BY cards_per_account
ORDER BY cards_per_account;


-- ==========================================================================
-- Q15: Sanity check — portfolio product vs PCQ offered product.
-- Does visa_prod_cd from the portfolio match offer_prod_latest from PCQ?
-- ==========================================================================
SELECT
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.visa_prod_cd,
    COUNT(DISTINCT r.acct_no) AS accounts
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
    ON p.acct_no = r.acct_no
    AND p.me_dt >= r.treatmt_start_dt
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
GROUP BY
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.visa_prod_cd
ORDER BY
    r.offer_prod_latest,
    accounts DESC;
