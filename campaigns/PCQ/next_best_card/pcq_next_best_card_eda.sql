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
-- Approved broken out by ASC category. Rates against total population per product.
-- Expected output: ~14 rows (2 groups × 7 products).
-- ==========================================================================
SELECT
    test_group_latest,
    offer_prod_latest,
    offer_prod_latest_name,
    COUNT(*) AS total_clients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY test_group_latest), 2) AS pct_of_group,
    SUM(app_approved) AS total_approved,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END) AS approved_no_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS rate_total_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS rate_period_asc_pct
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
-- Q10: Sanity check — approved clients with multiple credit card products.
-- Same client approved for more than one card?
-- Expected output: count of clients.
-- If overlap_count = 0, each client approved for exactly one product.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
    GROUP BY clnt_no
    HAVING COUNT(DISTINCT offer_prod_latest) > 1
) multi_product;
