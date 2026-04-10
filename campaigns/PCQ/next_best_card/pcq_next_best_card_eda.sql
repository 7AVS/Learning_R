-- PCQ Next Best Card Test — Exploratory Analysis
-- Source: dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
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
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
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
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest
ORDER BY
    test_group_latest;


-- ==========================================================================
-- Q3: Conversion by test group × ASC source category.
-- All waves combined. One row per group × ASC category.
-- Total clients, total approved, and approval rate in a single row.
-- Expected output: ~8 rows (2 groups × (3 ASC categories + 1 total)).
-- ==========================================================================
SELECT
    test_group_latest,
    asc_on_app_source,
    COUNT(*) AS total_clients,
    SUM(app_approved) AS total_approved,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS approval_rate_pct
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    asc_on_app_source
ORDER BY
    test_group_latest,
    asc_on_app_source;


-- ==========================================================================
-- Q4: Same as Q3 but split by wave (treatmt_start_dt).
-- Shows if conversion patterns differ between Jan and Feb deployments.
-- Expected output: ~16 rows (2 groups × 2 waves × (3 ASC + 1 total)).
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source,
    COUNT(*) AS total_clients,
    SUM(app_approved) AS total_approved,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS approval_rate_pct
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source
ORDER BY
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source;


-- ==========================================================================
-- Q5: Card product distribution by test group (all waves combined).
-- Total, approved, approval rate, and share of group per product.
-- Expected output: ~14 rows (2 groups × 7 products).
-- ==========================================================================
SELECT
    test_group_latest,
    offer_prod_latest,
    offer_prod_latest_name,
    COUNT(*) AS total_clients,
    SUM(app_approved) AS total_approved,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS approval_rate_pct,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY test_group_latest), 2) AS pct_of_group
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
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
-- Approval aggregated into columns, not rows.
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt,
    asc_on_app_source,
    offer_prod_latest,
    offer_prod_latest_name,
    response_channel_grp,
    response_channel,
    COUNT(*) AS total_clients,
    SUM(app_approved) AS total_approved,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 2) AS approval_rate_pct,
    AVG(days_to_respond * 1.0) AS avg_days_to_respond,
    MIN(days_to_respond) AS min_days_to_respond,
    MAX(days_to_respond) AS max_days_to_respond
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt,
    asc_on_app_source,
    offer_prod_latest,
    offer_prod_latest_name,
    response_channel_grp,
    response_channel
ORDER BY
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source,
    total_clients DESC;
