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
-- Q3: Conversion by test group × ASC source category × approval status.
-- All waves combined. This is the core answer: how does each group convert,
-- and what does the ASC source distribution look like?
-- Expected output: ~12 rows (2 groups × 3 ASC categories × 2 approval).
-- ==========================================================================
SELECT
    test_group_latest,
    asc_on_app_source,
    app_approved,
    COUNT(*) AS clients
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    asc_on_app_source,
    app_approved
ORDER BY
    test_group_latest,
    asc_on_app_source,
    app_approved;


-- ==========================================================================
-- Q4: Same as Q3 but split by wave (treatmt_start_dt).
-- Shows if conversion patterns differ between Jan and Feb deployments.
-- Expected output: ~24 rows.
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source,
    app_approved,
    COUNT(*) AS clients
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source,
    app_approved
ORDER BY
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source,
    app_approved;


-- ==========================================================================
-- Q5: Card product distribution by test group (all waves combined).
-- What cards are being offered in control vs test?
-- Expected output: ~14 rows (2 groups × 7 products).
-- ==========================================================================
SELECT
    test_group_latest,
    offer_prod_latest,
    offer_prod_latest_name,
    COUNT(*) AS clients
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    offer_prod_latest,
    offer_prod_latest_name
ORDER BY
    test_group_latest,
    clients DESC;


-- ==========================================================================
-- Q6: Full detail dump — finest grain for Excel pivot.
-- Run this last. All dimensions included for ad-hoc pivoting.
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    treatmt_end_dt,
    asc_on_app_source,
    app_approved,
    offer_prod_latest,
    offer_prod_latest_name,
    response_channel_grp,
    response_channel,
    COUNT(*) AS clients,
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
    app_approved,
    offer_prod_latest,
    offer_prod_latest_name,
    response_channel_grp,
    response_channel
ORDER BY
    test_group_latest,
    treatmt_start_dt,
    asc_on_app_source,
    app_approved,
    clients DESC;
