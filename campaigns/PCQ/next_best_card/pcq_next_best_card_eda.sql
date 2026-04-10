-- PCQ Next Best Card Test — Exploratory Analysis
-- Source: dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
-- Test groups: NG3_1ST (control — 1st recommended card) vs NG3_2ND (test — 2nd recommended card)
-- Purpose: Single dump at the finest useful grain. Pivot in Excel.

SELECT
    test_group_latest,
    treatmt_start_dt,
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
