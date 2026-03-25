-- PCQ: Expected Value Decile vs Model Score Decile Comparison
-- Platform: Starburst/Trino
-- Source: dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
-- NOTE: chnl_dm is NULL (not 0) for non-DM clients — use COALESCE
--
-- This file contains two queries:
--   Query 1: Same baseline metrics as pcq_baseline_rates.sql, but cut by expected_value_decile
--            instead of model_score_decile. Use this to see if EV decile has sharper lift.
--   Query 2: Cross-tab of model_score_decile vs expected_value_decile. Use this to understand
--            how correlated the two rankings are — if they diverge, the signals differ.

-- ============================================================
-- QUERY 1: Baseline Rates by Expected Value Decile
-- Same columns and logic as pcq_baseline_rates.sql.
-- Swap: model_score_decile → expected_value_decile in SELECT, GROUP BY, ORDER BY.
-- ============================================================

SELECT
    tactic_id,
    treatmt_start_dt,
    expected_value_decile,
    tpa_ita,
    COUNT(*)                                                AS total_clients,
    -- DM coverage
    SUM(COALESCE(chnl_dm, 0))                               AS dm_targeted,
    ROUND(100.0 * SUM(COALESCE(chnl_dm, 0)) / COUNT(*), 1) AS dm_coverage_pct,
    -- Overall success
    SUM(COALESCE(app_approved, 0))                          AS approved,
    SUM(COALESCE(app_completed, 0))                         AS completed,
    ROUND(100.0 * SUM(COALESCE(app_approved, 0)) / COUNT(*), 2) AS approval_rate_pct,
    -- Success BY DM flag (with DM vs without DM)
    SUM(CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN COALESCE(app_approved, 0) ELSE 0 END) AS approved_with_dm,
    SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN COALESCE(app_approved, 0) ELSE 0 END) AS approved_no_dm,
    SUM(CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 1 ELSE 0 END) AS pop_with_dm,
    SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN 1 ELSE 0 END) AS pop_no_dm,
    CASE
        WHEN SUM(CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 1 ELSE 0 END) > 0
        THEN ROUND(100.0
             * SUM(CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN COALESCE(app_approved, 0) ELSE 0 END)
             / SUM(CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 1 ELSE 0 END), 2)
    END                                                     AS approval_rate_with_dm,
    CASE
        WHEN SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN 1 ELSE 0 END) > 0
        THEN ROUND(100.0
             * SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN COALESCE(app_approved, 0) ELSE 0 END)
             / SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN 1 ELSE 0 END), 2)
    END                                                     AS approval_rate_no_dm,
    -- Response channel breakdown (where approved clients applied from)
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'Online'            THEN 1 ELSE 0 END) AS approved_via_online,
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'Mobile'            THEN 1 ELSE 0 END) AS approved_via_mobile,
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'Branch/Advice Ctr' THEN 1 ELSE 0 END) AS approved_via_branch,
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'Other'             THEN 1 ELSE 0 END) AS approved_via_other,
    -- ASC attribution
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC'  THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC'     THEN 1 ELSE 0 END) AS approved_no_asc,
    -- Avg days to respond
    ROUND(AVG(CASE WHEN app_approved = 1 THEN days_to_respond END), 1)  AS avg_days_to_approve
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
GROUP BY
    tactic_id,
    treatmt_start_dt,
    expected_value_decile,
    tpa_ita
ORDER BY
    tactic_id,
    expected_value_decile,
    tpa_ita;


-- ============================================================
-- QUERY 2: Cross-Tab — Model Score Decile vs Expected Value Decile
-- Shows how the two rankings align at the client level.
-- If deciles track closely (diagonal pattern), the signals are similar.
-- Off-diagonal mass means the rankings diverge — EV reorders clients
-- that the model score alone would have ranked differently.
-- ============================================================

SELECT
    model_score_decile,
    expected_value_decile,
    COUNT(*)                           AS clients,
    SUM(COALESCE(chnl_dm, 0))          AS dm_targeted,
    SUM(COALESCE(app_approved, 0))     AS approved
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
GROUP BY
    model_score_decile,
    expected_value_decile
ORDER BY
    model_score_decile,
    expected_value_decile;
