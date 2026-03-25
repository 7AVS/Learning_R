-- PCQ Baseline Response Rates by Decile and Tactic ID
-- Platform: Starburst/Trino
-- Source: dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
-- NOTE: chnl_dm is NULL (not 0) for non-DM clients — use COALESCE

SELECT
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    tpa_ita,
    COUNT(*)                                              AS total_clients,
    -- DM coverage
    SUM(COALESCE(chnl_dm, 0))                             AS dm_targeted,
    ROUND(100.0 * SUM(COALESCE(chnl_dm, 0)) / COUNT(*), 1) AS dm_coverage_pct,
    -- Overall success
    SUM(COALESCE(app_approved, 0))                        AS approved,
    SUM(COALESCE(app_completed, 0))                       AS completed,
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
    END                                                   AS approval_rate_with_dm,
    CASE
        WHEN SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN 1 ELSE 0 END) > 0
        THEN ROUND(100.0
             * SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN COALESCE(app_approved, 0) ELSE 0 END)
             / SUM(CASE WHEN COALESCE(chnl_dm, 0) = 0 THEN 1 ELSE 0 END), 2)
    END                                                   AS approval_rate_no_dm,
    -- Response channel breakdown (discover actual values)
    SUM(CASE WHEN app_approved = 1 THEN 1 ELSE 0 END)    AS total_approved_check,
    COUNT(DISTINCT CASE WHEN app_approved = 1 THEN response_channel_grp END) AS distinct_resp_chnl_grps,
    COUNT(DISTINCT CASE WHEN app_approved = 1 THEN asc_on_app_source END)    AS distinct_asc_sources,
    -- Avg days to respond
    ROUND(AVG(CASE WHEN app_approved = 1 THEN days_to_respond END), 1) AS avg_days_to_approve
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
GROUP BY
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    tpa_ita
ORDER BY
    tactic_id,
    model_score_decile,
    tpa_ita;

-- Separate query: discover actual response_channel_grp and asc_on_app_source values
SELECT
    response_channel_grp,
    asc_on_app_source,
    COUNT(*) AS cnt
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
  AND app_approved = 1
GROUP BY response_channel_grp, asc_on_app_source
ORDER BY cnt DESC;
