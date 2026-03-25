-- PCQ Baseline Response Rates by Decile and Tactic ID
-- Platform: Starburst/Trino
-- Source: dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp

SELECT
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    tpa_ita,
    COUNT(*)                                              AS total_clients,
    -- DM coverage
    SUM(chnl_dm)                                          AS dm_targeted,
    ROUND(100.0 * SUM(chnl_dm) / COUNT(*), 1)            AS dm_coverage_pct,
    -- Overall success
    SUM(app_approved)                                     AS approved,
    SUM(app_completed)                                    AS completed,
    ROUND(100.0 * SUM(app_approved) / COUNT(*), 3)        AS approval_rate_pct,
    ROUND(100.0 * SUM(app_completed) / COUNT(*), 3)       AS completion_rate_pct,
    -- Success BY DM flag (with DM vs without DM)
    SUM(CASE WHEN chnl_dm = 1 THEN app_approved ELSE 0 END)  AS approved_with_dm,
    SUM(CASE WHEN chnl_dm = 0 THEN app_approved ELSE 0 END)  AS approved_no_dm,
    SUM(CASE WHEN chnl_dm = 1 THEN 1 ELSE 0 END)             AS pop_with_dm,
    SUM(CASE WHEN chnl_dm = 0 THEN 1 ELSE 0 END)             AS pop_no_dm,
    CASE
        WHEN SUM(CASE WHEN chnl_dm = 1 THEN 1 ELSE 0 END) > 0
        THEN ROUND(100.0 * SUM(CASE WHEN chnl_dm = 1 THEN app_approved ELSE 0 END)
             / SUM(CASE WHEN chnl_dm = 1 THEN 1 ELSE 0 END), 3)
    END                                                   AS approval_rate_with_dm,
    CASE
        WHEN SUM(CASE WHEN chnl_dm = 0 THEN 1 ELSE 0 END) > 0
        THEN ROUND(100.0 * SUM(CASE WHEN chnl_dm = 0 THEN app_approved ELSE 0 END)
             / SUM(CASE WHEN chnl_dm = 0 THEN 1 ELSE 0 END), 3)
    END                                                   AS approval_rate_no_dm,
    -- Response channel breakdown (where did converters come from)
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'DM'     THEN 1 ELSE 0 END) AS approved_via_dm,
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'Online' THEN 1 ELSE 0 END) AS approved_via_online,
    SUM(CASE WHEN app_approved = 1 AND response_channel_grp = 'Branch' THEN 1 ELSE 0 END) AS approved_via_branch,
    -- ASC attribution
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period'    THEN 1 ELSE 0 END) AS approved_period_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source IS NULL       THEN 1 ELSE 0 END) AS approved_no_asc,
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
    tpa_ita
