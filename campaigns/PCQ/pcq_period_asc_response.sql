-- PCQ Response Analysis: Period-ASC attribution × Response Channel × Targeted Channel (DM)
-- Platform: Starburst/Trino
-- Source: dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
--
-- Per Daniel Chin: filter to asc_on_app_source = 'Period-ASC' for TRUE campaign-attributed
-- conversions. The table includes ALL applications — Period-ASC filters to those that were
-- offered and applied through the campaign offer (gets RATE, TERMS, POINTS, Fee Waiver).
-- Without this filter, response counts are grossly overstated.

-- ============================================================================
-- QUERY 1: Approved by response_channel × asc_on_app_source × DM targeted
-- Shows: where did people apply, was it attributed, and were they targeted with DM?
-- ============================================================================
SELECT
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    -- Targeted channel: did this client get DM?
    CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 'DM Targeted' ELSE 'No DM' END AS dm_flag,
    -- Response channel: where did they apply?
    response_channel_grp,
    -- Attribution: was it through the campaign offer?
    asc_on_app_source,
    -- Counts
    COUNT(*)                          AS approved_count,
    ROUND(AVG(days_to_respond), 1)    AS avg_days
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
  AND app_approved = 1
GROUP BY
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 'DM Targeted' ELSE 'No DM' END,
    response_channel_grp,
    asc_on_app_source
ORDER BY
    tactic_id,
    model_score_decile,
    dm_flag,
    response_channel_grp,
    asc_on_app_source;

-- ============================================================================
-- QUERY 2: Summary by decile — Period-ASC only (true campaign conversions)
-- This is the "correct" approval rate per Daniel's guidance
-- ============================================================================
SELECT
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    COUNT(*)                                              AS total_clients,
    SUM(COALESCE(chnl_dm, 0))                             AS dm_targeted,
    -- ALL approvals (overstated — includes organic)
    SUM(COALESCE(app_approved, 0))                        AS approved_all,
    ROUND(100.0 * SUM(COALESCE(app_approved, 0)) / COUNT(*), 3) AS rate_all_pct,
    -- Period-ASC approvals only (true campaign attribution)
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END)
          / COUNT(*), 3)                                  AS rate_period_asc_pct,
    -- Period-ASC with DM vs without DM
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND COALESCE(chnl_dm, 0) = 1 THEN 1 ELSE 0 END) AS period_asc_with_dm,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND COALESCE(chnl_dm, 0) = 0 THEN 1 ELSE 0 END) AS period_asc_no_dm,
    -- Period-ASC by response channel
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND response_channel_grp = 'Online' THEN 1 ELSE 0 END)            AS period_asc_via_online,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND response_channel_grp = 'Mobile' THEN 1 ELSE 0 END)            AS period_asc_via_mobile,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND response_channel_grp = 'Branch/Advice Ctr' THEN 1 ELSE 0 END) AS period_asc_via_branch,
    -- Other ASC (not campaign attributed but still approved)
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC' THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC' THEN 1 ELSE 0 END)    AS approved_no_asc
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
GROUP BY
    tactic_id,
    treatmt_start_dt,
    model_score_decile
ORDER BY
    tactic_id,
    model_score_decile;

-- ============================================================================
-- QUERY 3: Same as Query 2 but also split by DM targeted vs not
-- This is the key table for the MDE baseline
-- ============================================================================
SELECT
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 'DM Targeted' ELSE 'No DM' END AS dm_flag,
    COUNT(*)                                              AS clients,
    -- Period-ASC approvals only
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END)
          / COUNT(*), 3)                                  AS rate_period_asc_pct,
    -- All approvals for comparison
    SUM(COALESCE(app_approved, 0))                        AS approved_all,
    ROUND(100.0 * SUM(COALESCE(app_approved, 0)) / COUNT(*), 3) AS rate_all_pct,
    -- Response channel for Period-ASC
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND response_channel_grp = 'Online' THEN 1 ELSE 0 END)            AS pasc_online,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND response_channel_grp = 'Mobile' THEN 1 ELSE 0 END)            AS pasc_mobile,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC'
             AND response_channel_grp = 'Branch/Advice Ctr' THEN 1 ELSE 0 END) AS pasc_branch
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE mnemonic = 'PCQ'
  AND decsn_year = 2026
GROUP BY
    tactic_id,
    treatmt_start_dt,
    model_score_decile,
    CASE WHEN COALESCE(chnl_dm, 0) = 1 THEN 'DM Targeted' ELSE 'No DM' END
ORDER BY
    tactic_id,
    model_score_decile,
    dm_flag;
