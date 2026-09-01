-- ============================================================================
-- e12 — PCL deployment calendar from the TACTIC EVENT BASE (evidence query)
-- Question it answers for anyone who asks: "why are there almost no
-- post-Aug-14 PCL leads in the experiment read?"
-- Evidence: one row per PCL deployment day since June. The last deployment
-- date speaks for itself — no assertions needed.
-- Tactic base = deployment source of truth (arms/deployments from tactic,
-- curated tables only for conversion). Mnemonic self-verifies: both 'PCL'
-- and 'PLI' are scanned; whichever exists shows up in the mne column.
-- Engine: TERADATA-DIRECT. ~5-15 rows.
-- ============================================================================

SELECT
    substr(tactic_id, 8, 3)         AS mne,            -- self-verifies PCL vs PLI
    treatmt_strt_dt                 AS deploy_dt,
    COUNT(DISTINCT tactic_id)       AS tactic_ids,
    COUNT(*)                        AS row_ct,
    COUNT(DISTINCT clnt_no)         AS clnt_ct,
    MIN(treatmt_end_dt)             AS min_end_dt,
    MAX(treatmt_end_dt)             AS max_end_dt
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) IN ('PCL', 'PLI')
  AND treatmt_strt_dt >= DATE '2026-06-01'
GROUP BY 1, 2
ORDER BY 1, 2
