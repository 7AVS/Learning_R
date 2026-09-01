-- ============================================================================
-- e4 — PCL RESPONSE read (the confirmatory cannibalization outcome)
-- PCL response = limit-increase conversion (responder_cli) on a PCL lead
-- received by a CRV-experiment account AFTER go-live. Counts only, no rates.
-- PRIMARY CONTRAST: TG8 & flag Y (no banner)  vs  TG4 (banner) — same rule
-- stratum, random split. FALSIFICATION: TG8 & flag N vs TG1 (both dark, ~0).
-- NEVER compare TG4 vs TG1 (rule-based populations, not causal).
-- STATUS: waits for the first PCL wave AFTER 2026-08-14 to load (e3b: latest
-- wave = 2026-08-05, contaminated — arms assigned mid-wave). Rerun as-is.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE
--   * PCL lead scope: treatmt_strt_dt >= DATE '2026-08-14' (first clean wave
--     will be September's).
--   * channel kept as OUTPUT DIMENSION (ruling: PCL = MB; confirm empirically,
--     filter at read time, not in SQL).
--   * Join = acct key (e3c-proven, Q04-comparable). clnt-key sensitivity cut
--     is a separate rerun, not built in.
-- ============================================================================

WITH expt AS (
    -- experiment population, CELL FROZEN AT FIRST POST-GO-LIVE ASSIGNMENT
    -- (hard rule: rules use treatment-sensitive inputs; never re-stratify)
    SELECT visa_acct_no, clnt_no, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
)
SELECT
    TRUNC(e.assign_dt, 'MON')          AS cohort_month,     -- assignment cohort
    TRUNC(p.treatmt_strt_dt, 'MON')    AS pcl_wave_month,   -- PCL lead wave
    e.tst_grp_cd,
    e.pass_flag,
    p.channel,
    COUNT(*)                           AS pcl_leads,
    COUNT(DISTINCT p.acct_no)          AS pcl_lead_accts,
    SUM(p.responder_cli)               AS pcl_responders
FROM expt e
JOIN dl_mr_prod.cards_pli_decision_resp p
  ON p.acct_no = e.visa_acct_no
 AND p.treatmt_strt_dt >= DATE '2026-08-14'   -- redundant constant for pushdown
WHERE p.treatmt_strt_dt >= e.assign_dt        -- lead born on/after this account's assignment
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
