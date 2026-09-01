-- ============================================================================
-- e6b — PCL RESPONSE VINTAGE (limit-increase event curve since PCL lead start)
-- One row per cell x week-since-lead-start: responders' dt_cl_change timing.
-- dt_cl_change = limit-change event date on the SAME curated PCL row; proven
-- airtight for responders (Q17b v1 2026-07-13: unproven bucket EMPTY —
-- always populated). CAVEAT carried from Q17 series: no increase_decrease
-- filter anywhere in prior work — "PLI take" not verified to exclude
-- decreases; column exists if we ever want the guard.
-- STATUS: waits for first clean PCL wave (September). Counts only.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — vintage clock starts at the PCL LEAD's treatmt_strt_dt
-- (offer-maturity curve, comparable across cells because leads are same-wave).
-- ============================================================================

WITH expt AS (
    SELECT visa_acct_no, clnt_no, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
leads AS (
    -- experiment-era PCL leads for experiment accounts (acct key, e3c-proven)
    SELECT e.tst_grp_cd, e.pass_flag,
           p.acct_no, p.treatmt_strt_dt, p.responder_cli, p.dt_cl_change,
           e.assign_dt
    FROM expt e
    JOIN dl_mr_prod.cards_pli_decision_resp p
      ON p.acct_no = e.visa_acct_no
     AND p.treatmt_strt_dt >= DATE '2026-08-14'
    WHERE p.treatmt_strt_dt >= e.assign_dt
),
denom AS (
    SELECT tst_grp_cd, pass_flag,
           TRUNC(treatmt_strt_dt, 'MON') AS pcl_wave_month,
           COUNT(*) AS cell_leads
    FROM leads
    GROUP BY 1, 2, 3
)
SELECT
    TRUNC(l.treatmt_strt_dt, 'MON')                        AS pcl_wave_month,  -- cohort = lead wave
    l.tst_grp_cd,
    l.pass_flag,
    (l.dt_cl_change - l.treatmt_strt_dt) / 7               AS week_bin,        -- 0 = days 0-6
    COUNT(*)                                               AS responders,
    MAX(d.cell_leads)                                      AS cell_leads       -- denominator
FROM leads l
JOIN denom d
  ON d.tst_grp_cd = l.tst_grp_cd AND d.pass_flag = l.pass_flag
 AND d.pcl_wave_month = TRUNC(l.treatmt_strt_dt, 'MON')
WHERE l.responder_cli = 1
  AND l.dt_cl_change >= l.treatmt_strt_dt   -- attribute event to this wave (Q12 guard)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
