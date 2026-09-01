-- ============================================================================
-- e2 — CRV live experiment: per-strategy cell structure (RPT_GRP_CD)
-- Follows e1 (2026-08-31): PCRVRG codes live in RPT_GRP_CD; TG1/TG4/TG8 + @132
-- flag confirmed live and populated on controls.
-- Decision this query answers (ONE read, ~30 rows):
--   1) WHICH PCRVRG codes are live (expect PCRVRG12/13/14/15; PCRVRG06 fate?)
--   2) Per strategy: does each show the designed cells (TG8 both flags,
--      TG4=Y only, TG1=N only)?
--   3) Does e1's yellow flag (TG8 5.18% among failers vs 4.92% among passers;
--      TG8 pass rate 13.54% vs treated 14.19%, z~4.5) concentrate in ONE
--      strategy (config issue) or spread uniformly (static-holdout drift)?
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — same floor as e1: TREATMT_STRT_DT >= DATE '2026-08-14'.
-- ============================================================================

SELECT
    TRUNC(treatmt_strt_dt, 'MON')                    AS cohort_month,
    rpt_grp_cd,                                          -- expect PCRVRG12..15
    tst_grp_cd,                                          -- TG8 / TG4 / TG1
    substr(tactic_decisn_vrb_info, 121, 8)           AS channel_at_121,
    substr(tactic_decisn_vrb_info, 132, 1)           AS flag_at_132,
    COUNT(*)                                         AS row_ct,           -- account grain (per e1)
    COUNT(DISTINCT clnt_no)                          AS clnt_ct
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_strt_dt >= DATE '2026-08-14'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 2, 3, 4, 5
