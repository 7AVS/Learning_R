-- ============================================================================
-- b1 — VBU model-offer baseline census (the two MODEL offers only)
-- Experiment target: CPX→AIB model path — AIB_25K_R_55 vs AIB_25K_NR
-- (the $55-rebate classifier decision). Before designing the 50/50 + holdout,
-- read the in-market history: cell structure, volumes, waves.
-- Decision (ONE read, ~30-50 rows):
--   1) Population per cohort month for each model offer (sizing for MDE).
--   2) Does a control exist WITHIN these offer cells (C_* group + BVBUNM*
--      action code + channel XX) -> quasi-holdout for a baseline causal read.
--   3) What N/N1/N2 test groups are, by volume + code pairing (they carry
--      ACTION codes, so NOT control; decode empirically).
-- Rules: ALL config from tactic ([[tactic-for-arms rule]]); TREATMT_MN 'NM' =
-- not-communicated (CRV/Virgile analogy — treat as hypothesis until volumes
-- confirm). Offer field @34 len 15 is fixed-width -> TRIM before comparing.
-- Engine: TERADATA-DIRECT.
-- ============================================================================
-- ANDRE: DECIDE — discovery floor treatmt_strt_dt >= DATE '2026-05-01'
--   (widen if the model offers deployed earlier).
-- ============================================================================

SELECT
    TRUNC(treatmt_strt_dt, 'MON')                       AS cohort_month,
    TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15))        AS offer,
    TST_GRP_CD,
    TREATMT_MN,
    CASE WHEN TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM'
         ELSE 'COMM' END                                AS comm_flag,   -- hypothesis column
    COUNT(*)                                            AS row_ct,
    COUNT(DISTINCT clnt_no)                             AS clnt_ct,
    MIN(treatmt_strt_dt)                                AS min_strt,
    MAX(treatmt_end_dt)                                 AS max_end
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'VBU'
  AND treatmt_strt_dt >= DATE '2026-05-01'
  AND TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15))
      IN ('AIB_25K_R_55', 'AIB_25K_NR')                 -- exact IN-list, the two MODEL offers
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4
