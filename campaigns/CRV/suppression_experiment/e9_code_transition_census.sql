-- ============================================================================
-- e9 — old/new strategy-code transition census (population completeness check)
-- Andre's challenge: the harness anchors population on treatmt_strt_dt >=
-- 2026-08-14. If any NEW-code deployment started early/late, the date floor
-- silently mis-scopes. Codes define the design; dates only describe it.
-- Decision this query answers (ONE read, ~10 rows — one per strategy code):
--   1) Earliest date each NEW code (PCRVRG12-15) appears — exactly 08-14?
--   2) Latest date each OLD code (PCRVRG05/06/09/10/11) appears — any overlap
--      past go-live?
--   3) Volume outside the floor: new-code rows BEFORE 08-14 = accounts the
--      current harness is missing (expect 0; any other answer -> switch the
--      harness population CTE to code-based selection).
--   4) @132 flag coverage per code (is the flag populated on old codes too?)
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — census window treatmt_strt_dt >= DATE '2026-06-01'
--   (2.5 months: enough to see the full transition; widen if old codes
--   predate June).
-- ============================================================================

SELECT
    rpt_grp_cd,
    MIN(treatmt_strt_dt)                    AS first_deploy_dt,
    MAX(treatmt_strt_dt)                    AS last_deploy_dt,
    COUNT(*)                                AS row_ct,
    COUNT(DISTINCT visa_acct_no)            AS acct_ct,
    -- rows the current 08-14 floor EXCLUDES (nonzero on a NEW code = harness gap)
    SUM(CASE WHEN treatmt_strt_dt <  DATE '2026-08-14' THEN 1 ELSE 0 END) AS rows_before_0814,
    SUM(CASE WHEN treatmt_strt_dt >= DATE '2026-08-14' THEN 1 ELSE 0 END) AS rows_from_0814,
    -- test-group mix (TG1 only exists in the new format)
    SUM(CASE WHEN tst_grp_cd = 'TG8' THEN 1 ELSE 0 END) AS tg8_ct,
    SUM(CASE WHEN tst_grp_cd = 'TG4' THEN 1 ELSE 0 END) AS tg4_ct,
    SUM(CASE WHEN tst_grp_cd = 'TG1' THEN 1 ELSE 0 END) AS tg1_ct,
    -- @132 flag coverage per code
    SUM(CASE WHEN substr(tactic_decisn_vrb_info, 132, 1) = 'Y' THEN 1 ELSE 0 END) AS flag_y,
    SUM(CASE WHEN substr(tactic_decisn_vrb_info, 132, 1) = 'N' THEN 1 ELSE 0 END) AS flag_n
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_strt_dt >= DATE '2026-06-01'
GROUP BY 1
ORDER BY 2, 1
