-- ============================================================================
-- e1 — CRV live-experiment deployment probe (design vs backend)
-- Experiment live 2026-08-14: PCRVRG11/09/10/05 -> PCRVRG12/13/14/15,
-- routing: pass either of 2 rules -> TG4 (banner), fail both -> TG1 (blocked),
-- TG8 = 5% static do-not-contact control.
-- Decision this query answers (ONE read, ~10-20 rows):
--   1) Which test groups exist post-go-live? (TG1 present = new design live)
--   2) Do TG8 shares look ~5%? (formal SRM test = next step, after this)
--   3) Channel mix per TG (@121): does any TG1 row carry IM? (spec anomaly check)
--   4) Is @132 (pass-2-rules flag) populated — INCLUDING on TG8 control rows?
--   5) Where do the PCRVRG strategy codes live? (5 indicator columns; codes
--      were found NOWHERE in prior repo queries — do not assume a location)
-- Engine: Starburst/Trino federation (Trino syntax; DG6V01 addressed bare).
-- ============================================================================
-- ANDRE: DECIDE
--   * Date floor = DATE '2026-08-14' on TREATMT_STRT_DT (go-live per Andre).
--     TREATMT_EFF_DT is the field that matched curated offer_start_date in
--     vintage work — if this probe returns nothing, rerun floor on EFF_DT.
--   * @121 width 8 (channel; verified in prior CRV queries).
--   * @132 width 1 (Amy's pass-2-rules flag, per Virgile's email). UNVERIFIED:
--     the email describes the DMC file; TACTIC_DECISN_VRB_INFO may not align
--     byte-for-byte. If flag_at_132 comes back constant/blank, next probe
--     dumps bytes 121-150 in chunks.
-- ============================================================================

SELECT
    date_trunc('month', treatmt_strt_dt)             AS cohort_month,
    tst_grp_cd,                                          -- expect TG8 / TG4 / TG1
    substr(tactic_decisn_vrb_info, 121, 8)           AS channel_at_121,  -- expect EM_IM_DO / IM / blank-XX
    substr(tactic_decisn_vrb_info, 132, 1)           AS flag_at_132,     -- pass-2-rules flag (must exist on TG8 too)
    -- volumes: grain is row-level; client vs account counts shown side by side
    COUNT(*)                                         AS row_ct,
    COUNT(DISTINCT clnt_no)                          AS clnt_ct,
    COUNT(DISTINCT visa_acct_no)                     AS acct_ct,
    COUNT(DISTINCT tactic_id)                        AS tactic_id_ct,
    -- treatment-window sanity: everything should start on/after 2026-08-14
    MIN(treatmt_strt_dt)                             AS min_strt_dt,
    MAX(treatmt_strt_dt)                             AS max_strt_dt,
    -- eyeball samples: if the strategy code sits inside TACTIC_ID, it shows here
    MIN(tactic_id)                                   AS tactic_id_sample_min,
    MAX(tactic_id)                                   AS tactic_id_sample_max,
    -- strategy-code hunt: 1 = the string 'CRVRG' (matches PCRVRG05..15) appears
    -- in that column for at least one row of this cell
    MAX(CASE WHEN tactic_id              LIKE '%CRVRG%' THEN 1 ELSE 0 END) AS code_in_tactic_id,
    MAX(CASE WHEN tactic_cell_cd         LIKE '%CRVRG%' THEN 1 ELSE 0 END) AS code_in_cell_cd,
    MAX(CASE WHEN rpt_grp_cd             LIKE '%CRVRG%' THEN 1 ELSE 0 END) AS code_in_rpt_grp_cd,
    MAX(CASE WHEN tactic_decisn_vrb_info LIKE '%CRVRG%' THEN 1 ELSE 0 END) AS code_in_decisn_vrb,
    MAX(CASE WHEN addnl_decisn_data1     LIKE '%CRVRG%' THEN 1 ELSE 0 END) AS code_in_addnl_data1
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'          -- MNE at positions 8-10
  AND treatmt_strt_dt >= DATE '2026-08-14'     -- go-live floor (pushdown constant)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
