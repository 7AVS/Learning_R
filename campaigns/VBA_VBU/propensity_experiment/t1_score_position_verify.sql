-- ============================================================================
-- t1 — verify the tactic-side score position against curated ground truth
-- Andre's find: TACTIC_DECISN_VRB_INFO @21 len 8 = model score (confident);
-- @50 = decile? (unsure — sampled wide for eyeball).
-- Ground truth: curated model_score on the same (clnt_no, tactic_id).
-- STMT 1 (10 rows): side-by-side eyeball — raw @21 string vs model_score,
--   plus @50 area (4 chars) and the offer for orientation.
-- STMT 2 (1 row): match rate — parsed @21 vs model_score within 1e-4;
--   TRYCAST returns NULL on junk instead of erroring.
-- If match ~100% -> score comes from TACTIC (config side stays pure);
-- curated becomes cross-check only, consistent with the architecture rule.
-- Engine: TERADATA-DIRECT.
-- ============================================================================

-- STMT 1 — eyeball sample
SELECT e.clnt_no,
       substr(e.TACTIC_DECISN_VRB_INFO, 21, 8)  AS tactic_score_raw,
       c.model_score                            AS curated_model_score,
       substr(e.TACTIC_DECISN_VRB_INFO, 50, 4)  AS pos50_area,
       TRIM(substr(e.TACTIC_DECISN_VRB_INFO, 34, 15)) AS offer,
       CASE WHEN e.TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM' ELSE 'COMM' END AS comm_flag
FROM dg6v01.tactic_evnt_ip_ar_hist e
JOIN dl_mr_prod.cards_bizups_vbu_descresp_clnt c
  ON c.clnt_no = e.clnt_no AND c.tactic_id = e.tactic_id
WHERE substr(e.tactic_id, 8, 3) = 'VBU'
  AND e.treatmt_strt_dt >= DATE '2026-06-01'
  AND TRIM(substr(e.TACTIC_DECISN_VRB_INFO, 34, 15))
      IN ('AIB_25K_R_55', 'AIB_25K_NR')
SAMPLE 10;

-- STMT 2 — match rate (one row)
SELECT
    COUNT(*)                                            AS joined_rows,
    SUM(CASE WHEN TRYCAST(TRIM(substr(e.TACTIC_DECISN_VRB_INFO,21,8)) AS DECIMAL(18,6))
              IS NOT NULL THEN 1 ELSE 0 END)            AS tactic_score_parsed,
    SUM(CASE WHEN c.model_score IS NOT NULL THEN 1 ELSE 0 END) AS curated_score_present,
    SUM(CASE WHEN ABS(TRYCAST(TRIM(substr(e.TACTIC_DECISN_VRB_INFO,21,8)) AS DECIMAL(18,6))
                      - c.model_score) < 0.0001 THEN 1 ELSE 0 END) AS scores_match,
    MIN(TRYCAST(TRIM(substr(e.TACTIC_DECISN_VRB_INFO,21,8)) AS DECIMAL(18,6))) AS tactic_min,
    MAX(TRYCAST(TRIM(substr(e.TACTIC_DECISN_VRB_INFO,21,8)) AS DECIMAL(18,6))) AS tactic_max,
    MIN(c.model_score)                                  AS curated_min,
    MAX(c.model_score)                                  AS curated_max
FROM dg6v01.tactic_evnt_ip_ar_hist e
JOIN dl_mr_prod.cards_bizups_vbu_descresp_clnt c
  ON c.clnt_no = e.clnt_no AND c.tactic_id = e.tactic_id
WHERE substr(e.tactic_id, 8, 3) = 'VBU'
  AND e.treatmt_strt_dt >= DATE '2026-06-01'
  AND TRIM(substr(e.TACTIC_DECISN_VRB_INFO, 34, 15))
      IN ('AIB_25K_R_55', 'AIB_25K_NR');
