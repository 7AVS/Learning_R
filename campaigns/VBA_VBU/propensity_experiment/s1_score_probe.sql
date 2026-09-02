-- ============================================================================
-- s1 — model score probe (two statements, one decision: where does the score
-- live and is it usable for the discrimination read + score-band sizing?)
-- STMT 1: CURATED side — Andre says the field is `model_score` on
--   cards_bizups_vbu_descresp_clnt. Coverage/range per wave x offer x arm.
--   CRITICAL: controls must carry scores too, or the appetizer is dead.
--   (If STMT 1 errors on the column name, run HELP TABLE and correct — the
--   field name is from voice notes, not verified.)
-- STMT 2: TACTIC side — eyeball hunt: dump the full 150-byte decisioning
--   string for a few COMM rows; scores are sometimes stuffed unstructured.
-- Engine: TERADATA-DIRECT.
-- ============================================================================

-- STMT 1 — curated score coverage (~12-18 rows)
SELECT
    TRUNC(e.treatmt_strt_dt, 'MON')                    AS cohort_month,
    TRIM(substr(e.TACTIC_DECISN_VRB_INFO, 34, 15))     AS offer,
    CASE WHEN e.TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM'
         ELSE 'COMM' END                               AS comm_flag,
    COUNT(DISTINCT e.clnt_no)                          AS clnts,
    SUM(CASE WHEN c.model_score IS NULL THEN 1 ELSE 0 END) AS score_null,
    MIN(c.model_score)                                 AS score_min,
    AVG(c.model_score)                                 AS score_avg,
    MAX(c.model_score)                                 AS score_max
FROM dg6v01.tactic_evnt_ip_ar_hist e
JOIN dl_mr_prod.cards_bizups_vbu_descresp_clnt c
  ON c.clnt_no   = e.clnt_no
 AND c.tactic_id = e.tactic_id
WHERE substr(e.tactic_id, 8, 3) = 'VBU'
  AND e.treatmt_strt_dt >= DATE '2026-06-01'
  AND TRIM(substr(e.TACTIC_DECISN_VRB_INFO, 34, 15))
      IN ('AIB_25K_R_55', 'AIB_25K_NR')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- STMT 2 — tactic-side eyeball: full decisioning string, a few rows per arm
SELECT TREATMT_MN, TST_GRP_CD,
       TACTIC_DECISN_VRB_INFO
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'VBU'
  AND treatmt_strt_dt >= DATE '2026-07-01'
  AND TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15)) = 'AIB_25K_R_55'
SAMPLE 5;
