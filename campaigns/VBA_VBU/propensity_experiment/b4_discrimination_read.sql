-- ============================================================================
-- b4 — model discrimination read (the "free appetizer")
-- Score @21,8 VERIFIED 100% vs curated (t1); @50,1 = decile candidate
-- (1 = highest propensity; monotone vs score in t1 sample).
-- Two curves per offer: conversion by decile for COMM and for NOT_COMM.
--   * COMM curve rising toward decile 1  -> the model discriminates.
--   * COMM-minus-NOT_COMM gap by decile  -> where communication lift lives
--     (sizing input for the score-band design).
--   * NOT_COMM curve = organic gradient (holdout is random within the served
--     population -> causal read per decile, thin cells caveat ~150/decile).
-- Counts only; rates in the pivot. ~40 rows.
-- Engine: TERADATA-DIRECT.
-- ============================================================================

WITH expt AS (
    SELECT clnt_no, tactic_id,
           TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15)) AS offer,
           CASE WHEN TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM'
                ELSE 'COMM' END                         AS comm_flag,
           TRIM(substr(TACTIC_DECISN_VRB_INFO, 50, 2))  AS score_decile,   -- 1=best; 2 chars so
                                                                           -- decile 10 stops folding into 1 (b4 v1 bug)
           TRYCAST(TRIM(substr(TACTIC_DECISN_VRB_INFO, 21, 8))
                   AS DECIMAL(18,6))                    AS model_score
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'VBU'
      AND treatmt_strt_dt >= DATE '2026-06-01'
      AND TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15))
          IN ('AIB_25K_R_55', 'AIB_25K_NR')
)
SELECT
    e.offer,
    e.comm_flag,
    e.score_decile,
    COUNT(DISTINCT e.clnt_no)                          AS clnts,
    COUNT(DISTINCT CASE WHEN c.responder_targetproduct = 1
                        THEN c.clnt_no END)            AS resp_target,
    -- decile sanity: score ranges must be monotone across deciles
    MIN(e.model_score)                                 AS score_min,
    MAX(e.model_score)                                 AS score_max
FROM expt e
JOIN dl_mr_prod.cards_bizups_vbu_descresp_clnt c
  ON c.clnt_no   = e.clnt_no
 AND c.tactic_id = e.tactic_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
