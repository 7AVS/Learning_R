-- ============================================================================
-- b3 — baseline conversion read (model offers, per wave x offer x arm)
-- Bridge proven 100% (b2). This reads the outcome: target-product upgrades
-- per cell, counts only. Feeds: (1) per-offer communication lift (causal —
-- borrowed randomization from the NM holdout), (2) baseline levels for the
-- MDE design of the propensity experiment.
-- Decision (ONE read, ~12-18 rows): baseline rates per wave; lift per offer;
-- wave count (b2 totals suggest an August wave beyond Jun/Jul — cohort_month
-- will show it).
-- Grain guard: curated has ~810 dup rows — count DISTINCT clients, never SUM.
-- Outcome = responder_targetproduct (0/1; never the CHAR `responder`).
-- Engine: TERADATA-DIRECT.
-- ============================================================================

WITH expt AS (
    SELECT clnt_no, tactic_id,
           TRUNC(treatmt_strt_dt, 'MON')                AS cohort_month,
           TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15)) AS offer,
           CASE WHEN TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM'
                ELSE 'COMM' END                         AS comm_flag
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'VBU'
      AND treatmt_strt_dt >= DATE '2026-06-01'
      AND TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15))
          IN ('AIB_25K_R_55', 'AIB_25K_NR')
)
SELECT
    e.cohort_month,
    e.offer,
    e.comm_flag,
    COUNT(DISTINCT e.clnt_no)                          AS clnts,
    COUNT(DISTINCT CASE WHEN c.responder_targetproduct = 1
                        THEN c.clnt_no END)            AS resp_target,
    COUNT(DISTINCT CASE WHEN c.responder_anyproduct = 1
                        THEN c.clnt_no END)            AS resp_any,
    -- maturity context
    CURRENT_DATE                                       AS run_dt,
    MAX(CASE WHEN c.responder_targetproduct = 1
             THEN c.dt_prod_change_client END)         AS last_change_dt,
    MIN(c.response_start)                              AS min_resp_start,
    MAX(c.response_end)                                AS max_resp_end
FROM expt e
JOIN dl_mr_prod.cards_bizups_vbu_descresp_clnt c
  ON c.clnt_no   = e.clnt_no
 AND c.tactic_id = e.tactic_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
