-- ============================================================================
-- b2 — tactic ↔ curated bridge proof (July wave, model offers only)
-- The pp_vbu v1 scar: a tactic-join once returned ZERO rows — so the bridge
-- is proven before any conversion is read. Curated grain = (clnt_no,
-- tactic_id) [locked 2026-08-18], so the join is deployment-exact by
-- construction (same rule as CRV's acct+tactic_id).
-- Decision (ONE read, ~4 rows):
--   1) Match rate per offer x comm_flag (expect ~100%; zero = scar repeats).
--   2) Cross-check: curated `control` column (literal Action/Control) vs our
--      TREATMT_MN-derived comm_flag — must agree (NOT_COMM=Control, COMM=Action).
-- NO conversion numbers yet — bridge first (b3 reads outcomes).
-- Engine: TERADATA-DIRECT.
-- ============================================================================

WITH expt AS (
    SELECT clnt_no, tactic_id,
           TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15)) AS offer,
           CASE WHEN TREATMT_MN LIKE 'BVBUNM%' THEN 'NOT_COMM'
                ELSE 'COMM' END                         AS comm_flag
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'VBU'
      AND treatmt_strt_dt >= DATE '2026-06-01'          -- both waves (Jun-13 + Jul-10 launches)
      AND TRIM(substr(TACTIC_DECISN_VRB_INFO, 34, 15))
          IN ('AIB_25K_R_55', 'AIB_25K_NR')
)
SELECT
    e.offer,
    e.comm_flag,
    COUNT(DISTINCT e.clnt_no)                          AS tactic_clnts,
    COUNT(DISTINCT c.clnt_no)                          AS matched_clnts,
    COUNT(DISTINCT CASE WHEN c.control = 'Action'
                        THEN c.clnt_no END)            AS curated_action,
    COUNT(DISTINCT CASE WHEN c.control = 'Control'
                        THEN c.clnt_no END)            AS curated_control
FROM expt e
LEFT JOIN dl_mr_prod.cards_bizups_vbu_descresp_clnt c
  ON c.clnt_no   = e.clnt_no
 AND c.tactic_id = e.tactic_id                          -- deployment-exact
GROUP BY 1, 2
ORDER BY 1, 2
