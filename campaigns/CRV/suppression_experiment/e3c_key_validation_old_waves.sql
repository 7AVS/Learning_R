-- ============================================================================
-- e3c — join-key validation against PRE-experiment PCL waves
-- e3b (2026-08-31): max PCL treatmt_strt_dt = 2026-08-05 -> e3's zeros were a
-- TIMING gap (no experiment-era PCL waves yet), not a key failure. But the key
-- itself is still unproven. This validates it on June-August waves so e4 can
-- run the day the September wave loads.
-- Decision (ONE read, ~8 rows): does visa_acct_no = pcl.acct_no bridge, does
-- clnt_no bridge, and at what match rate per cell?
-- NOTE: matches here are DESCRIPTIVE ONLY (pre-experiment waves; arms did not
-- exist yet). Never read conversion off this.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — PCL wave floor DATE '2026-06-01' (last ~3 pre-experiment waves).
-- ============================================================================

WITH expt AS (
    SELECT DISTINCT
        visa_acct_no,
        clnt_no,
        tst_grp_cd,
        substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
),
pcl AS (
    SELECT acct_no, clnt_no, treatmt_strt_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt BETWEEN DATE '2026-06-01' AND DATE '2026-08-13'
)
SELECT
    CAST('acct_key' AS VARCHAR(10))            AS join_key,
    e.tst_grp_cd,
    e.pass_flag,
    COUNT(DISTINCT e.visa_acct_no)             AS expt_accts,
    COUNT(DISTINCT p.acct_no)                  AS matched_accts,
    COUNT(p.acct_no)                           AS pcl_leads
FROM expt e
LEFT JOIN pcl p
  ON p.acct_no = e.visa_acct_no
GROUP BY 2, 3

UNION ALL

SELECT
    CAST('clnt_key' AS VARCHAR(10))            AS join_key,
    e.tst_grp_cd,
    e.pass_flag,
    COUNT(DISTINCT e.visa_acct_no)             AS expt_accts,
    COUNT(DISTINCT p.clnt_no)                  AS matched_clnts,
    COUNT(p.clnt_no)                           AS pcl_leads
FROM expt e
LEFT JOIN pcl p
  ON p.clnt_no = e.clnt_no
GROUP BY 2, 3

ORDER BY 1, 2, 3
