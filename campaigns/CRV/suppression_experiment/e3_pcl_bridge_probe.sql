-- ============================================================================
-- e3 — CRV live experiment × PCL: join-key bridge + overlap volume probe
-- Decision this query answers (ONE read, ~8 rows):
--   1) Which key bridges the tactic table to the PCL curated table —
--      visa_acct_no = acct_no (assumed, NEVER verified) or clnt_no?
--   2) How many experiment accounts per cell (TG × flag) have a PCL lead
--      starting on/after 2026-08-14? (volume check before any conversion read)
-- NO conversion numbers here on purpose — key + volume first (e4 = the read).
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE
--   * PCL lead scope = treatmt_strt_dt >= DATE '2026-08-14' (leads born during
--     the experiment). No interval-intersection: experiment membership is
--     sticky from go-live.
--   * No PCL channel filter in THIS probe (volumes for all channels; the
--     %MB% vs %IM% decision applies to e4, the conversion read).
-- ============================================================================

WITH expt AS (
    -- experiment population at account grain (e1/e2: acct_ct = row_ct per cell)
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
    -- PCL leads born during the experiment (curated table, Q04-proven source)
    SELECT acct_no, clnt_no, treatmt_strt_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-08-14'
)
-- account-key bridge: visa_acct_no = pcl.acct_no
SELECT
    CAST('acct_key' AS VARCHAR(10))            AS join_key,     -- cast: Teradata UNION truncation quirk
    e.tst_grp_cd,
    e.pass_flag,
    COUNT(DISTINCT e.visa_acct_no)             AS expt_accts,
    COUNT(DISTINCT p.acct_no)                  AS accts_with_pcl_lead,
    COUNT(p.acct_no)                           AS pcl_leads
FROM expt e
LEFT JOIN pcl p
  ON p.acct_no = e.visa_acct_no
GROUP BY 2, 3

UNION ALL

-- client-key bridge: clnt_no = clnt_no
SELECT
    CAST('clnt_key' AS VARCHAR(10))            AS join_key,
    e.tst_grp_cd,
    e.pass_flag,
    COUNT(DISTINCT e.visa_acct_no)             AS expt_accts,
    COUNT(DISTINCT p.clnt_no)                  AS accts_with_pcl_lead,   -- clients here, label kept for shape
    COUNT(p.clnt_no)                           AS pcl_leads
FROM expt e
LEFT JOIN pcl p
  ON p.clnt_no = e.clnt_no
GROUP BY 2, 3

ORDER BY 1, 2, 3
