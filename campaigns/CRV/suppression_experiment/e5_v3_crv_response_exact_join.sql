-- ============================================================================
-- e5 v3 — CRV RESPONSE scoreboard, DEPLOYMENT-EXACT join (supersedes v2)
-- v2 joined acct + date-floor 08-13 (offset INFERRED from e8 account math).
-- v3 removes the inference: join on (acct_no, tactic_id) — the exact
-- deployment record. An old-format offer can NEVER match: old deployments
-- have different tactic_ids. Date floor kept only as a pushdown hint.
-- Canon: tactic_id unique per deployment; no time-window joins.
-- VALIDATION built in: v3 totals must reproduce v2 (offers=accts per cell,
-- ~1,169,835 total, responders 1,635/63/171/9). Any gap = curated lag or
-- tactic_id format mismatch (then check TRIM/typing before trusting either).
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — pushdown floor offer_start_date >= DATE '2026-08-01'
--   (deliberately loose: the JOIN carries the correctness, not the date).
-- ============================================================================

WITH expt AS (
    SELECT visa_acct_no, tactic_id, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
)
SELECT
    TRUNC(e.assign_dt, 'MON')          AS cohort_month,
    e.tst_grp_cd,
    e.pass_flag,
    c.action_control,                       -- check column (TG1 = null expected)
    COUNT(*)                           AS offers,
    COUNT(DISTINCT c.acct_no)          AS accts,               -- must equal offers
    SUM(c.responder)                   AS crv_responders,
    MIN(c.offer_start_date)            AS min_offer_start,     -- expect 08-13 (PMCS offset, now observed not assumed)
    MAX(c.offer_end_date)              AS max_offer_end
FROM expt e
JOIN dl_mr_prod.cards_crv_install_decis_resp c
  ON c.acct_no   = e.visa_acct_no
 AND c.tactic_id = e.tactic_id                  -- deployment-exact: the whole point of v3
 AND c.offer_start_date >= DATE '2026-08-01'    -- loose pushdown only
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
