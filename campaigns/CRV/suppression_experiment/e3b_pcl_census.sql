-- ============================================================================
-- e3b — PCL curated table census (diagnosis of e3's all-zero result)
-- e3 (2026-08-31) matched ZERO PCL leads post-2026-08-14 on BOTH keys.
-- Decision this query answers (ONE read, ~15-25 rows):
--   1) Does dl_mr_prod.cards_pli_decision_resp contain ANY leads with
--      treatmt_strt_dt after 2026-08-14? (load lag / monthly wave timing vs
--      a real key mismatch)
--   2) What are the actual CHANNEL values and volumes by month (MB? IM?) —
--      settles the channel-filter question for e4 at the same time.
-- If Aug-14+ rows EXIST here but e3 matched zero -> key/type mismatch is real,
-- next probe compares key formats side by side. If none exist -> wait for load
-- (find out the refresh cadence) and rerun e3 later.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — census floor DATE '2026-01-01' (recent months only; enough
-- to see cadence and the Aug cutoff).
-- ============================================================================

SELECT
    TRUNC(treatmt_strt_dt, 'MON')   AS cohort_month,
    channel,
    COUNT(*)                        AS lead_ct,
    COUNT(DISTINCT acct_no)         AS acct_ct,
    COUNT(DISTINCT clnt_no)         AS clnt_ct,
    MIN(treatmt_strt_dt)            AS min_strt_dt,
    MAX(treatmt_strt_dt)            AS max_strt_dt
FROM dl_mr_prod.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2026-01-01'
GROUP BY 1, 2
ORDER BY 1, 2
