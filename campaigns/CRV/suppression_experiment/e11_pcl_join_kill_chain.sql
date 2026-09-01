-- ============================================================================
-- e11 — where do the ~140K Aug-14-actual PCL leads die in our join?
-- measurement_response_summary STMT 2 matched only 3 leads — ~3 orders of
-- magnitude below expectation. This walks the kill chain (ONE row):
--   leads_total          : PCL leads actually in market >= 2026-08-14 (e10b: ~140K)
--   leads_on_expt_accts  : ...whose acct_no is an experiment account (any assign date)
--   leads_after_assign   : ...AND actual_strt_dt >= that account's assign_dt
-- Readings:
--   leads_on_expt_accts tiny  -> population disjunction (Aug-14 PCL wave largely
--     excludes CRV-decisioned accounts — orchestration suppression? different
--     product base?) or a key/type issue specific to this slice.
--   big -> tiny at stage 3    -> assign-date condition is the killer (leads on
--     accounts assigned AFTER Aug 14) -> rethink scope rule.
-- Engine: TERADATA-DIRECT.
-- ============================================================================

WITH expt AS (
    SELECT visa_acct_no, treatmt_strt_dt AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
pcl AS (
    SELECT acct_no, clnt_no, actual_strt_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2026-06-01'
      AND actual_strt_dt  >= DATE '2026-08-14'
)
SELECT
    COUNT(*)                                            AS leads_total,
    COUNT(DISTINCT p.acct_no)                           AS accts_total,
    SUM(CASE WHEN e.visa_acct_no IS NOT NULL
             THEN 1 ELSE 0 END)                         AS leads_on_expt_accts,
    SUM(CASE WHEN e.visa_acct_no IS NOT NULL
              AND p.actual_strt_dt >= e.assign_dt
             THEN 1 ELSE 0 END)                         AS leads_after_assign,
    MIN(p.actual_strt_dt)                               AS min_actual,
    MAX(p.actual_strt_dt)                               AS max_actual
FROM pcl p
LEFT JOIN expt e
  ON e.visa_acct_no = p.acct_no
