-- ============================================================================
-- e5 — CRV RESPONSE read (what gating the banner costs CRV)
-- CRV response = card installment take-up (INSTL_TXN_DT) by experiment
-- accounts after their assignment. Counts only, no rates.
-- READS: TG8&Y vs TG4 = banner effect on installments among passers;
--        TG8&N vs TG1 = falsification (both dark).
-- CLIENT-LEVEL: the installment detail table is keyed by CLNT_NO only (same
-- join Amy's rule-1 SQL used) — counts here are clients, not accounts.
-- Runs NOW (installments flow continuously; no wave dependency).
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE
--   * Take-up window: INSTL_TXN_DT >= account's assign_dt (and >= 2026-08-14).
--   * Join pattern copied from Amy's rule-1 SQL:
--     CAST(tactic clnt_no AS VARCHAR(20)) = TRIM(instl clnt_no).
-- ============================================================================

WITH expt AS (
    -- frozen cell at first post-go-live assignment (same CTE as e4)
    SELECT visa_acct_no, clnt_no, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
expt_clnt AS (
    -- collapse to client grain for the client-keyed installment join;
    -- a client's cell = their first-assigned account's cell
    SELECT clnt_no, tst_grp_cd, pass_flag, MIN(assign_dt) AS assign_dt
    FROM expt
    GROUP BY 1, 2, 3
)
SELECT
    TRUNC(e.assign_dt, 'MON')          AS cohort_month,
    e.tst_grp_cd,
    e.pass_flag,
    TRUNC(s.INSTL_TXN_DT, 'MON')       AS instl_month,
    COUNT(DISTINCT e.clnt_no)          AS clients_with_takeup,
    COUNT(*)                           AS instl_txns
FROM expt_clnt e
JOIN P3C.CR_CRD_INSTL_ACTVAT_DTL s
  ON TRIM(s.CLNT_NO) = CAST(e.clnt_no AS VARCHAR(20))
 AND s.INSTL_TXN_DT >= DATE '2026-08-14'      -- redundant constant for pushdown
WHERE s.INSTL_TXN_DT >= e.assign_dt
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
