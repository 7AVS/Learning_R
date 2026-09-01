-- ============================================================================
-- e13a — discover the credit-limit-increase EVENT CODE (raw success path)
-- Goal: a PCL success detector OUTSIDE the curated pipeline. DFP has no limit
-- column (documented; catalog bans inventing fields). Candidate source =
-- D3CV12A.CR_CRD_ACCT_EVNT_DLY (account event table; AUH uses dtl_evnt_typ_cd
-- = 191 for auth-user-add; columns per vintages/auh_vintage_monthly.sql).
-- Method: use PROVEN ground truth — 2026 PCL responders and their airtight
-- dt_cl_change dates — and see which event code(s) fire ON that exact day.
-- Decision (ONE read, ~10-30 rows): the code with near-total responder
-- coverage = the limit-change event. If NO code covers responders, the event
-- table doesn't carry limit changes -> fall back to p3c.appl_fact_dly
-- (cr_lmt_approved, the VBA/VBU success source).
-- Denominator for coverage: 698,696 responders in 2026 (e10 STMT 1).
-- Engine: TERADATA-DIRECT.
-- ============================================================================

WITH resp AS (
    SELECT acct_no, dt_cl_change
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE responder_cli = 1
      AND treatmt_strt_dt >= DATE '2026-01-01'
      AND dt_cl_change IS NOT NULL
)
SELECT
    e.dtl_evnt_typ_cd,
    COUNT(*)                    AS evnt_rows,
    COUNT(DISTINCT r.acct_no)   AS responder_accts_covered   -- vs 698,696 total
FROM resp r
JOIN D3CV12A.CR_CRD_ACCT_EVNT_DLY e
  ON e.acct_no = r.acct_no
 AND e.evnt_dt = r.dt_cl_change
GROUP BY 1
ORDER BY 3 DESC
