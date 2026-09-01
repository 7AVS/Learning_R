-- ============================================================================
-- e6a — CRV RESPONSE VINTAGE (installment take-up curve since assignment)
-- One row per cell x week-since-assignment: how many clients took their FIRST
-- installment N weeks after assignment. Cumulative curve built at read time
-- (running sum over week_bin); denominators per cell included.
-- Runs NOW. Counts only. Client grain (installment table is client-keyed).
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — first-take only (repeat installments ignored); weekly bins.
-- ============================================================================

WITH expt AS (
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
    SELECT clnt_no, tst_grp_cd, pass_flag, MIN(assign_dt) AS assign_dt
    FROM expt
    GROUP BY 1, 2, 3
),
first_take AS (
    -- first installment activation per client on/after assignment
    SELECT e.clnt_no, e.tst_grp_cd, e.pass_flag, e.assign_dt,
           MIN(s.INSTL_TXN_DT) AS first_instl_dt
    FROM expt_clnt e
    JOIN P3C.CR_CRD_INSTL_ACTVAT_DTL s
      ON TRIM(s.CLNT_NO) = CAST(e.clnt_no AS VARCHAR(20))
     AND s.INSTL_TXN_DT >= DATE '2026-08-14'
    WHERE s.INSTL_TXN_DT >= e.assign_dt
    GROUP BY 1, 2, 3, 4
),
denom AS (
    SELECT tst_grp_cd, pass_flag, COUNT(*) AS cell_clients
    FROM expt_clnt
    GROUP BY 1, 2
)
SELECT
    TRUNC(f.assign_dt, 'MON')                        AS cohort_month,
    f.tst_grp_cd,
    f.pass_flag,
    (f.first_instl_dt - f.assign_dt) / 7             AS week_bin,     -- 0 = days 0-6
    COUNT(*)                                         AS first_takers,
    MAX(d.cell_clients)                              AS cell_clients  -- denominator, repeated per row
FROM first_take f
JOIN denom d
  ON d.tst_grp_cd = f.tst_grp_cd AND d.pass_flag = f.pass_flag
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4
