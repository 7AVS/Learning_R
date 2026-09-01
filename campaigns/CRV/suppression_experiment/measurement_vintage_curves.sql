-- ============================================================================
-- MEASUREMENT — VINTAGE CURVES (CRV + PCL)                      [FINAL FILE]
-- The permanent vintage builder for the CRV live experiment. Iterate HERE;
-- e6a/e6b are the frozen originals. Two statements:
--   STMT 1 — CRV response vintage: weeks from ASSIGNMENT to first installment
--            take, per cell                                      [runs weekly]
--   STMT 2 — PCL response vintage: weeks from PCL LEAD START to the
--            dt_cl_change limit-change event, per cell           [runs when a
--            post-go-live PCL wave is loaded; first = September 2026]
-- Output = long format, counts per weekly bin + cell denominator; cumulative
-- curve = running sum at read time (Excel/py). Same design, contrasts, and
-- hard rules as measurement_response_summary.sql (see its header).
-- CRV event dates come from the RAW installment table (P3C) — the curated
-- `responder` flag has no event date. PCL event date = dt_cl_change on the
-- curated PCL row (proven always-populated for responders, Q17b v1).
-- CAVEAT: increase_decrease is NOT filtered (Q17 convention) — "PLI take"
-- may include limit decreases; add the guard if a shipped number needs it.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- STMT 1 — CRV vintage (validated e6a; client grain — P3C is client-keyed)
-- ---------------------------------------------------------------------------
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
    SELECT e.clnt_no, e.tst_grp_cd, e.pass_flag, e.assign_dt,
           MIN(s.INSTL_TXN_DT) AS first_instl_dt
    FROM expt_clnt e
    JOIN P3C.CR_CRD_INSTL_ACTVAT_DTL s
      ON TRIM(s.CLNT_NO) = CAST(e.clnt_no AS VARCHAR(20))   -- Amy's rule-1 join pattern
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
    (f.first_instl_dt - f.assign_dt) / 7             AS week_bin,      -- 0 = days 0-6
    COUNT(*)                                         AS first_takers,
    MAX(d.cell_clients)                              AS cell_clients   -- denominator
FROM first_take f
JOIN denom d
  ON d.tst_grp_cd = f.tst_grp_cd AND d.pass_flag = f.pass_flag
GROUP BY 1, 2, 3, 4
ORDER BY 2, 3, 4;

-- ---------------------------------------------------------------------------
-- STMT 2 — PCL vintage (validated e6b; account grain; zero rows until a
-- post-08-14 PCL wave loads)
-- ---------------------------------------------------------------------------
WITH expt AS (
    SELECT visa_acct_no, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
leads AS (
    SELECT e.tst_grp_cd, e.pass_flag,
           p.acct_no, p.treatmt_strt_dt, p.responder_cli, p.dt_cl_change
    FROM expt e
    JOIN dl_mr_prod.cards_pli_decision_resp p
      ON p.acct_no = e.visa_acct_no
     AND p.treatmt_strt_dt >= DATE '2026-08-14'
    WHERE p.treatmt_strt_dt >= e.assign_dt
),
denom AS (
    SELECT tst_grp_cd, pass_flag,
           TRUNC(treatmt_strt_dt, 'MON') AS pcl_wave_month,
           COUNT(*) AS cell_leads
    FROM leads
    GROUP BY 1, 2, 3
)
SELECT
    TRUNC(l.treatmt_strt_dt, 'MON')                        AS pcl_wave_month,
    l.tst_grp_cd,
    l.pass_flag,
    (l.dt_cl_change - l.treatmt_strt_dt) / 7               AS week_bin,
    COUNT(*)                                               AS responders,
    MAX(d.cell_leads)                                      AS cell_leads
FROM leads l
JOIN denom d
  ON d.tst_grp_cd = l.tst_grp_cd AND d.pass_flag = l.pass_flag
 AND d.pcl_wave_month = TRUNC(l.treatmt_strt_dt, 'MON')
WHERE l.responder_cli = 1
  AND l.dt_cl_change >= l.treatmt_strt_dt      -- attribute event to this wave
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
