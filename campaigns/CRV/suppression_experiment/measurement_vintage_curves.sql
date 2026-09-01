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
-- ONE DEFINITION END TO END (fixed 2026-08-31 after Andre caught the split):
-- CRV event date = First_Response_Date ON THE SAME CURATED ROW as `responder`
-- (HELP TABLE 2026-08-31) — same table, same attribution as the summary; the
-- vintage MUST sum to the summary's crv_responders per cell (tie-out check;
-- resp_no_date column must be ~0 or the curve undercounts). Raw P3C path
-- (e6a) demoted to optional cross-check. PCL event date = dt_cl_change on the
-- curated PCL row (proven always-populated for responders, Q17b v1).
-- CAVEAT (PCL): increase_decrease is NOT filtered (Q17 convention) — "PLI
-- take" may include limit decreases; add the guard if a shipped number needs it.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- STMT 1 — CRV vintage (curated, account grain, deployment-exact join;
-- clock = weeks since ASSIGNMENT, event = First_Response_Date)
-- ---------------------------------------------------------------------------
WITH expt AS (
    SELECT visa_acct_no, tactic_id, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag,
           treatmt_strt_dt                        AS assign_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
),
offers AS (
    SELECT e.tst_grp_cd, e.pass_flag, e.assign_dt,
           c.acct_no, c.responder, c.first_response_date
    FROM expt e
    JOIN dl_mr_prod.cards_crv_install_decis_resp c
      ON c.acct_no   = e.visa_acct_no
     AND c.tactic_id = e.tactic_id                  -- deployment-exact
     AND c.offer_start_date >= DATE '2026-08-01'    -- loose pushdown only
),
denom AS (
    SELECT tst_grp_cd, pass_flag,
           COUNT(*)                                          AS cell_accts,
           SUM(CASE WHEN responder = 1
                     AND first_response_date IS NULL
                    THEN 1 ELSE 0 END)                       AS resp_no_date  -- must be ~0
    FROM offers
    GROUP BY 1, 2
)
SELECT
    TRUNC(o.assign_dt, 'MON')                          AS cohort_month,
    o.tst_grp_cd,
    o.pass_flag,
    (o.first_response_date - o.assign_dt) / 7          AS week_bin,     -- 0 = days 0-6
    COUNT(*)                                           AS responders,
    MAX(d.cell_accts)                                  AS cell_accts,   -- denominator
    MAX(d.resp_no_date)                                AS resp_no_date  -- undated responders (check)
FROM offers o
JOIN denom d
  ON d.tst_grp_cd = o.tst_grp_cd AND d.pass_flag = o.pass_flag
WHERE o.responder = 1
  AND o.first_response_date IS NOT NULL
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
