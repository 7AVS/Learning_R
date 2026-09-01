-- ============================================================================
-- e5 v2 — CRV RESPONSE summary, CURATED path (replaces raw-P3C e5 as primary;
-- raw e5 kept as cross-check). Simple, reusable cell scoreboard: one row per
-- experiment cell, counts only. Approved by e8 probe (2026-08-31).
-- READS: TG8&Y vs TG4 = banner effect on CRV take-up among passers;
--        TG8&N vs TG1 = falsification (e8 peek: identical, as designed).
--        NEVER TG4 vs TG1.
-- e8 TRAPS built in:
--   * TG1 rows have NULL action_control on curated -> arms come from the
--     TACTIC side (TG + flag), action_control shown only as a check column.
--   * offer_start_date = PMCS processing date, ~1 business day BEFORE
--     treatmt_strt_dt -> curated floor 2026-08-13, NOT 08-14.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — curated floor DATE '2026-08-13' (e8 date-offset finding).
--   Expected tie-out: total accts ≈ 1,169,843 (e1) minus stragglers.
-- ============================================================================

WITH expt AS (
    SELECT visa_acct_no, tst_grp_cd,
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
    c.action_control,                       -- check column only (TG1 = null, expected)
    COUNT(*)                           AS offers,
    COUNT(DISTINCT c.acct_no)          AS accts,
    SUM(c.responder)                   AS crv_responders,
    MIN(c.offer_start_date)            AS min_offer_start,
    MAX(c.offer_end_date)              AS max_offer_end     -- maturity horizon of this cohort
FROM expt e
JOIN dl_mr_prod.cards_crv_install_decis_resp c
  ON c.acct_no = e.visa_acct_no
 AND c.offer_start_date >= DATE '2026-08-13'   -- e8: PMCS date ~1bd before treatmt_strt_dt
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4
