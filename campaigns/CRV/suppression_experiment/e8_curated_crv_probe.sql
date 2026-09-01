-- ============================================================================
-- e8 — curated CRV table probe: can e5/e6a switch off the raw installment table?
-- Curated = dl_mr_prod.cards_crv_install_decis_resp (fields catalogued
-- 2026-06-02: acct_no, tactic_id, offer_start_date/offer_end_date,
-- action_control, channels_deployed, responder, decile, new_decile).
-- Decision this query answers (ONE read, ~20-30 rows):
--   1) COVERAGE: does the curated table carry the experiment deployments
--      (offers >= 2026-08-14), incl. TG1 blocked accounts and TG8 controls?
--   2) ARM MAPPING: how does action_control (Action/Control) line up with our
--      TG8/TG4/TG1 cells? (Is TG1 'Action'? Is TG8 'Control'?)
--   3) RESPONDER FIELD: what raw values does `responder` take per cell (type/
--      coding never verified on CRV curated — output the value itself).
-- If coverage + mapping are clean -> e5/e6a get curated twins (acct grain,
-- responder-based) and the raw P3C path becomes the cross-check.
-- Engine: TERADATA-DIRECT syntax.
-- ============================================================================
-- ANDRE: DECIDE — curated scope = offer_start_date >= DATE '2026-08-14'.
-- ============================================================================

WITH expt AS (
    SELECT visa_acct_no, tst_grp_cd,
           substr(tactic_decisn_vrb_info, 132, 1) AS pass_flag
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2026-08-14'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY visa_acct_no
                               ORDER BY treatmt_strt_dt, tactic_id) = 1
)
SELECT
    TRUNC(c.offer_start_date, 'MON')       AS cohort_month,
    COALESCE(e.tst_grp_cd, 'NOT_IN_EXPT')  AS tst_grp_cd,     -- curated rows missing from tactic pull
    COALESCE(e.pass_flag,  '-')            AS pass_flag,
    c.action_control,
    c.responder,                                -- raw value on purpose (coding unverified)
    COUNT(*)                               AS row_ct,
    COUNT(DISTINCT c.acct_no)              AS acct_ct,
    MIN(c.offer_start_date)                AS min_offer_start,
    MAX(c.offer_start_date)                AS max_offer_start
FROM dl_mr_prod.cards_crv_install_decis_resp c
LEFT JOIN expt e
  ON e.visa_acct_no = c.acct_no
WHERE c.offer_start_date >= DATE '2026-08-14'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
