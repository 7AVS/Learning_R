-- crv_feb_shortfall_proof.sql
-- PROOF of why the tactic Feb cohort has FEWER accounts than the curated Feb cohort.
-- Take the curated Feb-2026 Action accounts, and look them up in tactic RESTRICTED
-- to the Feb-2026 CRV wave (Jan-Apr 2026 only) -- so we do NOT drag in the prior-year
-- deployments those same accounts also had.
-- Read: of the curated Feb-Action accounts, how many land in tactic month = Feb,
-- how many SPILL to Jan/Mar (tactic dates the same deployment a few days off), and how
-- many sit under TG8 instead of TG4. Spill + arm-flip = the shortfall.
-- Engine: TERADATA-DIRECT (both EDW).

WITH cur AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS k
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) = DATE '2026-02-01'
      AND TRIM(action_control) = 'Action'
)
SELECT
    (t.treatmt_strt_dt - (EXTRACT(DAY FROM t.treatmt_strt_dt) - 1)) AS tactic_month,
    t.tst_grp_cd,
    COUNT(DISTINCT t.visa_acct_no) AS n_accts
FROM cur c
JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
    ON  CAST(t.visa_acct_no AS DECIMAL(38,0)) = c.k
    AND SUBSTR(t.tactic_id, 8, 3) = 'CRV'
    AND t.treatmt_strt_dt BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
GROUP BY 1, 2
ORDER BY tactic_month, t.tst_grp_cd;

-- How to read it:
--   curated Feb Action = ~1,831,016 accounts (your number).
--   Sum of TG4 rows at tactic_month=2026-02 should be close to that.
--   Any TG4 rows at 2026-01 or 2026-03 = accounts the tactic DATE pushed into a
--     different month  -> they leave the tactic Feb cohort  -> tactic Feb < curated Feb.
--   Any TG8 rows = accounts the curated calls Action but tactic tags Control.
--   If the total across all rows is well BELOW 1,831,016, some curated accounts have
--     NO Jan-Apr 2026 CRV tactic wave at all -> that is a different problem (curated
--     carries accounts not deployed in this wave), and this proves that too.
