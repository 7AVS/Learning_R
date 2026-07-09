-- crv_feb_cell_scatter_diagnostic.sql
-- Purpose: explain the paradox — tactic population is BIGGER than Data Lab overall
-- (only_datalab=0, tactic has ~1M extra non-TG4/TG8 accounts), yet per-cohort-arm
-- tactic comes out SMALLER (Feb Action: tactic 1,802,110 vs datalab 1,831,016).
-- Take Data Lab's (Feb, Action) accounts and see WHERE tactic actually bins them.
-- If they scatter across other months or into TG8, that re-binning IS the per-cell gap.
-- tac_month IS NULL = account truly not in tactic (expected ~0, since only_datalab=0).
--
-- Engine: TERADATA-DIRECT. Both tables are EDW (no EDL here) — no catalog prefix,
-- Teradata syntax (first-of-month via EXTRACT, SUBSTR). Account keys normalized to
-- numeric (visa_acct_no vs curated acct_no may be stored as different types/formats).

WITH dl AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS k
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) = DATE '2026-02-01'
      AND TRIM(action_control) = 'Action'
),
tac AS (
    SELECT
        CAST(visa_acct_no AS DECIMAL(38,0))                          AS k,
        (treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1))  AS tac_month,
        tst_grp_cd
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'CRV'
)
SELECT
    t.tac_month,
    t.tst_grp_cd,
    COUNT(DISTINCT dl.k) AS n_accts
FROM dl
LEFT JOIN tac t
    ON t.k = dl.k
GROUP BY t.tac_month, t.tst_grp_cd
ORDER BY n_accts DESC;
