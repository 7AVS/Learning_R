-- crv_deployment_date_match_test.sql
-- Question: for the SAME account, does Data Lab's deployment date (offer_start_date)
-- equal tactic's deployment date (treatmt_strt_dt)? Hypothesis: they're a few days
-- apart, so at month boundaries the same deployment lands in different cohort months.
-- Each Data Lab deployment is paired to the account's NEAREST tactic date, then we
-- look at the day gap and whether the MONTH differs.
-- Engine: TERADATA-DIRECT (both tables EDW). No catalog prefix. Run block by block.

-- ============================================================================
-- BLOCK 1 -- HEADLINE: distribution of the day gap (tactic date - datalab date)
-- A spike at a single offset (e.g. +3) = the two sources systematically date the
-- same deployment a few days apart.
-- ============================================================================
WITH dl AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS k, offer_start_date
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE TRIM(action_control) IN ('Action','Control')
),
tac AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS k, treatmt_strt_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'CRV'
),
paired AS (
    SELECT dl.k, dl.offer_start_date, tac.treatmt_strt_dt,
           (tac.treatmt_strt_dt - dl.offer_start_date) AS diff_days
    FROM dl
    JOIN tac ON tac.k = dl.k
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dl.k, dl.offer_start_date
        ORDER BY ABS(tac.treatmt_strt_dt - dl.offer_start_date)
    ) = 1
)
SELECT diff_days, COUNT(*) AS n_deployments
FROM paired
GROUP BY diff_days
ORDER BY n_deployments DESC;

-- ============================================================================
-- BLOCK 2 -- THE MONTH TEST: how many deployments land in a DIFFERENT month
-- across the two sources (this is what shifts cohort_size per cohort_month).
-- ============================================================================
WITH dl AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS k, offer_start_date
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE TRIM(action_control) IN ('Action','Control')
),
tac AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS k, treatmt_strt_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'CRV'
),
paired AS (
    SELECT dl.k, dl.offer_start_date, tac.treatmt_strt_dt
    FROM dl
    JOIN tac ON tac.k = dl.k
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dl.k, dl.offer_start_date
        ORDER BY ABS(tac.treatmt_strt_dt - dl.offer_start_date)
    ) = 1
)
SELECT
    CASE WHEN (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1))
            = (treatmt_strt_dt  - (EXTRACT(DAY FROM treatmt_strt_dt)  - 1))
         THEN 'same_month' ELSE 'DIFFERENT_month' END AS month_match,
    COUNT(*) AS n_deployments
FROM paired
GROUP BY 1
ORDER BY n_deployments DESC;

-- ============================================================================
-- BLOCK 3 -- EVIDENCE: 100 sample accounts where the dates disagree, worst first.
-- See "this deployment is X, the tactic one is a few days later" per account.
-- ============================================================================
WITH dl AS (
    SELECT DISTINCT CAST(acct_no AS DECIMAL(38,0)) AS k, offer_start_date
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE TRIM(action_control) IN ('Action','Control')
),
tac AS (
    SELECT DISTINCT CAST(visa_acct_no AS DECIMAL(38,0)) AS k, treatmt_strt_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'CRV'
),
paired AS (
    SELECT dl.k, dl.offer_start_date, tac.treatmt_strt_dt,
           (tac.treatmt_strt_dt - dl.offer_start_date) AS diff_days
    FROM dl
    JOIN tac ON tac.k = dl.k
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dl.k, dl.offer_start_date
        ORDER BY ABS(tac.treatmt_strt_dt - dl.offer_start_date)
    ) = 1
)
SELECT TOP 100 k AS acct, offer_start_date, treatmt_strt_dt, diff_days
FROM paired
WHERE diff_days <> 0
ORDER BY ABS(diff_days) DESC;
