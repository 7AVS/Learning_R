-- 01_cohort_arm_summary.sql
-- CRV (installments) x Success Library reconciliation, step 1 of 5.
-- ENGINE   : Starburst/Trino (federated dg6v01 <-> edl0_im).
-- Output   : one row per cohort_month x arm: treated accounts, accounts with >=1
--            installment-purchase event in the Success Library. Counts only.
-- Arms     : DG6V01.TACTIC_EVNT_IP_AR_HIST, substr(tactic_id,8,3)='CRV',
--            tst_grp_cd='TG8' -> Control else Action
--            (source: campaigns/CRV/vintage_reconciliation/crv_vintage_v2_production.sql).
-- Cohort   : month of treatmt_eff_dt (effective date; treatmt_strt_dt misclassifies ~1.7%).
--            Same account in one (cohort_month, arm) cell across waves is collapsed with
--            MIN(treatmt_eff_dt)/MAX(treatmt_end_dt). Counted again in other cohorts.
-- Success  : edl0_im.prod_zp10_prod_staging.measurement_events_v2,
--            event_cd='p_card_installmt_purch', event_date inside the account's own
--            [treatmt_eff_dt, treatmt_end_dt] window (canonical CRV rule, no fixed N days).
-- Key      : CAST(visa_acct_no AS DECIMAL(38,0)) = CAST(acct_no AS DECIMAL(38,0)).
-- Guard    : population and arm come only from the tactic table.
-- Run date : not yet executed.

WITH

-- CRV deployments from 2026-01-01, collapsed to one row per account x cohort x arm
tactic_cohort AS (
    SELECT
        visa_acct_no,
        date_trunc('month', treatmt_eff_dt)                             AS cohort_month,
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END     AS arm,
        MIN(treatmt_eff_dt)                                             AS acct_treat_dt,
        MAX(treatmt_end_dt)                                             AS acct_treat_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_eff_dt >= DATE '2026-01-01'
      AND treatmt_eff_dt IS NOT NULL
    GROUP BY
        visa_acct_no,
        date_trunc('month', treatmt_eff_dt),
        CASE WHEN tst_grp_cd = 'TG8' THEN 'Control' ELSE 'Action' END
),

-- one row per cohort_month x arm: treated population + the cell's own treat-date bounds
cohort_arm_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT visa_acct_no) AS treated_accts,
        MIN(acct_treat_dt)           AS first_treat_dt,
        MAX(acct_treat_dt)           AS last_treat_dt
    FROM tactic_cohort
    GROUP BY cohort_month, arm
),

-- Success Library hits inside each account's own [treatmt_eff_dt, treatmt_end_dt] window.
-- Partition keys (event_cd, event_date) filtered before the join; account key normalized
-- via widening DECIMAL(38,0) cast on both sides (join-condition predicates don't push
-- down, so the constant event_date floor is kept alongside the dynamic window bound).
success_events AS (
    SELECT
        t.visa_acct_no,
        t.cohort_month,
        t.arm
    FROM tactic_cohort t
    JOIN edl0_im.prod_zp10_prod_staging.measurement_events_v2 m
        ON CAST(t.visa_acct_no AS DECIMAL(38,0)) = CAST(m.acct_no AS DECIMAL(38,0))
    WHERE m.event_cd = 'p_card_installmt_purch'
      AND m.event_date >= DATE '2026-01-01'
      AND m.event_date BETWEEN t.acct_treat_dt AND t.acct_treat_end_dt
),

-- accounts with >=1 success, per cohort_month x arm
success_cells AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT visa_acct_no) AS success_accts
    FROM success_events
    GROUP BY cohort_month, arm
)

SELECT
    c.cohort_month,
    c.arm,
    c.treated_accts,
    COALESCE(s.success_accts, 0) AS success_accts,
    c.first_treat_dt,
    c.last_treat_dt
FROM cohort_arm_cells c
LEFT JOIN success_cells s
    ON  s.cohort_month = c.cohort_month
    AND s.arm          = c.arm
ORDER BY c.cohort_month, c.arm;
