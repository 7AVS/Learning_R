-- PCL decile distribution across three cohorts: Action overlap, Control overlap, no-overlap PCL leads.
-- PCL-LEAD CENTRIC: unit = one PCL-mobile deployment per account. Match CTEs replace EXISTS-in-CASE.
-- Counts only. Andre computes shares in Excel.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        decile,
        new_decile
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- Teradata-safe: EXISTS is illegal inside CASE. Overlap is resolved in match CTEs
-- (non-equi join on acct + window intersection); LEFT JOIN + IS NOT NULL gives the flag.
-- Unique on (acct_no, treatmt_strt_dt, treatmt_end_dt) so no row multiplication on join-back.
action_match AS (
    SELECT
        p.acct_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt
    FROM pcl_universe p
    JOIN crv_action ca
      ON ca.acct_no           = p.acct_no
     AND ca.offer_start_date <= p.treatmt_end_dt
     AND ca.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
control_match AS (
    SELECT
        p.acct_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt
    FROM pcl_universe p
    JOIN crv_control cc
      ON cc.acct_no           = p.acct_no
     AND cc.offer_start_date <= p.treatmt_end_dt
     AND cc.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
pcl_flagged AS (
    SELECT
        p.decile,
        p.new_decile,
        CASE WHEN am.acct_no  IS NOT NULL THEN 1 ELSE 0 END AS has_action_overlap,
        CASE WHEN cm1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_control_overlap
    FROM pcl_universe p
    LEFT JOIN action_match am
      ON am.acct_no         = p.acct_no
     AND am.treatmt_strt_dt = p.treatmt_strt_dt
     AND am.treatmt_end_dt  = p.treatmt_end_dt
    LEFT JOIN control_match cm1
      ON cm1.acct_no         = p.acct_no
     AND cm1.treatmt_strt_dt = p.treatmt_strt_dt
     AND cm1.treatmt_end_dt  = p.treatmt_end_dt
),
labeled AS (
    SELECT
        decile,
        new_decile,
        CASE
            WHEN has_action_overlap  = 1 THEN CAST('action_overlap'  AS VARCHAR(20))
            WHEN has_control_overlap = 1 THEN CAST('control_overlap' AS VARCHAR(20))
            ELSE                              CAST('no_overlap'       AS VARCHAR(20))
        END AS cohort
    FROM pcl_flagged
)
SELECT
    cohort,
    decile,
    new_decile,
    COUNT(*) AS n_leads
FROM labeled
GROUP BY cohort, decile, new_decile
ORDER BY 1, 2, 3
;
