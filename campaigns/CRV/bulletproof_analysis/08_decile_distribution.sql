-- PCL decile distribution across three cohorts: Action overlap, Control overlap, no-overlap PCL leads.
-- CRV decile not available on curated table. PCL decile and new_decile only.
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
      AND channel LIKE '%IM%'
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
-- Flag each PCL lead by cohort: Action overlap, Control overlap, neither
-- A PCL lead can match both arms across different waves — classify per earliest CRV match found
pcl_flagged AS (
    SELECT
        p.acct_no,
        p.treatmt_strt_dt,
        p.decile,
        p.new_decile,
        MAX(CASE WHEN a.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS has_action_overlap,
        MAX(CASE WHEN ct.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS has_control_overlap
    FROM pcl_universe p
    LEFT JOIN crv_action a
      ON a.acct_no           = p.acct_no
     AND a.offer_start_date <= p.treatmt_end_dt
     AND a.offer_end_date   >= p.treatmt_strt_dt
    LEFT JOIN crv_control ct
      ON ct.acct_no           = p.acct_no
     AND ct.offer_start_date <= p.treatmt_end_dt
     AND ct.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.decile, p.new_decile
),
labeled AS (
    SELECT
        decile,
        new_decile,
        CASE
            WHEN has_action_overlap  = 1 THEN 'action_overlap'
            WHEN has_control_overlap = 1 THEN 'control_overlap'
            ELSE 'no_overlap'
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
ORDER BY cohort, decile, new_decile
;
