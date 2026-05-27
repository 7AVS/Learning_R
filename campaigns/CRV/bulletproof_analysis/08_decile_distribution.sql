-- PCL decile distribution across three cohorts: Action overlap, Control overlap, no-overlap PCL leads.
-- PCL-LEAD CENTRIC: unit = one PCL-mobile deployment per account. EXISTS flags replace LEFT JOIN + GROUP BY.
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
-- Single scan over pcl_universe; EXISTS flags both arms with no fan-out, no intermediate GROUP BY.
pcl_flagged AS (
    SELECT
        p.decile,
        p.new_decile,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM crv_action a
                WHERE a.acct_no           = p.acct_no
                  AND a.offer_start_date <= p.treatmt_end_dt
                  AND a.offer_end_date   >= p.treatmt_strt_dt
            ) THEN 1 ELSE 0
        END AS has_action_overlap,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM crv_control ct
                WHERE ct.acct_no           = p.acct_no
                  AND ct.offer_start_date <= p.treatmt_end_dt
                  AND ct.offer_end_date   >= p.treatmt_strt_dt
            ) THEN 1 ELSE 0
        END AS has_control_overlap
    FROM pcl_universe p
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
