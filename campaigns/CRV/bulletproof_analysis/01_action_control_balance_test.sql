-- Balance test: does the CRV Action:Control ratio hold in the PCL-mobile overlap subset?
-- Lead grain: each (CRV wave × account × arm) is one observation. No dedup.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        'Action' AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        'Control' AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
crv_all AS (
    SELECT acct_no, offer_start_date, offer_end_date, arm FROM crv_action
    UNION ALL
    SELECT acct_no, offer_start_date, offer_end_date, arm FROM crv_control
),
-- Full CRV population monthly counts (lead grain)
full_crv_monthly AS (
    SELECT
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS deploy_month,
        SUM(CASE WHEN arm = 'Action'  THEN 1 ELSE 0 END) AS action_count,
        SUM(CASE WHEN arm = 'Control' THEN 1 ELSE 0 END) AS control_count
    FROM crv_all
    GROUP BY offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)
),
-- Overlap subset: each CRV lead (wave × account) that overlaps any PCL-mobile deployment
-- One row per CRV lead (not deduped — each CRV lead is its own observation)
overlap_arms AS (
    SELECT
        c.acct_no,
        c.arm,
        c.offer_start_date - (EXTRACT(DAY FROM c.offer_start_date) - 1) AS deploy_month
    FROM crv_all c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.arm, c.offer_start_date
),
overlap_monthly AS (
    SELECT
        deploy_month,
        SUM(CASE WHEN arm = 'Action'  THEN 1 ELSE 0 END) AS action_count,
        SUM(CASE WHEN arm = 'Control' THEN 1 ELSE 0 END) AS control_count
    FROM overlap_arms
    GROUP BY deploy_month
)
SELECT
    'full_crv'  AS cohort,
    deploy_month,
    action_count,
    control_count
FROM full_crv_monthly

UNION ALL

SELECT
    'overlap_pcl_mobile' AS cohort,
    deploy_month,
    action_count,
    control_count
FROM overlap_monthly

ORDER BY cohort, deploy_month
;
