-- Balance test: does the CRV Action:Control ratio hold in the PCL-mobile overlap subset?
-- Lead grain: each (CRV wave x account x arm) is one observation. No dedup.
-- One row per month with full-CRV and overlap counts side-by-side for direct comparison.

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
        CAST('Action' AS VARCHAR(10)) AS arm
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
        CAST('Control' AS VARCHAR(10)) AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
crv_all AS (
    SELECT acct_no, offer_start_date, offer_end_date, arm FROM crv_action
    UNION ALL
    SELECT acct_no, offer_start_date, offer_end_date, arm FROM crv_control
),
full_crv_monthly AS (
    SELECT
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS deploy_month,
        SUM(CASE WHEN arm = 'Action'  THEN 1 ELSE 0 END) AS full_action_leads,
        SUM(CASE WHEN arm = 'Control' THEN 1 ELSE 0 END) AS full_control_leads
    FROM crv_all
    GROUP BY 1
),
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
        SUM(CASE WHEN arm = 'Action'  THEN 1 ELSE 0 END) AS overlap_action_leads,
        SUM(CASE WHEN arm = 'Control' THEN 1 ELSE 0 END) AS overlap_control_leads
    FROM overlap_arms
    GROUP BY deploy_month
)
SELECT
    f.deploy_month,
    f.full_action_leads,
    f.full_control_leads,
    o.overlap_action_leads,
    o.overlap_control_leads
FROM full_crv_monthly f
LEFT JOIN overlap_monthly o
  ON f.deploy_month = o.deploy_month
ORDER BY 1
;
