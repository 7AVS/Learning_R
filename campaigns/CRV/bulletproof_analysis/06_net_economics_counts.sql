-- Net economics: counts of leads, CRV responders, and PCL responders in the overlap cohort.
-- Counts only. No rates, no $ math. Andre computes derived metrics in Excel.
-- Lead grain: each (CRV wave × account × arm) that overlaps a PCL-mobile deployment.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_deploy_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month
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
        responder,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- Action: one CRV lead = one row; take max PCL responder across overlapping PCL deployments
overlap_action AS (
    SELECT
        c.acct_no,
        c.offer_start_date,
        c.crv_month,
        c.responder                   AS crv_responder,
        MAX(p.responder_cli)          AS pcl_responder
    FROM crv_action c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.offer_start_date, c.crv_month, c.responder
),
-- Control: same pattern
overlap_control AS (
    SELECT
        c.acct_no,
        c.offer_start_date,
        c.crv_month,
        c.responder                   AS crv_responder,
        MAX(p.responder_cli)          AS pcl_responder
    FROM crv_control c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.offer_start_date, c.crv_month, c.responder
),
-- Overall summary
overall AS (
    SELECT
        'overall'                        AS deploy_month,
        COUNT(*)                         AS n_action_leads,
        SUM(crv_responder)               AS crv_responder_count_on_action,
        SUM(pcl_responder)               AS pcl_responder_count_action_overlap,
        0                                AS n_control_leads,
        0                                AS crv_responder_count_on_control,
        0                                AS pcl_responder_count_control_overlap
    FROM overlap_action
    UNION ALL
    SELECT
        'overall'                        AS deploy_month,
        0                                AS n_action_leads,
        0                                AS crv_responder_count_on_action,
        0                                AS pcl_responder_count_action_overlap,
        COUNT(*)                         AS n_control_leads,
        SUM(crv_responder)               AS crv_responder_count_on_control,
        SUM(pcl_responder)               AS pcl_responder_count_control_overlap
    FROM overlap_control
),
overall_agg AS (
    SELECT
        deploy_month,
        SUM(n_action_leads)                       AS n_action_leads,
        SUM(crv_responder_count_on_action)         AS crv_responder_count_on_action,
        SUM(pcl_responder_count_action_overlap)    AS pcl_responder_count_action_overlap,
        SUM(n_control_leads)                       AS n_control_leads,
        SUM(crv_responder_count_on_control)        AS crv_responder_count_on_control,
        SUM(pcl_responder_count_control_overlap)   AS pcl_responder_count_control_overlap
    FROM overall
    GROUP BY deploy_month
),
-- Monthly breakdown
monthly_action AS (
    SELECT
        CAST(crv_month AS VARCHAR(20))   AS deploy_month,
        COUNT(*)                         AS n_action_leads,
        SUM(crv_responder)               AS crv_responder_count_on_action,
        SUM(pcl_responder)               AS pcl_responder_count_action_overlap,
        0                                AS n_control_leads,
        0                                AS crv_responder_count_on_control,
        0                                AS pcl_responder_count_control_overlap
    FROM overlap_action
    GROUP BY crv_month
),
monthly_control AS (
    SELECT
        CAST(crv_month AS VARCHAR(20))   AS deploy_month,
        0                                AS n_action_leads,
        0                                AS crv_responder_count_on_action,
        0                                AS pcl_responder_count_action_overlap,
        COUNT(*)                         AS n_control_leads,
        SUM(crv_responder)               AS crv_responder_count_on_control,
        SUM(pcl_responder)               AS pcl_responder_count_control_overlap
    FROM overlap_control
    GROUP BY crv_month
),
monthly_agg AS (
    SELECT
        deploy_month,
        SUM(n_action_leads)                       AS n_action_leads,
        SUM(crv_responder_count_on_action)         AS crv_responder_count_on_action,
        SUM(pcl_responder_count_action_overlap)    AS pcl_responder_count_action_overlap,
        SUM(n_control_leads)                       AS n_control_leads,
        SUM(crv_responder_count_on_control)        AS crv_responder_count_on_control,
        SUM(pcl_responder_count_control_overlap)   AS pcl_responder_count_control_overlap
    FROM (
        SELECT * FROM monthly_action
        UNION ALL
        SELECT * FROM monthly_control
    ) x
    GROUP BY deploy_month
)
SELECT * FROM overall_agg
UNION ALL
SELECT * FROM monthly_agg
ORDER BY deploy_month
;
