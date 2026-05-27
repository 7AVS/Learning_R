-- Throttling scenarios: for each cap N in {2, 3, 4, 5}, how many Action contacts would be cut,
-- and what CRV/PCL conversions would be forgone?
-- cumulative_action_contacts_to_date = running count of CRV-Action deployments per account since 2024-10-01.

WITH crv_action_raw AS (
    SELECT
        acct_no,
        offer_start_date,
        responder,
        channels_deployed
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
-- Cumulative contact count per account, ordered by deployment date
crv_ranked AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder,
        ROW_NUMBER() OVER (
            PARTITION BY acct_no
            ORDER BY offer_start_date
        ) AS cumulative_action_contacts_to_date
    FROM crv_action_raw
),
pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
),
-- For each Action contact, flag whether it has a PCL overlap and the PCL response
crv_with_pcl AS (
    SELECT
        r.acct_no,
        r.offer_start_date,
        r.responder                AS crv_responder,
        r.cumulative_action_contacts_to_date,
        MAX(CASE WHEN p.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS has_pcl_overlap,
        MAX(p.responder_cli)                                    AS pcl_responder
    FROM crv_ranked r
    LEFT JOIN pcl_universe p
      ON p.acct_no           = r.acct_no
     AND r.offer_start_date <= p.treatmt_end_dt
     AND r.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY r.acct_no, r.offer_start_date, r.responder, r.cumulative_action_contacts_to_date
),
-- One row per cap value using a cross-join to a small inline cap table
caps AS (
    SELECT 2 AS cap_n
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
)
SELECT
    caps.cap_n,
    COUNT(*)
        AS n_contacts_total,
    SUM(CASE WHEN cumulative_action_contacts_to_date <= caps.cap_n THEN 1 ELSE 0 END)
        AS n_contacts_under_cap,
    SUM(CASE WHEN cumulative_action_contacts_to_date >  caps.cap_n THEN 1 ELSE 0 END)
        AS n_contacts_over_cap,
    SUM(CASE WHEN cumulative_action_contacts_to_date >  caps.cap_n
             AND crv_responder = 1 THEN 1 ELSE 0 END)
        AS n_crv_responders_among_removed,
    SUM(CASE WHEN cumulative_action_contacts_to_date >  caps.cap_n
             AND has_pcl_overlap = 1 THEN 1 ELSE 0 END)
        AS n_pcl_overlap_leads_among_removed,
    SUM(CASE WHEN cumulative_action_contacts_to_date >  caps.cap_n
             AND has_pcl_overlap = 1
             AND pcl_responder   = 1 THEN 1 ELSE 0 END)
        AS n_pcl_responders_among_removed_overlap
FROM crv_with_pcl
CROSS JOIN caps
GROUP BY caps.cap_n
ORDER BY caps.cap_n
;
