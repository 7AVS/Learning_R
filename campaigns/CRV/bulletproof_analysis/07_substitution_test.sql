-- Substitution test: for each CRV-Action (and Control) lead overlapping a PCL deployment,
-- did the account convert CRV only, PCL only, both, or neither?
-- Lead grain: one row per (CRV wave × account × arm). PCL responder = max across overlapping waves.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder
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
        responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
overlap_action AS (
    SELECT
        c.acct_no,
        c.offer_start_date,
        c.responder                AS crv_resp,
        MAX(p.responder_cli)       AS pcl_resp
    FROM crv_action c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.offer_start_date, c.responder
),
overlap_control AS (
    SELECT
        c.acct_no,
        c.offer_start_date,
        c.responder                AS crv_resp,
        MAX(p.responder_cli)       AS pcl_resp
    FROM crv_control c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.offer_start_date, c.responder
)
SELECT
    CAST('Action' AS VARCHAR(10))                               AS arm,
    COUNT(*)                                                    AS n_overlap_leads,
    SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 0 THEN 1 ELSE 0 END) AS n_crv_converters_only,
    SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 1 THEN 1 ELSE 0 END) AS n_pcl_converters_only,
    SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1 THEN 1 ELSE 0 END) AS n_both_converters,
    SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 0 THEN 1 ELSE 0 END) AS n_neither_converters
FROM overlap_action

UNION ALL

SELECT
    CAST('Control' AS VARCHAR(10))                              AS arm,
    COUNT(*),
    SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 0 THEN 1 ELSE 0 END),
    SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 0 THEN 1 ELSE 0 END)
FROM overlap_control

ORDER BY 1
;
