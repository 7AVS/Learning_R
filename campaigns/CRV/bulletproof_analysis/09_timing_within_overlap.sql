-- Arrival-order analysis: for PCL-mobile leads with CRV overlap, which offer started first?
--   crv_first = the CRV offer started BEFORE the PCL offer for that lead
--   pcl_first = the PCL offer started first
--   same_day  = same start date
-- PCL-LEAD CENTRIC. Per lead: MIN overlapping CRV offer_start_date vs the PCL treatmt_strt_dt.
-- MONTHLY (overall + per pcl_month) so you can check the order pattern is stable month to month.
-- Counts only (Q06 idiom); PCL rate and the action-vs-control gap are computed in Excel.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT acct_no, offer_start_date AS crv_strt_dt, offer_end_date AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date AS crv_strt_dt, offer_end_date AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
action_earliest AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.pcl_month, p.responder_cli, MIN(c.crv_strt_dt) AS earliest_crv_strt_dt
    FROM pcl_universe p JOIN crv_action c
      ON c.acct_no = p.acct_no AND c.crv_strt_dt <= p.pcl_end_dt AND c.crv_end_dt >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.pcl_month, p.responder_cli
),
control_earliest AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.pcl_month, p.responder_cli, MIN(c.crv_strt_dt) AS earliest_crv_strt_dt
    FROM pcl_universe p JOIN crv_control c
      ON c.acct_no = p.acct_no AND c.crv_strt_dt <= p.pcl_end_dt AND c.crv_end_dt >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.pcl_month, p.responder_cli
),
combined AS (
    SELECT CAST('Action' AS VARCHAR(10)) AS arm, pcl_month, responder_cli,
        CASE WHEN earliest_crv_strt_dt < pcl_strt_dt THEN CAST('crv_first' AS VARCHAR(10))
             WHEN earliest_crv_strt_dt > pcl_strt_dt THEN CAST('pcl_first' AS VARCHAR(10))
             ELSE                                          CAST('same_day'  AS VARCHAR(10)) END AS arrival_order
    FROM action_earliest
    UNION ALL
    SELECT CAST('Control' AS VARCHAR(10)), pcl_month, responder_cli,
        CASE WHEN earliest_crv_strt_dt < pcl_strt_dt THEN CAST('crv_first' AS VARCHAR(10))
             WHEN earliest_crv_strt_dt > pcl_strt_dt THEN CAST('pcl_first' AS VARCHAR(10))
             ELSE                                          CAST('same_day'  AS VARCHAR(10)) END AS arrival_order
    FROM control_earliest
)
SELECT
    CAST('overall' AS VARCHAR(20)) AS pcl_month,
    arm,
    arrival_order,
    COUNT(*)                                           AS n_leads,
    SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END) AS pcl_responders
FROM combined
GROUP BY arm, arrival_order

UNION ALL

SELECT
    CAST(pcl_month AS VARCHAR(20)),
    arm,
    arrival_order,
    COUNT(*),
    SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)
FROM combined
GROUP BY pcl_month, arm, arrival_order

ORDER BY 1, 2, 3
;
