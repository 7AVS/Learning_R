-- When CRV and PCL deploy to the same account, which offer arrives first?
-- Lead grain: each (CRV wave × account × arm) joined to each overlapping PCL deployment.
-- Classify arrival order; aggregate Action vs Control PCL response counts by arrival bucket.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        CAST('Action' AS VARCHAR(10)) AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        CAST('Control' AS VARCHAR(10)) AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
crv_all AS (
    SELECT acct_no, crv_strt_dt, crv_end_dt, arm FROM crv_action
    UNION ALL
    SELECT acct_no, crv_strt_dt, crv_end_dt, arm FROM crv_control
),
-- Each (CRV lead × PCL deployment) overlap — lead grain, no dedup
overlap_classified AS (
    SELECT
        p.acct_no,
        c.arm,
        p.responder_cli,
        CASE
            WHEN c.crv_strt_dt < p.pcl_strt_dt THEN 'crv_first'
            WHEN c.crv_strt_dt > p.pcl_strt_dt THEN 'pcl_first'
            ELSE 'same_day'
        END AS arrival_order
    FROM crv_all c
    INNER JOIN pcl_universe p
      ON p.acct_no      = c.acct_no
     AND c.crv_strt_dt <= p.pcl_end_dt
     AND c.crv_end_dt  >= p.pcl_strt_dt
),
-- Arrival-order distribution by arm
arm_order_counts AS (
    SELECT
        arm,
        arrival_order,
        COUNT(*)           AS leads,
        SUM(responder_cli) AS responders
    FROM overlap_classified
    GROUP BY arm, arrival_order
),
-- Gap (control minus action) per arrival_order bucket
gap_by_order AS (
    SELECT
        arrival_order,
        SUM(CASE WHEN arm = 'Action'  THEN leads       ELSE 0 END) AS n_action,
        SUM(CASE WHEN arm = 'Control' THEN leads       ELSE 0 END) AS n_control,
        SUM(CASE WHEN arm = 'Action'  THEN responders  ELSE 0 END) AS resp_action,
        SUM(CASE WHEN arm = 'Control' THEN responders  ELSE 0 END) AS resp_control
    FROM arm_order_counts
    GROUP BY arrival_order
)
-- Section 1: counts by arm × arrival order
SELECT
    CAST('arm_x_arrival_counts' AS VARCHAR(30))      AS section,
    CAST(arm || '|' || arrival_order AS VARCHAR(30)) AS slice,
    leads                                            AS n_action,
    NULL                                             AS n_control,
    responders                                       AS resp_action,
    NULL                                             AS resp_control,
    NULL                                             AS gap_control_minus_action
FROM arm_order_counts

UNION ALL

-- Section 2: gap per arrival order bucket
SELECT
    CAST('gap_by_arrival_order' AS VARCHAR(30))      AS section,
    CAST(arrival_order AS VARCHAR(30))               AS slice,
    n_action,
    n_control,
    resp_action,
    resp_control,
    CAST(resp_control AS DECIMAL(12,6)) / NULLIF(n_control, 0)
        - CAST(resp_action  AS DECIMAL(12,6)) / NULLIF(n_action,  0) AS gap_control_minus_action
FROM gap_by_order

ORDER BY 1, 2
;
