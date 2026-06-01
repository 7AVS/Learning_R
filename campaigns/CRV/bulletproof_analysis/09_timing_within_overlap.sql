-- Arrival-order analysis: for PCL-mobile leads with CRV overlap, which offer started first?
-- PCL-LEAD CENTRIC: unit = one PCL-mobile deployment per account. Split by Action vs Control overlap.
-- For each PCL lead, find MIN overlapping CRV offer_start_date across all matching waves; compare to pcl treatmt_strt_dt.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- Per PCL lead: earliest CRV-Action offer_start_date across all overlapping Action waves.
action_earliest AS (
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.responder_cli,
        MIN(c.crv_strt_dt) AS earliest_crv_strt_dt
    FROM pcl_universe p
    INNER JOIN crv_action c
      ON c.acct_no       = p.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.responder_cli
),
-- Per PCL lead: earliest CRV-Control offer_start_date across all overlapping Control waves.
control_earliest AS (
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.responder_cli,
        MIN(c.crv_strt_dt) AS earliest_crv_strt_dt
    FROM pcl_universe p
    INNER JOIN crv_control c
      ON c.acct_no       = p.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.responder_cli
),
-- Classify arrival order per PCL lead (Action arm).
action_classified AS (
    SELECT
        responder_cli,
        CASE
            WHEN earliest_crv_strt_dt < pcl_strt_dt THEN CAST('crv_first' AS VARCHAR(10))
            WHEN earliest_crv_strt_dt > pcl_strt_dt THEN CAST('pcl_first' AS VARCHAR(10))
            ELSE                                          CAST('same_day'  AS VARCHAR(10))
        END AS arrival_order
    FROM action_earliest
),
-- Classify arrival order per PCL lead (Control arm).
control_classified AS (
    SELECT
        responder_cli,
        CASE
            WHEN earliest_crv_strt_dt < pcl_strt_dt THEN CAST('crv_first' AS VARCHAR(10))
            WHEN earliest_crv_strt_dt > pcl_strt_dt THEN CAST('pcl_first' AS VARCHAR(10))
            ELSE                                          CAST('same_day'  AS VARCHAR(10))
        END AS arrival_order
    FROM control_earliest
),
-- Arrival-order distribution by arm.
arm_order_counts AS (
    SELECT
        CAST('Action'  AS VARCHAR(10)) AS arm,
        arrival_order,
        COUNT(*)                       AS n_leads,
        SUM(responder_cli)             AS pcl_responders
    FROM action_classified
    GROUP BY arrival_order

    UNION ALL

    SELECT
        CAST('Control' AS VARCHAR(10)),
        arrival_order,
        COUNT(*),
        SUM(responder_cli)
    FROM control_classified
    GROUP BY arrival_order
),
-- Gap (Control minus Action) per arrival_order bucket.
gap_by_order AS (
    SELECT
        arrival_order,
        SUM(CASE WHEN arm = 'Action'  THEN n_leads        ELSE 0 END) AS n_action,
        SUM(CASE WHEN arm = 'Control' THEN n_leads        ELSE 0 END) AS n_control,
        SUM(CASE WHEN arm = 'Action'  THEN pcl_responders ELSE 0 END) AS resp_action,
        SUM(CASE WHEN arm = 'Control' THEN pcl_responders ELSE 0 END) AS resp_control
    FROM arm_order_counts
    GROUP BY arrival_order
)
-- Section 1: counts by arm x arrival order
SELECT
    CAST('arm_x_arrival_counts' AS VARCHAR(30))                       AS section,
    CAST(arm || '|' || arrival_order AS VARCHAR(30))                  AS slice,
    n_leads                                                           AS n_action,
    NULL                                                              AS n_control,
    pcl_responders                                                    AS resp_action,
    NULL                                                              AS resp_control,
    NULL                                                              AS gap_control_minus_action
FROM arm_order_counts

UNION ALL

-- Section 2: PCL-response gap per arrival order bucket
SELECT
    CAST('gap_by_arrival_order' AS VARCHAR(30))                       AS section,
    CAST(arrival_order AS VARCHAR(30))                                AS slice,
    n_action,
    n_control,
    resp_action,
    resp_control,
    NULL AS gap_control_minus_action   -- counts only; compute the rate gap in Excel
FROM gap_by_order

ORDER BY 1, 2
;
