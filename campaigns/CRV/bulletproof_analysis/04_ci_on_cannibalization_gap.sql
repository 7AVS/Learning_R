-- Statistical significance test on the CRV-Action vs CRV-Control cannibalization gap in PCL response.
-- Lead grain: one row per (CRV wave x account x arm) that overlaps a PCL-mobile deployment.
-- All statistical math forced to FLOAT explicitly to avoid Teradata DECIMAL precision overflow.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli
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
overlap_action AS (
    SELECT
        c.acct_no,
        c.offer_start_date,
        MAX(p.responder_cli) AS pcl_responded
    FROM crv_action c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.offer_start_date
),
overlap_control AS (
    SELECT
        c.acct_no,
        c.offer_start_date,
        MAX(p.responder_cli) AS pcl_responded
    FROM crv_control c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY c.acct_no, c.offer_start_date
),
agg AS (
    SELECT
        CAST(SUM(a.n_action)     AS FLOAT) AS n_action,
        CAST(SUM(a.resp_action)  AS FLOAT) AS resp_action,
        CAST(SUM(a.n_control)    AS FLOAT) AS n_control,
        CAST(SUM(a.resp_control) AS FLOAT) AS resp_control
    FROM (
        SELECT
            CAST(COUNT(*)            AS FLOAT) AS n_action,
            CAST(SUM(pcl_responded)  AS FLOAT) AS resp_action,
            CAST(0                   AS FLOAT) AS n_control,
            CAST(0                   AS FLOAT) AS resp_control
        FROM overlap_action
        UNION ALL
        SELECT
            CAST(0                   AS FLOAT),
            CAST(0                   AS FLOAT),
            CAST(COUNT(*)            AS FLOAT),
            CAST(SUM(pcl_responded)  AS FLOAT)
        FROM overlap_control
    ) a
),
stats AS (
    SELECT
        n_action,
        resp_action,
        n_control,
        resp_control,
        CASE WHEN n_action  > CAST(0 AS FLOAT) THEN resp_action  / n_action  ELSE NULL END AS p_action,
        CASE WHEN n_control > CAST(0 AS FLOAT) THEN resp_control / n_control ELSE NULL END AS p_control
    FROM agg
),
se_calc AS (
    SELECT
        n_action, resp_action, n_control, resp_control,
        p_action, p_control,
        p_control - p_action AS gap,
        SQRT(
              p_action  * (CAST(1 AS FLOAT) - p_action)  / n_action
            + p_control * (CAST(1 AS FLOAT) - p_control) / n_control
        ) AS se
    FROM stats
)
SELECT
    n_action,
    resp_action,
    n_control,
    resp_control,
    p_action,
    p_control,
    -- gap: positive = control higher than action = cannibalization signal
    gap,
    se,
    gap - CAST(1.96 AS FLOAT) * se AS ci_lower,
    gap + CAST(1.96 AS FLOAT) * se AS ci_upper,
    CASE WHEN se > CAST(0 AS FLOAT) THEN gap / se ELSE NULL END AS z_stat,
    CASE
        WHEN (gap - CAST(1.96 AS FLOAT) * se) > CAST(0 AS FLOAT)
          OR (gap + CAST(1.96 AS FLOAT) * se) < CAST(0 AS FLOAT) THEN 1
        ELSE 0
    END AS significant_at_95
FROM se_calc
;
