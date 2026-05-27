-- Statistical significance test on the CRV-Action vs CRV-Control cannibalization gap in PCL response.
-- Lead grain: one row per (CRV wave x account x arm) that overlaps a PCL-mobile deployment.
-- Uses FLOAT for the statistical math to avoid DECIMAL precision overflow on large n.

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
-- Action overlap: each (CRV-Action lead x PCL deployment) pair that overlaps.
-- Take one PCL row per CRV lead (max responder_cli across PCL deployments).
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
-- Control overlap: each (CRV-Control lead x PCL deployment) pair that overlaps.
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
        SUM(a.n_action)     AS n_action,
        SUM(a.resp_action)  AS resp_action,
        SUM(a.n_control)    AS n_control,
        SUM(a.resp_control) AS resp_control
    FROM (
        SELECT COUNT(*) AS n_action, SUM(pcl_responded) AS resp_action, 0 AS n_control, 0 AS resp_control
        FROM overlap_action
        UNION ALL
        SELECT 0, 0, COUNT(*), SUM(pcl_responded)
        FROM overlap_control
    ) a
),
stats AS (
    SELECT
        n_action,
        resp_action,
        n_control,
        resp_control,
        CAST(resp_action  AS FLOAT) / NULLIF(CAST(n_action AS FLOAT), 0) AS p_action,
        CAST(resp_control AS FLOAT) / NULLIF(CAST(n_control AS FLOAT), 0) AS p_control
    FROM agg
),
se_calc AS (
    SELECT
        n_action,
        resp_action,
        n_control,
        resp_control,
        p_action,
        p_control,
        p_control - p_action AS gap,
        SQRT(  p_action  * (1.0 - p_action)  / NULLIF(CAST(n_action AS FLOAT), 0)
             + p_control * (1.0 - p_control) / NULLIF(CAST(n_control AS FLOAT), 0) ) AS se
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
    gap - 1.96 * se AS ci_lower,
    gap + 1.96 * se AS ci_upper,
    gap / NULLIF(se, 0) AS z_stat,
    CASE
        WHEN (gap - 1.96 * se) > 0 OR (gap + 1.96 * se) < 0 THEN 1
        ELSE 0
    END AS significant_at_95
FROM se_calc
;
