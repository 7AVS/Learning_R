-- Bidirectional overlap shares — four directions for randomization confidence:
--   1. % of PCL-mobile leads overlapping CRV-Action
--   2. % of CRV-Action leads overlapping PCL-mobile
--   3. % of PCL-mobile leads overlapping CRV-Control
--   4. % of CRV-Control leads overlapping PCL-mobile
-- If Action and Control overlap with PCL at similar rates, randomization holds at the
-- overlap stage and the Action-vs-Control comparison in Q04 is well-grounded.
-- Lead grain: each (wave x account) is one observation.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month
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
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- PCL flagged for CRV-Action overlap
pcl_flagged_action AS (
    SELECT
        p.acct_no,
        p.pcl_month,
        CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_overlap
    FROM pcl_universe p
    LEFT JOIN crv_action c
      ON c.acct_no       = p.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no, p.pcl_strt_dt ORDER BY p.pcl_strt_dt) = 1
),
-- PCL flagged for CRV-Control overlap
pcl_flagged_control AS (
    SELECT
        p.acct_no,
        p.pcl_month,
        CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_overlap
    FROM pcl_universe p
    LEFT JOIN crv_control c
      ON c.acct_no       = p.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no, p.pcl_strt_dt ORDER BY p.pcl_strt_dt) = 1
),
-- CRV-Action flagged for PCL-mobile overlap
crv_action_flagged AS (
    SELECT
        c.acct_no,
        c.crv_month,
        CASE WHEN p.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_overlap
    FROM crv_action c
    LEFT JOIN pcl_universe p
      ON p.acct_no       = c.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.crv_strt_dt ORDER BY c.crv_strt_dt) = 1
),
-- CRV-Control flagged for PCL-mobile overlap
crv_control_flagged AS (
    SELECT
        c.acct_no,
        c.crv_month,
        CASE WHEN p.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_overlap
    FROM crv_control c
    LEFT JOIN pcl_universe p
      ON p.acct_no       = c.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.crv_strt_dt ORDER BY c.crv_strt_dt) = 1
)

-- Direction 1 (PCL ∩ CRV-Action) — overall
SELECT
    CAST('pcl_with_crv_action' AS VARCHAR(40)) AS direction,
    CAST('overall'             AS VARCHAR(20)) AS deploy_month,
    COUNT(*)                                   AS total,
    SUM(has_overlap)                           AS overlap_count
FROM pcl_flagged_action

UNION ALL

-- Direction 1 — per-month
SELECT
    CAST('pcl_with_crv_action' AS VARCHAR(40)),
    CAST(pcl_month             AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM pcl_flagged_action
GROUP BY pcl_month

UNION ALL

-- Direction 2 (CRV-Action ∩ PCL) — overall
SELECT
    CAST('crv_action_with_pcl' AS VARCHAR(40)),
    CAST('overall'             AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM crv_action_flagged

UNION ALL

-- Direction 2 — per-month
SELECT
    CAST('crv_action_with_pcl' AS VARCHAR(40)),
    CAST(crv_month             AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM crv_action_flagged
GROUP BY crv_month

UNION ALL

-- Direction 3 (PCL ∩ CRV-Control) — overall
SELECT
    CAST('pcl_with_crv_control' AS VARCHAR(40)),
    CAST('overall'              AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM pcl_flagged_control

UNION ALL

-- Direction 3 — per-month
SELECT
    CAST('pcl_with_crv_control' AS VARCHAR(40)),
    CAST(pcl_month              AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM pcl_flagged_control
GROUP BY pcl_month

UNION ALL

-- Direction 4 (CRV-Control ∩ PCL) — overall
SELECT
    CAST('crv_control_with_pcl' AS VARCHAR(40)),
    CAST('overall'              AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM crv_control_flagged

UNION ALL

-- Direction 4 — per-month
SELECT
    CAST('crv_control_with_pcl' AS VARCHAR(40)),
    CAST(crv_month              AS VARCHAR(20)),
    COUNT(*),
    SUM(has_overlap)
FROM crv_control_flagged
GROUP BY crv_month

ORDER BY 1, 2
;
