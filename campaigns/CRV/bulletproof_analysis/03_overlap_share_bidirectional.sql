-- Bidirectional overlap shares: (1) what % of PCL-mobile leads overlap with CRV-Action-IM,
-- and (2) what % of CRV-Action-IM leads overlap with PCL-mobile. Monthly breakdown for each direction.
-- Lead grain: each (wave × account) is one observation.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
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
-- Direction 1 (PCL side): flag each PCL lead as overlapping or not
-- QUALIFY dedup to one flag per (acct_no, pcl_strt_dt) in case of multi-CRV matches
pcl_flagged AS (
    SELECT
        p.acct_no,
        p.pcl_month,
        CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_crv_overlap
    FROM pcl_universe p
    LEFT JOIN crv_action c
      ON c.acct_no       = p.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no, p.pcl_strt_dt ORDER BY p.pcl_strt_dt) = 1
),
-- Direction 2 (CRV side): flag each CRV-Action lead as overlapping or not
-- QUALIFY dedup to one flag per (acct_no, crv_strt_dt) in case of multi-PCL matches
crv_flagged AS (
    SELECT
        c.acct_no,
        c.crv_month,
        CASE WHEN p.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_pcl_overlap
    FROM crv_action c
    LEFT JOIN pcl_universe p
      ON p.acct_no       = c.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.acct_no, c.crv_strt_dt ORDER BY c.crv_strt_dt) = 1
),
dir1_overall AS (
    SELECT
        'pcl_with_crv_overlap' AS direction,
        'overall'              AS deploy_month,
        COUNT(*)               AS total,
        SUM(has_crv_overlap)   AS overlap_count
    FROM pcl_flagged
),
dir1_monthly AS (
    SELECT
        'pcl_with_crv_overlap'         AS direction,
        CAST(pcl_month AS VARCHAR(20)) AS deploy_month,
        COUNT(*)                       AS total,
        SUM(has_crv_overlap)           AS overlap_count
    FROM pcl_flagged
    GROUP BY pcl_month
),
dir2_overall AS (
    SELECT
        'crv_action_with_pcl_overlap' AS direction,
        'overall'                     AS deploy_month,
        COUNT(*)                      AS total,
        SUM(has_pcl_overlap)          AS overlap_count
    FROM crv_flagged
),
dir2_monthly AS (
    SELECT
        'crv_action_with_pcl_overlap'  AS direction,
        CAST(crv_month AS VARCHAR(20)) AS deploy_month,
        COUNT(*)                       AS total,
        SUM(has_pcl_overlap)           AS overlap_count
    FROM crv_flagged
    GROUP BY crv_month
)
SELECT * FROM dir1_overall
UNION ALL
SELECT * FROM dir1_monthly
UNION ALL
SELECT * FROM dir2_overall
UNION ALL
SELECT * FROM dir2_monthly

ORDER BY 1, 2
;
