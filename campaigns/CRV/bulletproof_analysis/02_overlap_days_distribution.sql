-- Overlap-days distribution for CRV-Action and CRV-Control leads that overlap PCL-mobile.
-- Lead grain: each (CRV wave x account) is one observation. No dedup.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month,
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
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month,
        CAST('Control' AS VARCHAR(10)) AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
crv_all AS (
    SELECT acct_no, crv_strt_dt, crv_end_dt, crv_month, arm FROM crv_action
    UNION ALL
    SELECT acct_no, crv_strt_dt, crv_end_dt, crv_month, arm FROM crv_control
),
-- Join each CRV lead to any overlapping PCL deployment; take max overlap across PCL waves
overlap_raw AS (
    SELECT
        c.acct_no,
        c.crv_strt_dt,
        c.crv_month,
        c.arm,
        MAX(
            LEAST(c.crv_end_dt, p.pcl_end_dt)
            - GREATEST(c.crv_strt_dt, p.pcl_strt_dt)
            + 1
        ) AS overlap_days
    FROM crv_all c
    INNER JOIN pcl_universe p
      ON p.acct_no        = c.acct_no
     AND c.crv_strt_dt   <= p.pcl_end_dt
     AND c.crv_end_dt    >= p.pcl_strt_dt
    GROUP BY c.acct_no, c.crv_strt_dt, c.crv_month, c.arm
)
-- Overall distribution — Action
SELECT
    CAST('Action'  AS VARCHAR(10))                                     AS arm,
    CAST('overall' AS VARCHAR(20))                                     AS crv_month,
    COUNT(*)                                                           AS n,
    AVG(CAST(overlap_days AS DECIMAL(12,4)))                           AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days)         AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days)         AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days)         AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days)         AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days)         AS p90,
    MIN(overlap_days)                                                  AS min_days,
    MAX(overlap_days)                                                  AS max_days
FROM overlap_raw
WHERE arm = 'Action'

UNION ALL

-- Per CRV deployment month — Action
SELECT
    CAST('Action' AS VARCHAR(10))                                      AS arm,
    CAST(crv_month AS VARCHAR(20))                                     AS crv_month,
    COUNT(*)                                                           AS n,
    AVG(CAST(overlap_days AS DECIMAL(12,4)))                           AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days)         AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days)         AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days)         AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days)         AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days)         AS p90,
    MIN(overlap_days)                                                  AS min_days,
    MAX(overlap_days)                                                  AS max_days
FROM overlap_raw
WHERE arm = 'Action'
GROUP BY crv_month

UNION ALL

-- Overall distribution — Control
SELECT
    CAST('Control' AS VARCHAR(10))                                     AS arm,
    CAST('overall' AS VARCHAR(20))                                     AS crv_month,
    COUNT(*)                                                           AS n,
    AVG(CAST(overlap_days AS DECIMAL(12,4)))                           AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days)         AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days)         AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days)         AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days)         AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days)         AS p90,
    MIN(overlap_days)                                                  AS min_days,
    MAX(overlap_days)                                                  AS max_days
FROM overlap_raw
WHERE arm = 'Control'

UNION ALL

-- Per CRV deployment month — Control
SELECT
    CAST('Control' AS VARCHAR(10))                                     AS arm,
    CAST(crv_month AS VARCHAR(20))                                     AS crv_month,
    COUNT(*)                                                           AS n,
    AVG(CAST(overlap_days AS DECIMAL(12,4)))                           AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days)         AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days)         AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days)         AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days)         AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days)         AS p90,
    MIN(overlap_days)                                                  AS min_days,
    MAX(overlap_days)                                                  AS max_days
FROM overlap_raw
WHERE arm = 'Control'
GROUP BY crv_month

ORDER BY 1, 2
;
