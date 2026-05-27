-- Overlap-days distribution for PCL-mobile leads that overlap CRV-Action or CRV-Control.
-- PCL-LEAD CENTRIC: unit = one PCL deployment per account. overlap_days = MAX across all CRV waves that touch it.
-- 8 sections: all_leads / pcl_responders × Action / Control × overall / per pcl_month.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt                                                            AS pcl_strt_dt,
        treatmt_end_dt                                                             AS pcl_end_dt,
        responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1)                 AS pcl_month
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
-- One row per PCL lead with Action overlap; overlap_days = MAX across all overlapping CRV-Action waves.
overlap_action AS (
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.pcl_month,
        p.responder_cli,
        MAX(
            LEAST(c.crv_end_dt, p.pcl_end_dt)
            - GREATEST(c.crv_strt_dt, p.pcl_strt_dt)
            + 1
        ) AS overlap_days
    FROM pcl_universe p
    INNER JOIN crv_action c
      ON c.acct_no        = p.acct_no
     AND c.crv_strt_dt   <= p.pcl_end_dt
     AND c.crv_end_dt    >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.pcl_month, p.responder_cli
),
-- One row per PCL lead with Control overlap; overlap_days = MAX across all overlapping CRV-Control waves.
overlap_control AS (
    SELECT
        p.acct_no,
        p.pcl_strt_dt,
        p.pcl_month,
        p.responder_cli,
        MAX(
            LEAST(c.crv_end_dt, p.pcl_end_dt)
            - GREATEST(c.crv_strt_dt, p.pcl_strt_dt)
            + 1
        ) AS overlap_days
    FROM pcl_universe p
    INNER JOIN crv_control c
      ON c.acct_no        = p.acct_no
     AND c.crv_strt_dt   <= p.pcl_end_dt
     AND c.crv_end_dt    >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.pcl_month, p.responder_cli
)

-- All overlap PCL leads — Action overall
SELECT
    CAST('all_leads'  AS VARCHAR(20))                                  AS subset,
    CAST('Action'     AS VARCHAR(10))                                  AS arm,
    CAST('overall'    AS VARCHAR(20))                                  AS pcl_month,
    COUNT(*)                                                           AS n,
    AVG(CAST(overlap_days AS DECIMAL(12,4)))                           AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days)         AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days)         AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days)         AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days)         AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days)         AS p90,
    MIN(overlap_days)                                                  AS min_days,
    MAX(overlap_days)                                                  AS max_days
FROM overlap_action

UNION ALL

-- All overlap PCL leads — Action per-month
SELECT
    CAST('all_leads' AS VARCHAR(20)),
    CAST('Action'    AS VARCHAR(10)),
    CAST(pcl_month   AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_action
GROUP BY pcl_month

UNION ALL

-- All overlap PCL leads — Control overall
SELECT
    CAST('all_leads' AS VARCHAR(20)),
    CAST('Control'   AS VARCHAR(10)),
    CAST('overall'   AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_control

UNION ALL

-- All overlap PCL leads — Control per-month
SELECT
    CAST('all_leads' AS VARCHAR(20)),
    CAST('Control'   AS VARCHAR(10)),
    CAST(pcl_month   AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_control
GROUP BY pcl_month

UNION ALL

-- PCL responders only — Action overall
SELECT
    CAST('pcl_responders' AS VARCHAR(20)),
    CAST('Action'         AS VARCHAR(10)),
    CAST('overall'        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_action
WHERE responder_cli = 1

UNION ALL

-- PCL responders only — Action per-month
SELECT
    CAST('pcl_responders' AS VARCHAR(20)),
    CAST('Action'         AS VARCHAR(10)),
    CAST(pcl_month        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_action
WHERE responder_cli = 1
GROUP BY pcl_month

UNION ALL

-- PCL responders only — Control overall
SELECT
    CAST('pcl_responders' AS VARCHAR(20)),
    CAST('Control'        AS VARCHAR(10)),
    CAST('overall'        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_control
WHERE responder_cli = 1

UNION ALL

-- PCL responders only — Control per-month
SELECT
    CAST('pcl_responders' AS VARCHAR(20)),
    CAST('Control'        AS VARCHAR(10)),
    CAST(pcl_month        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_control
WHERE responder_cli = 1
GROUP BY pcl_month

ORDER BY 1, 2, 3
;
