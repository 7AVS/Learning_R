-- Overlap-days distribution for CRV-Action and CRV-Control leads that overlap PCL-mobile.
-- Lead grain: each (CRV wave x account) is one observation. No dedup.
-- Two subsets: all overlap leads, and overlap leads where PCL responded (responder_cli=1).
-- Comparing the two subsets tells us whether shorter overlap correlates with PCL conversion.

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
-- One row per CRV lead. overlap_days = max calendar overlap across PCL waves it touches.
-- pcl_resp = 1 if any overlapping PCL wave converted.
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
        ) AS overlap_days,
        MAX(p.responder_cli) AS pcl_resp
    FROM crv_all c
    INNER JOIN pcl_universe p
      ON p.acct_no        = c.acct_no
     AND c.crv_strt_dt   <= p.pcl_end_dt
     AND c.crv_end_dt    >= p.pcl_strt_dt
    GROUP BY c.acct_no, c.crv_strt_dt, c.crv_month, c.arm
)

-- All overlap leads — Action overall
SELECT
    CAST('all_leads'  AS VARCHAR(20))                                  AS subset,
    CAST('Action'     AS VARCHAR(10))                                  AS arm,
    CAST('overall'    AS VARCHAR(20))                                  AS crv_month,
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

-- All overlap leads — Action per-month
SELECT
    CAST('all_leads' AS VARCHAR(20)),
    CAST('Action'    AS VARCHAR(10)),
    CAST(crv_month   AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_raw
WHERE arm = 'Action'
GROUP BY crv_month

UNION ALL

-- All overlap leads — Control overall
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
FROM overlap_raw
WHERE arm = 'Control'

UNION ALL

-- All overlap leads — Control per-month
SELECT
    CAST('all_leads' AS VARCHAR(20)),
    CAST('Control'   AS VARCHAR(10)),
    CAST(crv_month   AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_raw
WHERE arm = 'Control'
GROUP BY crv_month

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
FROM overlap_raw
WHERE arm = 'Action' AND pcl_resp = 1

UNION ALL

-- PCL responders only — Action per-month
SELECT
    CAST('pcl_responders' AS VARCHAR(20)),
    CAST('Action'         AS VARCHAR(10)),
    CAST(crv_month        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_raw
WHERE arm = 'Action' AND pcl_resp = 1
GROUP BY crv_month

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
FROM overlap_raw
WHERE arm = 'Control' AND pcl_resp = 1

UNION ALL

-- PCL responders only — Control per-month
SELECT
    CAST('pcl_responders' AS VARCHAR(20)),
    CAST('Control'        AS VARCHAR(10)),
    CAST(crv_month        AS VARCHAR(20)),
    COUNT(*),
    AVG(CAST(overlap_days AS DECIMAL(12,4))),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY overlap_days),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY overlap_days),
    MIN(overlap_days),
    MAX(overlap_days)
FROM overlap_raw
WHERE arm = 'Control' AND pcl_resp = 1
GROUP BY crv_month

ORDER BY 1, 2, 3
;
