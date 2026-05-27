-- Time from CRV offer start to PCL CLI effective date, for Action leads that overlap a PCL-mobile deployment.
-- dt_cl_change = PCL CLI effective date (not a response-click date — it reflects when the limit change posted).
-- Distribution overall and per CRV deployment month.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        dt_cl_change,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
      AND dt_cl_change IS NOT NULL
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS crv_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
-- Lead grain: each (CRV lead × PCL deployment) pair with a CLI effective date
overlap_timed AS (
    SELECT
        c.acct_no,
        c.crv_month,
        p.dt_cl_change - c.offer_start_date AS days_crv_to_pcl_response
    FROM crv_action c
    INNER JOIN pcl_universe p
      ON p.acct_no           = c.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
)
-- Overall distribution
SELECT
    'overall'                                                              AS crv_month,
    COUNT(*)                                                               AS n,
    AVG(CAST(days_crv_to_pcl_response AS DECIMAL(12,4)))                   AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p90,
    MIN(days_crv_to_pcl_response)                                          AS min_days,
    MAX(days_crv_to_pcl_response)                                          AS max_days
FROM overlap_timed

UNION ALL

-- Per CRV deployment month
SELECT
    CAST(crv_month AS VARCHAR(20))                                         AS crv_month,
    COUNT(*)                                                               AS n,
    AVG(CAST(days_crv_to_pcl_response AS DECIMAL(12,4)))                   AS mean_days,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY days_crv_to_pcl_response) AS p90,
    MIN(days_crv_to_pcl_response)                                          AS min_days,
    MAX(days_crv_to_pcl_response)                                          AS max_days
FROM overlap_timed
GROUP BY crv_month

ORDER BY 1
;
