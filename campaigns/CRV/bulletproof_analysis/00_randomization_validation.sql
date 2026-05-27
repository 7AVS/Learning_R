-- CRV randomization check: sticky (account-level) vs per-wave assignment
-- Source: dl_mr_prod.cards_crv_install_decis_resp, offer_start_date >= 2024-10-01

-- Result A: account bucket summary (action_only / control_only / both)
SELECT
    CASE
        WHEN ever_action = 1 AND ever_control = 0 THEN 'action_only'
        WHEN ever_action = 0 AND ever_control = 1 THEN 'control_only'
        WHEN ever_action = 1 AND ever_control = 1 THEN 'both'
        ELSE 'unassigned'
    END AS assignment_bucket,
    COUNT(*) AS account_count,
    SUM(COUNT(*)) OVER () AS total_accounts,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(6,2)) AS pct_share
FROM (
    SELECT
        acct_no,
        MAX(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END) AS ever_action,
        MAX(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END) AS ever_control
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
    GROUP BY acct_no
) acct_summary
GROUP BY 1
ORDER BY account_count DESC;


-- Result B: sample of up to 100 accounts in 'both' bucket — for visual inspection
SELECT
    acct_no,
    COUNT(DISTINCT offer_start_date) AS wave_count,
    SUM(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END) AS action_wave_count,
    SUM(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END) AS control_wave_count
FROM dl_mr_prod.cards_crv_install_decis_resp
WHERE offer_start_date >= DATE '2024-10-01'
GROUP BY acct_no
HAVING MAX(CASE WHEN action_control = 'Action'  THEN 1 ELSE 0 END) = 1
   AND MAX(CASE WHEN action_control = 'Control' THEN 1 ELSE 0 END) = 1
QUALIFY ROW_NUMBER() OVER (ORDER BY wave_count DESC, acct_no) <= 100;


-- ============================================================
-- Result C: deployment period and frequency characterization
-- Three metrics per campaign:
--   wave_duration_days       — how many days each wave runs (treatmt_strt_dt to end)
--   waves_per_month          — how many distinct waves are deployed per calendar month
--   waves_per_acct_per_month — how often the same account appears in multiple waves
--                              within a single month (multi-touch indicator)
-- Wave identity:
--   CRV: distinct (offer_start_date, offer_end_date)
--   PCL: distinct (treatmt_strt_dt, treatmt_end_dt)
-- ============================================================

WITH crv_action_waves AS (
    SELECT DISTINCT
        offer_start_date,
        offer_end_date,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS wave_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control_waves AS (
    SELECT DISTINCT
        offer_start_date,
        offer_end_date,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS wave_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
pcl_waves AS (
    SELECT DISTINCT
        treatmt_strt_dt AS strt_dt,
        treatmt_end_dt  AS end_dt,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS wave_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
-- Per-account per-month wave counts (for multi-touch metric)
crv_action_acct_month AS (
    SELECT
        acct_no,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS wave_month,
        COUNT(DISTINCT offer_start_date) AS waves_in_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
    GROUP BY acct_no,
             offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)
),
crv_control_acct_month AS (
    SELECT
        acct_no,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS wave_month,
        COUNT(DISTINCT offer_start_date) AS waves_in_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
    GROUP BY acct_no,
             offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)
),
pcl_acct_month AS (
    SELECT
        acct_no,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS wave_month,
        COUNT(DISTINCT treatmt_strt_dt) AS waves_in_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
    GROUP BY acct_no,
             treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1)
),
-- Per-month wave count (for waves_per_month metric)
crv_action_month_counts AS (
    SELECT wave_month, COUNT(*) AS waves_in_month
    FROM crv_action_waves
    GROUP BY wave_month
),
crv_control_month_counts AS (
    SELECT wave_month, COUNT(*) AS waves_in_month
    FROM crv_control_waves
    GROUP BY wave_month
),
pcl_month_counts AS (
    SELECT wave_month, COUNT(*) AS waves_in_month
    FROM pcl_waves
    GROUP BY wave_month
)

-- 1. Wave duration — CRV-Action
SELECT
    CAST('CRV-Action'         AS VARCHAR(20)) AS campaign,
    CAST('wave_duration_days' AS VARCHAR(40)) AS metric,
    COUNT(*)                                                                              AS n_obs,
    AVG(CAST(offer_end_date - offer_start_date + 1 AS FLOAT))                             AS mean_val,
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1)  AS p10,
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1)  AS p25,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1)  AS p50,
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1)  AS p75,
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1)  AS p90,
    MIN(offer_end_date - offer_start_date + 1)                                            AS min_val,
    MAX(offer_end_date - offer_start_date + 1)                                            AS max_val
FROM crv_action_waves

UNION ALL

-- 2. Wave duration — CRV-Control
SELECT
    CAST('CRV-Control'        AS VARCHAR(20)),
    CAST('wave_duration_days' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(offer_end_date - offer_start_date + 1 AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY offer_end_date - offer_start_date + 1),
    MIN(offer_end_date - offer_start_date + 1),
    MAX(offer_end_date - offer_start_date + 1)
FROM crv_control_waves

UNION ALL

-- 3. Wave duration — PCL-mobile
SELECT
    CAST('PCL-mobile'         AS VARCHAR(20)),
    CAST('wave_duration_days' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(end_dt - strt_dt + 1 AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1),
    MIN(end_dt - strt_dt + 1),
    MAX(end_dt - strt_dt + 1)
FROM pcl_waves

UNION ALL

-- 4. Waves per month — CRV-Action
SELECT
    CAST('CRV-Action'      AS VARCHAR(20)),
    CAST('waves_per_month' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(waves_in_month AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY waves_in_month),
    MIN(waves_in_month),
    MAX(waves_in_month)
FROM crv_action_month_counts

UNION ALL

-- 5. Waves per month — CRV-Control
SELECT
    CAST('CRV-Control'     AS VARCHAR(20)),
    CAST('waves_per_month' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(waves_in_month AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY waves_in_month),
    MIN(waves_in_month),
    MAX(waves_in_month)
FROM crv_control_month_counts

UNION ALL

-- 6. Waves per month — PCL-mobile
SELECT
    CAST('PCL-mobile'      AS VARCHAR(20)),
    CAST('waves_per_month' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(waves_in_month AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY waves_in_month),
    MIN(waves_in_month),
    MAX(waves_in_month)
FROM pcl_month_counts

UNION ALL

-- 7. Waves per acct per month — CRV-Action  (multi-touch indicator)
SELECT
    CAST('CRV-Action'               AS VARCHAR(20)),
    CAST('waves_per_acct_per_month' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(waves_in_month AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY waves_in_month),
    MIN(waves_in_month),
    MAX(waves_in_month)
FROM crv_action_acct_month

UNION ALL

-- 8. Waves per acct per month — CRV-Control
SELECT
    CAST('CRV-Control'              AS VARCHAR(20)),
    CAST('waves_per_acct_per_month' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(waves_in_month AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY waves_in_month),
    MIN(waves_in_month),
    MAX(waves_in_month)
FROM crv_control_acct_month

UNION ALL

-- 9. Waves per acct per month — PCL-mobile
SELECT
    CAST('PCL-mobile'               AS VARCHAR(20)),
    CAST('waves_per_acct_per_month' AS VARCHAR(40)),
    COUNT(*),
    AVG(CAST(waves_in_month AS FLOAT)),
    PERCENTILE_DISC(0.10) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY waves_in_month),
    PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY waves_in_month),
    MIN(waves_in_month),
    MAX(waves_in_month)
FROM pcl_acct_month

ORDER BY 1, 2
;
