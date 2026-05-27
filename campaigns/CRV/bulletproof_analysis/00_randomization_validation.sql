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
-- Result C: deployment characterization (one row per campaign)
--
-- For each campaign we need two simple things:
--   1. How long does each deployment / wave last (days)
--   2. How often does the campaign deploy (waves per calendar month)
--
-- Wave identity = DISTINCT (start_date, end_date) within the campaign.
-- CRV-Control dropped from this view (Control's end-date convention
-- differs from Action — not comparable as a "deployment duration").
-- waves_per_acct_per_month dropped (was the spool culprit and is
-- already covered by Q02's overlap-days distribution).
-- ============================================================

WITH crv_action_waves AS (
    SELECT DISTINCT
        offer_start_date AS strt_dt,
        offer_end_date   AS end_dt,
        offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS deploy_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
pcl_waves AS (
    SELECT DISTINCT
        treatmt_strt_dt AS strt_dt,
        treatmt_end_dt  AS end_dt,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS deploy_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action_monthly_counts AS (
    SELECT deploy_month, COUNT(*) AS waves_in_month
    FROM crv_action_waves
    GROUP BY deploy_month
),
pcl_monthly_counts AS (
    SELECT deploy_month, COUNT(*) AS waves_in_month
    FROM pcl_waves
    GROUP BY deploy_month
),
-- Wave-level stats per campaign (single row each)
crv_action_wave_agg AS (
    SELECT
        COUNT(*)                                                                  AS n_distinct_waves,
        AVG(CAST(end_dt - strt_dt + 1 AS FLOAT))                                  AS duration_mean,
        PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1)        AS duration_p50,
        MIN(end_dt - strt_dt + 1)                                                 AS duration_min,
        MAX(end_dt - strt_dt + 1)                                                 AS duration_max
    FROM crv_action_waves
),
pcl_wave_agg AS (
    SELECT
        COUNT(*)                                                                  AS n_distinct_waves,
        AVG(CAST(end_dt - strt_dt + 1 AS FLOAT))                                  AS duration_mean,
        PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY end_dt - strt_dt + 1)        AS duration_p50,
        MIN(end_dt - strt_dt + 1)                                                 AS duration_min,
        MAX(end_dt - strt_dt + 1)                                                 AS duration_max
    FROM pcl_waves
),
-- Monthly waves-count stats per campaign (single row each)
crv_action_month_agg AS (
    SELECT
        COUNT(*)                                  AS n_months_in_window,
        AVG(CAST(waves_in_month AS FLOAT))        AS waves_per_month_mean,
        MIN(waves_in_month)                       AS waves_per_month_min,
        MAX(waves_in_month)                       AS waves_per_month_max
    FROM crv_action_monthly_counts
),
pcl_month_agg AS (
    SELECT
        COUNT(*)                                  AS n_months_in_window,
        AVG(CAST(waves_in_month AS FLOAT))        AS waves_per_month_mean,
        MIN(waves_in_month)                       AS waves_per_month_min,
        MAX(waves_in_month)                       AS waves_per_month_max
    FROM pcl_monthly_counts
)
SELECT
    CAST('CRV-Action' AS VARCHAR(20)) AS campaign,
    w.n_distinct_waves,
    w.duration_mean,
    w.duration_p50,
    w.duration_min,
    w.duration_max,
    m.n_months_in_window,
    m.waves_per_month_mean,
    m.waves_per_month_min,
    m.waves_per_month_max
FROM crv_action_wave_agg w
CROSS JOIN crv_action_month_agg m

UNION ALL

SELECT
    CAST('PCL-mobile' AS VARCHAR(20)),
    pw.n_distinct_waves,
    pw.duration_mean,
    pw.duration_p50,
    pw.duration_min,
    pw.duration_max,
    pm.n_months_in_window,
    pm.waves_per_month_mean,
    pm.waves_per_month_min,
    pm.waves_per_month_max
FROM pcl_wave_agg pw
CROSS JOIN pcl_month_agg pm

ORDER BY 1
;

