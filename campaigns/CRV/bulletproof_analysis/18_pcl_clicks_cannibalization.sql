-- 18_pcl_clicks_cannibalization.sql
--
-- Measures PCL mobile-banner CLICKS (and impressions) by CRV arm — full base.
-- Click-twin of H1's 1.08pp conversion gap: does CRV exposure reduce PCL clicks,
-- not just final conversions?
--
-- Clean comparison is CRV Action vs CRV Control (randomised assignment).
-- No-overlap group is included for continuity but is NOT the causal comparison
-- (selection bias: no-overlap clients were never CRV-eligible).
--
-- Faithful copy of crv_pcl_overlap_summary.sql Section E (three-way split,
-- Teradata LEFT-JOIN + flag pattern, pcl_month derivation) with these changes:
--   1. PCL source CTE uses channel LIKE '%MB%' (mobile banner, matching 04/06).
--   2. clicked_mb and impression_mb added to pcl_universe SELECT.
--   3. Final aggregation adds n_clicks and n_impressions per arm.
--   4. No filter on responder_cli — full base (clicks happen before conversion).
--   5. Counts only. No rates, no ratios.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli,
        clicked_mb,
        impression_mb,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_im_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control_pool AS (
    -- No channel filter on Control: Control clients aren't deployed to any
    -- channel by definition (channels_deployed will be blank/null).
    -- Population = full CRV eligibility pool randomly held out from any
    -- deployment. Selection-matched to Action; broader than IM-intended-only.
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
overlap_action_keys AS (
    SELECT DISTINCT
        p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_im_action c
      ON c.acct_no           = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
),
overlap_control_keys AS (
    SELECT DISTINCT
        p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_control_pool c
      ON c.acct_no           = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
),
pcl_flagged AS (
    SELECT
        p.pcl_month,
        p.responder_cli,
        p.clicked_mb,
        p.impression_mb,
        CASE WHEN oa.acct_no IS NOT NULL
             THEN 1 ELSE 0 END AS overlap_action_flag,
        CASE WHEN oa.acct_no IS NULL AND oc.acct_no IS NOT NULL
             THEN 1 ELSE 0 END AS overlap_control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no         = p.acct_no
     AND oa.treatmt_strt_dt = p.treatmt_strt_dt
     AND oa.treatmt_end_dt  = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no         = p.acct_no
     AND oc.treatmt_strt_dt = p.treatmt_strt_dt
     AND oc.treatmt_end_dt  = p.treatmt_end_dt
)
SELECT
    pcl_month,
    -- leads (denominators)
    COUNT(*)                                                                                  AS total_pcl_leads,
    SUM(overlap_action_flag)                                                                  AS overlap_action_leads,
    SUM(overlap_control_flag)                                                                 AS overlap_control_leads,
    SUM(CASE WHEN overlap_action_flag = 0 AND overlap_control_flag = 0
             THEN 1 ELSE 0 END)                                                               AS no_overlap_leads,
    -- responders (conversion twin — kept for comparability with H1)
    SUM(CASE WHEN overlap_action_flag  = 1 THEN responder_cli ELSE 0 END)                     AS overlap_action_responders,
    SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)                     AS overlap_control_responders,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0
             THEN responder_cli ELSE 0 END)                                                   AS no_overlap_responders,
    -- clicks (new — primary outcome for this query)
    SUM(CASE WHEN overlap_action_flag  = 1 THEN clicked_mb ELSE 0 END)                        AS overlap_action_clicks,
    SUM(CASE WHEN overlap_control_flag = 1 THEN clicked_mb ELSE 0 END)                        AS overlap_control_clicks,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0
             THEN clicked_mb ELSE 0 END)                                                      AS no_overlap_clicks,
    -- impressions
    SUM(CASE WHEN overlap_action_flag  = 1 THEN impression_mb ELSE 0 END)                     AS overlap_action_impressions,
    SUM(CASE WHEN overlap_control_flag = 1 THEN impression_mb ELSE 0 END)                     AS overlap_control_impressions,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0
             THEN impression_mb ELSE 0 END)                                                   AS no_overlap_impressions
FROM pcl_flagged
GROUP BY pcl_month
ORDER BY pcl_month
;
