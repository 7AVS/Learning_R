-- PCL Sales Modal — P4: conversion crossed with dismissal (the joint P3 lacks).
-- P3 gives marginal counts (dismissed_clients, converted_clients) but never crosses
-- them, so it cannot answer "conversion rate among dismissers." P4 collapses each
-- client into ONE engagement segment and counts conversion inside it.
-- PLI modal = it_item_id IN ('i_333067','i_333070'). Counts only; rates derived client-side.
-- Engine: Starburst/Trino. Window: May+June deployments, May-July exposure.
--
-- SEGMENT (per client, mutually exclusive; dismiss wins so a select-without-view
-- still lands as a dismisser — and any such row is a data check):
--   dismissed              = closed/not-now the modal
--   exposed_not_dismissed  = >=1 view, never dismissed (ignored or clicked through)
--   not_exposed            = no modal view at all
--
-- READ AS DESCRIPTIVE ONLY. Dismissal is post-treatment AND self-selected (a collider):
-- the disinterest that makes someone close the modal is the same disinterest that makes
-- them not respond. You CANNOT read this causally ("dismissing lowered conversion").
-- The honest contrast is WITHIN the exposed arm: dismissed vs exposed_not_dismissed.
-- not_exposed is carried for reference only (it's an exposure gap, not a dismissal effect).
-- Control/champion never saw the modal, so its dismissed/exposed segments are the
-- leakage population (suspect by design — see p2d); only its not_exposed conversion is
-- a clean baseline. conversion = responder_cli = whole-campaign response (any channel),
-- not modal-attributed.

WITH pop_raw AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
    decile,
    responder_cli,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn   -- first deployment per client
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-07-01'
),
pop AS (
  SELECT clnt_no, arm, strategy, decile, responder_cli,
         date_format(treatmt_strt_dt, '%Y-%m') AS cohort_month   -- first-deployment month (May vs June)
  FROM pop_raw
  WHERE rn = 1
),
modal AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name,
    it_creative_name
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND it_item_id IN ('i_333067','i_333070')          -- PLI sales modal
),
per_client AS (
  SELECT
    p.clnt_no, p.arm, p.strategy, p.cohort_month, p.decile, p.responder_cli,
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_views,                    -- every fire (~2x double-fire)
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS exposures,      -- distinct sessions = times seen
    MAX(CASE WHEN m.event_name = 'select_promotion'
              AND ( LOWER(m.it_creative_name) LIKE '%close%'
                 OR LOWER(m.it_creative_name) LIKE '%not now%'
                 OR LOWER(m.it_creative_name) LIKE '%dismiss%' )
             THEN 1 ELSE 0 END) AS dismissed
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm, p.strategy, p.cohort_month, p.decile, p.responder_cli
),
segmented AS (
  SELECT
    strategy, cohort_month, arm, decile, responder_cli, raw_views, exposures, dismissed,
    CASE WHEN dismissed = 1  THEN 'dismissed'
         WHEN raw_views > 0  THEN 'exposed_not_dismissed'
         ELSE 'not_exposed' END AS engagement,
    CASE WHEN exposures >= 5 THEN '5+' ELSE CAST(exposures AS VARCHAR) END AS exposure_bin   -- '0'..'4', '5+' (distinct sessions)
  FROM per_client
)
SELECT
  strategy,
  cohort_month,
  arm,
  decile,
  engagement,
  exposure_bin,                                                          -- 0 within 'dismissed' = select w/o logged view (data check)
  COUNT(*)                                            AS clients,           -- denominator for conversion rate within cell
  SUM(raw_views)                                      AS total_views,       -- raw fires (validation lens vs distinct sessions)
  SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)  AS converted_clients  -- numerator; rate = converted/clients (client-side)
FROM segmented
GROUP BY strategy, cohort_month, arm, decile, engagement, exposure_bin
ORDER BY strategy, cohort_month, arm, decile, engagement, exposure_bin;
