-- PCL Sales Modal — P3: THE measurement (one query, both arms).
-- Dismissal & conversion by exposure, by strategy x cohort x arm x decile.
-- PLI modal = it_item_id IN ('i_333067','i_333070'). Counts only; rates derived client-side.
-- Engine: Starburst/Trino. Window: May+June deployments, May-July exposure.
--
-- Champion is included so it's visible, BUT its exposure/engagement numbers are
-- suspect by design (champion shouldn't be exposed — that's what p2d investigates).
-- Read champion's conversion (esp. exposure_bin = 0) as the baseline; treat its
-- dismissal/exposure as provisional until the leakage finding lands.
--
-- ASSUMPTIONS (adjustable): exposure unit = distinct session; exposures over full
-- May-July window; one row per client = first deployment; dismissal = close/not-now
-- creative; cohort_month = first-deployment month.
-- NOTE: conversion = responder_cli = whole-campaign response (any channel), not
-- modal-attributed; exposure is post-treatment (descriptive, not causal).

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
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_views,           -- every fire (carries the ~2x double-fire)
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS exposures,
    MAX(CASE WHEN m.event_name = 'select_promotion'
              AND ( LOWER(m.it_creative_name) LIKE '%close%'
                 OR LOWER(m.it_creative_name) LIKE '%not now%'
                 OR LOWER(m.it_creative_name) LIKE '%dismiss%' )
             THEN 1 ELSE 0 END) AS dismissed
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm, p.strategy, p.cohort_month, p.decile, p.responder_cli
)
SELECT
  strategy,
  cohort_month,
  arm,
  decile,
  CASE WHEN exposures >= 20 THEN 20 ELSE exposures END  AS exposure_bin,   -- 0..19, 20 = 20+
  COUNT(*)                                              AS clients,
  SUM(raw_views)                                        AS total_views,          -- raw view fires (validation lens; ~2x occasions)
  SUM(dismissed)                                        AS dismissed_clients,
  SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)    AS converted_clients
FROM per_client
GROUP BY strategy, cohort_month, arm, decile, CASE WHEN exposures >= 20 THEN 20 ELSE exposures END
ORDER BY strategy, cohort_month, arm, decile, exposure_bin;
