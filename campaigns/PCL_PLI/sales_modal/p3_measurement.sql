-- PCL Sales Modal — P3: THE measurement. Dismissal & conversion by exposure,
-- by strategy (BAU/NTC) x arm (champion/challenger) x decile.
-- PLI modal = it_item_id IN ('i_333067','i_333070'). Counts only; rates derived client-side.
-- Engine: Starburst/Trino. Window: May+June deployments, May-July exposure.
--
-- LOCKED ASSUMPTIONS (all adjustable — flagged so they don't hide):
--   1. Exposure unit = distinct SESSION with a modal view (one viewing occasion;
--      naturally dedups the ms-apart double-fire). Swap to day if it balloons.
--   2. Exposures counted over the full May-July window (not restricted to the
--      client's first-deployment window). Refinement if needed.
--   3. One row per client = their FIRST deployment (earliest treatmt_strt_dt) for
--      arm/strategy/decile/conversion. Arm is stable so this is clean; conversion
--      is that deployment's responder_cli.
--   4. Dismissal = clicked a close/dismiss creative (it_creative_name ~ close/not
--      now/dismiss). Validate against the full it_creative_name catalog before final.
--
-- READING IT:
--   dismissal rate @ exposure N = dismissed_clients / clients
--   conversion rate @ exposure N = converted_clients / clients
--   exposure_bin = 0  -> never saw the modal (champion = no-modal CONVERSION BASELINE)
--   NOTE: conversion-by-exposure WITHIN an arm is descriptive (exposure is
--   post-treatment/self-selected); the champion bin-0 rate is the causal anchor.

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
    p.clnt_no, p.arm, p.strategy, p.decile, p.cohort_month, p.responder_cli,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS exposures,
    MAX(CASE WHEN m.event_name = 'select_promotion'
              AND ( LOWER(m.it_creative_name) LIKE '%close%'
                 OR LOWER(m.it_creative_name) LIKE '%not now%'
                 OR LOWER(m.it_creative_name) LIKE '%dismiss%' )
             THEN 1 ELSE 0 END) AS dismissed
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm, p.strategy, p.decile, p.cohort_month, p.responder_cli
)
SELECT
  strategy,
  cohort_month,
  arm,
  decile,
  CASE WHEN exposures >= 20 THEN 20 ELSE exposures END  AS exposure_bin,   -- 0..19, 20 = 20+
  COUNT(*)                                              AS clients,
  SUM(dismissed)                                        AS dismissed_clients,
  SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)    AS converted_clients
FROM per_client
GROUP BY strategy, cohort_month, arm, decile, CASE WHEN exposures >= 20 THEN 20 ELSE exposures END
ORDER BY strategy, cohort_month, arm, decile, exposure_bin;
