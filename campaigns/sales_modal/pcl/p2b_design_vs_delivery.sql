-- PCL Sales Modal — P2b: design split (denominator) vs actual modal delivery.
-- FINDING to refine: BOTH champion (NMS) and challenger (WMS) appear to get the
-- PLI sales modal. This quantifies it and tells us WHICH design this really is:
--   same promotion both arms  -> leakage (champion is NOT a clean no-modal baseline)
--   different promotion / arm  -> a modal-VARIANT test, not on/off
-- Exposure is BINARY here (did the client view it at all, yes/no) — no view counts.
-- Counts/volumes only. Denominator = decision-table population (pre conversion/engagement).
-- Engine: Starburst/Trino.

-- ============================================================
-- QA — THE decider: which PLI promotion(s) does each arm actually see?
-- If the same it_promotion_name shows big numbers in BOTH arms -> same modal (leak).
-- If arms split across different promotion names -> variant test.
-- Also confirms the filter isn't catching an unintended campaign.
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
ga AS (
  SELECT DISTINCT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_promotion_name,
    it_item_id,
    it_location_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06')
    AND event_name = 'view_promotion'
    AND it_location_id LIKE '%Sales_Modal%'
    AND (it_promotion_name LIKE '%PLI%' OR it_promotion_name LIKE '%PCL%')
)
SELECT
  g.it_promotion_name,
  g.it_item_id,
  g.it_location_id,
  p.arm,
  COUNT(DISTINCT g.clnt_no) AS clients
FROM ga g
JOIN pop p ON g.clnt_no = p.clnt_no
GROUP BY 1,2,3,4
ORDER BY g.it_promotion_name, p.arm;


-- ============================================================
-- Q1 — headline volumes: STRATEGY x ARM. Design denominator vs who got served.
-- 'clients' = decision-table population (the split as developed).
-- 'viewed_modal_clients' = of those, how many viewed the PLI modal at least once.
-- ============================================================
WITH pop AS (
  SELECT DISTINCT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
viewed AS (
  SELECT DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06')
    AND event_name = 'view_promotion'
    AND it_location_id LIKE '%Sales_Modal%'
    AND (it_promotion_name LIKE '%PLI%' OR it_promotion_name LIKE '%PCL%')
)
SELECT
  p.strategy,
  p.arm,
  COUNT(*)                                                       AS clients,               -- design denominator
  COUNT(CASE WHEN v.clnt_no IS NOT NULL THEN 1 END)              AS viewed_modal_clients   -- actually served
FROM pop p
LEFT JOIN viewed v ON v.clnt_no = p.clnt_no
GROUP BY p.strategy, p.arm
ORDER BY p.strategy, p.arm;


-- ============================================================
-- Q2 — full dimensions: STRATEGY x ARM x DECILE. Same two volumes.
-- This is the design-vs-delivery table across every cut you asked for.
-- ============================================================
WITH pop AS (
  SELECT DISTINCT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
    decile
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
viewed AS (
  SELECT DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06')
    AND event_name = 'view_promotion'
    AND it_location_id LIKE '%Sales_Modal%'
    AND (it_promotion_name LIKE '%PLI%' OR it_promotion_name LIKE '%PCL%')
)
SELECT
  p.strategy,
  p.arm,
  p.decile,
  COUNT(*)                                                       AS clients,
  COUNT(CASE WHEN v.clnt_no IS NOT NULL THEN 1 END)              AS viewed_modal_clients
FROM pop p
LEFT JOIN viewed v ON v.clnt_no = p.clnt_no
GROUP BY p.strategy, p.arm, p.decile
ORDER BY p.strategy, p.arm, p.decile;
