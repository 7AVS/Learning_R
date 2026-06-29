-- PCL Sales Modal — P2d: characterize the champion-exposure finding so it's shareable.
-- Design intent: challenger gets the modal, champion does NOT. We observed champion
-- exposure. This quantifies it on OUR modal ids only (i_333067/i_333070), and tests
-- whether both arms see the SAME id (true leak) or split by id (variant test).
-- Counts only. Both arms. May+June deployments, May-July exposure. Engine: Trino.

WITH pop_raw AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-07-01'
),
pop AS (
  SELECT clnt_no, arm, strategy,
         date_format(treatmt_strt_dt, '%Y-%m') AS cohort_month
  FROM pop_raw
  WHERE rn = 1
),
modal AS (
  SELECT DISTINCT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_item_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND event_name = 'view_promotion'
    AND it_item_id IN ('i_333067','i_333070')          -- OUR modal only (not the shared surface)
)

-- ============================================================
-- Q1 — MAGNITUDE: of each arm's population, how many were exposed to OUR modal?
-- champion exposed_clients ~ 0  -> false alarm (earlier signal was the shared surface).
-- champion exposed_clients large -> real leak; magnitude = exposed/clients.
-- ============================================================
SELECT
  p.strategy,
  p.cohort_month,
  p.arm,
  COUNT(DISTINCT p.clnt_no)  AS clients,          -- arm population (denominator)
  COUNT(DISTINCT m.clnt_no)  AS exposed_clients   -- exposed to i_333067/i_333070
FROM pop p
LEFT JOIN modal m ON m.clnt_no = p.clnt_no
GROUP BY p.strategy, p.cohort_month, p.arm
ORDER BY p.strategy, p.cohort_month, p.arm;


-- ============================================================
-- Q2 — SAME-ID vs SPLIT: which arm sees which id.
-- Both ids show both arms  -> same modal in both = true leakage.
-- id1 = champion only, id2 = challenger only -> VARIANT test, not on/off (no fault).
-- ============================================================
WITH pop_raw AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-07-01'
),
pop AS ( SELECT clnt_no, arm, date_format(treatmt_strt_dt, '%Y-%m') AS cohort_month FROM pop_raw WHERE rn = 1 ),
modal AS (
  SELECT DISTINCT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_item_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND event_name = 'view_promotion'
    AND it_item_id IN ('i_333067','i_333070')
)
SELECT
  m.it_item_id,
  p.cohort_month,
  p.arm,
  COUNT(DISTINCT m.clnt_no) AS exposed_clients
FROM pop p
JOIN modal m ON m.clnt_no = p.clnt_no
GROUP BY m.it_item_id, p.cohort_month, p.arm
ORDER BY m.it_item_id, p.cohort_month, p.arm;
