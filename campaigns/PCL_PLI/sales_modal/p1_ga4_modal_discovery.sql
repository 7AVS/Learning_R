-- PCL Sales Modal — P1: discover the modal's GA4 identifier (it_item_id)
-- Engine: Starburst/Trino (mixed EDW dw00_im + GA4 edl0_im -> Trino syntax)
-- Anchor: clients KNOWN to be in the WMS (with-modal) arm should overwhelmingly
-- show the modal's banner in GA4; the no-modal (NMS) arm should not.
-- New creative => May-only window. Widen month if first pass is empty.

-- ============================================================
-- Q1 — rank what the WMS arm actually sees in GA4
-- The modal should top the list by distinct clients (and converters).
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    strategy_id,
    responder_cli
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R___WMS%'        -- challenger = with modal
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')         -- BAU / NTC
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
ga AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_item_id,
    it_item_name,
    it_promotion_name,
    event_name,
    it_creative_name,
    it_location_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026'
    AND month IN ('05','06')
    AND event_name IN ('view_promotion','select_promotion')
)
SELECT
  g.it_item_id,
  g.it_item_name,
  g.it_promotion_name,
  g.event_name,
  g.it_creative_name,        -- p_%/n_% prefixes reveal the dismiss button
  g.it_location_id,          -- slot/placement of the modal
  COUNT(DISTINCT g.clnt_no) AS clients,
  COUNT(DISTINCT CASE WHEN g.event_name = 'view_promotion' THEN g.clnt_no END) AS viewers,
  COUNT(DISTINCT CASE WHEN p.responder_cli = 1 THEN g.clnt_no END) AS converters
FROM ga g
JOIN pop p ON g.clnt_no = p.clnt_no
GROUP BY 1,2,3,4,5,6
ORDER BY clients DESC;


-- ============================================================
-- Q2 — validate the candidate: a true modal id is HIGH in WMS, ~0 in NMS.
-- Run after Q1; the modal id should sit at the top with near-zero nms_clients.
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R___WMS%' THEN 'WMS'
         WHEN report_groups_period LIKE '%R___NMS%' THEN 'NMS' END AS arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R___WMS%' OR report_groups_period LIKE '%R___NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
ga AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_item_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026'
    AND month IN ('05','06')
    AND event_name = 'view_promotion'
)
SELECT
  g.it_item_id,
  COUNT(DISTINCT CASE WHEN p.arm = 'WMS' THEN g.clnt_no END) AS wms_clients,
  COUNT(DISTINCT CASE WHEN p.arm = 'NMS' THEN g.clnt_no END) AS nms_clients
FROM ga g
JOIN pop p ON g.clnt_no = p.clnt_no
GROUP BY 1
ORDER BY wms_clients DESC;
