-- PCL Sales Modal — DIAG (corrected): it_promotion_name is EMPTY; it_location_id
-- is populated and LOWER(...) LIKE '%modal%' matches. So we key off it_location_id
-- and expose it_item_id / it_item_name to find what separates the PLI modal from
-- the PCD cardupgrade modal sharing the same surface.
-- Split by ARM -> one run answers BOTH: (1) which field isolates PLI, and
-- (2) leakage — does the same modal show up for champion AND challenger.
-- Engine: Starburst/Trino. Window: May+June deployments, May-July exposure.

WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-07-01'
),
ga AS (
  SELECT DISTINCT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_location_id,
    it_item_id,
    it_item_name
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND event_name = 'view_promotion'
    AND LOWER(it_location_id) LIKE '%modal%'          -- the WORKING filter
)
SELECT
  g.it_location_id,
  g.it_item_id,
  g.it_item_name,
  p.arm,
  COUNT(DISTINCT g.clnt_no) AS clients
FROM ga g
JOIN pop p ON g.clnt_no = p.clnt_no
GROUP BY 1,2,3,4
ORDER BY g.it_item_name, p.arm, clients DESC
LIMIT 60;
