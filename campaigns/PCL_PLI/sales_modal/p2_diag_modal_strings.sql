-- PCL Sales Modal — DIAG: why did P2 Q-B come back all zeros?
-- Root cause: the P2/P2b filter strings don't match the real GA4 values
-- (suspect: the '_' in '%Sales_Modal%' is a LIKE wildcard; and/or '%PLI%' is the
-- wrong promotion token). This uses a LOOSE, lowercased, no-underscore filter to
-- pull the ACTUAL it_location_id / it_promotion_name / it_item_id the WMS clients
-- saw, so we fix the P2 and P2b filters against reality — once.
-- Engine: Starburst/Trino.

WITH pop AS (
  SELECT CAST(clnt_no AS BIGINT) AS clnt_no
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R____WMS%'              -- challenger (known modal-served)
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
ga AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    it_location_id,
    it_promotion_name,
    it_item_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06')
    AND event_name = 'view_promotion'
    AND ( LOWER(it_location_id)  LIKE '%modal%'             -- any "...modal..." surface (no '_' trap)
       OR LOWER(it_promotion_name) LIKE '%modal%'
       OR LOWER(it_promotion_name) LIKE '%pli%'
       OR LOWER(it_promotion_name) LIKE '%pcl%' )
)
SELECT
  g.it_location_id,
  g.it_promotion_name,
  g.it_item_id,
  COUNT(DISTINCT g.clnt_no) AS clients
FROM ga g
JOIN pop p ON g.clnt_no = p.clnt_no
GROUP BY 1,2,3
ORDER BY clients DESC
LIMIT 40;
