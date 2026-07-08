-- P8: confirm the real modal id + read its label. P7 arm-contrast flagged i_333067/i_333070
-- as contaminated (champion sees them). Candidates i_308392 / i_335273 should be challenger-only.
-- This pulls the item NAME + creative + surface beside the arm contrast so we can (a) confirm
-- champion is empty and (b) read the 'VCL' label + check it's on the Sales_Modal surface.
-- Engine: Starburst/Trino (GA4 + curated). Counts only. 9881-safe (join-key casts in predicate).
-- The right modal id = challenger_viewers high, champion_viewers ~0.

WITH arm AS (
  SELECT
    clnt_no,                                    -- raw, uncast (avoids Starburst ROUND pushdown / 9881)
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
),
surface AS (
  SELECT up_srf_id2_value, it_item_id, it_item_name, it_creative_name, it_location_id
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND event_name = 'view_promotion'
    AND it_item_id IN ('i_308392','i_335273','i_333067','i_333070')   -- new candidates + failed reference
)
SELECT
  s.it_item_id,
  MAX(s.it_item_name)      AS item_name,
  MAX(s.it_creative_name)  AS creative_name,
  MAX(s.it_location_id)    AS location_id,
  COUNT(DISTINCT CASE WHEN a.arm = 'challenger' THEN a.clnt_no END) AS challenger_viewers,
  COUNT(DISTINCT CASE WHEN a.arm = 'champion'   THEN a.clnt_no END) AS champion_viewers
FROM surface s
JOIN arm a ON CAST(a.clnt_no AS BIGINT) = TRY_CAST(s.up_srf_id2_value AS BIGINT)   -- casts stay in Trino
GROUP BY s.it_item_id
ORDER BY challenger_viewers DESC;
