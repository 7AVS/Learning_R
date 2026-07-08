-- P11-disc: profile modal GA4 events + creatives to define a richer ENGAGEMENT taxonomy.
-- Today we only detect VIEW (view_promotion) and DISMISS (select_promotion close/not-now).
-- We want to add POSITIVE INTERACTION = select_promotion that is NOT a dismiss (CTA click).
-- But we don't know the positive-select creative text - this shows every creative per event so we
-- can label positive vs dismiss correctly (no guessing). If select_promotion has ONLY close/not-now
-- creatives, there is no positive-interaction signal to add and the taxonomy stays 3-state.
-- Modal id = i_308392 (+ i_335273). Engine: Starburst/Trino (GA4 only - no federation, no 9881).

SELECT
  event_name,
  it_creative_name,
  COUNT(*)                          AS rows,
  COUNT(DISTINCT up_srf_id2_value)  AS clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('05','06','07')
  AND it_item_id IN ('i_308392','i_335273')
  AND event_name IN ('view_promotion','select_promotion')
GROUP BY event_name, it_creative_name
ORDER BY event_name, clients DESC;
