-- s5b — Android id tiebreaker (output ~30 rows, one screenshot)
-- s5 found the Android card-details slot tracked at scale (case b), but the id column in the
-- Stmt 3 photo contradicted s4 (87342 = 638 ANDROID events). This pins id x platform x location.
-- Read: the numeric ids under the android locations = the Android twins for the allowlist.

SELECT
    it_location_id,
    platform,
    it_promotion_id,
    it_item_name,
    COUNT(*) AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('02','03','04')
  AND event_name = 'view_promotion'
  AND LOWER(it_location_id) IN ('android_credit_card_details_m1', 'ios_credit_card_details_m1', 'i_ios_credit_card_details_m1')
GROUP BY 1, 2, 3, 4
ORDER BY n_events DESC
LIMIT 30
