-- s5 — Android gap discriminator (output: ~6 rows, one screenshot)
-- s4 showed our banner ids are 99.998% iOS. Question: is Android untagged app-wide (a),
-- or do Android banners log under DIFFERENT promotion ids (b)?
-- Read: if ANDROID view_promotion volume is large overall but zero on our ids -> (b);
-- if ANDROID is near-zero app-wide -> (a), tagging gap, digital-team question.

SELECT
    platform,
    COUNT(*) AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients,
    COUNT(DISTINCT it_promotion_id) AS n_promotion_ids
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('02','03','04')
  AND event_name = 'view_promotion'
GROUP BY 1
ORDER BY n_events DESC
