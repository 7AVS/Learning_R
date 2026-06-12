-- s6 — Why did s4's IN-list miss Android? (output ~20 rows)
-- s5b shows promotion_id 87342 under the android card slot at 9.9M events; s4's exact
-- IN ('87342',...) counted 638 ANDROID events. If Android's it_promotion_id carries hidden
-- characters/whitespace, both are true. LENGTH exposes it.

SELECT
    platform,
    it_promotion_id,
    LENGTH(it_promotion_id)       AS id_length,
    COUNT(*)                      AS n_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('02','03','04')
  AND event_name = 'view_promotion'
  AND LOWER(it_location_id) LIKE '%android%credit%card%details%'
GROUP BY 1, 2, 3
ORDER BY n_events DESC
LIMIT 20
