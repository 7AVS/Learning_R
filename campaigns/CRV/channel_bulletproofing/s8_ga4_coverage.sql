-- s8 — GA4 coverage window for the PCL+CRV banners (output: ~20 rows, one screenshot)
-- Purpose: find how far back GA4 view_promotion data exists for our banner ids, by month.
-- Determines the usable window for any cumulative channel-frequency measure (Stmt 2 rebuild).
-- Months with zero/low volume before a clear start = no banner / no data = would be false zeros.

SELECT
    year,
    month,
    COUNT(*) AS n_view_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE event_name = 'view_promotion'
  AND it_item_id IN (
        'i_87340','i_87342','i_87343','i_87344',
        'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
        'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
  )
GROUP BY year, month
ORDER BY year, month
