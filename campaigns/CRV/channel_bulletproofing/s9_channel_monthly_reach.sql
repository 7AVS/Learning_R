-- ============================================================================
-- ENGINE: Starburst/Trino — Trino syntax. GA4 table on EDL.
-- s9 — Monthly channel reach trend: CRV and PCL banners, Feb 2025 – May 2026.
-- Metric: view-days (COUNT DISTINCT event_date per client), NOT raw event counts.
--   Raw counts are ~2× inflated (view_item/view_promotion twin pairs per s2).
-- Impression: event_name = 'view_promotion' (view_item discarded — s2 FINAL).
-- Identity: it_item_id allowlist (s2 FINAL; s7 confirmed format-stable key).
--   CRV  it_item_ids: 'i_87340','i_87342','i_87343','i_87344'
--   PCL  it_item_ids: 'i_156764','i_156788','i_162326','i_167715','i_167716',
--                     'i_167717','i_289661','i_289662','i_289664','i_289665',
--                     'i_289666','i_289698'
-- Client: TRY_CAST(up_srf_id2_value AS BIGINT).
-- Partition window: Feb 2025 – May 2026 (June 2026 partial — EXCLUDED).
-- Output: 2 curves (CRV / PCL) × 16 months.
--   Plot clients_reached over months to see monthly audience trend per banner.
--   client_view_days = distinct (client, event_date) pairs — intensity proxy.
--   raw_view_events shown for QA only; not a frequency metric.
-- ============================================================================

SELECT
    year,
    month,
    CASE
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                            'i_167717','i_289661','i_289662','i_289664','i_289665',
                            'i_289666','i_289698')                   THEN 'PCL'
    END                                              AS banner_family,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))          AS clients_reached,
    COUNT(DISTINCT (TRY_CAST(up_srf_id2_value AS BIGINT), event_date)) AS client_view_days,
    COUNT(*)                                         AS raw_view_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE event_name = 'view_promotion'
  AND it_item_id IN (
      'i_87340','i_87342','i_87343','i_87344',
      'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
      'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
  )
  AND (
      (year = '2025' AND month IN ('02','03','04','05','06','07','08','09','10','11','12'))
   OR (year = '2026' AND month IN ('01','02','03','04','05'))
  )
GROUP BY
    year,
    month,
    CASE
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                            'i_167717','i_289661','i_289662','i_289664','i_289665',
                            'i_289666','i_289698')                   THEN 'PCL'
    END
ORDER BY banner_family, year, month;
