-- ============================================================================
-- ENGINE: Starburst/Trino — Trino syntax. GA4 table on EDL.
-- s10 — Per-client view-day saturation distribution: CRV and PCL, Feb 2025 – May 2026.
-- Metric: COUNT(DISTINCT event_date) per client per banner_family = view_days.
--   Not raw event counts (twin-pair inflation per s2); not deployment-anchored.
-- Impression: event_name = 'view_promotion' (view_item discarded — s2 FINAL).
-- Identity: it_item_id allowlist (s2 FINAL; s7 confirmed format-stable key).
--   CRV  it_item_ids: 'i_87340','i_87342','i_87343','i_87344'
--   PCL  it_item_ids: 'i_156764','i_156788','i_162326','i_167715','i_167716',
--                     'i_167717','i_289661','i_289662','i_289664','i_289665',
--                     'i_289666','i_289698'
-- Client: TRY_CAST(up_srf_id2_value AS BIGINT).
-- Partition window: Feb 2025 – May 2026 (June 2026 partial — EXCLUDED).
-- Output: histogram — x=view_days, y=n_clients, one curve per banner_family.
--   Uncapped tail: shows full distribution including outliers.
--   No deployment join, no overlap filter — pure channel-side exposure frequency.
-- ============================================================================

WITH client_viewdays AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS client,
        CASE
            WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
            WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                                'i_167717','i_289661','i_289662','i_289664','i_289665',
                                'i_289666','i_289698')                   THEN 'PCL'
        END                                  AS banner_family,
        COUNT(DISTINCT event_date)           AS view_days
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
        TRY_CAST(up_srf_id2_value AS BIGINT),
        CASE
            WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
            WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                                'i_167717','i_289661','i_289662','i_289664','i_289665',
                                'i_289666','i_289698')                   THEN 'PCL'
        END
)
SELECT
    banner_family,
    view_days,
    COUNT(*) AS n_clients
FROM client_viewdays
GROUP BY banner_family, view_days
ORDER BY banner_family, view_days;
