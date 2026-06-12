-- s4 — Surface check for the updated ID allowlist (output: ~25 rows, one screenshot)
-- Question: do any of our banner ids fire on WEB (www.rbcroyalbank.com) vs mobile app?
-- Rerun photos hinted web rows under the expanded lists. If yes, web must be split from
-- mobile in every downstream query (four-surface rule, see s2_code_selection.md).
-- Identity key = it_item_id ('i_'+offer id) per s7 2026-06-12: format-stable all platforms, zero disagreement, catches rows where promotion_id is absent.

SELECT
    CASE
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        ELSE 'PCL'
    END AS banner_family,
    it_item_id,
    platform,
    COUNT(*) AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('02','03','04')
  AND event_name = 'view_promotion'
  AND it_item_id IN (
        'i_87340','i_87342','i_87343','i_87344',
        'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
        'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
  )
GROUP BY 1, 2, 3
ORDER BY banner_family, it_item_id, n_events DESC
