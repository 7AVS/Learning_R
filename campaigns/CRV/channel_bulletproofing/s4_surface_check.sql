-- s4 — Surface check for the updated ID allowlist (output: ~25 rows, one screenshot)
-- Question: do any of our banner ids fire on WEB (www.rbcroyalbank.com) vs mobile app?
-- Rerun photos hinted web rows under the expanded lists. If yes, web must be split from
-- mobile in every downstream query (four-surface rule, see s2_code_selection.md).
-- ID allowlist updated 2026-06-12 (digital team list).
-- 2026-06-12: promotion-id matching switched to numeric cast (Android stores ids as '87342.0' float strings; string IN-lists excluded Android)

SELECT
    CASE
        WHEN TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (87340,87342,87343,87344) THEN 'CRV'
        ELSE 'PCL'
    END AS banner_family,
    it_promotion_id,
    platform,
    COUNT(*) AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('02','03','04')
  AND event_name = 'view_promotion'
  AND TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (
        87340,87342,87343,87344,
        156764,156788,162326,167715,167716,167717,
        289661,289662,289664,289665,289666,289698
  )
GROUP BY 1, 2, 3
ORDER BY banner_family, it_promotion_id, n_events DESC
