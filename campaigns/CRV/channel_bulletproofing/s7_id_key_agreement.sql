-- s7 — Which id is the trustworthy key? (output ~12 rows, one screenshot)
-- s6 learning: it_promotion_id is platform-polluted ('87342.0' on Android, absent on web);
-- it_item_id ('i_'+id) looks format-stable. Before re-keying the contract to it_item_id,
-- verify: (a) do the two ids AGREE row by row, (b) is item_id ever missing on payload rows?
-- One suspicious Android row showed promotion 162326 with item i_156764 (cross-wire?).

SELECT
    platform,
    CASE
        WHEN it_item_id IS NULL OR it_item_id = '(not set)'            THEN 'item_id_missing'
        WHEN it_item_id NOT LIKE 'i\_%' ESCAPE '\'                     THEN 'item_id_no_prefix'
        WHEN TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT)
             = TRY_CAST(SUBSTR(it_item_id, 3) AS BIGINT)               THEN 'ids_agree'
        WHEN it_promotion_id IS NULL OR it_promotion_id = '(not set)'  THEN 'promotion_id_missing'
        ELSE 'IDS_DISAGREE'
    END AS id_relationship,
    COUNT(*) AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('02','03','04')
  AND event_name = 'view_promotion'
  AND (
        TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) IN (
            87340,87342,87343,87344,
            156764,156788,162326,167715,167716,167717,
            289661,289662,289664,289665,289666,289698
        )
     OR it_item_id IN (
            'i_87340','i_87342','i_87343','i_87344',
            'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
            'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
        )
  )
GROUP BY 1, 2
ORDER BY platform, n_events DESC
