-- Diagnostic: surface every (event_name, it_creative_name) combo + new_class + counts.
-- Any row with new_class = 'OTH' is an unmapped creative_name — add it to the IN list.
-- Swap the item filter at the bottom to test CTU or O2P.

SELECT
    event_name,
    it_creative_name,
    CASE
        WHEN lower(event_name) = 'view_promotion' THEN 'view'
        WHEN it_creative_name IN (
            'n_Non intéressé','n_Not interested','n_Not now','n_Pas maintenant',
            'Not now','Pas maintenant','n_close','Close'
        ) THEN 'click_n'
        WHEN it_creative_name IN (
            'p_Chat to learn more','p_Chat with us','p_Clavarder avec nous',
            'p_Clavardez pour en savoir plus','Chat to learn more',
            'Clavardez pour en savoir plus','VSA_OFFER_SF'
        ) THEN 'click_p'
        ELSE 'OTH'
    END                                                              AS new_class,
    COUNT(*)                                                         AS events,
    COUNT(DISTINCT COALESCE(TRY_CAST(up_srf_id2_value AS BIGINT),
                            TRY_CAST(ep_srf_id2 AS BIGINT)))         AS distinct_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('04','05','06')
  AND event_date >= DATE '2026-04-01'
  AND it_item_name IN (   -- PCD; swap with the CTU/O2P filters below as needed
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
        'PB_CC_ALL_26_02_RBC_PCD_PPCN',
        'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
  )
  -- CTU:  AND lower(it_item_id) IN ('i_300102')
  -- O2P:  AND lower(it_item_id) IN ('i_298045')
  AND lower(event_name) IN ('view_promotion','select_promotion')
GROUP BY 1,2,3
ORDER BY new_class, events DESC
;
