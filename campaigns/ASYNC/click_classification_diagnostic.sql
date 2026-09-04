-- ═══════════════════════════════════════════════════════════════════════════════
-- Ad-hoc diagnostics — async banner measurement.
-- Append new ad-hoc queries here rather than creating separate files.
-- Each section is independent; run one at a time.
-- ═══════════════════════════════════════════════════════════════════════════════


-- ── 1) Click classification ────────────────────────────────────────────────────
-- Surfaces every (event_name, it_creative_name) combo + new_class + counts.
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


-- ── 2) Campaign × test_group × banner mapping (PCD) ────────────────────────────
-- Cross-tab to see if each (strategy_seg_cd, test_groups_period) sees one banner
-- or many.
--   One row per (campaign_id, test_group)            → isolated pathway
--   Multiple rows with different banner names        → flow-through / multi-banner
-- For CTU / O2P, swap the cohort source (TACTIC_EVNT_IP_AR_HIST with their tactic_ids)
-- and the GA4 item filter.

WITH
cohort AS (
    SELECT clnt_no, response_start, response_end, strategy_seg_cd, test_groups_period
    FROM dw00_im.dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent IN ('2026111PCD','2026125PCD')
      AND response_start >= DATE '2026-04-01'
),

ga4 AS (
    SELECT
        event_date, event_name, it_item_name,
        COALESCE(TRY_CAST(up_srf_id2_value AS BIGINT), TRY_CAST(ep_srf_id2 AS BIGINT)) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND lower(event_name) IN ('view_promotion','select_promotion')
)

SELECT
    c.strategy_seg_cd                                     AS campaign_id,
    c.test_groups_period                                  AS test_group,
    g.it_item_name                                        AS banner,
    COUNT(*)                                              AS events,
    COUNT(DISTINCT c.clnt_no)                             AS distinct_clients
FROM cohort c
INNER JOIN ga4 g
    ON  g.clnt_no = c.clnt_no
    AND g.event_date BETWEEN c.response_start
                         AND date_add('day', 60, c.response_start)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
