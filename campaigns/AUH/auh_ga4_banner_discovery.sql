-- AUH Phase 2 OLB banner discovery (Starburst/Trino)
-- Source of IDs: Salesforce offer info (pics/20260611_060435.jpg, "Auth user offer salesforce info.xlsx")
-- 8 creatives, all INLINE_CARD_OMNI / OLB_Account_SummaryOMNI. Phase 2 launch 2026-04-30 (tactic 2026119AUH).
-- Run Q1a/Q1b/Q2 to find which GA4 field carries the AUH banner; group by event_name resolves view_promotion vs view_item.


-- Q1a: it_item_id = 'i_' || offer_id (CTU i_300102 / O2P i_298045 convention)
SELECT
    it_item_id,
    it_item_name,
    event_name,
    COUNT(*)                              AS events,
    COUNT(DISTINCT up_srf_id2_value)      AS users,
    MIN(event_date)                       AS first_dt,
    MAX(event_date)                       AS last_dt
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND event_date >= DATE '2026-04-30'
  AND it_item_id IN ('i_300108','i_308317','i_308314','i_308315',
                     'i_308333','i_308334','i_308335','i_308336')
GROUP BY 1, 2, 3
ORDER BY 1, 3;


-- Q1b: bare offer_id in it_promotion_id (CRV/PCL convention)
SELECT
    it_promotion_id,
    it_item_id,
    it_item_name,
    event_name,
    COUNT(*)                              AS events,
    COUNT(DISTINCT up_srf_id2_value)      AS users,
    MIN(event_date)                       AS first_dt,
    MAX(event_date)                       AS last_dt
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND event_date >= DATE '2026-04-30'
  AND it_promotion_id IN ('300108','308317','308314','308315',
                          '308333','308334','308335','308336')
GROUP BY 1, 2, 3, 4
ORDER BY 1, 4;


-- Q2: exact Salesforce item names (lowercased exact match, no substrings)
SELECT
    it_item_id,
    it_item_name,
    event_name,
    COUNT(*)                              AS events,
    COUNT(DISTINCT up_srf_id2_value)      AS users,
    MIN(event_date)                       AS first_dt,
    MAX(event_date)                       AS last_dt
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND event_date >= DATE '2026-04-30'
  AND lower(it_item_name) IN (
      'pb-dm_cc_all_26_04_rbccmptgt_auh_nonrewards_olb',
      'pb-dm_cc_all_26_04_rbccmptgt_auh_rewardsnonoffer_olb',
      'pb-dm_cc_iav_26_04_rbccmptgto_auh_offeriav_olb',
      'pb-dm_cc_gcp_26_04_rbccmptgto_auh_offergcp_olb',
      'pb-dm_cc_mc4_26_04_rbccmptgto_auh_offermc4_olb',
      'pb-dm_cc_mc2_26_04_rbccmptgto_auh_offermc2_olb',
      'pb-dm_cc_avp_26_04_rbccmptgto_auh_offeravp_olb',
      'pb-dm_cc_gpr_26_04_rbccmptgto_auh_offergpr_olb')
GROUP BY 1, 2, 3
ORDER BY 2, 3;


-- Q3: wide net fallback — anything tagged AUH in the Salesforce mnemonic
SELECT
    ip_sf_campaign_mnemonic,
    it_item_id,
    it_promotion_id,
    it_item_name,
    event_name,
    COUNT(*)                              AS events,
    COUNT(DISTINCT up_srf_id2_value)      AS users
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND event_date >= DATE '2026-04-30'
  AND ip_sf_campaign_mnemonic = 'AUH'
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESC;
