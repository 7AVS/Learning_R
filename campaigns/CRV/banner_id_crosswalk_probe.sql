-- banner_id_crosswalk_probe.sql
-- Lock the GA4 key for the PCL/CRV banners against the digital team's Excel (pics 185734 + 185634).
--
-- The Excel gives Id + Offer Name. The Offer Names (PB_CC_ALL_..._VCL-LimitIncrease / VCL-Joint /
-- CC-Instalments) ARE the real GA4 creatives — the NBO-PB_CC_PCL_* names were the decisioning layer
-- and never appear in GA4 (that's why PLI came back blank).
--
-- QUESTION: does GA4 it_item_id == the Excel Id (156764, 156788, 162326, 289661-289666, 87340-87344)?
--   - if YES  -> we match on it_item_id (punctuation-proof, exact, the right GA4 key).
--   - if NO   -> the Offer Name is the only link, and the Excel writes 'VCL-LimitIncrease-CLI_Q'
--                while GA4 stores 'vcl-limitincrease_cli_q' (hyphen vs underscore), so we normalise.
--
-- Shows GA4 it_item_id NEXT TO it_item_name for every VCL / CRV banner so we can line them up to the
-- Excel by eye. The LIKE here is ONLY to discover the id<->name pairing once; the analysis itself
-- will then run off the exact it_item_id list.
SELECT
    it_item_id,
    lower(it_item_name) AS it_item_name,
    MIN(event_date)     AS first_seen,
    MAX(event_date)     AS last_seen,
    COUNT(*)            AS n_impressions
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2025', '2026')
  AND lower(event_name) = 'view_promotion'
  AND platform IN ('IOS', 'ANDROID')
  AND ( lower(it_item_name) LIKE '%vcl-limitincrease%'
     OR lower(it_item_name) LIKE '%vcl-joint%'
     OR lower(it_item_name) LIKE '%cc-instalments%' )
GROUP BY it_item_id, lower(it_item_name)
ORDER BY n_impressions DESC
;
