-- =============================================================================
-- GA4 Ecommerce Table — Field Mapping Reference
-- =============================================================================
-- Table: edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
-- Platform: Starburst (Trino-compatible SQL)
-- EDA date: 2026-04-01 (March + April 2026 data)
-- Source: Query 0a-0f from pcd_esv_banner_explore.sql
--
-- This document catalogs what each field contains so we don't have to
-- re-run exploratory queries. Use this as the reference for building
-- any campaign tracker against GA4.
--
-- =============================================================================
-- FIELD MAPPING — What lives where
-- =============================================================================
--
-- BANNER/PROMO TRACKING FIELDS:
--
--   it_item_name              THE field for promo/banner names.
--                             Contains: PB_CC_*, PB_CHEQ_*, PB_SAV_*, NBO-PB_*, NOM_* patterns
--                             This is where Rajani's confirmed promo names land.
--                             USE THIS for banner identification.
--
--   selected_promotion_name   NOT for promo names. Contains Salesforce insight names:
--                             OLM_INSIGHT_SF (145M events), mortgage_insight_sf,
--                             INSIGHT_NEEDS_ATTENTION, OFFER_GHOST_ACCOUNT, etc.
--                             Also contains user ACTION labels on select_promotion events:
--                             "n_Not Interested", "p_View offer", "DDA_OFFER_SF"
--                             DO NOT use this for banner identification.
--
--   it_creative_name          Contains creative IDs: CRTV-289524, CRTV-289526, etc.
--                             Plus some descriptive names: ASP_AutoSavingsIntroduction_UC2,
--                             BudgetRecommendationSpendingCategory_UC1, etc.
--                             Useful for creative-level breakdowns, not for campaign filtering.
--
--   ip_sf_campaign_mnemonic   Campaign mnemonic codes. GOLD for campaign-level filtering.
--                             Top values (March-April 2026):
--                               (blank)    — 1.26B events
--                               SPOTLIGHT  — 155M
--                               ESV        — 148M
--                               VCL        — 43M
--                               PCD        — 38M  ← PCD already has touchpoints (not async banner yet)
--                               O2P        — 21M
--                               MMC        — 19M
--                               EBG        — 14M
--                               PCQ        — 13M  ← PCQ
--                               PCL        — 836K ← PCL
--                               RCL        — 796K
--                               PPW        — 645K
--                               PPQ        — 435K
--                               BOL        — 418K
--                               LTA        — 336K
--                               NBR        — 206K
--                               PAL        — 132K
--                               RCU        — 116K
--                               API        — 109K
--                               tao        — 74K
--                               AUS        — 65K
--                               VBA        — 62K  ← VBA
--                               PBM        — 46K
--                             Note: case-sensitive (ESV vs esv, tao vs TAO exist separately)
--
-- EVENT TYPES (event_name):
--
--   view_promotion             Banner VIEW/impression. Highest volume promo event.
--                              613M events, 18.4M users.
--                              USE THIS for banner views.
--
--   select_promotion           Banner CLICK/selection.
--                              721K events, ~24K users.
--                              USE THIS for banner clicks.
--
--   experience_impression      General experience impressions. 96M events, 7.7M users.
--                              May overlap with banner impressions — not needed for
--                              promo-specific tracking.
--
--   view_item                  Product/item page view. 371M events. Not promo-specific.
--   view_item_list             Item list view. 96M events.
--   select_item                Item selection. 1.1M events.
--   session_start              Session start. 227M events.
--   first_visit                First visit. 24M events.
--   begin_checkout             Checkout start. 2.6M events.
--   progress_checkout          Checkout progress. 4.7M events.
--   purchase                   Purchase. 1.1M events.
--   model_result               Model result. 4.3M events.
--   add_to_cart                Cart add. 135K events.
--   click                      Generic click. 354K events.
--   begin_lead                 Lead start. 176K events.
--   begin_tool                 Tool start. 235K events.
--   generate_lead              Lead generated. 11K events.
--   generate_quote             Quote generated. 25K events.
--   generate_tool              Tool generated. 82K events.
--   progress_lead              Lead progress. 147K events.
--   progress_quote             Quote progress. 454K events.
--   progress_tool              Tool progress. 246K events.
--   begin_quote                Quote start. 6.5K events.
--   remove_from_cart           Cart removal. 6.2K events.
--
-- PLATFORMS:
--   IOS       — 717M events, 6.0M users
--   WEB       — 483M events, 25.0M users
--   ANDROID   — 250M events, 2.3M users
--
-- =============================================================================
-- BANNER TRACKER RECIPE (for any campaign)
-- =============================================================================
--
-- 1. Filter:  it_item_name IN (<confirmed promo names from Rajani/deployment team>)
-- 2. Events:  event_name IN ('view_promotion', 'select_promotion')
-- 3. Optional: ip_sf_campaign_mnemonic = '<MNE>' as belt-and-suspenders
-- 4. Group by: event_date, event_name, platform
-- 5. Pivot:   Views = view_promotion count, Clicks = select_promotion count
-- 6. CTR:     Clicks / Views (on unique_users basis)
--
-- =============================================================================
-- ESV VALIDATION FINDINGS (2026-04-01)
-- =============================================================================
--
-- Confirmed ESV promo names from Rajani (2026-03-18):
--   1. PB_CHEQ_ALL_26_02_RBC_HISA_Offer_Hub_Banner       — NOT showing in data
--   2. PB_CHEQ_ALL_26_02_RBC_HISA_PDA_Product_Page        — ACTIVE (11.5K viewers, 1.3K clickers)
--   3. PB_CHEQ_ALL_26_02_RBC_ESV_HISA_Acquisition_Campaign_Banner — NOT showing in data
--
-- Other ESV traffic: PB_SAV_HIES_26_01_* entries dominate ESV volume.
-- These are older/different ESV variants not in Rajani's confirmed list.
--
-- ESV CTR (PDA_Product_Page only):
--   select_promotion / view_promotion = 1,264 / 11,536 = ~11% on users
--   (Events basis: 1,797 / 52,685 = ~3.4%)
--
-- On select_promotion events, selected_promotion_name shows the user ACTION:
--   "n_Not Interested" = user dismissed
--   "p_View offer"     = user clicked to view the offer
--   "DDA_OFFER_SF"     = Salesforce action label
--   These can be used to separate positive clicks from dismissals.
--
-- =============================================================================


-- =============================================================================
-- EXISTING PCD PROMO NAMES (pre-async, as of March 2026)
-- =============================================================================
-- These are OLDER PCD campaigns, NOT the new async banners launching ~April 20.
-- The new async banners will use PB_CC_ALL_26_02_RBC_PCD_* prefix (from Rajani).
-- These older ones use NBO_PB_CC_PCD_* or NBO-PB_CC_PCD_* prefixes.
--
--   NBO_PB_CC_PCD_24_09_RBC_AVP_70K          (Sep 2024) — 57,500 viewers, 981 clickers, 1.7% CTR
--   NBO-PB_CC_PCD_22_10_RBC_IOP-CreditCard-Upgrade (Oct 2022) — 54,106 viewers, 828 clickers, 1.5% CTR
--   NBO-PB_CC_PCD_22_10_RBC_ION-CreditCard-Upgrade (Oct 2022) — 52,618 viewers, 843 clickers, 1.6% CTR
--   NBO_PB_CC_PCD_24_09_RBC_IAV              (Sep 2024) — 43,667 viewers, 931 clickers, 2.1% CTR
--   NBO_PB_CC_PCD_24_09_RBC_AVP_25K          (Sep 2024) — 15,207 viewers, 159 clickers, 1.0% CTR
--   NBO_PB_CC_PCD_24_09_RBC_GCP              (Sep 2024) — 14,541 viewers, 296 clickers, 2.0% CTR
--
-- Baseline PCD banner CTR range: 1.0% - 2.1% (user basis, March 2026 data)
-- Use this as comparison baseline for the new async banners post-launch.
--
-- =============================================================================
-- VALIDATION QUERY: Re-run to verify field mapping is still accurate
-- =============================================================================
-- Run this periodically if you suspect the schema or tagging has changed.

SELECT
    event_name,
    it_item_name,
    selected_promotion_name,
    ip_sf_campaign_mnemonic,
    platform,
    COUNT(DISTINCT user_pseudo_id) AS unique_users,
    COUNT(*)                       AS total_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE
    year = '2026'
    AND month = '04'
    AND event_name IN ('view_promotion', 'select_promotion')
    AND ip_sf_campaign_mnemonic IN ('PCD', 'ESV')
GROUP BY
    event_name,
    it_item_name,
    selected_promotion_name,
    ip_sf_campaign_mnemonic,
    platform
ORDER BY total_events DESC
LIMIT 100;
