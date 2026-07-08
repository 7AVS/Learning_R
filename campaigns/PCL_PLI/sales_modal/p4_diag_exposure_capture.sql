-- P4-DIAG: why is modal exposure ~1% when comparable GA4 banner reach is 65-85%?
-- Isolates the CAPTURE layer on the GA4 side (no pop join yet). Counts only.
-- Engine: Starburst/Trino. Window: May-July 2026.
-- Decision rule at the bottom. Run each block, screenshot, compare client counts.

-- ============================================================================
-- BLOCK 1 - What fires under OUR item_ids? (event x location profile)
-- If 'clients' here ~= 1% of pop, item_id is capturing almost nothing.
-- ============================================================================
SELECT
  event_name,
  it_location_id,
  COUNT(*)                                                    AS rows,
  COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))        AS clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('05','06','07')
  AND it_item_id IN ('i_333067','i_333070')
GROUP BY event_name, it_location_id
ORDER BY rows DESC;

-- ============================================================================
-- BLOCK 2 - What fires on the modal SURFACE, ignoring item_id?
-- This is the true ceiling of exposable clients. If BLOCK 2 clients >> BLOCK 1
-- clients, the it_item_id filter is dropping most impressions = the culprit.
-- ============================================================================
SELECT
  event_name,
  COUNT(*)                                                    AS rows,
  COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))        AS clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('05','06','07')
  AND it_location_id IN ('IOS_Sales_Modal','Android_Sales_Modal')
GROUP BY event_name
ORDER BY rows DESC;

-- ============================================================================
-- BLOCK 3 - Every item_id present on the modal surface (view_promotion only).
-- Reveals: id-format variants (i_333067 vs 333067), extra PLI ids we missed,
-- and the shared PCD cardupgrade modal riding the same surface.
-- ============================================================================
SELECT
  it_item_id,
  it_promotion_name,
  COUNT(*)                                                    AS rows,
  COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))        AS clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('05','06','07')
  AND it_location_id IN ('IOS_Sales_Modal','Android_Sales_Modal')
  AND event_name = 'view_promotion'
GROUP BY it_item_id, it_promotion_name
ORDER BY rows DESC;

-- ============================================================================
-- DECISION RULE
--   BLOCK 2 clients >> BLOCK 1 clients  -> it_item_id filter is the culprit.
--       Fix P3/P4 to capture on it_location_id (+ PLI isolator from BLOCK 3),
--       not it_item_id IN ('i_333067','i_333070').
--   BLOCK 3 shows plain '333067'/'333070' (no i_ prefix) with big client counts
--       -> id-format split (CRV installments precedent). Add both formats.
--   BLOCK 2 clients also ~1% of pop -> capture is NOT the problem; the loss is
--       at the pop join or it_location_id is wrong. Next step: funnel to pop.
--   BLOCK 1 shows item_id is NULL on view_promotion but populated on
--       select_promotion -> impressions carry no item_id at all; must use location.
-- ============================================================================
