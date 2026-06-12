-- cb03_position_order_check.sql
--
-- OPTIONAL — directly tests the channel's "PLI displayed in front" claim.
--   Run after CB01 confirms which impression event fires and what IDs are live.
-- PURPOSE: (1) confirm it_item_index encodes screen position for PCL and CRV banners
--   (confirmed primary field per RBC Salesforce mapping May 2024 — displayOrder);
--   (2) cross-tab position distribution by banner family and overlap arm;
--   (3) secondary EDA on it_creative_slot (= primaryButton.title on mobile, NOT position)
--   and it_location_id (areaName: hero_banner / sub_banner_# / callout_#);
--   (4) test whether CRV impressions consistently precede PCL impressions within the same
--   app session, which would support a slot-competition story even if reach is equal.
-- Uses the FULL ecommerce table (not _reduced) — it_item_index dropped from _reduced.
-- UNIVERSE window: Feb–Apr 2026 (year/month pruned). Open-ended CRV IDs consistent with Q20.
-- Trino/Starburst syntax. Counts only — no rate columns.
-- OPEN QUESTION: mobile carousel firing behavior (does view_promotion fire for all banners
--   at load, or only on swipe-into-view?) is NOT documented in RBC analytics specs.
--   Confirm with digital team before interpreting position distributions as exposure counts.

-- ============================================================
-- STATEMENT 1 — it_item_index position distribution (PRIMARY)
-- ============================================================
-- it_item_index = displayOrder per RBC Salesforce GA4 mapping (confirmed May 2024).
-- This is the primary field for inferring screen position of the banner slot.
-- view_promotion only — confirmed impression event for banners (view_item = product
-- detail page visit, not an impression).

-- 1a: banner_family × it_item_index — all clients, view_promotion, Feb–Apr 2026
SELECT
    CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666') THEN 'PCL'
         WHEN it_promotion_id IN ('87348','87342','87343','87344') THEN 'CRV' END  AS banner_family,
    it_item_index,
    COUNT(*)                                                                        AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                           AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name = 'view_promotion'
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',
        '87348','87342','87343','87344'
  )
GROUP BY 1, 2
ORDER BY 1, 2, 3 DESC
;

-- 1b: banner_family × it_item_index × overlap_arm — view_promotion, Feb–Apr 2026
-- overlap_arm = 'overlap' if the client received BOTH PCL and CRV view_promotion
-- events in the same window; 'non_overlap' otherwise.
SELECT
    CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666') THEN 'PCL'
         WHEN it_promotion_id IN ('87348','87342','87343','87344') THEN 'CRV' END  AS banner_family,
    it_item_index,
    CASE WHEN TRY_CAST(up_srf_id2_value AS BIGINT) IN (
            -- clients with BOTH PCL and CRV view_promotion events in the same window
            SELECT TRY_CAST(up_srf_id2_value AS BIGINT)
            FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
            WHERE year  IN ('2026')
              AND month IN ('02', '03', '04')
              AND event_name = 'view_promotion'
              AND it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
            INTERSECT
            SELECT TRY_CAST(up_srf_id2_value AS BIGINT)
            FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
            WHERE year  IN ('2026')
              AND month IN ('02', '03', '04')
              AND event_name = 'view_promotion'
              AND it_promotion_id IN ('87348','87342','87343','87344')
        ) THEN 'overlap' ELSE 'non_overlap' END  AS overlap_arm,
    COUNT(*)                                      AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name = 'view_promotion'
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',
        '87348','87342','87343','87344'
  )
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;

-- ============================================================
-- STATEMENT 2 — it_creative_slot and it_location_id EDA (SECONDARY)
-- ============================================================
-- it_creative_slot = primaryButton.title on mobile per RBC mapping — this is button
-- text (e.g. "Apply Now"), NOT banner position. Do not interpret as a slot index.
-- it_location_id = areaName (e.g. hero_banner, sub_banner_1, callout_2) — may encode
-- placement area on the page; useful for confirming whether PCL and CRV compete in the
-- same zone.
-- Both scoped to view_promotion only (same rationale as Statement 1).

-- 2a: it_creative_slot distribution for PCL + CRV, view_promotion, Feb–Apr 2026
SELECT
    it_creative_slot,
    COUNT(*)   AS n_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name = 'view_promotion'
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',   -- PCL
        '87348','87342','87343','87344'                                           -- CRV
  )
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50
;

-- 2b: banner_family × it_location_id — areaName zone placement, view_promotion, Feb–Apr 2026
SELECT
    CASE WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666') THEN 'PCL'
         WHEN it_promotion_id IN ('87348','87342','87343','87344') THEN 'CRV' END  AS banner_family,
    it_location_id,
    COUNT(*)                                                                        AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))                           AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name = 'view_promotion'
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',   -- PCL
        '87348','87342','87343','87344'                                           -- CRV
  )
GROUP BY 1, 2
ORDER BY 1, 3 DESC
;

-- ============================================================
-- STATEMENT 3 — within-session impression ordering
-- ============================================================
-- Sessions with BOTH a PCL impression event AND a CRV impression event in Feb–Apr 2026.
-- For each such session: is the first PCL impression timestamp earlier or later than the
-- first CRV impression timestamp? Counts by month. "PCL first" vs "CRV first" tells us
-- whether the channel systematically places CRV ahead of PCL in the same session, which
-- would be consistent with attention displacement even if reach counts are equal.

WITH
ga4_pcl AS (
    SELECT
        ep_ga_session_id,
        TRY_CAST(up_srf_id2_value AS BIGINT)  AS clnt_no,
        MIN(event_timestamp)                   AS first_pcl_ts,
        year,
        month
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_item', 'view_promotion')
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
      )
    GROUP BY ep_ga_session_id, TRY_CAST(up_srf_id2_value AS BIGINT), year, month
),
ga4_crv AS (
    SELECT
        ep_ga_session_id,
        TRY_CAST(up_srf_id2_value AS BIGINT)  AS clnt_no,
        MIN(event_timestamp)                   AS first_crv_ts,
        year,
        month
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_item', 'view_promotion')
      AND it_promotion_id IN (
            '87348','87342','87343','87344'   -- CRV
      )
    GROUP BY ep_ga_session_id, TRY_CAST(up_srf_id2_value AS BIGINT), year, month
),
both_sessions AS (
    SELECT
        p.month,
        CASE WHEN p.first_pcl_ts < c.first_crv_ts  THEN 'pcl_first'
             WHEN p.first_pcl_ts > c.first_crv_ts  THEN 'crv_first'
             ELSE                                        'same_timestamp' END  AS impression_order
    FROM ga4_pcl p
    INNER JOIN ga4_crv c
      ON  c.ep_ga_session_id = p.ep_ga_session_id
      AND c.clnt_no          = p.clnt_no
)
SELECT
    month,
    impression_order,
    COUNT(*)   AS sessions
FROM both_sessions
GROUP BY 1, 2
ORDER BY 1, 2
;
