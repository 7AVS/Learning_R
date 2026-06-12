-- cb03_position_order_check.sql
--
-- OPTIONAL — directly tests the channel's "PLI displayed in front" claim.
--   Run after CB01 confirms which impression event fires and what IDs are live.
-- PURPOSE: (1) discover whether it_creative_slot / it_item_index encode screen position
--   for PCL and CRV banners; (2) test whether CRV impressions consistently precede PCL
--   impressions within the same app session, which would support a slot-competition story
--   even if reach is equal.
-- Uses the FULL ecommerce table (not _reduced) — position fields dropped in reduced.
-- UNIVERSE window: Feb–Apr 2026 (year/month pruned). Open-ended CRV IDs consistent with Q20.
-- Trino/Starburst syntax. Counts only — no rate columns. LIMIT 50 on field EDA statements.

-- ============================================================
-- STATEMENT 1 — position field EDA
-- ============================================================
-- Discover whether it_creative_slot or it_item_index encode screen position.
-- Two separate GROUP BYs, ordered by count DESC, limited to top 50 values each.
-- Run both; if both are null/blank/single-value, position data is not available here.

-- 1a: it_creative_slot distribution for PCL + CRV impression events
SELECT
    it_creative_slot,
    COUNT(*)   AS n_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name IN ('view_item', 'view_promotion')
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',   -- PCL
        '87348','87342','87343','87344'                                           -- CRV
  )
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50
;

-- 1b: it_item_index distribution for PCL + CRV impression events
SELECT
    it_item_index,
    COUNT(*)   AS n_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name IN ('view_item', 'view_promotion')
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',   -- PCL
        '87348','87342','87343','87344'                                           -- CRV
  )
GROUP BY 1
ORDER BY 2 DESC
LIMIT 50
;

-- ============================================================
-- STATEMENT 2 — within-session impression ordering
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
