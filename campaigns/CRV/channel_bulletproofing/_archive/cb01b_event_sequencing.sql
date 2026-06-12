-- cb01b_event_sequencing.sql
--
-- PURPOSE: Test whether view_item / view_promotion / select_promotion form a sequential funnel
--   (standard GA4 spec predicts view_promotion → select_promotion → view_item, i.e. view_item
--   is post-click) vs independent/duplicate impression events. Complements cb01 which showed
--   which events fire; this query shows whether they are ordered.
-- UNIVERSE: PCL promotion IDs Feb–Apr 2026. GA4 _reduced table (has event_timestamp µs and
--   ep_ga_session_id). Counts only — no rate columns.
-- Trino/Starburst syntax.

-- ============================================================
-- STATEMENT 1 — session event-pattern census
-- ============================================================
-- Per session (ep_ga_session_id + client), which of the 3 event types occurred for PCL promo IDs?
-- One row per pattern (has_view_promotion / has_view_item / has_select_promotion each 0/1).
-- Shows nesting/exclusivity at a glance — e.g. do view_item sessions always have view_promotion?

WITH session_flags AS (
    SELECT
        ep_ga_session_id,
        TRY_CAST(up_srf_id2_value AS BIGINT)                               AS clnt_no,
        MAX(CASE WHEN event_name = 'view_promotion'   THEN 1 ELSE 0 END)   AS has_view_promotion,
        MAX(CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END)   AS has_view_item,
        MAX(CASE WHEN event_name = 'select_promotion' THEN 1 ELSE 0 END)   AS has_select_promotion
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_promotion', 'view_item', 'select_promotion')
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
      )
      AND ep_ga_session_id IS NOT NULL
      AND TRY_CAST(up_srf_id2_value AS BIGINT) IS NOT NULL
    GROUP BY ep_ga_session_id, TRY_CAST(up_srf_id2_value AS BIGINT)
)
SELECT
    has_view_promotion,
    has_view_item,
    has_select_promotion,
    COUNT(*)                                  AS sessions,
    COUNT(DISTINCT clnt_no)                   AS clients
FROM session_flags
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2 DESC, 3 DESC
;

-- ============================================================
-- STATEMENT 2 — ordering within sessions that have both events
-- ============================================================
-- For sessions containing both members of each pair, count which event fires FIRST
-- (by MIN event_timestamp). Three pairs:
--   view_promotion vs view_item       — is impression before or after view_item?
--   select_promotion vs view_item     — does the click precede view_item? (post-click confirmation)
--   view_promotion vs select_promotion — does impression precede click? (expected funnel order)
-- Output: one row per (comparison_pair, first_event) with session count.

WITH event_times AS (
    SELECT
        ep_ga_session_id,
        TRY_CAST(up_srf_id2_value AS BIGINT)                               AS clnt_no,
        MIN(CASE WHEN event_name = 'view_promotion'   THEN event_timestamp END) AS ts_vp,
        MIN(CASE WHEN event_name = 'view_item'        THEN event_timestamp END) AS ts_vi,
        MIN(CASE WHEN event_name = 'select_promotion' THEN event_timestamp END) AS ts_sp
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_promotion', 'view_item', 'select_promotion')
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
      )
      AND ep_ga_session_id IS NOT NULL
      AND TRY_CAST(up_srf_id2_value AS BIGINT) IS NOT NULL
    GROUP BY ep_ga_session_id, TRY_CAST(up_srf_id2_value AS BIGINT)
),
pairs AS (
    -- pair 1: view_promotion vs view_item
    SELECT
        'view_promotion vs view_item'                                       AS comparison_pair,
        CASE WHEN ts_vp < ts_vi  THEN 'view_promotion'
             WHEN ts_vi < ts_vp  THEN 'view_item'
             ELSE                     'equal' END                           AS first_event,
        COUNT(*)                                                            AS sessions
    FROM event_times
    WHERE ts_vp IS NOT NULL AND ts_vi IS NOT NULL
    GROUP BY 2

    UNION ALL

    -- pair 2: select_promotion vs view_item
    SELECT
        'select_promotion vs view_item'                                     AS comparison_pair,
        CASE WHEN ts_sp < ts_vi  THEN 'select_promotion'
             WHEN ts_vi < ts_sp  THEN 'view_item'
             ELSE                     'equal' END                           AS first_event,
        COUNT(*)                                                            AS sessions
    FROM event_times
    WHERE ts_sp IS NOT NULL AND ts_vi IS NOT NULL
    GROUP BY 2

    UNION ALL

    -- pair 3: view_promotion vs select_promotion
    SELECT
        'view_promotion vs select_promotion'                                AS comparison_pair,
        CASE WHEN ts_vp < ts_sp  THEN 'view_promotion'
             WHEN ts_sp < ts_vp  THEN 'select_promotion'
             ELSE                     'equal' END                           AS first_event,
        COUNT(*)                                                            AS sessions
    FROM event_times
    WHERE ts_vp IS NOT NULL AND ts_sp IS NOT NULL
    GROUP BY 2
)
SELECT
    comparison_pair,
    first_event,
    sessions
FROM pairs
ORDER BY comparison_pair, sessions DESC
;

-- ============================================================
-- STATEMENT 3 — single-client drill-down template
-- ============================================================
-- Parameterized: set the client number below, then run to eyeball one client's full event
-- sequence in the window. Useful for confirming the ordering pattern found in Stmt 2.

-- SET CLIENT HERE
-- clnt_no = 123456789

SELECT
    event_date,
    event_timestamp,
    ep_ga_session_id,
    event_name,
    it_promotion_id,
    it_creative_name
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND event_name IN ('view_promotion', 'view_item', 'select_promotion')
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666'   -- PCL
  )
  AND TRY_CAST(up_srf_id2_value AS BIGINT) = 123456789   -- SET CLIENT HERE
ORDER BY event_timestamp
;
