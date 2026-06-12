-- s3_event_timestamp_test.sql
-- Identity key = it_item_id ('i_'+offer id) per s7 2026-06-12: format-stable all platforms, zero disagreement, catches rows where promotion_id is absent.
-- PURPOSE: Are view_item and view_promotion twins? s1 showed 1:1 volumes; this proves/refutes
--   at the timestamp level — same client, same session, same banner code.
-- Trino syntax. _reduced table. Counts only.

-- ============================================================
-- STATEMENT 1 — twin test: view_item vs view_promotion timestamp comparison
-- ============================================================
-- Sessions (ep_ga_session_id + client) having BOTH a view_item AND a view_promotion
-- on the SAME it_promotion_id, PCL or CRV ids, Feb–Apr 2026.
-- Compare MIN(event_timestamp) per event type within (session, promotion_id).
-- Buckets: same_timestamp (exact), within_1_second (abs diff <= 1,000,000 µs, not equal),
--   view_promotion_first (>1s, view_promotion earlier), view_item_first (>1s, view_item earlier).
-- One row per banner_family × bucket.

WITH raw AS (
    SELECT
        CASE
            WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698') THEN 'PCL'
            WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        END                                                             AS banner_family,
        ep_ga_session_id,
        TRY_CAST(up_srf_id2_value AS BIGINT)                           AS clnt_no,
        it_item_id,
        MIN(CASE WHEN event_name = 'view_promotion' THEN event_timestamp END) AS ts_vp,
        MIN(CASE WHEN event_name = 'view_item'      THEN event_timestamp END) AS ts_vi
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_promotion', 'view_item')
      AND it_item_id IN (
            'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698',   -- PCL
            'i_87340','i_87342','i_87343','i_87344'                                                                                               -- CRV
      )
      AND ep_ga_session_id IS NOT NULL
      AND TRY_CAST(up_srf_id2_value AS BIGINT) IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
session_pairs AS (
    SELECT
        banner_family,
        CASE
            WHEN ts_vp = ts_vi                                 THEN 'same_timestamp'
            WHEN ABS(ts_vp - ts_vi) <= 1000000                 THEN 'within_1_second'
            WHEN ts_vp < ts_vi                                 THEN 'view_promotion_first'
            ELSE                                                    'view_item_first'
        END                                                     AS bucket
    FROM raw
    WHERE ts_vp IS NOT NULL AND ts_vi IS NOT NULL
)
SELECT
    banner_family,
    bucket,
    COUNT(*)                                                    AS sessions
FROM session_pairs
GROUP BY 1, 2
ORDER BY banner_family, sessions DESC
;

-- ============================================================
-- STATEMENT 2 — click ordering sanity: select_promotion vs view_promotion
-- ============================================================
-- Sessions having BOTH select_promotion and view_promotion on the same promotion_id.
-- Counts of view_promotion_first vs select_promotion_first vs same_timestamp per banner_family.
-- Clicks should follow impressions — sanity check on timestamp reliability.

WITH raw AS (
    SELECT
        CASE
            WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698') THEN 'PCL'
            WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        END                                                             AS banner_family,
        ep_ga_session_id,
        TRY_CAST(up_srf_id2_value AS BIGINT)                           AS clnt_no,
        it_item_id,
        MIN(CASE WHEN event_name = 'view_promotion'   THEN event_timestamp END) AS ts_vp,
        MIN(CASE WHEN event_name = 'select_promotion' THEN event_timestamp END) AS ts_sp
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND event_name IN ('view_promotion', 'select_promotion')
      AND it_item_id IN (
            'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698',   -- PCL
            'i_87340','i_87342','i_87343','i_87344'                                                                                               -- CRV
      )
      AND ep_ga_session_id IS NOT NULL
      AND TRY_CAST(up_srf_id2_value AS BIGINT) IS NOT NULL
    GROUP BY 1, 2, 3, 4
),
session_pairs AS (
    SELECT
        banner_family,
        CASE
            WHEN ts_vp = ts_sp  THEN 'same_timestamp'
            WHEN ts_vp < ts_sp  THEN 'view_promotion_first'
            ELSE                    'select_promotion_first'
        END                                                     AS bucket
    FROM raw
    WHERE ts_vp IS NOT NULL AND ts_sp IS NOT NULL
)
SELECT
    banner_family,
    bucket,
    COUNT(*)                                                    AS sessions
FROM session_pairs
GROUP BY 1, 2
ORDER BY banner_family, sessions DESC
;

-- ============================================================
-- STATEMENT 3 — single-client drill-down
-- ============================================================
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
  AND it_item_id IN (
        'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698',   -- PCL
        'i_87340','i_87342','i_87343','i_87344'                                                                                               -- CRV
  )
  AND TRY_CAST(up_srf_id2_value AS BIGINT) = 123456789   -- SET CLIENT HERE
ORDER BY event_timestamp
;
