-- cb04_journey_vocabulary_census.sql
--
-- PURPOSE: GA4 journey-vocabulary census for the channel bulletproofing track.
--   Maps what event_name / ep_firebase_screen / ep_details strings actually appear
--   in sessions touching the PCL or CRV banners. Feeds CB01–CB03 interpretation and
--   reconciles against the CLI dashboard's documented logic strings.
-- TABLE: tsz_00198_data_ga4_ecommerce_reduced (_reduced has the history; full table only
--   holds ~2 weeks). Both ep_firebase_screen and ep_details are present in _reduced.
--   it_creative_name is also in _reduced — p.../n... prefix discrimination is viable.
--   ip_sf_* block is ABSENT from _reduced (dropped at ingestion) — no GA4-native
--   Action/Control or offer-window filtering; arm comes from the tactic table.
-- FIELD NOTE: ep_firebase_screen and ep_details are varchar in _reduced; exact-match
--   equality used throughout (dashboard convention). No ep_item_index in _reduced —
--   use it_item_index (confirmed present in full table only; not in the 60-col list).
-- PERIOD: Feb–Apr 2026 (year IN ('2026') AND month IN ('02','03','04')).
-- CONVENTIONS: Trino syntax. Counts only. No rate columns.

-- ============================================================
-- STMT 1 — Banner-anchored vocabulary census
-- ============================================================
-- Sessions (ep_ga_session_id + client) touching a PCL or CRV promotion_id in Feb-Apr 2026.
-- For ALL events in those sessions: GROUP BY event_name, ep_firebase_screen, ep_details
-- to reveal every screen/action string that appears in the post-banner journey.

WITH banner_sessions AS (
    SELECT DISTINCT
        up_srf_id2_value                                                AS client_key,
        ep_ga_session_id
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year  IN ('2026')
      AND month IN ('02', '03', '04')
      AND it_promotion_id IN (
            '156764','156788','162326','289661','289662','289664','289665','289666',  -- PCL
            '87348','87342','87343','87344'                                           -- CRV
      )
),
session_events AS (
    SELECT
        g.event_name,
        g.ep_firebase_screen,
        g.ep_details,
        g.up_srf_id2_value                                              AS client_key,
        g.ep_ga_session_id
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced g
    INNER JOIN banner_sessions s
      ON  s.client_key     = g.up_srf_id2_value
      AND s.ep_ga_session_id = g.ep_ga_session_id
    WHERE g.year  IN ('2026')
      AND g.month IN ('02', '03', '04')
)
SELECT
    event_name,
    ep_firebase_screen,
    ep_details,
    COUNT(*)                                                            AS n_events,
    COUNT(DISTINCT client_key)                                          AS n_clients,
    COUNT(DISTINCT ep_ga_session_id)                                    AS n_sessions
FROM session_events
GROUP BY 1, 2, 3
ORDER BY n_events DESC
LIMIT 300
;

-- ============================================================
-- STMT 2 — Creative-name census by banner family
-- ============================================================
-- For events directly on the PCL or CRV promotion_ids: what it_creative_name values
-- appear? Reveals the actual p.../n... prefix strings the dashboard's click classifier uses.

SELECT
    CASE
        WHEN it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
            THEN 'PCL'
        WHEN it_promotion_id IN ('87348','87342','87343','87344')
            THEN 'CRV'
        ELSE 'other'
    END                                                                 AS banner_family,
    event_name,
    it_creative_name,
    COUNT(*)                                                            AS n_events,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))               AS n_clients,
    COUNT(DISTINCT ep_ga_session_id)                                    AS n_sessions
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year  IN ('2026')
  AND month IN ('02', '03', '04')
  AND it_promotion_id IN (
        '156764','156788','162326','289661','289662','289664','289665','289666',  -- PCL
        '87348','87342','87343','87344'                                           -- CRV
  )
GROUP BY 1, 2, 3
ORDER BY banner_family, n_events DESC
;

-- ============================================================
-- STMT 3 — Documented-string presence check (CLI dashboard logic)
-- ============================================================
-- One row per documented string from the CLI GA4 mobile dashboard.
-- Exact-match count + distinct clients, then a family-level LIKE total at the end
-- so any drift (hyphen variants, case changes) is visible by subtraction.

SELECT
    'ep_firebase_screen'                                                AS field,
    'credit limit increase'                                             AS logic_string,
    'Offer Page'                                                        AS dashboard_label,
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase' THEN 1 END)
                                                                        AS n_exact,
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END) AS n_clients_exact
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_firebase_screen',
    'credit limit increase completed - successful',
    'Success',
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase completed - successful' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase completed - successful'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_firebase_screen',
    'credit limit increase completed - failure',
    'Declined',
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase completed - failure' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase completed - failure'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_firebase_screen',
    'credit limit increase - failure - technical issues',
    'Mid-Tier Technical Error',
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase - failure - technical issues' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase - failure - technical issues'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_firebase_screen',
    'credit limit increase - technical error',
    'Technical Error (variant 1)',
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase - technical error' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase - technical error'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_firebase_screen',
    'credit limit increase - connection error',
    'Technical Error (variant 2)',
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase - connection error' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase - connection error'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_firebase_screen',
    'credit limit increase offer not available',
    'Offer Not Available (deeplink)',
    COUNT(CASE WHEN ep_firebase_screen = 'credit limit increase offer not available' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen = 'credit limit increase offer not available'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'tap - credit limit increase - full amount',
    'Full Amount',
    COUNT(CASE WHEN ep_details = 'tap - credit limit increase - full amount' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'tap - credit limit increase - full amount'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'tap - credit limit increase - other amount',
    'Other/Partial Amount',
    COUNT(CASE WHEN ep_details = 'tap - credit limit increase - other amount' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'tap - credit limit increase - other amount'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'tap - credit card account - notice - credit limit increase offer view details',
    'CRM Banner Positive Click (ep_details variant)',
    COUNT(CASE WHEN ep_details = 'tap - credit card account - notice - credit limit increase offer view details' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'tap - credit card account - notice - credit limit increase offer view details'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'tap - credit limit increase - decline cli offer',
    'Declined (ep_details variant)',
    COUNT(CASE WHEN ep_details = 'tap - credit limit increase - decline cli offer' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'tap - credit limit increase - decline cli offer'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'view - credit card account - notice - credit limit increase offer not available',
    'Offer Not Available (CRM notice)',
    COUNT(CASE WHEN ep_details = 'view - credit card account - notice - credit limit increase offer not available' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'view - credit card account - notice - credit limit increase offer not available'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'track - credit limit increase - deep link success - product page successful landing',
    'Deeplink Success',
    COUNT(CASE WHEN ep_details = 'track - credit limit increase - deep link success - product page successful landing' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'track - credit limit increase - deep link success - product page successful landing'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    'track - credit limit increase - deep link failure - capture dl not supported error',
    'Deeplink Failure',
    COUNT(CASE WHEN ep_details = 'track - credit limit increase - deep link failure - capture dl not supported error' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details = 'track - credit limit increase - deep link failure - capture dl not supported error'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

-- Drift-detection family totals (LIKE '%credit limit increase%')
-- Difference vs sum of exact rows above = volume with unexpected variants.
SELECT
    'ep_firebase_screen',
    '[FAMILY] %credit limit increase%',
    'drift check — firebase_screen family total',
    COUNT(CASE WHEN ep_firebase_screen LIKE '%credit limit increase%' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_firebase_screen LIKE '%credit limit increase%'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

UNION ALL

SELECT
    'ep_details',
    '[FAMILY] %credit limit increase%',
    'drift check — ep_details family total',
    COUNT(CASE WHEN ep_details LIKE '%credit limit increase%' THEN 1 END),
    COUNT(DISTINCT CASE WHEN ep_details LIKE '%credit limit increase%'
                        THEN TRY_CAST(up_srf_id2_value AS BIGINT) END)
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2026') AND month IN ('02','03','04')

ORDER BY field, logic_string
;
