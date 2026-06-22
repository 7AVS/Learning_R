-- s3_installments_behavior_discovery.sql
-- ENGINE: Starburst/Trino. Schema ref: schemas/ga4_tables_schema.md
--
-- PURPOSE: understand how the inline "pay in installments" CTA behaves as a channel,
--   so we can design the next experiment. NOT a competitor for the M1 banner slot
--   (different screen) — this is a behavioral lens. Builds on s1 (ecommerce screen census)
--   and the 2026-06-21 finding: the installments item shows in ecommerce as
--   it_item_name = 'PB_CC_ALL_21_06_RBC_CC-INSTALMENTS-INT_ONLY', and a custom
--   event_name='tap' with ep_details='tap - credit card transaction details - posted transaction'
--   shows in the NARROW table. We do not yet know if that tap IS the installments CTA or just
--   navigation, nor whether an impression (view_promotion) fires. These queries find out.
--
-- KNOWN IDENTIFIERS (use, don't guess):
--   CRV banner (M1 slot):       it_item_id IN ('i_87340','i_87342','i_87343','i_87344')
--   Installments item (ecom):   it_item_name LIKE '%instalment%'  (British spelling, one L)
--   Client key (both tables):   up_srf_id2_value (integer; = CLNT_NO)
--   Session key (both tables):  user_pseudo_id + ep_ga_session_id ; order by event_timestamp
--
-- WINDOW: default recent window below. Edit the year/month IN-lists per query.
--   ECOM table: use the _reduced table (Feb-2025+ history). If the installments item does NOT
--   appear in _reduced, switch to the full ...ga4_ecommerce table (holds only ~2 recent weeks).
--   NARROW table: using full ...ga4_narrow for richest ep_* fields; _narrow_reduced has more history.
--
-- Trino rules: no QUALIFY, no NULLIFZERO; LOWER() for matching; always filter year AND month.

-- ============================================================
-- STMT 1 — Does the installments tag have an IMPRESSION? (denominator question)
-- ============================================================
-- Which event_name(s) carry the installments item in ecommerce? If view_promotion appears,
-- an exposure denominator exists (measure like the banner). If only select_promotion / nothing,
-- the impression lives elsewhere (the narrow tap) and the denominator must be defined differently.
SELECT
    event_name,
    it_location_id,
    platform,
    COUNT(*)                            AS n_events,
    COUNT(DISTINCT up_srf_id2_value)    AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('05','06','07')
  AND LOWER(it_item_name) LIKE '%instalment%'
GROUP BY 1, 2, 3
ORDER BY n_events DESC
LIMIT 30
;

-- ============================================================
-- STMT 2 — Installments variants + where they live (open mind)
-- ============================================================
-- All distinct installments item identifiers + their screen/slot. Catches variants beyond
-- INT_ONLY and gives us the exact location_id / firebase_screen for later filtering.
SELECT DISTINCT
    it_item_name,
    it_item_id,
    it_location_id,
    it_promotion_id,
    it_promotion_name,
    ep_firebase_screen,
    platform
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('05','06','07')
  AND LOWER(it_item_name) LIKE '%instalment%'
LIMIT 50
;

-- ============================================================
-- STMT 3 — What fires on the transaction-details screen? (narrow)
-- ============================================================
-- Profile event_name + ep_details on the transaction-details screen. KEY AMBIGUITY:
-- the tap you saw may be "open a posted transaction" (navigation), NOT the installments CTA.
-- This lists every event/label there so we can tell navigation from the actual CTA.
SELECT
    event_name,
    ep_details,
    ep_firebase_screen,
    ep_location_id,
    platform,
    COUNT(*)                            AS n_events,
    COUNT(DISTINCT up_srf_id2_value)    AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026'
  AND month IN ('05','06','07')
  AND (
        LOWER(ep_details)         LIKE '%transaction details%'
     OR LOWER(ep_firebase_screen) LIKE '%transaction%'
  )
GROUP BY 1, 2, 3, 4, 5
ORDER BY n_events DESC
LIMIT 40
;

-- ============================================================
-- STMT 4 — Where does "installments" appear in narrow AT ALL? (find the real CTA)
-- ============================================================
-- Keyword sweep, not limited to the transaction screen. Finds the event/label that is
-- specifically the installments CTA (vs generic navigation taps) and what signal it carries.
SELECT
    event_name,
    ep_details,
    ep_event_label_details,
    ep_location_id,
    COUNT(*)                            AS n_events,
    COUNT(DISTINCT up_srf_id2_value)    AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026'
  AND month IN ('05','06','07')
  AND (
        LOWER(ep_details)            LIKE '%instalment%'
     OR LOWER(ep_details)            LIKE '%bnpl%'
     OR LOWER(ep_details)            LIKE '%pay%plan%'
     OR LOWER(ep_event_label_details) LIKE '%instalment%'
     OR LOWER(event_name)            LIKE '%instalment%'
  )
GROUP BY 1, 2, 3, 4
ORDER BY n_events DESC
LIMIT 40
;

-- ============================================================
-- STMT 5 — Do the tables join, and does the path connect in one visit?
-- ============================================================
-- For a few installments clients, interleave ecommerce + narrow events by session and time.
-- Confirms (a) the session key (ep_ga_session_id) lines up across tables, (b) whether a
-- view (ecommerce) and the tap (narrow) happen in the SAME session = a real within-visit path,
-- vs asynchronous "ever/ever". Scroll the output; it's a small client sample.
WITH inst_clients AS (
    SELECT DISTINCT up_srf_id2_value AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('06','07')
      AND LOWER(it_item_name) LIKE '%instalment%'
    LIMIT 3
),
ecom AS (
    SELECT up_srf_id2_value AS clnt_no, user_pseudo_id, ep_ga_session_id,
           event_timestamp, event_name,
           it_item_name AS label, CAST('ecommerce' AS VARCHAR(20)) AS src
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('06','07')
      AND up_srf_id2_value IN (SELECT clnt_no FROM inst_clients)
),
narr AS (
    SELECT up_srf_id2_value AS clnt_no, user_pseudo_id, ep_ga_session_id,
           event_timestamp, event_name,
           ep_details AS label, CAST('narrow' AS VARCHAR(20)) AS src
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE year = '2026' AND month IN ('06','07')
      AND up_srf_id2_value IN (SELECT clnt_no FROM inst_clients)
)
SELECT clnt_no, ep_ga_session_id, src, event_name, label, event_timestamp
FROM (SELECT * FROM ecom UNION ALL SELECT * FROM narr)
ORDER BY clnt_no, ep_ga_session_id, event_timestamp
LIMIT 80
;

-- ============================================================
-- STMT 6 — How new / how big is this channel?
-- ============================================================
-- Monthly distinct clients touching the installments item, by platform. Finds the launch
-- window and sizes the channel. Scans 2025-2026 to catch the start.
SELECT
    year,
    month,
    platform,
    COUNT(*)                            AS n_events,
    COUNT(DISTINCT up_srf_id2_value)    AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year IN ('2025','2026')
  AND LOWER(it_item_name) LIKE '%instalment%'
GROUP BY 1, 2, 3
ORDER BY year, month, platform
LIMIT 60
;

-- ============================================================
-- STMT 7 — Independence: CRV banner population vs installments population
-- ============================================================
-- Cross-tab of distinct clients: saw the CRV banner, engaged installments, and both.
-- Confirms the surfaces are separable and shows how the populations relate.
-- NOTE: align the window to where BOTH are active — CRV banner and the installments tag
-- may not share the same months. Widen month IN-lists if one side is ~0.
WITH crv AS (
    SELECT DISTINCT up_srf_id2_value AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('05','06','07')
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344')
),
inst AS (
    SELECT DISTINCT up_srf_id2_value AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('05','06','07')
      AND LOWER(it_item_name) LIKE '%instalment%'
)
SELECT
    (SELECT COUNT(*) FROM crv)                              AS crv_banner_clients,
    (SELECT COUNT(*) FROM inst)                             AS instalment_clients,
    (SELECT COUNT(*) FROM crv JOIN inst USING (clnt))       AS both_clients
;
