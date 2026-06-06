-- =============================================================================
-- CRV x PCL — Banner Identifier DISCOVERY (mobile)
-- =============================================================================
-- Purpose:
--   Before we can measure banner IMPRESSIONS on the CRV x PCL overlap cohort
--   (the next build step), we need the GA4 identifiers for each campaign's
--   mobile banner. We have two of the four already; this file discovers the
--   missing two (PLI/PCL and CRV) so the impression query can be parameterized.
--
-- Run in: Starburst / Trino  (GA4 catalog + Teradata catalog, federated)
--   Cross-catalog join pattern is proven in async_mb_tracker/async_combined_tracker.sql
--   (GA4 g INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t ON t.CLNT_NO = g.up_srf_id2_value)
--
-- GA4 table: edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
--   - view_promotion  = banner IMPRESSION
--   - select_promotion = banner CLICK
--   - it_item_name / it_item_id = the banner identifier
--   - ip_sf_campaign_mnemonic   = campaign-level filter (GOLD when populated)
--   - platform IN ('IOS','ANDROID','WEB')  -> mobile = IOS + ANDROID
--   - partitioned by year/month/day (varchar) — ALWAYS filter to avoid full scan
--
-- =============================================================================
-- KNOWN banner identifiers (reference — do NOT re-discover these)
-- =============================================================================
--   CTU  -> it_item_id = 'i_300102'                       (async_combined_tracker.sql)
--   O2P  -> it_item_id = 'i_298045'                       (async_combined_tracker.sql)
--   PCD  -> it_item_name LIKE 'PB_CC_ALL_26_02_RBC_PCD_%' (pcd_async_banner_explore.sql)
--   PCL  -> NO reliable GA4 tag. ip_sf_campaign_mnemonic = 'PCL' does NOT dependably
--           map to the PCL banner, so the mnemonic path is OUT.
--           DISCOVER BY ELIMINATION on a tightened PCL population (Q1-Q2).
--   CRV  -> NOT present in the ip_sf_campaign_mnemonic top list at all.
--           DISCOVER BY ELIMINATION on the CRV mobile population (Q3-Q6).
--
-- =============================================================================
-- OPEN ASSUMPTIONS TO VALIDATE WHILE RUNNING (flagged, not silently assumed)
-- =============================================================================
--  A1. Mobile cut. In GA4 we cut mobile via platform IN ('IOS','ANDROID').
--      The overlap analysis cut mobile via the TACTIC channel stamp
--      (PCL '%MB%' on curated; CRV '%IM%' backend quirk). These are different
--      lenses on "mobile" — confirm they agree before locking the impression query.
--  A2. Multi-banner contamination. A CRV/PCL-population client also sees other
--      campaigns' banners. Elimination must EXCLUDE all known item_ids and then
--      HOPE a single residual banner dominates the tightened cohort. If two-plus
--      banners are close, the population isn't tight enough / the banner isn't
--      isolable from GA4 alone — flag it, don't guess.
--  A5. Discovery filters are NOT measurement filters. The responder_cli = 1 cut
--      (Q1) deliberately biases toward people who engaged, purely to raise the
--      odds of catching the banner. The eventual IMPRESSION measurement must use
--      the FULL exposed population, NOT this responder-only subset.
--  A3. GA4 retention. ga4_ecommerce field-mapping EDA used 2026 data. The overlap
--      window starts 2024-10-01; GA4 may not reach that far back. Discovery uses
--      recent partitions (2026) — sufficient to NAME the banner. Widen later only
--      if the impression measurement needs the full historical window.
--  A4. Join key. up_srf_id2_value (GA4) = CLNT_NO (tactic hist), per the proven
--      async tracker. Confirm dtype alignment if a join returns zero rows.
-- =============================================================================


-- =============================================================================
-- PART 1 — PLI / PCL BANNER DISCOVERY (BY ELIMINATION)
-- =============================================================================
-- The mnemonic path is OUT. We cannot detect the PCL banner via
-- ip_sf_campaign_mnemonic = 'PCL'. Instead we tighten the PCL curated
-- population down to clients with the HIGHEST chance of having actually seen
-- the mobile banner, join them to GA4, strip the banners we already know, and
-- hope a single residual banner dominates — that one is PCL.
--
-- THE TIGHTENING FILTERS (all on dl_mr_prod.cards_pli_decision_resp):
--   1. response_channel = 'Mobile' -> the client RESPONDED on the mobile channel
--   2. responder_cli = 1           -> responded (best proxy that they saw it)
--   3. mobile_active_at_decision = 'Y' -> mobile-eligible at decision time (could
--      render). NOTE: 'Y'/NULL flag, NOT 1/0. = 'Y' excludes NULLs (NULL = not eligible).
-- PLUS a TEMPORAL window — the single biggest noise-killer:
--   the GA4 banner event (view_promotion / select_promotion) must fall WITHIN the
--   PCL treatment window [treatmt_strt_dt, treatmt_end_dt]. This ties the
--   impression/click to the actual deployment, dropping banners the client saw at
--   unrelated times. event_date is cast to DATE to compare against the Teradata dates.
-- Together = the cleanest "almost certainly saw the PCL mobile banner" subset we can
-- build. (See A5: this is a DISCOVERY cut only, not for measurement.)

-- ---------------------------------------------------------------------------
-- Q1) PCL banner by elimination on the triple-filtered PCL population.
--     Join the tightened PCL clients to GA4 banner events, exclude known
--     banners (CTU / O2P / PCD), and rank what's left. The dominant residual
--     it_item_id = the PCL mobile banner.
-- ---------------------------------------------------------------------------
SELECT
    g.it_item_id,
    g.it_item_name,
    g.it_creative_name,
    g.ip_sf_campaign_mnemonic,
    g.platform,
    COUNT(DISTINCT CASE WHEN lower(g.event_name) = 'view_promotion'
                        THEN g.up_srf_id2_value END) AS view_users,
    COUNT(DISTINCT CASE WHEN lower(g.event_name) = 'select_promotion'
                        THEN g.up_srf_id2_value END) AS click_users,
    COUNT(*)                                         AS events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
INNER JOIN dl_mr_prod.cards_pli_decision_resp p
    ON CAST(p.clnt_no AS BIGINT) = CAST(g.up_srf_id2_value AS BIGINT)
   AND p.response_channel          = 'Mobile'  -- (1) responded on mobile channel
   AND p.responder_cli             = 1         -- (2) responded
   AND p.mobile_active_at_decision = 'Y'       -- (3) mobile-eligible at decision
   AND p.treatmt_strt_dt >= DATE '2026-01-01'
   -- exposure must land INSIDE the PCL treatment window
   AND CAST(g.event_date AS DATE) BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
WHERE g.year = '2026'
  AND lower(g.event_name) IN ('view_promotion', 'select_promotion')
  -- strip already-attributed banners so the PCL residual stands out
  AND COALESCE(g.it_item_id, '') NOT IN ('i_300102', 'i_298045')
  AND lower(COALESCE(g.it_item_name, '')) NOT LIKE 'pb_cc_all_26_02_rbc_pcd%'
GROUP BY
    g.it_item_id, g.it_item_name, g.it_creative_name,
    g.ip_sf_campaign_mnemonic, g.platform
ORDER BY view_users DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Q2) SINGLE-BANNER CHECK — does one banner really dominate? (A2)
--     The whole method rests on "hope there's only one banner for these guys."
--     This counts how concentrated the tightened cohort's banner views are.
--     If the top it_item_id dwarfs the rest -> safe to name it PCL.
--     If two-plus are close -> population isn't isolating PCL; do NOT guess.
-- ---------------------------------------------------------------------------
SELECT
    g.it_item_id,
    g.it_item_name,
    COUNT(DISTINCT g.up_srf_id2_value)                                   AS view_users,
    CAST(COUNT(DISTINCT g.up_srf_id2_value) AS DOUBLE)
        / NULLIF(SUM(COUNT(DISTINCT g.up_srf_id2_value)) OVER (), 0)     AS share_of_cohort_views
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
INNER JOIN dl_mr_prod.cards_pli_decision_resp p
    ON CAST(p.clnt_no AS BIGINT) = CAST(g.up_srf_id2_value AS BIGINT)
   AND p.response_channel          = 'Mobile'
   AND p.responder_cli             = 1
   AND p.mobile_active_at_decision = 'Y'
   AND p.treatmt_strt_dt >= DATE '2026-01-01'
   AND CAST(g.event_date AS DATE) BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
WHERE g.year = '2026'
  AND lower(g.event_name) = 'view_promotion'
  AND COALESCE(g.it_item_id, '') NOT IN ('i_300102', 'i_298045')
  AND lower(COALESCE(g.it_item_name, '')) NOT LIKE 'pb_cc_all_26_02_rbc_pcd%'
GROUP BY g.it_item_id, g.it_item_name
ORDER BY view_users DESC
LIMIT 50;


-- =============================================================================
-- PART 2 — CRV BANNER DISCOVERY (BY ELIMINATION)
-- =============================================================================
-- CRV is NOT in the ip_sf_campaign_mnemonic top list, so the direct path used
-- for PCL won't work. Three angles, run in order; stop when one resolves it.

-- ---------------------------------------------------------------------------
-- Q3) Mnemonic census — is CRV hiding under a case/spelling variant?
--     The field mapping notes ip_sf_campaign_mnemonic is CASE-SENSITIVE
--     ('tao' vs 'TAO' coexist). CRV may appear as 'crv', 'CRV', 'CRVI', etc.
--     Lists every mnemonic that fires promo events so we can spot a CRV-like tag.
-- ---------------------------------------------------------------------------
SELECT
    ip_sf_campaign_mnemonic,
    COUNT(DISTINCT up_srf_id2_value) AS unique_users,
    COUNT(*)                         AS events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year = '2026'
  AND lower(event_name) IN ('view_promotion', 'select_promotion')
GROUP BY ip_sf_campaign_mnemonic
ORDER BY events DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Q4) Name-pattern scan — CRV = installment / "Create your own" plan offers.
--     CRV banners likely carry an installment-themed promo name. Cast a wide
--     net across it_item_name for CRV-ish tokens and surface the identifiers.
--     Tighten/loosen the LIKE list against what Q3 / Q5 reveal.
-- ---------------------------------------------------------------------------
SELECT
    it_item_id,
    it_item_name,
    ip_sf_campaign_mnemonic,
    event_name,
    platform,
    COUNT(DISTINCT up_srf_id2_value) AS unique_users,
    COUNT(*)                         AS events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
WHERE year = '2026'
  AND lower(event_name) IN ('view_promotion', 'select_promotion')
  AND (
        lower(it_item_name) LIKE '%crv%'
     OR lower(it_item_name) LIKE '%install%'
     OR lower(it_item_name) LIKE '%instal%'
     OR lower(it_item_name) LIKE '%payplan%'
     OR lower(it_item_name) LIKE '%pay_plan%'
     OR lower(it_item_name) LIKE '%pay plan%'
     OR lower(it_item_name) LIKE '%instalment%'
  )
GROUP BY it_item_id, it_item_name, ip_sf_campaign_mnemonic, event_name, platform
ORDER BY events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Q5) ELIMINATION on the CRV mobile population — the definitive path.
--     Take clients in the CRV tactic (mobile via the '%IM%' backend quirk,
--     CRV-Control TG8 excluded), pull every banner they viewed/clicked in GA4,
--     and EXCLUDE all banners we can already attribute (CTU, O2P, PCD, PCL).
--     The dominant residual it_item_id served to CRV clients = the CRV banner.
--
--     IMPORTANT (A2): the exclusion list MUST include the PCL it_item_id once
--     Q1 reveals it. Add it to the NOT IN list below before trusting the result.
-- ---------------------------------------------------------------------------
SELECT
    g.it_item_id,
    g.it_item_name,
    g.ip_sf_campaign_mnemonic,
    g.platform,
    COUNT(DISTINCT CASE WHEN lower(g.event_name) = 'view_promotion'
                        THEN g.up_srf_id2_value END) AS view_users,
    COUNT(DISTINCT CASE WHEN lower(g.event_name) = 'select_promotion'
                        THEN g.up_srf_id2_value END) AS click_users,
    COUNT(*)                                         AS events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
    ON t.CLNT_NO = g.up_srf_id2_value
   AND substr(t.TACTIC_ID, 8, 3) = 'CRV'
   AND substr(t.TACTIC_DECISN_VRB_INFO, 121, 8) LIKE '%IM%'  -- CRV mobile quirk
   AND t.TST_GRP_CD <> 'TG8'                                 -- exclude CRV Control
   AND t.TREATMT_STRT_DT >= DATE '2026-01-01'
WHERE g.year = '2026'
  AND lower(g.event_name) IN ('view_promotion', 'select_promotion')
  -- exclude already-attributed banners (PCD by name, others by id)
  AND COALESCE(g.it_item_id, '') NOT IN ('i_300102', 'i_298045' /* , '<PCL_id_from_Q1>' */ )
  AND lower(COALESCE(g.it_item_name, '')) NOT LIKE 'pb_cc_all_26_02_rbc_pcd%'
GROUP BY g.it_item_id, g.it_item_name, g.ip_sf_campaign_mnemonic, g.platform
ORDER BY events DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Q6) CRV banner CROSS-CHECK — concentration test.
--     Once Q5 names a candidate CRV it_item_id, confirm it concentrates in the
--     CRV population vs the general base. A real CRV banner should show a much
--     higher per-user incidence among CRV-tactic clients than among all users.
--     Replace '<CRV_id_candidate>' before running.
-- ---------------------------------------------------------------------------
-- SELECT
--     'crv_population' AS cohort,
--     COUNT(DISTINCT g.up_srf_id2_value) AS users_seeing_candidate
-- FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
-- INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
--     ON t.CLNT_NO = g.up_srf_id2_value
--    AND substr(t.TACTIC_ID, 8, 3) = 'CRV'
--    AND substr(t.TACTIC_DECISN_VRB_INFO, 121, 8) LIKE '%IM%'
--    AND t.TST_GRP_CD <> 'TG8'
-- WHERE g.year = '2026'
--   AND lower(g.event_name) = 'view_promotion'
--   AND g.it_item_id = '<CRV_id_candidate>'
-- UNION ALL
-- SELECT
--     'all_base' AS cohort,
--     COUNT(DISTINCT up_srf_id2_value) AS users_seeing_candidate
-- FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
-- WHERE year = '2026'
--   AND lower(event_name) = 'view_promotion'
--   AND it_item_id = '<CRV_id_candidate>';


-- =============================================================================
-- AFTER DISCOVERY — record findings here, then wire into the impression query
-- =============================================================================
--   PCL banner:  it_item_id = ____________  it_item_name = ____________________
--   CRV banner:  it_item_id = ____________  it_item_name = ____________________
--   Mobile cut confirmed consistent (A1)?  Y / N  ___________________________
--   Notes: _________________________________________________________________
-- =============================================================================
