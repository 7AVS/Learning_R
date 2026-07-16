-- pcq_ms_banner_engagement_discovery.sql
-- PCQ Modal Sales (MS) — find the GA4 banner for MS clients (approximation by name, anchored on converters).
-- Engine: STARBURST (Trino) — GA4 is EDL (edl0_im), so the Teradata tables are reached via federation.
--
-- We have no documented banner name for PCQ MS. Two angles that should converge on the SAME it_item_name:
--   QUERY 1  banner-side: which it_item_name values containing 'pcq'/'iav' exist in the window.
--   QUERY 2  client-side: take ACTUAL MS converters, join GA4 on the confirmed key, rank the banners THEY
--            touched. This proves join key + table + banner at once. The PCQ/IAV banner should top the list.
-- Confirmed facts (table_catalog_notes §2): banner name = it_item_name; join key = up_srf_id2_value = clnt_no
--   (TRY_CAST GA4 side only); view = view_promotion, click = select_promotion; events double-fire ->
--   count DISTINCT clients, never raw rows; prune on year/month (varchar).


-- ============================================================================
-- QUERY 0: confirm exact test_group_latest codes for the sales-model arms.
--   Andre: sales model = NG3_CHLG (challenger) + NG3_CHLN. Verify spelling before trusting the IN-list.
-- ============================================================================
SELECT
    test_group_latest,
    COUNT(*)                  AS rows_acct_grain,
    COUNT(DISTINCT clnt_no)   AS clients
FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
WHERE decsn_year       = 2026
  AND tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-06-01'
  AND test_group_latest LIKE 'NG3%'
GROUP BY test_group_latest
ORDER BY clients DESC;


-- ============================================================================
-- QUERY 1: banner discovery — banners with BOTH 'pcq' AND 'iav' in the name (token order-agnostic).
-- ============================================================================
SELECT
    it_item_name,
    it_item_id,
    lower(event_name)                       AS evt,
    COUNT(DISTINCT up_srf_id2_value)        AS clients,
    COUNT(*)                                AS events_double_fire
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND month IN ('06', '07')
  AND lower(event_name) IN ('view_promotion', 'select_promotion')
  AND lower(it_item_name) LIKE '%pcq%'
  AND lower(it_item_name) LIKE '%iav%'
GROUP BY it_item_name, it_item_id, lower(event_name)
ORDER BY clients DESC;


-- ============================================================================
-- QUERY 2: anchor on sales-model converters — which banners do they actually engage with?
--   Sales-model converters = approved PCQ clients (post-Jun) in test_group_latest NG3_CHLG/NG3_CHLN.
--   No tactic-event scan needed — the test group lives in curated. Confirm codes with QUERY 0 first.
--   Returns rows => join key works. Top it_item_name reveals the real MS banner name.
-- ============================================================================
WITH ms_conv AS (
    SELECT DISTINCT r.clnt_no
    FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp r
    WHERE r.decsn_year       = 2026
      AND r.tpa_ita          = 'TPA'
      AND r.treatmt_start_dt >= DATE '2026-06-01'
      AND r.app_approved     = 1
      AND r.test_group_latest IN ('NG3_CHLG', 'NG3_CHLN')
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        it_item_name,
        it_item_id,
        lower(event_name)                    AS evt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND month IN ('06', '07')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
)
SELECT
    g.it_item_name,
    g.it_item_id,
    g.evt,
    COUNT(DISTINCT g.clnt_no)                AS ms_converters_engaged,
    COUNT(*)                                 AS events_double_fire
FROM ga4 g
JOIN ms_conv c
  ON c.clnt_no = g.clnt_no
GROUP BY g.it_item_name, g.it_item_id, g.evt
ORDER BY ms_converters_engaged DESC;
