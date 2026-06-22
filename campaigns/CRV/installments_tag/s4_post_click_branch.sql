-- s4_post_click_branch.sql
-- ENGINE: Starburst/Trino. Schema ref: schemas/ga4_tables_schema.md
--
-- PURPOSE: follow the branch BELOW the banner (visualized in installments_lineage.html).
--   s3 proved the installments offer IS the CRV M1 banner (id 87342, VSA_OFFER_SF) with
--   view_promotion + select_promotion. Open question: after a client CLICKS that banner,
--   where do they go? And do the other 3 CRV creatives carry different offers?
--   The actual installment-plan CONVERSION is NOT in GA4 — it lives in the CRV curated
--   install table (Teradata). This file closes only the GA4 half; STMT 3 below is a stub
--   for the EDW join once the curated table name/fields are confirmed.
--
-- Trino rules: no QUALIFY/NULLIFZERO; LOWER() match; filter year AND month.
-- Identity: filter the installments banner by it_item_name LIKE '%instalment%' (proven),
--   NOT by id-string (it_item_id prefix 'i_' vs plain is ambiguous in the OCR — avoid it).

-- ============================================================
-- STMT 1 — Post-click destination: where does an installments-banner click land?
-- ============================================================
-- For sessions where the installments banner was clicked (select_promotion), look at the
-- NEXT events in the SAME session within 10 minutes, in the narrow table. Profiles the
-- screen/event clients hit right after the click = the real branch destination.
WITH clicks AS (
    SELECT up_srf_id2_value AS clnt_no, user_pseudo_id, ep_ga_session_id,
           MIN(event_timestamp) AS click_ts
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month = '06'
      AND event_name = 'select_promotion'
      AND LOWER(it_item_name) LIKE '%instalment%'
    GROUP BY 1, 2, 3
)
SELECT
    n.ep_firebase_screen,
    n.event_name,
    n.ep_details,
    COUNT(*)                              AS n_events_after_click,
    COUNT(DISTINCT n.up_srf_id2_value)    AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow n
JOIN clicks c
  ON  n.user_pseudo_id   = c.user_pseudo_id
  AND n.ep_ga_session_id = c.ep_ga_session_id
WHERE n.year = '2026' AND n.month = '06'
  AND n.event_timestamp >  c.click_ts
  AND n.event_timestamp <= c.click_ts + 600000000   -- 600s in microseconds = same-session, 10-min window
GROUP BY 1, 2, 3
ORDER BY n_clients DESC
LIMIT 40
;

-- ============================================================
-- STMT 2 — Do the other 3 CRV creatives carry different offers? (Andre's question)
-- ============================================================
-- Profile all 4 CRV creatives by name/promotion. 87342 = installments; what are 87340/43/44?
-- Filter on it_promotion_id (plain numeric, confirmed in s3 STMT 2) to dodge the it_item_id prefix issue.
SELECT
    it_promotion_id,
    it_item_id,
    it_item_name,
    it_promotion_name,
    COUNT(*)                              AS n_events,
    COUNT(DISTINCT up_srf_id2_value)      AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('05','06')
  AND it_promotion_id IN ('87340','87342','87343','87344')
GROUP BY 1, 2, 3, 4
ORDER BY n_clients DESC
LIMIT 30
;

-- ============================================================
-- STMT 3 — EDW conversion join (STUB — needs CRV curated install table name/fields)
-- ============================================================
-- The actual installment-plan creation + economics is in the CRV curated install table
-- (Teradata; see reference: CRV install columns, install_details economics). Once the exact
-- table + plan-flag + date fields are confirmed, join GA4 banner clickers to plan creators
-- on clnt_no (= up_srf_id2_value) to complete the funnel: viewed -> clicked -> plan created.
-- Left as a stub on purpose — do NOT guess the table/columns.
--
-- SELECT <plan_count>, <plan_amount> ...
-- FROM <crv_curated_install_table>
-- WHERE <decision/plan date in window>
--   AND clnt_no IN (SELECT clnt_no FROM clicks);   -- clickers from STMT 1
