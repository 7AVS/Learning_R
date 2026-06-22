-- s5_creative_profile_fix.sql
-- FIXES s4 STMT 2 (which was WRONG). s4 filtered `it_promotion_id IN ('87340'..'87344')`,
-- but those numbers are CRV `it_item_id` values (canon: 'i_87340','i_87342','i_87343','i_87344') —
-- a DIFFERENT field from it_promotion_id. 87342 matched only because its it_promotion_id also = 87342.
-- So s4's "only 87342 runs" was a wrong-field artifact, NOT proof one creative runs. This re-checks.
-- ENGINE: Starburst/Trino. Schema ref: schemas/ga4_tables_schema.md

-- ============================================================
-- STMT 1 — Profile the 4 CRV creatives by it_item_id (both 'i_' and plain formats)
-- ============================================================
SELECT
    it_item_id,
    it_item_name,
    it_promotion_id,
    it_promotion_name,
    COUNT(*)                          AS n_events,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month IN ('05','06')
  AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                     '87340','87342','87343','87344')
GROUP BY 1, 2, 3, 4
ORDER BY n_clients DESC
LIMIT 40
;

-- ============================================================
-- STMT 2 — Enumerate ALL promotions on the M1 slot (no id pre-filter — let the data show reality)
-- ============================================================
-- Shows every creative actually served in the credit-card-details M1 slot + the real
-- it_promotion_id <-> it_promotion_name <-> it_item_id mapping. Tells us whether CRV has more
-- creatives than the 4 known ids, and which promotion_id the installments offer truly uses.
SELECT
    it_promotion_id,
    it_promotion_name,
    it_item_id,
    it_item_name,
    COUNT(DISTINCT up_srf_id2_value)  AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month = '06'
  AND event_name = 'view_promotion'
  AND ( LOWER(it_location_id) LIKE '%credit%card%details%'
     OR LOWER(it_location_id) LIKE '%android%credit%card%detail%' )
GROUP BY 1, 2, 3, 4
ORDER BY n_clients DESC
LIMIT 60
;
