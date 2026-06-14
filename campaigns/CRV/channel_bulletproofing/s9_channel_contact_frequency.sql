-- ============================================================================
-- ENGINE: Starburst/Trino — Trino syntax. GA4 table on EDL; Statement 3 also
--   federates dl_mr_prod (Teradata EDW, no catalog prefix) via Starburst.
-- Descriptive channel contact-frequency for CRV and PCL mobile banners.
-- GA4 coverage: Feb 2025 – May 2026 (June 2026 partial — EXCLUDED from all statements).
-- Metric: distinct view-days (COUNT DISTINCT event_date per client). Raw view-event
--   counts are ~2× inflated due to view_item/view_promotion twin pairs (per s2) — never use.
-- Contract: event_name = 'view_promotion' (view_item discarded — s2 FINAL).
--   Identity key: it_item_id (format-stable per s7); see allowlists in each statement.
-- ============================================================================


-- ============================================================================
-- STATEMENT 1 — Monthly reach trend
--   Monthly unique clients reached and client-view-day totals per banner family
--   (CRV + PCL), Feb 2025 – May 2026. Two curves × 16 months.
--   raw_view_events included for QA only — not a frequency metric.
-- ============================================================================

SELECT
    year,
    month,
    CASE
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                            'i_167717','i_289661','i_289662','i_289664','i_289665',
                            'i_289666','i_289698')                   THEN 'PCL'
    END                                              AS banner_family,
    COUNT(DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT))          AS clients_reached,
    COUNT(DISTINCT (TRY_CAST(up_srf_id2_value AS BIGINT), event_date)) AS client_view_days,
    COUNT(*)                                         AS raw_view_events
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE event_name = 'view_promotion'
  AND it_item_id IN (
      'i_87340','i_87342','i_87343','i_87344',
      'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
      'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
  )
  AND (
      (year = '2025' AND month IN ('02','03','04','05','06','07','08','09','10','11','12'))
   OR (year = '2026' AND month IN ('01','02','03','04','05'))
  )
GROUP BY
    year,
    month,
    CASE
        WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
        WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                            'i_167717','i_289661','i_289662','i_289664','i_289665',
                            'i_289666','i_289698')                   THEN 'PCL'
    END
ORDER BY banner_family, year, month;


-- ============================================================================
-- STATEMENT 2 — Per-client view-day distribution / saturation histogram
--   How many distinct calendar days did each client see each banner family?
--   Output: histogram (view_days × n_clients) per banner family.
--   No deployment join, no overlap filter — pure channel-side exposure frequency.
--   Uncapped tail preserves outliers.
-- ============================================================================

WITH client_viewdays AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS client,
        CASE
            WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
            WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                                'i_167717','i_289661','i_289662','i_289664','i_289665',
                                'i_289666','i_289698')                   THEN 'PCL'
        END                                  AS banner_family,
        COUNT(DISTINCT event_date)           AS view_days
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_name = 'view_promotion'
      AND it_item_id IN (
          'i_87340','i_87342','i_87343','i_87344',
          'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
          'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
      )
      AND (
          (year = '2025' AND month IN ('02','03','04','05','06','07','08','09','10','11','12'))
       OR (year = '2026' AND month IN ('01','02','03','04','05'))
      )
    GROUP BY
        TRY_CAST(up_srf_id2_value AS BIGINT),
        CASE
            WHEN it_item_id IN ('i_87340','i_87342','i_87343','i_87344') THEN 'CRV'
            WHEN it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716',
                                'i_167717','i_289661','i_289662','i_289664','i_289665',
                                'i_289666','i_289698')                   THEN 'PCL'
        END
)
SELECT
    banner_family,
    view_days,
    COUNT(*) AS n_clients
FROM client_viewdays
GROUP BY banner_family, view_days
ORDER BY banner_family, view_days;


-- ============================================================================
-- STATEMENT 3 — Deployment-anchored saturation, PCL only
--   Within each client's actual deployment window, how many days had at least
--   one banner view? Framing: "30 of 90 days seen." One row per days_seen bucket.
--   Fully-observed windows only: treatmt_strt_dt >= 2025-02-01 AND
--   treatmt_end_dt <= 2026-05-31 (both endpoints within GA4 coverage window).
--
--   NOTE — CRV deployment-anchored DEFERRED:
--     dl_mr_prod.cards_crv_install_decis_resp has NO clnt_no column (schema confirmed
--     2026-05-14 in crv_pcl_curated_schemas.md; the table grain is acct_no, and clnt_no
--     was not visible in any captured screen). GA4's client identity key is
--     up_srf_id2_value = clnt_no (client-level), not acct_no. A CRV deployment-anchored
--     query would require resolving acct_no → clnt_no via CIDM or a client-level bridge
--     table. This is not guessed here. Add CRV block once the client-key bridge is
--     confirmed and tested. PCL-only is built below.
-- ============================================================================

-- PCL deployment-anchored saturation
WITH pcl_deployments AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        date_diff('day', treatmt_strt_dt, treatmt_end_dt) AS window_days
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE channel LIKE '%MB%'
      AND treatmt_strt_dt >= DATE '2025-02-01'
      AND treatmt_end_dt   <= DATE '2026-05-31'
),
ga4_pcl AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_name = 'view_promotion'
      AND it_item_id IN (
          'i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
          'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698'
      )
      AND (
          (year = '2025' AND month IN ('02','03','04','05','06','07','08','09','10','11','12'))
       OR (year = '2026' AND month IN ('01','02','03','04','05'))
      )
),
dep_saturation AS (
    SELECT
        d.clnt_no,
        d.treatmt_strt_dt,
        d.window_days,
        COUNT(DISTINCT g.event_date) AS days_seen
    FROM pcl_deployments d
    LEFT JOIN ga4_pcl g
      ON  g.clnt_no    = d.clnt_no
      AND g.event_date BETWEEN d.treatmt_strt_dt AND d.treatmt_end_dt
    GROUP BY d.clnt_no, d.treatmt_strt_dt, d.window_days
)
SELECT
    'PCL'                     AS banner_family,
    days_seen,
    COUNT(*)                  AS n_deployments,
    CAST(AVG(window_days) AS DECIMAL(6,1)) AS avg_window_days
FROM dep_saturation
GROUP BY days_seen
ORDER BY days_seen;
