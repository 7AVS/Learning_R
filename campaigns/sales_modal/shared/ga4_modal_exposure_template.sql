-- ga4_modal_exposure_template.sql
-- SHARED PARAMETERIZED TEMPLATE — GA4 Sales-Modal exposure/dismiss classification.
-- Engine: STARBURST/TRINO (federated: campaign curated table via dw00_im + GA4 via edl0_im).
--   No QUALIFY, no TOP, no NULLIFZERO. 9881-safe: GA4-side clnt_no cast only (TRY_CAST), never cast
--   or divide the Teradata-side column.
--
-- Generalized from: campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql (the `modal` and `per_client`
--   CTEs — GA4 exposure/dismiss classification only). The population CTE (`pop`/`pop1` in p9) is
--   CAMPAIGN-SPECIFIC and NOT generalized here — plug in each campaign's own cohort definition at
--   <POPULATION_CTE>.
--
-- ============================================================================
-- HEADER NOTES (read before using)
-- 1. EXPOSURE UNIT = distinct GA4 session (ep_ga_session_id), not raw view rows. Decided in
--    campaigns/sales_modal/pcl/p2_exposure_universe.sql (Q-B): raw view_rows balloon (double-fire,
--    session revisits); session count is the unit whose tail is sane. Do not swap to raw row counts
--    without re-running that diagnostic for the new campaign.
-- 2. This block ONLY applies to campaigns with a served/not-served (or challenger/champion) arm
--    contrast in the population CTE — the exposure/dismiss split is meaningless without a clean
--    no-modal baseline to compare against (see p9 header: champion validates ~0 modal views).
-- 3. it_item_id values MUST be confirmed by ARM CONTRAST (served vs not-served volumes), never by
--    it_item_name label — GA4 item ids for Sales_Modal are mislabeled/reused across campaigns (PCL's
--    modal is registered under a VCL-labeled id). See campaigns/sales_modal/pcl/modal_item_id_lookup.md
--    for the full id->campaign table and the P7/P8 arm-contrast method. Do not trust a name match alone.
-- ============================================================================
--
-- PARAMETERS (fill in before running — confirm every value against a fresh arm-contrast for the new
-- campaign; do not carry PCL's ids/patterns over by assumption):
--   <IT_ITEM_ID_LIST>            confirmed GA4 item ids for this campaign's modal, e.g.
--                                   PCL -> 'i_308392','i_335273'
--   <DISMISS_CREATIVE_PATTERNS>  LOWER(it_creative_name) LIKE patterns identifying a dismiss/close
--                                   click, e.g. PCL -> '%close%', '%not now%', '%dismiss%'
--                                   WATCH (per p9): creative_name may read '(not set)' on view rows —
--                                   verify the dismiss bucket actually populates before trusting it.
--   <GA4_YEAR_MONTH_FILTERS>     year = '<YYYY>' AND month IN ('<MM>', ...) — partition columns,
--                                   ALWAYS filter both (varchar partitions; event_date alone won't prune)
--   <POPULATION_CTE>             campaign-specific cohort definition (clnt_no, arm, cohort_month, plus
--                                   any segmentation columns) — NOT part of this generalization; build
--                                   per-campaign from that campaign's own curated-table query.
--
-- COLUMN NAMES NOT VERIFIED OUTSIDE PCL — <DISMISS_CREATIVE_PATTERNS> and <IT_ITEM_ID_LIST> are PCL
-- values pulled verbatim from p9; no other campaign's dismiss-creative text or item id has been
-- confirmed in this repo. Do not reuse PCL's literals for another campaign without re-running the
-- arm-contrast (P7/P8-style) discovery first.

WITH pop1 AS (
  <POPULATION_CTE>   -- clnt_no, arm, cohort_month (treatment-start-derived), plus any dims to carry
),
modal AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS DECIMAL(14,0)) AS clnt_no,   -- cast GA4 side only (9881-safe)
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name,
    it_creative_name
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE <GA4_YEAR_MONTH_FILTERS>
    AND it_item_id IN (<IT_ITEM_ID_LIST>)
),
per_client AS (
  SELECT
    p.clnt_no, p.arm, p.cohort_month,
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_views,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS exposures,
    MAX(CASE WHEN m.event_name = 'select_promotion'
              AND ( LOWER(m.it_creative_name) LIKE <DISMISS_CREATIVE_PATTERNS>
                 -- repeat "OR LOWER(m.it_creative_name) LIKE '<pattern>'" per additional pattern
                  )
             THEN 1 ELSE 0 END) AS dismissed
  FROM pop1 p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm, p.cohort_month
),
segmented AS (
  SELECT
    cohort_month, arm, raw_views,
    CASE WHEN dismissed = 1  THEN 'dismissed'
         WHEN raw_views > 0  THEN 'exposed_not_dismissed'
         ELSE 'not_exposed' END AS engagement,
    CASE WHEN exposures >= 5 THEN '5+' ELSE CAST(exposures AS VARCHAR) END AS exposure_bin
  FROM per_client
)
SELECT
  cohort_month,
  arm,
  engagement,
  exposure_bin,
  COUNT(*)             AS clients,      -- denominator per cell
  SUM(raw_views)       AS total_views   -- raw view fires, counts only
FROM segmented
GROUP BY cohort_month, arm, engagement, exposure_bin
ORDER BY cohort_month, arm, engagement, exposure_bin;
