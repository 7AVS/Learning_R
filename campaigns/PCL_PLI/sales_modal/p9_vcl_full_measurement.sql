-- P9: complete PLI sales-modal measurement on the CONFIRMED id.
-- P7/P8 arm contrast: i_308392 (VCL-labeled) = 176,440 challenger vs 8 champion - clean,
-- challenger-only, ~76% reach of the ~232K targeted. The PLI-NAMED i_333067/i_333070 are
-- contaminated (836 champion vs 1,564 challenger) and low-volume -> NOT the modal. So the real
-- PLI modal is registered under the VCL-labeled id; we track by BEHAVIOR, not label.
-- Population: challenger (WMS) + champion (NMS) ONLY. No strategy filter (drops 'Other' and the
--   BAU/NTC/udobank report groups). Full dims: engagement x exposure_bin. Conversion from curated.
-- Engine: Starburst/Trino (GA4 + curated). Counts only. 9881-safe: GA4 side cast only, clnt_no raw.
-- WATCH: creative_name was '(not set)' on the view rows, so the 'dismissed' bucket may be empty;
--   dismiss is read off select_promotion rows here - verify it populates before trusting it.

WITH pop AS (
  SELECT
    clnt_no,                                    -- raw, uncast (no Teradata ROUND pushdown)
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    responder_cli,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn   -- first deployment
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
),
pop1 AS (
  SELECT clnt_no, arm, responder_cli,
         date_format(treatmt_strt_dt, '%Y-%m') AS cohort_month
  FROM pop WHERE rn = 1
),
modal AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS DECIMAL(14,0)) AS clnt_no,   -- cast GA4 side (runs in Trino)
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name,
    it_creative_name
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND it_item_id IN ('i_308392','i_335273')                -- VCL-labeled = the real PLI modal
),
per_client AS (
  SELECT
    p.clnt_no, p.arm, p.cohort_month, p.responder_cli,
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_views,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS exposures,
    MAX(CASE WHEN m.event_name = 'select_promotion'
              AND ( LOWER(m.it_creative_name) LIKE '%close%'
                 OR LOWER(m.it_creative_name) LIKE '%not now%'
                 OR LOWER(m.it_creative_name) LIKE '%dismiss%' )
             THEN 1 ELSE 0 END) AS dismissed
  FROM pop1 p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm, p.cohort_month, p.responder_cli
),
segmented AS (
  SELECT
    arm, cohort_month, responder_cli, raw_views,
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
  COUNT(*)                                            AS clients,           -- denominator per cell
  SUM(raw_views)                                      AS total_views,       -- raw view fires
  SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)  AS converted_clients  -- rate = conv/clients (client-side)
FROM segmented
GROUP BY cohort_month, arm, engagement, exposure_bin
ORDER BY cohort_month, arm, engagement, exposure_bin;
