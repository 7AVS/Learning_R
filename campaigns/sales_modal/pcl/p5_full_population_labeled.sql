-- PCL Sales Modal — P5: full PLI population (May-June), nothing filtered out, everything labeled.
-- Goal: locate where the modal's ~22K GA4 viewers actually sit. P3/P4 keep only the
-- 2-strategy (BAU/NTC) WMS/NMS DoE slice, so ~20K viewers fall outside and vanish.
-- P5 removes the strategy + report-group filters (keeps only the May-June date window),
-- LABELS strategy and arm instead of dropping them, and re-runs exposure + conversion.
-- Read it as: "inside our DoE slice = these rows; everywhere else = those rows."
-- PLI modal = it_item_id IN ('i_333067','i_333070'). Counts only; rates derived client-side.
-- Engine: Starburst/Trino. Window: May-June deployments, May-July exposure.
--
-- CAVEAT vs P4: dedup to first-deployment is now GLOBAL (over ALL strategies), so a
-- client whose first PLI deployment was in a non-BAU/NTC strategy is labeled by THAT
-- strategy here. The BAU/NTC/challenger cells will therefore be close to, but not
-- identical to, the P4 pivot (P4 dedups within the 2-strategy slice). This is the
-- honest one-row-per-client view for a full-population locator.
-- conversion = responder_cli = whole-campaign response (any channel), not modal-attributed.

WITH pop_raw AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'   -- served the modal
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion'     -- withheld (leakage arm)
         ELSE 'other' END AS arm,                                       -- everything outside the DoE arms
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU'
                     WHEN 'M8RHS9OI' THEN 'NTC'
                     ELSE strategy_id END AS strategy,                  -- named for our 2, raw code for the rest
    responder_cli,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn   -- first deployment per client (global)
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-07-01'
),
pop AS (
  SELECT clnt_no, arm, strategy, responder_cli,
         date_format(treatmt_strt_dt, '%Y-%m') AS cohort_month
  FROM pop_raw
  WHERE rn = 1
),
modal AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name,
    it_creative_name
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND it_item_id IN ('i_333067','i_333070')          -- PLI sales modal (confirmed correct in p4 diag)
),
per_client AS (
  SELECT
    p.clnt_no, p.arm, p.strategy, p.cohort_month, p.responder_cli,
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_views,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS exposures,   -- distinct sessions = times seen
    MAX(CASE WHEN m.event_name = 'select_promotion'
              AND ( LOWER(m.it_creative_name) LIKE '%close%'
                 OR LOWER(m.it_creative_name) LIKE '%not now%'
                 OR LOWER(m.it_creative_name) LIKE '%dismiss%' )
             THEN 1 ELSE 0 END) AS dismissed
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm, p.strategy, p.cohort_month, p.responder_cli
),
segmented AS (
  SELECT
    strategy, cohort_month, arm, responder_cli, raw_views,
    CASE WHEN dismissed = 1  THEN 'dismissed'
         WHEN raw_views > 0  THEN 'exposed_not_dismissed'
         ELSE 'not_exposed' END AS engagement,
    CASE WHEN exposures >= 5 THEN '5+' ELSE CAST(exposures AS VARCHAR) END AS exposure_bin  -- '0'..'4','5+' distinct sessions
  FROM per_client
)
SELECT
  strategy,
  cohort_month,
  arm,
  engagement,
  exposure_bin,
  COUNT(*)                                            AS clients,           -- population in cell
  SUM(raw_views)                                      AS total_views,       -- raw view fires
  SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)  AS converted_clients  -- rate = converted/clients (client-side)
FROM segmented
GROUP BY strategy, cohort_month, arm, engagement, exposure_bin
ORDER BY strategy, cohort_month, arm, engagement, exposure_bin;
