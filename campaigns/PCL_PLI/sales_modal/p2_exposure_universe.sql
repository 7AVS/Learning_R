-- PCL Sales Modal — P2: the "exposure universe" — count the SAME modal exposures
-- every way at once, so we can SEE if raw views balloon and pick the right unit.
-- Engine: Starburst/Trino (mixed EDW dw00_im + GA4 edl0_im).
-- Scope: WMS (with-modal) PLI population, May deployments, PLI sales-modal events only.

-- ============================================================
-- Q-A — per CLIENT, all counting units side by side.
-- LEFT JOIN keeps WMS clients with ZERO modal views (assigned but app-inactive).
-- Heaviest viewers float to the top.
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    strategy_id,
    responder_cli
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R____WMS%'
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
modal AS (                                  -- PLI sales-modal events only
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name,
    event_date,
    CAST(event_timestamp / 1000000 AS BIGINT) AS event_sec   -- floor to second (collapses ms-apart double-fire)
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026'
    AND month IN ('05','06')
    AND it_location_id LIKE '%Sales_Modal%'
    AND (it_promotion_name LIKE '%PLI%' OR it_promotion_name LIKE '%PCL%')
)
SELECT
  p.clnt_no,
  p.strategy_id,
  COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END)                                            AS raw_view_rows,        -- every fire (double-counted)
  COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess || '|' || CAST(m.event_sec AS VARCHAR) END) AS view_occasions,  -- deduped (session+second)
  COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END)                              AS sessions,             -- distinct sessions with a view
  COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.event_date END)                        AS days,                 -- distinct days with a view
  COUNT(CASE WHEN m.event_name = 'select_promotion' THEN 1 END)                                          AS click_rows            -- raw clicks (creative split comes later)
FROM pop p
LEFT JOIN modal m ON m.clnt_no = p.clnt_no
GROUP BY p.clnt_no, p.strategy_id
ORDER BY raw_view_rows DESC;


-- ============================================================
-- Q-B — distribution summary across ALL units (the one-look "do we balloon?").
-- Compare median vs p90/p99/max for each unit. If raw_view_rows max is huge but
-- sessions/days stay small -> raw balloons, session is the sane "exposure".
-- raw_view_rows / view_occasions ~= 2 confirms the double-fire.
-- ============================================================
WITH pop AS (
  SELECT CAST(clnt_no AS BIGINT) AS clnt_no
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R____WMS%'
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
modal AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name,
    event_date,
    CAST(event_timestamp / 1000000 AS BIGINT) AS event_sec
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026'
    AND month IN ('05','06')
    AND it_location_id LIKE '%Sales_Modal%'
    AND (it_promotion_name LIKE '%PLI%' OR it_promotion_name LIKE '%PCL%')
),
per_client AS (
  SELECT
    p.clnt_no,
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_view_rows,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess || '|' || CAST(m.event_sec AS VARCHAR) END) AS view_occasions,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS sessions,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.event_date END) AS days
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no
)
SELECT
  COUNT(*)                                                  AS wms_clients,
  COUNT(CASE WHEN raw_view_rows > 0 THEN 1 END)             AS clients_served,      -- had >=1 modal view in GA4
  'raw_view_rows' AS unit, ROUND(AVG(raw_view_rows),1) AS mean,
    approx_percentile(raw_view_rows,0.5) AS p50, approx_percentile(raw_view_rows,0.9) AS p90,
    approx_percentile(raw_view_rows,0.99) AS p99, MAX(raw_view_rows) AS max
FROM per_client
UNION ALL
SELECT NULL, NULL, 'view_occasions', ROUND(AVG(view_occasions),1),
    approx_percentile(view_occasions,0.5), approx_percentile(view_occasions,0.9),
    approx_percentile(view_occasions,0.99), MAX(view_occasions) FROM per_client
UNION ALL
SELECT NULL, NULL, 'sessions', ROUND(AVG(sessions),1),
    approx_percentile(sessions,0.5), approx_percentile(sessions,0.9),
    approx_percentile(sessions,0.99), MAX(sessions) FROM per_client
UNION ALL
SELECT NULL, NULL, 'days', ROUND(AVG(days),1),
    approx_percentile(days,0.5), approx_percentile(days,0.9),
    approx_percentile(days,0.99), MAX(days) FROM per_client;
