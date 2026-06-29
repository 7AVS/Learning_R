-- PCL Sales Modal — P2: the "exposure universe" — count the SAME modal exposures
-- every way at once, so we can SEE if raw views balloon and pick the right unit.
-- Engine: Starburst/Trino (mixed EDW dw00_im + GA4 edl0_im).
-- Scope: BOTH arms of the PLI test (champion = NMS / no modal, challenger = WMS /
-- with modal), May deployments, PLI sales-modal events only.
--   * Champion is here for two reasons: (a) it is the no-modal CONVERSION baseline
--     carried into P3 (exposure count is post-treatment/self-selected, so the
--     randomized champion is the only clean causal anchor); (b) it validates that
--     NMS truly has ~0 modal views instead of us assuming it.
--   * Exposure curves themselves live in the challenger; champion should read ~0.

-- ============================================================
-- Q-A — OPTIONAL sanity peek only (NOT a deliverable). Top 50 heaviest viewers,
-- all counting units side by side, to confirm big counts are real users not a
-- join bug. Champion rows should all sit at ~0. For the answer run Q-B and Q-C.
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
    decile,
    responder_cli
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
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
  p.arm,
  p.strategy,
  p.decile,
  COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END)                                            AS raw_view_rows,
  COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess || '|' || CAST(m.event_sec AS VARCHAR) END) AS view_occasions,
  COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END)                              AS sessions,
  COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.event_date END)                        AS days,
  COUNT(CASE WHEN m.event_name = 'select_promotion' THEN 1 END)                                          AS click_rows
FROM pop p
LEFT JOIN modal m ON m.clnt_no = p.clnt_no
GROUP BY p.clnt_no, p.arm, p.strategy, p.decile
ORDER BY raw_view_rows DESC
LIMIT 50;


-- ============================================================
-- Q-B — distribution summary by ARM x UNIT (the one-look "do we balloon?" + the
-- champion validation). Champion rows should show ~0 served / ~0 exposures.
-- For challenger: compare median vs p90/p99/max per unit; the unit whose tail
-- lands near ~20 (not the hundreds) is the sane "exposure". raw/occasions ~= 2
-- confirms the double-fire.
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
         WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
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
    p.arm,
    COUNT(CASE WHEN m.event_name = 'view_promotion' THEN 1 END) AS raw_view_rows,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess || '|' || CAST(m.event_sec AS VARCHAR) END) AS view_occasions,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS sessions,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.event_date END) AS days
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.arm
)
SELECT arm, 'raw_view_rows' AS unit, COUNT(*) AS clients, COUNT(CASE WHEN raw_view_rows > 0 THEN 1 END) AS served,
       ROUND(AVG(raw_view_rows),1) AS mean, approx_percentile(raw_view_rows,0.5) AS p50,
       approx_percentile(raw_view_rows,0.9) AS p90, approx_percentile(raw_view_rows,0.99) AS p99, MAX(raw_view_rows) AS max
FROM per_client GROUP BY arm
UNION ALL
SELECT arm, 'view_occasions', COUNT(*), COUNT(CASE WHEN view_occasions > 0 THEN 1 END),
       ROUND(AVG(view_occasions),1), approx_percentile(view_occasions,0.5),
       approx_percentile(view_occasions,0.9), approx_percentile(view_occasions,0.99), MAX(view_occasions)
FROM per_client GROUP BY arm
UNION ALL
SELECT arm, 'sessions', COUNT(*), COUNT(CASE WHEN sessions > 0 THEN 1 END),
       ROUND(AVG(sessions),1), approx_percentile(sessions,0.5),
       approx_percentile(sessions,0.9), approx_percentile(sessions,0.99), MAX(sessions)
FROM per_client GROUP BY arm
UNION ALL
SELECT arm, 'days', COUNT(*), COUNT(CASE WHEN days > 0 THEN 1 END),
       ROUND(AVG(days),1), approx_percentile(days,0.5),
       approx_percentile(days,0.9), approx_percentile(days,0.99), MAX(days)
FROM per_client GROUP BY arm
ORDER BY unit, arm;


-- ============================================================
-- Q-C — challenger exposure distribution by the reporting dimensions:
-- STRATEGY (BAU/NTC) x DECILE, on sessions (swap to days if Q-B says raw balloons).
-- Champion excluded here: no exposures to distribute (its job is the P3 baseline).
-- ============================================================
WITH pop AS (
  SELECT
    CAST(clnt_no AS BIGINT) AS clnt_no,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
    decile
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R____WMS%'        -- challenger only
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-06-01'
),
modal AS (
  SELECT
    TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
    CAST(ep_ga_session_id AS VARCHAR) AS sess,
    event_name
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026'
    AND month IN ('05','06')
    AND it_location_id LIKE '%Sales_Modal%'
    AND (it_promotion_name LIKE '%PLI%' OR it_promotion_name LIKE '%PCL%')
),
per_client AS (
  SELECT
    p.clnt_no, p.strategy, p.decile,
    COUNT(DISTINCT CASE WHEN m.event_name = 'view_promotion' THEN m.sess END) AS sessions
  FROM pop p
  LEFT JOIN modal m ON m.clnt_no = p.clnt_no
  GROUP BY p.clnt_no, p.strategy, p.decile
)
SELECT
  strategy,
  decile,
  COUNT(*)                                       AS wms_clients,
  COUNT(CASE WHEN sessions > 0 THEN 1 END)       AS clients_served,
  approx_percentile(sessions, 0.5)               AS sess_p50,
  approx_percentile(sessions, 0.9)               AS sess_p90,
  approx_percentile(sessions, 0.99)              AS sess_p99,
  MAX(sessions)                                  AS sess_max
FROM per_client
GROUP BY 1, 2
ORDER BY strategy, decile;
