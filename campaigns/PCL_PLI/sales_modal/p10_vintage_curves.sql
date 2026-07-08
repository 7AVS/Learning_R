-- P10: VINTAGE curves for the PLI sales modal - CONVERSION and ENGAGEMENT, by days since deployment.
-- Long-format slicer: overall + strategy (BAU/NTC). Challenger arm only (the served arm).
-- Anchor = treatmt_strt_dt (both curves on one days-since-deployment clock, consistent with P3-P9).
--   engagement vintage_day = first modal view (GA4 event_date) - treatmt_strt_dt
--   conversion vintage_day = dt_cl_change (responder_cli=1) - treatmt_strt_dt
-- Modal id = i_308392 (+ tiny i_335273), the VCL-labeled real PLI modal (P7/P8 arm contrast).
-- Engine: Starburst/Trino (federates curated + GA4). Counts only; rate = cum_clients/base_clients client-side.
-- Champion (holdout) NOT included - these are challenger curves sliced by strategy. Conversion's
--   holdout counterfactual can be added as a baseline on request.
-- Pattern mirrors campaigns/PCD/pcd_2026111_vintage.sql. 9881-safe: GA4 side cast only, clnt_no raw.

WITH pop AS (                                          -- challenger (WMS) clients, first deployment
  SELECT
    clnt_no,
    CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
    responder_cli,
    dt_cl_change,
    treatmt_strt_dt,
    ROW_NUMBER() OVER (PARTITION BY clnt_no ORDER BY treatmt_strt_dt) AS rn
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE report_groups_period LIKE '%R____WMS%'
    AND treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
),
pop1 AS (
  SELECT clnt_no, strategy, responder_cli, dt_cl_change, treatmt_strt_dt
  FROM pop WHERE rn = 1
),
base AS (                                             -- fan out to slicer long-format
  SELECT clnt_no, strategy, responder_cli, dt_cl_change, treatmt_strt_dt,
         'overall' AS slicer_dim, 'ALL' AS slicer_value
  FROM pop1
  UNION ALL
  SELECT clnt_no, strategy, responder_cli, dt_cl_change, treatmt_strt_dt,
         'strategy' AS slicer_dim, strategy AS slicer_value
  FROM pop1
  WHERE strategy IN ('BAU','NTC')
),
eng AS (                                              -- first modal view per client (engagement date)
  SELECT TRY_CAST(up_srf_id2_value AS DECIMAL(14,0)) AS clnt_no, MIN(event_date) AS first_view_dt
  FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
  WHERE year = '2026' AND month IN ('05','06','07')
    AND it_item_id IN ('i_308392','i_335273')
    AND event_name = 'view_promotion'
  GROUP BY TRY_CAST(up_srf_id2_value AS DECIMAL(14,0))
),
events AS (                                            -- per-client first-event vintage_day, per metric
  SELECT b.slicer_dim, b.slicer_value, 'engagement' AS metric, b.clnt_no,
         date_diff('day', b.treatmt_strt_dt, e.first_view_dt) AS vintage_day
  FROM base b
  JOIN eng e ON e.clnt_no = b.clnt_no
  WHERE e.first_view_dt >= b.treatmt_strt_dt
  UNION ALL
  SELECT b.slicer_dim, b.slicer_value, 'conversion' AS metric, b.clnt_no,
         date_diff('day', b.treatmt_strt_dt, b.dt_cl_change) AS vintage_day
  FROM base b
  WHERE b.responder_cli = 1 AND b.dt_cl_change IS NOT NULL AND b.dt_cl_change >= b.treatmt_strt_dt
),
base_n AS (                                            -- denominator per slice (all challenger in slice)
  SELECT slicer_dim, slicer_value, COUNT(*) AS base_clients
  FROM base GROUP BY slicer_dim, slicer_value
),
daily AS (                                             -- daily first-event counts
  SELECT slicer_dim, slicer_value, metric, vintage_day, COUNT(DISTINCT clnt_no) AS n_events
  FROM events WHERE vintage_day >= 0
  GROUP BY slicer_dim, slicer_value, metric, vintage_day
),
spine AS (                                             -- day spine 0..max
  SELECT s AS vintage_day
  FROM UNNEST(sequence(0, (SELECT MAX(vintage_day) FROM daily))) AS t(s)
),
grid AS (                                              -- dense slice x metric x day
  SELECT d.slicer_dim, d.slicer_value, d.metric, sp.vintage_day
  FROM (SELECT DISTINCT slicer_dim, slicer_value, metric FROM daily) d
  CROSS JOIN spine sp
)
SELECT
  g.slicer_dim,
  g.slicer_value,
  g.metric,
  g.vintage_day,
  bn.base_clients,
  SUM(COALESCE(dl.n_events, 0)) OVER (
    PARTITION BY g.slicer_dim, g.slicer_value, g.metric
    ORDER BY g.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cum_clients
FROM grid g
LEFT JOIN daily dl
  ON dl.slicer_dim = g.slicer_dim AND dl.slicer_value = g.slicer_value
 AND dl.metric = g.metric AND dl.vintage_day = g.vintage_day
JOIN base_n bn ON bn.slicer_dim = g.slicer_dim AND bn.slicer_value = g.slicer_value
ORDER BY g.metric, g.slicer_dim, g.slicer_value, g.vintage_day;
