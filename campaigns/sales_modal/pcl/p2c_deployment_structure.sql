-- PCL Sales Modal — P2c: deployment STRUCTURE diagnostic (attribution risk).
-- PCL runs multiple deployments/month. A client can be champion in one deployment
-- and challenger in another, over OVERLAPPING windows -> conversion/exposure can't
-- be cleanly assigned to an arm. Before any attribution we must size this.
-- Pure curated table (no GA4) — independent of the modal-string fix.
-- Scope: May + June deployments, both arms, both strategies.
-- Counts only.

-- ============================================================
-- D1 — deployment calendar: each deployment's window, DURATION, and volume per arm.
-- Confirms whether windows are a fixed length (~60d) -> guaranteed measurement period.
-- ============================================================
SELECT
  parent_tactic_id,
  treatmt_strt_dt,
  treatmt_end_dt,
  date_diff('day', treatmt_strt_dt, treatmt_end_dt)        AS window_days,
  CASE strategy_id WHEN 'LZJ4PENS' THEN 'BAU' WHEN 'M8RHS9OI' THEN 'NTC' ELSE strategy_id END AS strategy,
  CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
       WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END AS arm,
  COUNT(DISTINCT clnt_no)                                  AS clients
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
  AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
  AND treatmt_strt_dt >= DATE '2026-05-01'
  AND treatmt_strt_dt <  DATE '2026-07-01'
GROUP BY parent_tactic_id, treatmt_strt_dt, treatmt_end_dt, 4, 5, 6
ORDER BY treatmt_strt_dt, strategy, arm;


-- ============================================================
-- D2 — client multiplicity & CONFLICT: how many deployments per client, and how
-- many clients land in BOTH arms across deployments (the un-attributable set).
-- clients_in_both_arms is THE number that decides clean-cohort vs date-attribution.
-- ============================================================
WITH per_client AS (
  SELECT
    clnt_no,
    COUNT(DISTINCT parent_tactic_id) AS n_deploys,
    COUNT(DISTINCT CASE WHEN report_groups_period LIKE '%R____WMS%' THEN 'challenger'
                        WHEN report_groups_period LIKE '%R____NMS%' THEN 'champion' END) AS n_arms
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
    AND strategy_id IN ('LZJ4PENS','M8RHS9OI')
    AND treatmt_strt_dt >= DATE '2026-05-01'
    AND treatmt_strt_dt <  DATE '2026-07-01'
  GROUP BY clnt_no
)
SELECT
  n_deploys,
  COUNT(*)                                      AS clients,
  COUNT(CASE WHEN n_arms > 1 THEN 1 END)        AS clients_in_both_arms
FROM per_client
GROUP BY n_deploys
ORDER BY n_deploys;
