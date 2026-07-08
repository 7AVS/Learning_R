-- PCL Sales Modal — P5 DEDUP DIAGNOSTIC.
-- Question: within the May-June window, does any client appear under >1 strategy,
-- >1 arm (challenger/champion), or >1 parent_tactic_id? Confirms the assumed mechanic
-- "one client = one strategy/arm per window" that P5's dedup relies on.
-- Table: dw00_im.dl_mr_prod.cards_pli_decision_resp (PLI-dedicated; no mnemonic filter).
-- Engine: Starburst/Trino. Counts + sample rows. Run each block, screenshot.
-- arm: WMS = challenger (served), NMS = champion (withheld), else = other.

-- ============================================================================
-- BLOCK 1 - Headline. One row. How many clients breach each dimension?
-- EXPECT: clients_multi_strategy = 0 and clients_multi_arm = 0 if the mechanic holds.
--         clients_multi_row MAY be > 0 (same client, multiple waves, SAME strategy) - benign.
-- ============================================================================
SELECT
  COUNT(*)                                        AS clients_total,        -- distinct clients (this subquery is 1 row/client)
  SUM(n_rows)                                     AS rows_total,           -- total decision rows in window
  SUM(CASE WHEN n_rows   > 1 THEN 1 ELSE 0 END)   AS clients_multi_row,    -- >1 deployment (any kind) - benign if same strategy
  SUM(CASE WHEN n_strat  > 1 THEN 1 ELSE 0 END)   AS clients_multi_strategy, -- THE check: two strategies = mechanic broken
  SUM(CASE WHEN n_arm    > 1 THEN 1 ELSE 0 END)   AS clients_multi_arm,    -- challenger<->champion flip = experiment broken
  SUM(CASE WHEN n_tactic > 1 THEN 1 ELSE 0 END)   AS clients_multi_tactic  -- >1 parent_tactic_id (new tactic across window)
FROM (
  SELECT
    clnt_no,
    COUNT(*)                          AS n_rows,
    COUNT(DISTINCT strategy_id)       AS n_strat,
    COUNT(DISTINCT parent_tactic_id)  AS n_tactic,
    COUNT(DISTINCT CASE WHEN report_groups_period LIKE '%WMS%' THEN 'challenger'
                        WHEN report_groups_period LIKE '%NMS%' THEN 'champion'
                        ELSE 'other' END) AS n_arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
  GROUP BY clnt_no
) x;

-- ============================================================================
-- BLOCK 2 - Shape of the duplication. Which (n_rows, n_strat, n_arm, n_tactic)
-- combos exist and how many clients in each. Separates benign repeat-waves
-- (n_strat=1) from real cross-strategy contamination (n_strat>1).
-- ============================================================================
SELECT
  n_rows, n_strat, n_arm, n_tactic,
  COUNT(*) AS clients
FROM (
  SELECT
    clnt_no,
    COUNT(*)                          AS n_rows,
    COUNT(DISTINCT strategy_id)       AS n_strat,
    COUNT(DISTINCT parent_tactic_id)  AS n_tactic,
    COUNT(DISTINCT CASE WHEN report_groups_period LIKE '%WMS%' THEN 'challenger'
                        WHEN report_groups_period LIKE '%NMS%' THEN 'champion'
                        ELSE 'other' END) AS n_arm
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
  GROUP BY clnt_no
) x
GROUP BY n_rows, n_strat, n_arm, n_tactic
ORDER BY clients DESC;

-- ============================================================================
-- BLOCK 3 - Actual offender records (multi-strategy OR multi-arm only).
-- Shows the real rows so you can SEE what a contaminated client looks like:
-- which strategies, arms, tactics, dates. EMPTY result = mechanic confirmed.
-- LIMIT 200 to stay readable if there are many.
-- ============================================================================
SELECT
  d.clnt_no,
  d.strategy_id,
  d.report_groups_period,
  d.tst_grp_cd,
  d.parent_tactic_id,
  d.treatmt_strt_dt
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp d
JOIN (
  SELECT clnt_no
  FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
  WHERE treatmt_strt_dt >= DATE '2026-05-01' AND treatmt_strt_dt < DATE '2026-07-01'
  GROUP BY clnt_no
  HAVING COUNT(DISTINCT strategy_id) > 1
      OR COUNT(DISTINCT CASE WHEN report_groups_period LIKE '%WMS%' THEN 'challenger'
                             WHEN report_groups_period LIKE '%NMS%' THEN 'champion'
                             ELSE 'other' END) > 1
) o ON o.clnt_no = d.clnt_no
WHERE d.treatmt_strt_dt >= DATE '2026-05-01' AND d.treatmt_strt_dt < DATE '2026-07-01'
ORDER BY d.clnt_no, d.treatmt_strt_dt
LIMIT 200;
