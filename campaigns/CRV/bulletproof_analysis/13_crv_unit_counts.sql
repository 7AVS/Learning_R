-- ===========================================================================
-- Q13 — CRV UNIT COUNTS (clean clients + clean installment plans)
-- ADDITIVE. Does not replace Q05 or Q06. Fixes the grain gap on the CRV-loss
-- side: Q06's crv_responders is a BINARY per-PCL-lead flag, not a count of
-- clients or plans. This query counts the real units so the net $ can be
-- valued at $36/installment (plans) or per converting client.
--
-- PCL-overlap cohort, split by CRV arm. COUNTS ONLY ($ math in Excel).
--
-- THREE DISTINCT UNITS (see METRICS_DICTIONARY.md):
--   n_exposed_clients    = distinct accts with ANY CRV wave overlapping PCL
--   n_converting_clients = distinct accts with >=1 CRV CONVERSION in overlap
--   n_install_plans      = installment plans held by those converters
--
-- TERADATA FIX (v2): removed scalar subqueries from the final SELECT.
--   Teradata rejects COUNT(DISTINCT ...) inside a correlated subquery
--   ("expecting one of ..."). Each metric is now a separate GROUP BY CTE,
--   joined on arm (overall) / arm+month (monthly).
--
-- FANOUT FIX vs Q05: plans are counted by joining install_details to a
--   DISTINCT (arm, acct_no, tactic_id) converter map — details are never
--   multiplied by duplicate decision rows.
-- ===========================================================================

WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),

-- All CRV waves in window, arm-labelled. Action = IM channel; Control = no filter.
crv_arm AS (
    SELECT
        acct_no, tactic_id, offer_start_date, offer_end_date, responder,
        CAST('Action' AS VARCHAR(10)) AS arm,
        CAST(offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS VARCHAR(20)) AS crv_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
    UNION ALL
    SELECT
        acct_no, tactic_id, offer_start_date, offer_end_date, responder,
        CAST('Control' AS VARCHAR(10)) AS arm,
        CAST(offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1) AS VARCHAR(20)) AS crv_month
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),

-- CRV waves that overlap a PCL-mobile window (EXISTS in WHERE = allowed, no fanout).
overlap_waves AS (
    SELECT c.acct_no, c.tactic_id, c.responder, c.arm, c.crv_month
    FROM crv_arm c
    WHERE EXISTS (
        SELECT 1 FROM pcl_universe p
        WHERE p.acct_no          = c.acct_no
          AND c.offer_start_date <= p.treatmt_end_dt
          AND c.offer_end_date   >= p.treatmt_strt_dt
    )
),

exposed    AS (SELECT DISTINCT arm, crv_month, acct_no FROM overlap_waves),
converters AS (SELECT DISTINCT arm, crv_month, acct_no, tactic_id
               FROM overlap_waves WHERE responder = 1),

-- Plan-count keys: DISTINCT so the join to install_details cannot fan out.
conv_keys_overall AS (SELECT DISTINCT arm, acct_no, tactic_id FROM converters),
conv_keys_monthly AS (SELECT DISTINCT arm, crv_month, acct_no, tactic_id FROM converters),
--   GUARD (validate once): a tactic wave should map to ONE crv_month. If any
--   (acct_no, tactic_id) spans 2+ months, monthly plan counts double-count.
--   Overall counts are unaffected.

-- ---- per-arm metric aggregates (OVERALL) ----
exp_overall  AS (SELECT arm, COUNT(DISTINCT acct_no) AS n_exposed_clients    FROM exposed    GROUP BY arm),
conv_overall AS (SELECT arm, COUNT(DISTINCT acct_no) AS n_converting_clients FROM converters GROUP BY arm),
plans_overall AS (
    SELECT k.arm, COUNT(*) AS n_install_plans
    FROM dl_mr_prod.cards_crv_install_details d
    INNER JOIN conv_keys_overall k
      ON k.acct_no = d.acct_no AND k.tactic_id = d.tactic_id
    GROUP BY k.arm
),

-- ---- per-arm-per-month metric aggregates (MONTHLY) ----
exp_monthly  AS (SELECT arm, crv_month, COUNT(DISTINCT acct_no) AS n_exposed_clients    FROM exposed    GROUP BY arm, crv_month),
conv_monthly AS (SELECT arm, crv_month, COUNT(DISTINCT acct_no) AS n_converting_clients FROM converters GROUP BY arm, crv_month),
plans_monthly AS (
    SELECT k.arm, k.crv_month, COUNT(*) AS n_install_plans
    FROM dl_mr_prod.cards_crv_install_details d
    INNER JOIN conv_keys_monthly k
      ON k.acct_no = d.acct_no AND k.tactic_id = d.tactic_id
    GROUP BY k.arm, k.crv_month
)

-- OVERALL rows (one per arm)
SELECT
    e.arm                                       AS arm,
    CAST('overall' AS VARCHAR(20))              AS slice,
    e.n_exposed_clients                         AS n_exposed_clients,
    COALESCE(c.n_converting_clients, 0)         AS n_converting_clients,
    COALESCE(p.n_install_plans, 0)              AS n_install_plans
FROM exp_overall e
LEFT JOIN conv_overall  c ON c.arm = e.arm
LEFT JOIN plans_overall p ON p.arm = e.arm

UNION ALL

-- MONTHLY rows (one per arm x crv_month)
SELECT
    e.arm,
    CAST(e.crv_month AS VARCHAR(20)),
    e.n_exposed_clients,
    COALESCE(c.n_converting_clients, 0),
    COALESCE(p.n_install_plans, 0)
FROM exp_monthly e
LEFT JOIN conv_monthly  c ON c.arm = e.arm AND c.crv_month = e.crv_month
LEFT JOIN plans_monthly p ON p.arm = e.arm AND p.crv_month = e.crv_month

ORDER BY 1, 2
;
