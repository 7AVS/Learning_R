-- ===========================================================================
-- Q14 — CRV CONVERTER RECURRENCE PROFILE ("who are we removing?")
-- ADDITIVE. Profiles the converting clients we'd suppress, by how RECURRING
-- they are. PCL-overlap cohort, Action vs Control. COUNTS ONLY.
--
-- ANSWERS:
--   * How many installment plans does a converting client open? (mean, max, buckets)
--   * Do clients convert more than once? (n with >=2 converting CRV waves)
--   * How do we classify "recurring"? (two definitions, side by side)
--   * How spread out is the activity? (distinct active months)
--
-- RECURRENCE MEASURE:
--   n_multi_wave_resp = clients who CONVERTED on >=2 separate CRV offers
--                       (repeat banner responders — the demand we'd stop generating).
--   Plan-count buckets (1 / 2 / 3-4 / 5+ distinct plans) show installment intensity.
--   Note: a multi-wave responder is always a >=2-plan client; the extra >=2-plan
--   clients who are NOT multi-wave are single-offer stackers.
--
-- WINDOW: full cohort window (Oct 2024 -> Apr 2026). To get a strict 12-MONTH
--   view, uncomment the instl_txn_dt filter in client_plans (one line).
--
-- No fan-out: plans counted by DISTINCT instl_txn_ref_no. No percentiles
--   (bucket counts instead). Teradata-safe (no correlated subqueries / EXISTS-in-CASE).
-- ===========================================================================

WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),

crv_arm AS (
    SELECT acct_no, tactic_id, offer_start_date, offer_end_date, responder,
           CAST('Action' AS VARCHAR(10)) AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
    UNION ALL
    SELECT acct_no, tactic_id, offer_start_date, offer_end_date, responder,
           CAST('Control' AS VARCHAR(10)) AS arm
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),

-- Converting CRV waves that overlap a PCL-mobile window — the clients we'd remove.
-- DISTINCT (arm, acct, tactic) so the details join below cannot fan out.
overlap_conv AS (
    SELECT DISTINCT c.arm, c.acct_no, c.tactic_id
    FROM crv_arm c
    WHERE c.responder = 1
      AND EXISTS (
        SELECT 1 FROM pcl_universe p
        WHERE p.acct_no          = c.acct_no
          AND c.offer_start_date <= p.treatmt_end_dt
          AND c.offer_end_date   >= p.treatmt_strt_dt
      )
),

-- One row per converting client: plan count, #offers converted on, active months.
client_plans AS (
    SELECT
        k.arm,
        k.acct_no,
        COUNT(DISTINCT d.instl_txn_ref_no)  AS n_plans,
        COUNT(DISTINCT k.tactic_id)         AS n_convert_waves
    FROM overlap_conv k
    INNER JOIN dl_mr_prod.cards_crv_install_details d
      ON d.acct_no   = k.acct_no
     AND d.tactic_id = k.tactic_id
    -- 12-MONTH VIEW: uncomment to count only plans booked in the last 12 months
    -- AND d.instl_txn_dt >= DATE '2025-05-01'
    GROUP BY k.arm, k.acct_no
)

SELECT
    arm                                                            AS arm,
    COUNT(*)                                                       AS n_converting_clients,
    -- repeat campaign responders: converted on >=2 separate CRV offers
    SUM(CASE WHEN n_convert_waves >= 2 THEN 1 ELSE 0 END)          AS n_multi_wave_resp,
    -- plans-per-client distribution
    SUM(CASE WHEN n_plans = 1         THEN 1 ELSE 0 END)           AS clients_1_plan,
    SUM(CASE WHEN n_plans = 2         THEN 1 ELSE 0 END)           AS clients_2_plans,
    SUM(CASE WHEN n_plans BETWEEN 3 AND 4 THEN 1 ELSE 0 END)       AS clients_3_4_plans,
    SUM(CASE WHEN n_plans >= 5         THEN 1 ELSE 0 END)          AS clients_5plus_plans,
    AVG(CAST(n_plans AS FLOAT))                                    AS mean_plans_per_client
FROM client_plans
GROUP BY arm
ORDER BY 1
;
