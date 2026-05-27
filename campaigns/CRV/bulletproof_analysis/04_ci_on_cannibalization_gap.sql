-- Statistical significance test on the CRV-Action vs CRV-Control cannibalization gap in PCL response.
-- PCL-LEAD CENTRIC (matching original Section E framing):
--   Unit of observation = one PCL-mobile lead (one row per PCL deployment per account).
--   Overlap flag        = does this PCL lead overlap with a CRV-Action / CRV-Control wave?
--   Outcome             = PCL responder_cli on this lead.
-- Output: 'overall' row + one row per PCL deployment month.
-- Spool-optimized: EXISTS semi-joins (no fan-out, no DISTINCT). Single pcl_universe scan.
-- All statistical math forced to FLOAT to avoid Teradata DECIMAL precision overflow.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- Single pass over pcl_universe; EXISTS semi-joins flag each PCL lead.
-- No fan-out, no DISTINCT — minimal spool footprint.
pcl_flagged AS (
    SELECT
        p.pcl_month,
        p.responder_cli,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM crv_action ca
                WHERE ca.acct_no            = p.acct_no
                  AND ca.offer_start_date  <= p.treatmt_end_dt
                  AND ca.offer_end_date    >= p.treatmt_strt_dt
            ) THEN 1 ELSE 0
        END AS overlap_action_flag,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM crv_control cc
                WHERE cc.acct_no            = p.acct_no
                  AND cc.offer_start_date  <= p.treatmt_end_dt
                  AND cc.offer_end_date    >= p.treatmt_strt_dt
            ) THEN 1 ELSE 0
        END AS overlap_control_flag
    FROM pcl_universe p
),
-- Aggregate to overall + per-month with one pass over the flagged set.
agg_overall AS (
    SELECT
        CAST('overall' AS VARCHAR(20))                                                                AS slice,
        CAST(SUM(overlap_action_flag)                                                       AS FLOAT) AS n_action,
        CAST(SUM(CASE WHEN overlap_action_flag  = 1 THEN responder_cli ELSE 0 END)          AS FLOAT) AS resp_action,
        CAST(SUM(overlap_control_flag)                                                      AS FLOAT) AS n_control,
        CAST(SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)          AS FLOAT) AS resp_control
    FROM pcl_flagged
),
agg_monthly AS (
    SELECT
        CAST(pcl_month AS VARCHAR(20))                                                                AS slice,
        CAST(SUM(overlap_action_flag)                                                       AS FLOAT) AS n_action,
        CAST(SUM(CASE WHEN overlap_action_flag  = 1 THEN responder_cli ELSE 0 END)          AS FLOAT) AS resp_action,
        CAST(SUM(overlap_control_flag)                                                      AS FLOAT) AS n_control,
        CAST(SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)          AS FLOAT) AS resp_control
    FROM pcl_flagged
    GROUP BY pcl_month
),
agg AS (
    SELECT * FROM agg_overall
    UNION ALL
    SELECT * FROM agg_monthly
),
stats AS (
    SELECT
        slice,
        n_action,
        resp_action,
        n_control,
        resp_control,
        CASE WHEN n_action  > CAST(0 AS FLOAT) THEN resp_action  / n_action  ELSE NULL END AS p_action,
        CASE WHEN n_control > CAST(0 AS FLOAT) THEN resp_control / n_control ELSE NULL END AS p_control
    FROM agg
),
se_calc AS (
    SELECT
        slice,
        n_action, resp_action, n_control, resp_control,
        p_action, p_control,
        p_control - p_action AS gap,
        SQRT(
              p_action  * (CAST(1 AS FLOAT) - p_action)  / n_action
            + p_control * (CAST(1 AS FLOAT) - p_control) / n_control
        ) AS se
    FROM stats
)
SELECT
    slice,
    n_action,
    resp_action,
    n_control,
    resp_control,
    p_action,
    p_control,
    -- gap: positive = control higher than action = cannibalization signal
    gap,
    se,
    gap - CAST(1.96 AS FLOAT) * se AS ci_lower,
    gap + CAST(1.96 AS FLOAT) * se AS ci_upper,
    CASE WHEN se > CAST(0 AS FLOAT) THEN gap / se ELSE NULL END AS z_stat,
    CASE
        WHEN (gap - CAST(1.96 AS FLOAT) * se) > CAST(0 AS FLOAT)
          OR (gap + CAST(1.96 AS FLOAT) * se) < CAST(0 AS FLOAT) THEN 1
        ELSE 0
    END AS significant_at_95
FROM se_calc
ORDER BY 1
;
