-- Campaign : PCD (Product Card Upgrade)
-- Source   : DL_MR_PROD.cards_pcd_ongoing_decis_resp (curated)
-- Success  : Primary = responder_targetproduct = 1 (converted to the targeted product)
--            Earliest dt_prod_change per client.
-- Anchor   : response_start (wave date in curated table)
-- Arm      : test_control_flag derived from test_groups_period suffix (%T=TEST, %C=CONTROL)
-- Engine   : Starburst/Trino (federation: DL_MR_PROD via Starburst)
-- Range    : response_start >= 2026-01-01

-- NOTE: PCD curated table is campaign-dedicated; no mnemonic filter needed.
-- ASYNC cohort_arm (strategy_seg_cd allowlist) is deliberately excluded here —
-- arm is test/control only per the simplified monthly-cohort contract.

WITH cohort_raw AS (
    SELECT
        clnt_no,
        response_start                                                AS anchor_dt,
        date_trunc('month', response_start)                           AS cohort_month,
        dt_prod_change,
        responder_targetproduct,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
        END                                                           AS arm
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE response_start >= DATE '2026-01-01'
      AND (
            TRIM(test_groups_period) LIKE '%T'
         OR TRIM(test_groups_period) LIKE '%C'
          )
),

cohort AS (
    SELECT *
    FROM cohort_raw
    WHERE arm IS NOT NULL
),

cohort_size AS (
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size
    FROM cohort
    GROUP BY 1, 2
),

-- First target-product conversion per client
first_success AS (
    SELECT
        clnt_no,
        cohort_month,
        arm,
        anchor_dt,
        MIN(dt_prod_change)                                           AS first_success_dt
    FROM cohort
    WHERE responder_targetproduct = 1
      AND dt_prod_change IS NOT NULL
    GROUP BY clnt_no, cohort_month, arm, anchor_dt
),

vintage_days_raw AS (
    SELECT
        cohort_month,
        arm,
        date_diff('day', anchor_dt, first_success_dt)                AS vintage_day,
        COUNT(DISTINCT clnt_no)                                       AS new_events
    FROM first_success
    WHERE date_diff('day', anchor_dt, first_success_dt) BETWEEN 0 AND 180
    GROUP BY 1, 2, 3
),

spine AS (
    SELECT
        cs.cohort_month,
        cs.arm,
        cs.cohort_size,
        t.vintage_day
    FROM cohort_size cs
    CROSS JOIN UNNEST(sequence(0, 180)) AS t(vintage_day)
),

joined AS (
    SELECT
        s.cohort_month,
        s.arm,
        s.vintage_day,
        s.cohort_size,
        COALESCE(r.new_events, 0)                                     AS new_events
    FROM spine s
    LEFT JOIN vintage_days_raw r
        ON  r.cohort_month = s.cohort_month
        AND r.arm          = s.arm
        AND r.vintage_day  = s.vintage_day
)

SELECT
    CAST('PCD' AS VARCHAR(10))                                        AS campaign,
    cohort_month,
    arm,
    vintage_day,
    cohort_size,
    SUM(new_events) OVER (
        PARTITION BY cohort_month, arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                 AS cum_events
FROM joined
ORDER BY cohort_month, arm, vintage_day
;
