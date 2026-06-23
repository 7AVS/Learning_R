-- Campaign : PCQ (Credit Card Acquisition / Modal Sales)
-- Source   : DL_MR_PROD.cards_tpa_pcq_decision_resp (curated)
-- Success  : Primary = app_approved = 1 AND asc_on_app_source = 'Period-ASC'
--            (Period-ASC gates the NUMERATOR only; cohort_size denominator = all targeted TPA clients)
--            Earliest qualifying days_to_respond per client.
-- Anchor   : treatmt_start_dt
-- Arm      : champion (NG3_CHMP) vs challenger (NG3_CHLN or NG3_CHLG) — per confirmed test group codes
-- Engine   : Starburst/Trino (federation: DL_MR_PROD via Starburst)
-- Range    : treatmt_start_dt >= 2026-01-01
--
-- RULES APPLIED:
--   tpa_ita = 'TPA'                       (PCQ standard — no ITA arm)
--   decsn_year = 2026                     (partition pruning)
--   Period-ASC gates numerator only       (asc_on_app_source = 'Period-ASC')
--   denominator = all targeted clients in arm (no Period-ASC filter on denominator)
--   arm codes: NG3_CHMP=champion, NG3_CHLN/NG3_CHLG=challenger
--
-- NOTE: days_to_respond is a pre-computed field in the curated table (vintage day
-- relative to treatmt_start_dt). Used directly — no date arithmetic needed.

WITH cohort AS (
    SELECT
        clnt_no,
        treatmt_start_dt,
        date_trunc('month', treatmt_start_dt)                         AS cohort_month,
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN 'champion'
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN 'challenger'
        END                                                           AS arm,
        -- Numerator: first approved Period-ASC application day
        MIN(CASE
            WHEN app_approved = 1
             AND TRIM(asc_on_app_source) = 'Period-ASC'
            THEN days_to_respond
        END)                                                          AS first_approved_day
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE decsn_year       = 2026
      AND tpa_ita          = 'TPA'
      AND treatmt_start_dt >= DATE '2026-01-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
    GROUP BY
        clnt_no,
        treatmt_start_dt,
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN 'champion'
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN 'challenger'
        END
),

cohort_size AS (
    -- Denominator: all targeted clients per cohort_month x arm (no Period-ASC filter)
    SELECT
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size
    FROM cohort
    WHERE arm IS NOT NULL
    GROUP BY 1, 2
),

vintage_days_raw AS (
    -- Numerator: clients with a Period-ASC approved application
    SELECT
        cohort_month,
        arm,
        first_approved_day                                            AS vintage_day,
        COUNT(DISTINCT clnt_no)                                       AS new_events
    FROM cohort
    WHERE arm IS NOT NULL
      AND first_approved_day IS NOT NULL
      AND first_approved_day BETWEEN 0 AND 180
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
    CAST('PCQ' AS VARCHAR(10))                                        AS campaign,
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
