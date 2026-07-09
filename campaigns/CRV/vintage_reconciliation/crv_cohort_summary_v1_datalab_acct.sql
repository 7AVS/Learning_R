-- crv_cohort_summary_v1_datalab_acct.sql
-- Account-grain twin of crv_cohort_summary_v1_datalab.sql: same population, cohort
-- anchor, arm derivation and responder logic, but the D3CV12A.CR_CRD_RPTS_ACCT
-- acct->clnt bridge is removed entirely and counts are done at acct_no grain.
-- Built to isolate how much of the dashboard gap is caused by the bridge itself.
-- Engine: Teradata-direct. No volatile tables needed.
-- Output: raw whole-number counts (cohort_size, responders) plus thousands-
-- separated display twins (cohort_size_fmt, responders_fmt) and a 2-decimal
-- response_rate_pct (divide-by-zero guarded via NULLIF). No population/success
-- logic changed — formatting/rate are computed off the same raw counts.

WITH
acct_base AS (
    SELECT
        acct_no,
        (offer_start_date - (EXTRACT(DAY FROM offer_start_date) - 1)) AS cohort_month,
        CAST(TRIM(action_control) AS VARCHAR(10)) AS arm,
        responder
    FROM DL_MR_PROD.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND TRIM(action_control) IN ('Action', 'Control')
),
final_counts AS (
    SELECT
        CAST('CRV' AS VARCHAR(10))                                    AS campaign,
        cohort_month,
        arm,
        COUNT(DISTINCT acct_no)                                       AS cohort_size,
        COUNT(DISTINCT CASE WHEN responder = 1 THEN acct_no END)      AS responders
    FROM acct_base
    GROUP BY cohort_month, arm
)

SELECT
    campaign,
    cohort_month,
    arm,
    cohort_size,
    responders,
    TRIM(CAST(CAST(cohort_size AS FORMAT 'zzz,zzz,zz9') AS VARCHAR(15)))  AS cohort_size_fmt,
    TRIM(CAST(CAST(responders AS FORMAT 'zzz,zzz,zz9') AS VARCHAR(15)))   AS responders_fmt,
    CAST(100.0 * responders / NULLIF(cohort_size, 0) AS DECIMAL(6,2))    AS response_rate_pct
FROM final_counts
ORDER BY cohort_month, arm;
