-- crv_cohort_summary_v1_datalab.sql
-- Per-cohort FULL-WINDOW summary version of crv_vintage_v1_datalab.sql.
-- Same population, cohort anchor, arm derivation and success (responder) logic as
-- the vintage file; the vintage_day spine and cumulative curve are dropped in favor
-- of one row per (cohort_month, arm) covering the entire deployment window. Built
-- for source reconciliation (dashboard vs Data Lab vs Production) with the time
-- axis removed so population/anchor/success differences are isolated.
-- Engine: Teradata-direct. No volatile tables needed here — the only reason the
-- vintage file used them was TDWM clearance for the SYS_CALENDAR cross join, and
-- that spine/cross join doesn't exist in this summary.
-- Output: raw whole-number counts (cohort_size, responders) plus thousands-
-- separated display twins (cohort_size_fmt, responders_fmt) and a 2-decimal
-- response_rate_pct (divide-by-zero guarded via NULLIF). No population/success
-- logic changed — formatting/rate are computed off the same raw counts.

WITH
bridge AS (
    SELECT
        c.acct_no,
        r.clnt_no
    FROM DL_MR_PROD.cards_crv_install_decis_resp c
    JOIN D3CV12A.CR_CRD_RPTS_ACCT r
        ON r.acct_no = CAST(c.acct_no AS DECIMAL(13,0))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.ME_DT DESC) = 1
),
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
client_base AS (
    SELECT
        b.clnt_no,
        a.cohort_month,
        a.arm,
        MAX(a.responder) AS is_responder
    FROM acct_base a
    JOIN bridge b ON b.acct_no = a.acct_no
    GROUP BY b.clnt_no, a.cohort_month, a.arm
),
final_counts AS (
    SELECT
        CAST('CRV' AS VARCHAR(10))                                    AS campaign,
        cohort_month,
        arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size,
        COUNT(DISTINCT CASE WHEN is_responder = 1 THEN clnt_no END)   AS responders
    FROM client_base
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
