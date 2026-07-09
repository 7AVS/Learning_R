-- crv_cohort_summary_v1_datalab.sql
-- Data Lab per-cohort summary: counts straight off the self-contained curated
-- table DL_MR_PROD.cards_crv_install_decis_resp at its own native grain. No
-- acct->clnt bridge -- the curated table is the source of truth for population,
-- responder flag and denominator; client vs account identity is irrelevant here.
-- (Grain only matters on the v2_production side, which is assembled from tactic
-- history + raw events.) This is the correct comparator for the dashboard, which
-- is built off this same curated table.
-- Engine: Teradata-direct. No volatile tables needed.
-- Output: campaign, cohort_month, arm, cohort_size (formatted), responders
-- (formatted), response_rate (percentage with % sign). cohort_size and
-- responders are thousands-separated whole numbers; response_rate is a
-- 2-decimal percentage string (divide-by-zero guarded via NULLIF). No
-- population/success logic changed — formatting/rate are computed off the
-- same raw counts.

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
    TRIM(CAST(CAST(cohort_size AS FORMAT 'zzz,zzz,zz9') AS VARCHAR(15)))  AS cohort_size,
    TRIM(CAST(CAST(responders AS FORMAT 'zzz,zzz,zz9') AS VARCHAR(15)))   AS responders,
    TRIM(CAST(CAST(100.0 * responders / NULLIF(cohort_size,0) AS DECIMAL(6,2)) (FORMAT 'zz9.99') AS VARCHAR(10))) || '%'  AS response_rate
FROM final_counts
ORDER BY cohort_month, arm;
