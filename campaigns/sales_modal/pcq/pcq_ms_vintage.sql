-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — cumulative converter curve, long-format output.
-- Engine: Teradata-direct (DL_MR_PROD.*, no catalog prefix, Teradata SQL syntax).
-- Descriptive only. Counts only — no rate columns.
--
-- OUTPUT GRAIN: one row per (wave_dt, arm, metric, decile_scope, vintage_day)
-- with cohort_size (fixed for that cell) and cum_events (cumulative first-event count).
--
-- NOTE: If a previous failed run left volatile tables in session, drop them first:
--   DROP TABLE vt_pcq_ms_cells;
--   DROP TABLE vt_pcq_days_spine;

/* ===== TEST GROUPS (confirmed from tactic table DG6V01.TACTIC_EVNT_IP_AR_HIST, 2026-06-19) =====
   champion   : NG3_CHMP            (is_MS = 0 — no Modal Sales)
   challenger : NG3_CHLN, NG3_CHLG  (is_MS = 1 — Modal Sales; NG3_CHLG is small, ~2-4k/wave)
   Only these 3 exact codes pass the population filter below.
   ============================================================================================= */

/* ===== CONFIRM PRODUCT FILTER (Andre to lock the target product before running) =====
   The curated table covers all PCQ products (see offer_prod_latest_name in pcq_ms_vs_benchmark.sql).
   If this vintage should be scoped to a single product, uncomment and set the value below in Step 1
   and in the client_base CTE in Step 3:
       AND r.offer_prod_latest_name = '<PRODUCT NAME HERE>'
   If multi-product is intentional, leave the filter commented out.
   ===================================================================================== */

/* ===== DECILE ORIENTATION =====
   Decile 1 is assumed = TOP of model (highest propensity).
   Confirm this against the model documentation before acting on top5 slice results.
   ================================ */

-- ============================================================================
-- STEP 1: cells — cohort_size per (wave_dt, arm, decile_scope)
-- Materialized as volatile table because it cross-joins with days_spine below.
-- wave_dt = treatmt_start_dt (the deployment wave). Each distinct value is one deployment.
-- tactic_id holds multiple deployments if a finer split is later needed — not used here.
-- A 'pooled' wave_dt text label is added per cell for the all-waves-combined row
-- (a literal label, so it cannot collide with the real 2026-06-01 wave date).
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_ms_cells AS (
    WITH client_base AS (
        SELECT
            clnt_no,
            treatmt_start_dt   AS wave_dt,
            CASE
                WHEN TRIM(test_group_latest) = 'NG3_CHMP'
                    THEN 'champion'
                WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG')
                    THEN 'challenger'
            END                AS arm,
            CAST(model_score_decile AS VARCHAR(10)) AS model_score_decile
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE decsn_year        = 2026
          AND tpa_ita           = 'TPA'
          AND treatmt_start_dt  >= DATE '2026-06-01'
          AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
          -- PRODUCT FILTER (uncomment if scoping to one product — see header block above):
          -- AND offer_prod_latest_name = '<PRODUCT NAME HERE>'
        GROUP BY
            clnt_no, treatmt_start_dt,
            CASE
                WHEN TRIM(test_group_latest) = 'NG3_CHMP'  THEN 'champion'
                WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN 'challenger'
            END,
            CAST(model_score_decile AS VARCHAR(10))
    ),
    -- Long-format by decile_scope: overall (all deciles) and top5
    scoped AS (
        -- overall: all deciles
        SELECT clnt_no, wave_dt, arm, CAST('overall' AS VARCHAR(10)) AS decile_scope
        FROM client_base
        UNION ALL
        -- top5: deciles 1-5 only (decile 1 assumed = top of model)
        SELECT clnt_no, wave_dt, arm, 'top5'
        FROM client_base
        WHERE model_score_decile IN ('1','2','3','4','5')
    ),
    -- Add a pooled-wave row (sentinel wave_dt) alongside per-wave rows
    with_pooled AS (
        -- Per-wave rows (actual wave date as text 'YYYY-MM-DD')
        SELECT CAST(CAST(wave_dt AS DATE FORMAT 'YYYY-MM-DD') AS VARCHAR(20)) AS wave_dt,
               CAST(arm AS VARCHAR(20))                  AS arm,
               decile_scope,
               clnt_no
        FROM scoped
        UNION ALL
        -- Pooled row: literal 'pooled' label collapses all waves (no date collision)
        SELECT CAST('pooled' AS VARCHAR(20)),
               CAST(arm AS VARCHAR(20)),
               decile_scope,
               clnt_no
        FROM scoped
    )
    SELECT
        wave_dt,
        arm,
        decile_scope,
        COUNT(DISTINCT clnt_no) AS cohort_size
    FROM with_pooled
    GROUP BY wave_dt, arm, decile_scope
) WITH DATA PRIMARY INDEX (wave_dt, arm, decile_scope) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_ms_cells COLUMN (wave_dt, arm, decile_scope);

-- ============================================================================
-- STEP 2: days_spine — 0..max_vintage_day
-- Materialized as volatile table for TDWM cross-join clearance.
-- ============================================================================
CREATE VOLATILE TABLE vt_pcq_days_spine AS (
    WITH max_day AS (
        SELECT COALESCE(MAX(vd), 14) AS max_vintage_day
        FROM (
            SELECT MAX(CASE WHEN app_approved  = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN days_to_respond END) AS vd
            FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
            WHERE decsn_year = 2026
              AND tpa_ita    = 'TPA'
              AND treatmt_start_dt >= DATE '2026-06-01'
              AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
            UNION ALL
            SELECT MAX(CASE WHEN app_completed = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN days_to_respond END)
            FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
            WHERE decsn_year = 2026
              AND tpa_ita    = 'TPA'
              AND treatmt_start_dt >= DATE '2026-06-01'
              AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
        ) t
    )
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM sys_calendar.calendar
    CROSS JOIN max_day m
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND m.max_vintage_day
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_days_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final long-format curve
-- metric = 'approved' | 'completed' — each anchored on the client's FIRST such event.
-- ============================================================================
WITH
client_base AS (
    SELECT
        clnt_no,
        treatmt_start_dt   AS wave_dt,
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'                     THEN 'champion'
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG')      THEN 'challenger'
        END                                                                AS arm,
        CAST(model_score_decile AS VARCHAR(10))                            AS model_score_decile,
        -- first-event per metric uses its OWN first-event date (per vintage convention)
        -- SUCCESS = Period-ASC attributed only (campaign-window applications). This gates the
        -- NUMERATOR only; cohort_size (denominator) stays all targeted clients in the arm.
        MIN(CASE WHEN app_approved  = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN days_to_respond END) AS first_approved_day,
        MIN(CASE WHEN app_completed = 1 AND TRIM(asc_on_app_source) = 'Period-ASC' THEN days_to_respond END) AS first_completed_day
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE decsn_year        = 2026
      AND tpa_ita           = 'TPA'
      AND treatmt_start_dt  >= DATE '2026-06-01'
      AND TRIM(test_group_latest) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLG')
      -- PRODUCT FILTER (uncomment if scoping to one product — see header block above):
      -- AND offer_prod_latest_name = '<PRODUCT NAME HERE>'
    GROUP BY
        clnt_no, treatmt_start_dt,
        CASE
            WHEN TRIM(test_group_latest) = 'NG3_CHMP'                THEN 'champion'
            WHEN TRIM(test_group_latest) IN ('NG3_CHLN', 'NG3_CHLG') THEN 'challenger'
        END,
        CAST(model_score_decile AS VARCHAR(10))
),
-- Expand by decile_scope: overall and top5
scoped AS (
    SELECT clnt_no, wave_dt, arm, CAST('overall' AS VARCHAR(10)) AS decile_scope,
           first_approved_day, first_completed_day
    FROM client_base
    UNION ALL
    SELECT clnt_no, wave_dt, arm, 'top5',
           first_approved_day, first_completed_day
    FROM client_base
    WHERE model_score_decile IN ('1','2','3','4','5')
),
-- Add pooled-wave sentinel rows alongside per-wave rows
with_pooled AS (
    SELECT CAST(CAST(wave_dt AS DATE FORMAT 'YYYY-MM-DD') AS VARCHAR(20)) AS wave_dt,
           CAST(arm AS VARCHAR(20))                         AS arm,
           decile_scope, first_approved_day, first_completed_day
    FROM scoped
    UNION ALL
    -- Pooled row: literal 'pooled' label, all waves combined (no date collision)
    SELECT CAST('pooled' AS VARCHAR(20)),
           CAST(arm AS VARCHAR(20)),
           decile_scope, first_approved_day, first_completed_day
    FROM scoped
),
-- Pivot to long format: one row per metric per client per cell
metric_long AS (
    SELECT wave_dt, arm, decile_scope,
           CAST('approved'  AS VARCHAR(20)) AS metric,
           first_approved_day               AS event_day
    FROM with_pooled
    WHERE first_approved_day IS NOT NULL
    UNION ALL
    SELECT wave_dt, arm, decile_scope,
           CAST('completed' AS VARCHAR(20)) AS metric,
           first_completed_day              AS event_day
    FROM with_pooled
    WHERE first_completed_day IS NOT NULL
),
-- Daily event counts per cell per metric
daily_counts AS (
    SELECT wave_dt, arm, decile_scope, metric, event_day AS vintage_day,
           CAST(COUNT(*) AS BIGINT) AS n_events
    FROM metric_long
    GROUP BY wave_dt, arm, decile_scope, metric, event_day
),
-- Dense grid: cells × spine × metrics
dense_grid AS (
    SELECT c.wave_dt, c.arm, c.decile_scope, c.cohort_size,
           m.metric, d.vintage_day
    FROM vt_pcq_ms_cells c
    CROSS JOIN vt_pcq_days_spine d
    CROSS JOIN (
        -- Teradata: each SELECT in a UNION must reference a table (error 3888).
        -- Anchor to the spine at vintage_day = 0 (exactly one row) to emit one row per metric.
        SELECT CAST('approved'  AS VARCHAR(20)) AS metric FROM vt_pcq_days_spine WHERE vintage_day = 0
        UNION ALL
        SELECT CAST('completed' AS VARCHAR(20))           FROM vt_pcq_days_spine WHERE vintage_day = 0
    ) m
)
SELECT
    g.wave_dt,
    g.arm,
    g.metric,
    g.decile_scope,
    g.vintage_day,
    g.cohort_size,
    SUM(COALESCE(dc.n_events, 0)) OVER (
        PARTITION BY g.wave_dt, g.arm, g.metric, g.decile_scope
        ORDER BY g.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_events
FROM dense_grid g
LEFT JOIN daily_counts dc
       ON g.wave_dt      = dc.wave_dt
      AND g.arm          = dc.arm
      AND g.metric       = dc.metric
      AND g.decile_scope = dc.decile_scope
      AND g.vintage_day  = dc.vintage_day
ORDER BY 1, 2, 3, 4, 5;

DROP TABLE vt_pcq_ms_cells;
DROP TABLE vt_pcq_days_spine;
