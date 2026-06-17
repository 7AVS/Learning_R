-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — cumulative converter curve, vintage_day = days since treatment start.
-- Engine: Teradata-direct (no EDL/GA4 source — no catalog prefix). Descriptive only, no control group.
--
-- REWRITTEN TO FIX CHANGING COHORT SIZES & TDWM PRODUCT JOIN BLOCKER:
-- 1. Uses Volatile Tables with COLLECT STATISTICS for the Day Spine and Cells to give the TDWM optimizer
--    accurate row estimates and allow the CROSS JOIN without throwing "F-uncnstrm PJ … rowest" errors.
-- 2. Uses a dense grid to ensure cohort_size denominators are stable across all vintage days when aggregating.
-- 3. tactic_id joined with TRIM to avoid byte-for-byte mismatch silently dropping MS targeted clients.

CREATE VOLATILE TABLE vt_pcq_ms_clients AS (
    SELECT DISTINCT CLNT_NO, TRIM(TACTIC_ID) AS TACTIC_ID
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
) WITH DATA PRIMARY INDEX (CLNT_NO, TACTIC_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_ms_clients COLUMN (CLNT_NO, TACTIC_ID);

CREATE VOLATILE TABLE vt_pcq_ms_base AS (
    SELECT
        r.clnt_no,
        TRIM(r.tactic_id) AS tactic_id,
        CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END             AS ms_targeted,
        COALESCE(r.model_score_decile, -1)                            AS model_score_decile,
        COALESCE(r.strtgy_seg_typ, '(null)')                          AS strtgy_seg_typ,
        COALESCE(r.test_group_latest, '(null)')                       AS test_group_latest,
        COALESCE(r.offer_prod_latest_name, '(null)')                  AS offer_prod_latest_name,
        MIN(CASE WHEN r.app_approved  = 1 THEN r.days_to_respond END) AS first_approved_day,
        MIN(CASE WHEN r.app_completed = 1 THEN r.days_to_respond END) AS first_completed_day
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    LEFT JOIN vt_pcq_ms_clients m
           ON m.CLNT_NO = r.clnt_no
          AND m.TACTIC_ID = TRIM(r.tactic_id)
    WHERE r.mnemonic         = 'PCQ'
      AND r.decsn_year       = 2026
      AND r.tpa_ita          = 'TPA'
      AND r.treatmt_start_dt >= DATE '2026-06-01'
    GROUP BY 
        r.clnt_no, TRIM(r.tactic_id),
        CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END,
        COALESCE(r.model_score_decile, -1),
        COALESCE(r.strtgy_seg_typ, '(null)'),
        COALESCE(r.test_group_latest, '(null)'),
        COALESCE(r.offer_prod_latest_name, '(null)')
) WITH DATA PRIMARY INDEX (clnt_no, tactic_id) ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE vt_pcq_ms_cells AS (
    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name,
        COUNT(*) AS cohort_size
    FROM vt_pcq_ms_base
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name
) WITH DATA PRIMARY INDEX (ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ, test_group_latest, offer_prod_latest_name) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_ms_cells COLUMN (ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ, test_group_latest, offer_prod_latest_name);

CREATE VOLATILE TABLE vt_pcq_days_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM sys_calendar.calendar
    WHERE vintage_day BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_days_spine COLUMN (vintage_day);

CREATE VOLATILE TABLE vt_pcq_ms_dense_grid AS (
    SELECT 
        c.ms_targeted, c.tactic_id, c.model_score_decile, c.strtgy_seg_typ,
        c.test_group_latest, c.offer_prod_latest_name, c.cohort_size,
        d.vintage_day
    FROM vt_pcq_ms_cells c
    CROSS JOIN vt_pcq_days_spine d
) WITH DATA PRIMARY INDEX (ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ, test_group_latest, offer_prod_latest_name, vintage_day) ON COMMIT PRESERVE ROWS;

WITH
daily_conversions AS (
    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name,
        first_approved_day AS vintage_day,
        CAST(COUNT(*) AS BIGINT) AS n_approved, CAST(0 AS BIGINT) AS n_completed
    FROM vt_pcq_ms_base
    WHERE first_approved_day BETWEEN 0 AND 90
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name, first_approved_day

    UNION ALL

    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name,
        first_completed_day AS vintage_day,
        CAST(0 AS BIGINT) AS n_approved, CAST(COUNT(*) AS BIGINT) AS n_completed
    FROM vt_pcq_ms_base
    WHERE first_completed_day BETWEEN 0 AND 90
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name, first_completed_day
),
daily_rollup AS (
    SELECT
        ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
        test_group_latest, offer_prod_latest_name, vintage_day,
        SUM(n_approved)  AS n_approved,
        SUM(n_completed) AS n_completed
    FROM daily_conversions
    GROUP BY ms_targeted, tactic_id, model_score_decile, strtgy_seg_typ,
             test_group_latest, offer_prod_latest_name, vintage_day
)
SELECT
    g.ms_targeted,
    g.tactic_id,
    g.model_score_decile,
    g.strtgy_seg_typ,
    g.test_group_latest,
    g.offer_prod_latest_name,
    g.vintage_day,
    COALESCE(r.n_approved, 0)  AS n_approved,
    COALESCE(r.n_completed, 0) AS n_completed,
    SUM(COALESCE(r.n_approved, 0)) OVER (
        PARTITION BY g.ms_targeted, g.tactic_id, g.model_score_decile,
                     g.strtgy_seg_typ, g.test_group_latest, g.offer_prod_latest_name
        ORDER BY g.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_approved,
    SUM(COALESCE(r.n_completed, 0)) OVER (
        PARTITION BY g.ms_targeted, g.tactic_id, g.model_score_decile,
                     g.strtgy_seg_typ, g.test_group_latest, g.offer_prod_latest_name
        ORDER BY g.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_completed,
    g.cohort_size
FROM vt_pcq_ms_dense_grid g
LEFT JOIN daily_rollup r
       ON g.ms_targeted            = r.ms_targeted
      AND g.tactic_id              = r.tactic_id
      AND g.model_score_decile     = r.model_score_decile
      AND g.strtgy_seg_typ         = r.strtgy_seg_typ
      AND g.test_group_latest      = r.test_group_latest
      AND g.offer_prod_latest_name = r.offer_prod_latest_name
      AND g.vintage_day            = r.vintage_day
ORDER BY 1, 2, 3, 4, 5, 6, 7;

DROP TABLE vt_pcq_ms_clients;
DROP TABLE vt_pcq_ms_base;
DROP TABLE vt_pcq_ms_cells;
DROP TABLE vt_pcq_days_spine;
DROP TABLE vt_pcq_ms_dense_grid;
