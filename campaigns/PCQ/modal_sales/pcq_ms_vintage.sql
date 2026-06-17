-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — cumulative converter curve, vintage_day = days since treatment start.
-- Engine: Teradata-direct (no EDL/GA4 source — no catalog prefix). Descriptive only, no control group.
--
-- REWRITTEN: 
-- 1. Uses the Long-Format Slicer Pattern (from PCD) to avoid cartesian explosions.
-- 2. Dynamically calculates the max vintage day based on the latest conversion, truncating the spine.

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
        COALESCE(CAST(r.model_score_decile AS VARCHAR(10)), '(null)') AS model_score_decile,
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
        COALESCE(CAST(r.model_score_decile AS VARCHAR(10)), '(null)'),
        COALESCE(r.strtgy_seg_typ, '(null)'),
        COALESCE(r.test_group_latest, '(null)'),
        COALESCE(r.offer_prod_latest_name, '(null)')
) WITH DATA PRIMARY INDEX (clnt_no, tactic_id) ON COMMIT PRESERVE ROWS;

-- Stacked Slicer Pattern (Fanned out to avoid wide-grain explosion)
CREATE VOLATILE TABLE vt_pcq_ms_stacked AS (
    SELECT clnt_no, tactic_id, ms_targeted, first_approved_day, first_completed_day,
           'overall' AS slicer_dim, 'ALL' AS slicer_value
    FROM vt_pcq_ms_base
    UNION ALL
    SELECT clnt_no, tactic_id, ms_targeted, first_approved_day, first_completed_day,
           'model_score_decile' AS slicer_dim, model_score_decile AS slicer_value
    FROM vt_pcq_ms_base
    UNION ALL
    SELECT clnt_no, tactic_id, ms_targeted, first_approved_day, first_completed_day,
           'strtgy_seg_typ' AS slicer_dim, strtgy_seg_typ AS slicer_value
    FROM vt_pcq_ms_base
    UNION ALL
    SELECT clnt_no, tactic_id, ms_targeted, first_approved_day, first_completed_day,
           'test_group_latest' AS slicer_dim, test_group_latest AS slicer_value
    FROM vt_pcq_ms_base
    UNION ALL
    SELECT clnt_no, tactic_id, ms_targeted, first_approved_day, first_completed_day,
           'offer_prod_latest_name' AS slicer_dim, offer_prod_latest_name AS slicer_value
    FROM vt_pcq_ms_base
) WITH DATA PRIMARY INDEX (clnt_no, tactic_id, slicer_dim, slicer_value) ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE vt_pcq_ms_cells AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, COUNT(DISTINCT clnt_no) AS cohort_size
    FROM vt_pcq_ms_stacked
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value
) WITH DATA PRIMARY INDEX (tactic_id, ms_targeted, slicer_dim, slicer_value) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_pcq_ms_cells COLUMN (tactic_id, ms_targeted, slicer_dim, slicer_value);

-- Dynamic Max Day (Truncates the spine)
CREATE VOLATILE TABLE vt_pcq_max_day AS (
    SELECT COALESCE(MAX(vintage_day), 14) AS max_vintage_day
    FROM (
        SELECT first_approved_day AS vintage_day FROM vt_pcq_ms_base
        UNION ALL
        SELECT first_completed_day FROM vt_pcq_ms_base
    ) t
) WITH DATA ON COMMIT PRESERVE ROWS;

CREATE VOLATILE TABLE vt_pcq_days_spine AS (
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM sys_calendar.calendar
    CROSS JOIN vt_pcq_max_day m
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND m.max_vintage_day
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_pcq_days_spine COLUMN (vintage_day);

CREATE VOLATILE TABLE vt_pcq_ms_dense_grid AS (
    SELECT c.tactic_id, c.ms_targeted, c.slicer_dim, c.slicer_value, c.cohort_size, d.vintage_day
    FROM vt_pcq_ms_cells c
    CROSS JOIN vt_pcq_days_spine d
) WITH DATA PRIMARY INDEX (tactic_id, ms_targeted, slicer_dim, slicer_value, vintage_day) ON COMMIT PRESERVE ROWS;

WITH daily_conversions AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, first_approved_day AS vintage_day,
           CAST(COUNT(*) AS BIGINT) AS n_approved, CAST(0 AS BIGINT) AS n_completed
    FROM vt_pcq_ms_stacked
    WHERE first_approved_day IS NOT NULL
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, first_approved_day

    UNION ALL

    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, first_completed_day AS vintage_day,
           CAST(0 AS BIGINT) AS n_approved, CAST(COUNT(*) AS BIGINT) AS n_completed
    FROM vt_pcq_ms_stacked
    WHERE first_completed_day IS NOT NULL
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, first_completed_day
),
daily_rollup AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, vintage_day,
           SUM(n_approved) AS n_approved, SUM(n_completed) AS n_completed
    FROM daily_conversions
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, vintage_day
)
SELECT
    g.tactic_id, g.ms_targeted, g.slicer_dim, g.slicer_value, g.vintage_day,
    COALESCE(r.n_approved, 0)  AS n_approved, COALESCE(r.n_completed, 0) AS n_completed,
    SUM(COALESCE(r.n_approved, 0)) OVER (
        PARTITION BY g.tactic_id, g.ms_targeted, g.slicer_dim, g.slicer_value
        ORDER BY g.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_approved,
    SUM(COALESCE(r.n_completed, 0)) OVER (
        PARTITION BY g.tactic_id, g.ms_targeted, g.slicer_dim, g.slicer_value
        ORDER BY g.vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_completed,
    g.cohort_size
FROM vt_pcq_ms_dense_grid g
LEFT JOIN daily_rollup r
       ON g.tactic_id    = r.tactic_id
      AND g.ms_targeted  = r.ms_targeted
      AND g.slicer_dim   = r.slicer_dim
      AND g.slicer_value = r.slicer_value
      AND g.vintage_day  = r.vintage_day
ORDER BY 1, 2, 3, 4, 5;

DROP TABLE vt_pcq_ms_clients;
DROP TABLE vt_pcq_ms_base;
DROP TABLE vt_pcq_ms_stacked;
DROP TABLE vt_pcq_ms_cells;
DROP TABLE vt_pcq_max_day;
DROP TABLE vt_pcq_days_spine;
DROP TABLE vt_pcq_ms_dense_grid;
