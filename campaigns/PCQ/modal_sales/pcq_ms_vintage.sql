-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — cumulative converter curve, vintage_day = days since treatment start.
-- Engine: Teradata-direct (no EDL/GA4 source — no catalog prefix). Descriptive only, no control group.
--
-- REWRITTEN: 
-- 1. Uses the Long-Format Slicer Pattern (from PCD) to avoid cartesian explosions.
-- 2. Dynamically calculates the max vintage day based on the latest conversion, truncating the spine.
-- 3. MINIMAL VOLATILE TABLES: Only cells and days_spine are materialized to bypass the TDWM 
--    unconstrained product join error. All other logic uses CTEs to prevent "table already exists" session errors.

-- NOTE: If you get "table already exists" from a previous failed run in your session, run these drops first:
-- DROP TABLE vt_pcq_ms_cells;
-- DROP TABLE vt_pcq_days_spine;

CREATE VOLATILE TABLE vt_pcq_ms_cells AS (
    WITH ms_clients AS (
        SELECT DISTINCT CLNT_NO, TRIM(TACTIC_ID) AS TACTIC_ID
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE TREATMT_STRT_DT >= DATE '2026-06-01'
          AND SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
          AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
    ),
    client_base AS (
        SELECT
            r.clnt_no,
            TRIM(r.tactic_id) AS tactic_id,
            CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END             AS ms_targeted,
            COALESCE(CAST(r.model_score_decile AS VARCHAR(10)), '(null)') AS model_score_decile,
            COALESCE(r.strtgy_seg_typ, '(null)')                          AS strtgy_seg_typ,
            COALESCE(r.test_group_latest, '(null)')                       AS test_group_latest,
            COALESCE(r.offer_prod_latest_name, '(null)')                  AS offer_prod_latest_name
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
        LEFT JOIN ms_clients m
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
    ),
    stacked AS (
        SELECT clnt_no, tactic_id, ms_targeted, CAST('overall' AS VARCHAR(50)) AS slicer_dim, CAST('ALL' AS VARCHAR(100)) AS slicer_value FROM client_base
        UNION ALL
        SELECT clnt_no, tactic_id, ms_targeted, 'model_score_decile', model_score_decile FROM client_base
        UNION ALL
        SELECT clnt_no, tactic_id, ms_targeted, 'strtgy_seg_typ', strtgy_seg_typ FROM client_base
        UNION ALL
        SELECT clnt_no, tactic_id, ms_targeted, 'test_group_latest', test_group_latest FROM client_base
        UNION ALL
        SELECT clnt_no, tactic_id, ms_targeted, 'offer_prod_latest_name', offer_prod_latest_name FROM client_base
    )
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, COUNT(DISTINCT clnt_no) AS cohort_size
    FROM stacked
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value
) WITH DATA PRIMARY INDEX (tactic_id, ms_targeted, slicer_dim, slicer_value) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_ms_cells COLUMN (tactic_id, ms_targeted, slicer_dim, slicer_value);

CREATE VOLATILE TABLE vt_pcq_days_spine AS (
    WITH max_day AS (
        SELECT COALESCE(MAX(vintage_day), 14) AS max_vintage_day
        FROM (
            SELECT MAX(CASE WHEN app_approved = 1 THEN days_to_respond END) AS vintage_day 
            FROM DL_MR_PROD.cards_tpa_pcq_decision_resp 
            WHERE mnemonic = 'PCQ' AND decsn_year = 2026 AND treatmt_start_dt >= DATE '2026-06-01'
            UNION ALL
            SELECT MAX(CASE WHEN app_completed = 1 THEN days_to_respond END) 
            FROM DL_MR_PROD.cards_tpa_pcq_decision_resp 
            WHERE mnemonic = 'PCQ' AND decsn_year = 2026 AND treatmt_start_dt >= DATE '2026-06-01'
        ) t
    )
    SELECT (calendar_date - DATE '1900-01-01') AS vintage_day
    FROM sys_calendar.calendar
    CROSS JOIN max_day m
    WHERE (calendar_date - DATE '1900-01-01') BETWEEN 0 AND m.max_vintage_day
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcq_days_spine COLUMN (vintage_day);

WITH 
dense_grid AS (
    SELECT c.tactic_id, c.ms_targeted, c.slicer_dim, c.slicer_value, c.cohort_size, d.vintage_day
    FROM vt_pcq_ms_cells c
    CROSS JOIN vt_pcq_days_spine d
),
ms_clients AS (
    SELECT DISTINCT CLNT_NO, TRIM(TACTIC_ID) AS TACTIC_ID
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
),
client_conversions AS (
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
    LEFT JOIN ms_clients m
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
),
stacked_conversions AS (
    SELECT tactic_id, ms_targeted, CAST('overall' AS VARCHAR(50)) AS slicer_dim, CAST('ALL' AS VARCHAR(100)) AS slicer_value, first_approved_day, first_completed_day FROM client_conversions
    UNION ALL
    SELECT tactic_id, ms_targeted, 'model_score_decile', model_score_decile, first_approved_day, first_completed_day FROM client_conversions
    UNION ALL
    SELECT tactic_id, ms_targeted, 'strtgy_seg_typ', strtgy_seg_typ, first_approved_day, first_completed_day FROM client_conversions
    UNION ALL
    SELECT tactic_id, ms_targeted, 'test_group_latest', test_group_latest, first_approved_day, first_completed_day FROM client_conversions
    UNION ALL
    SELECT tactic_id, ms_targeted, 'offer_prod_latest_name', offer_prod_latest_name, first_approved_day, first_completed_day FROM client_conversions
),
daily_conversions AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, first_approved_day AS vintage_day,
           CAST(COUNT(*) AS BIGINT) AS n_approved, CAST(0 AS BIGINT) AS n_completed
    FROM stacked_conversions
    WHERE first_approved_day IS NOT NULL
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, first_approved_day

    UNION ALL

    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, first_completed_day AS vintage_day,
           CAST(0 AS BIGINT) AS n_approved, CAST(COUNT(*) AS BIGINT) AS n_completed
    FROM stacked_conversions
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
FROM dense_grid g
LEFT JOIN daily_rollup r
       ON g.tactic_id    = r.tactic_id
      AND g.ms_targeted  = r.ms_targeted
      AND g.slicer_dim   = r.slicer_dim
      AND g.slicer_value = r.slicer_value
      AND g.vintage_day  = r.vintage_day
ORDER BY 1, 2, 3, 4, 5;

DROP TABLE vt_pcq_ms_cells;
DROP TABLE vt_pcq_days_spine;
