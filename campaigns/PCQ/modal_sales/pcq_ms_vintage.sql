-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — DENSE cumulative converter curve, days 0..90 since treatment start.
-- Engine: Teradata-direct (no EDL/GA4 source — do NOT add a catalog prefix or it fails).
--
-- Every cell has a row for EVERY day 0..90: cum_approved/cum_completed carry forward, cohort_size
-- is CONSTANT per cell across days. Dense spine is built as a volatile table with COLLECT STATISTICS
-- so the cross join stays under the TDWM unconstrained-product-join row-estimate threshold (a plain
-- SYS_CALENDAR cross join is blocked by TDWM rule F-uncnstrm PJ ... rowest).
--
-- Slicers are PRE-TREATMENT cohort dimensions ONLY: decile, strategy_seg, test_group_latest,
-- offered_product. response_channel is POST-treatment (set at response time) — it cannot define a
-- cohort denominator (cohort_size would vary by channel, which is wrong), so it is NOT a vintage
-- slicer. It stays in the summary as a descriptive split of responders.
--
-- Metrics are WIDE (cum_approved + cum_completed) so no metric-picking is needed.
-- vintage_day = days_to_respond, anchored on each client's FIRST conversion day (no double-count).

-- ---------------------------------------------------------------------------
-- 1) day spine 0..90 as a volatile table (accurate 91-row estimate keeps the
--    cross join in step 5 under the TDWM product-join row-estimate threshold)
-- ---------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_days (vintage_day INTEGER) ON COMMIT PRESERVE ROWS;

INSERT INTO vt_days
SELECT (calendar_date - DATE '2020-01-01')
FROM SYS_CALENDAR.CALENDAR
WHERE calendar_date BETWEEN DATE '2020-01-01' AND (DATE '2020-01-01' + 90);

COLLECT STATISTICS ON vt_days COLUMN vintage_day;

-- ---------------------------------------------------------------------------
-- 2) cohort base: post-Jun PCQ curated rows + MS flag + pre-treatment slicer sources
-- ---------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_base AS (
    SELECT
        r.clnt_no,
        r.tactic_id,
        CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS ms_targeted,
        r.model_score_decile,
        r.strtgy_seg_typ,
        r.test_group_latest,
        r.offer_prod_latest_name,
        r.app_completed,
        r.app_approved,
        r.days_to_respond
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    LEFT JOIN (
        SELECT DISTINCT CLNT_NO
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
          AND TREATMT_STRT_DT >= DATE '2026-06-01'
          AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
    ) m ON m.CLNT_NO = r.clnt_no
    WHERE r.mnemonic         = 'PCQ'
      AND r.decsn_year       = 2026
      AND r.treatmt_start_dt >= DATE '2026-06-01'
) WITH DATA
PRIMARY INDEX (clnt_no)
ON COMMIT PRESERVE ROWS;

-- ---------------------------------------------------------------------------
-- 3) stacked pre-treatment slicers (one cohort block per dimension)
-- ---------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_stacked AS (
    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('OVERALL' AS VARCHAR(50)) AS slicer_dim,
           CAST('ALL'     AS VARCHAR(50)) AS slicer_value
    FROM vt_base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('decile' AS VARCHAR(50)),
           COALESCE(CAST(model_score_decile AS VARCHAR(50)), '(null)')
    FROM vt_base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('strategy_seg' AS VARCHAR(50)),
           COALESCE(strtgy_seg_typ, '(null)')
    FROM vt_base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('test_group_latest' AS VARCHAR(50)),
           COALESCE(test_group_latest, '(null)')
    FROM vt_base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('offered_product' AS VARCHAR(50)),
           COALESCE(offer_prod_latest_name, '(null)')
    FROM vt_base
) WITH DATA
PRIMARY INDEX (tactic_id, ms_targeted, slicer_dim, slicer_value)
ON COMMIT PRESERVE ROWS;

-- ---------------------------------------------------------------------------
-- 4) distinct cohort cells + stats (accurate estimate for the cross join)
-- ---------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_cells AS (
    SELECT DISTINCT tactic_id, ms_targeted, slicer_dim, slicer_value
    FROM vt_stacked
) WITH DATA
PRIMARY INDEX (tactic_id, ms_targeted, slicer_dim, slicer_value)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cells COLUMN (tactic_id, ms_targeted, slicer_dim, slicer_value);

-- ---------------------------------------------------------------------------
-- 5) dense cumulative curve
-- ---------------------------------------------------------------------------
WITH
client_first AS (
    SELECT
        tactic_id, ms_targeted, slicer_dim, slicer_value, clnt_no,
        MIN(CASE WHEN app_approved  = 1 THEN days_to_respond END) AS first_approved_day,
        MIN(CASE WHEN app_completed = 1 THEN days_to_respond END) AS first_completed_day
    FROM vt_stacked
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, clnt_no
),
events AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value,
           first_approved_day AS vintage_day, 1 AS is_app, 0 AS is_comp
    FROM client_first
    WHERE first_approved_day BETWEEN 0 AND 90

    UNION ALL

    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value,
           first_completed_day, 0, 1
    FROM client_first
    WHERE first_completed_day BETWEEN 0 AND 90
),
daily_counts AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value, vintage_day,
           SUM(is_app)  AS n_approved,
           SUM(is_comp) AS n_completed
    FROM events
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, vintage_day
),
denominators AS (
    SELECT tactic_id, ms_targeted, slicer_dim, slicer_value,
           COUNT(DISTINCT clnt_no) AS cohort_size
    FROM vt_stacked
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value
),
scaffold AS (
    SELECT c.tactic_id, c.ms_targeted, c.slicer_dim, c.slicer_value, d.vintage_day
    FROM vt_cells c
    CROSS JOIN vt_days d
)
SELECT
    s.tactic_id,
    s.ms_targeted,
    s.slicer_dim,
    s.slicer_value,
    s.vintage_day,
    SUM(COALESCE(dc.n_approved, 0)) OVER (
        PARTITION BY s.tactic_id, s.ms_targeted, s.slicer_dim, s.slicer_value
        ORDER BY s.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_approved,
    SUM(COALESCE(dc.n_completed, 0)) OVER (
        PARTITION BY s.tactic_id, s.ms_targeted, s.slicer_dim, s.slicer_value
        ORDER BY s.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_completed,
    den.cohort_size
FROM scaffold s
LEFT JOIN daily_counts dc
    ON  dc.tactic_id    = s.tactic_id
    AND dc.ms_targeted  = s.ms_targeted
    AND dc.slicer_dim   = s.slicer_dim
    AND dc.slicer_value = s.slicer_value
    AND dc.vintage_day  = s.vintage_day
INNER JOIN denominators den
    ON  den.tactic_id    = s.tactic_id
    AND den.ms_targeted  = s.ms_targeted
    AND den.slicer_dim   = s.slicer_dim
    AND den.slicer_value = s.slicer_value
ORDER BY s.tactic_id, s.ms_targeted, s.slicer_dim, s.slicer_value, s.vintage_day;

-- ---------------------------------------------------------------------------
-- cleanup
-- ---------------------------------------------------------------------------
DROP TABLE vt_days;
DROP TABLE vt_cells;
DROP TABLE vt_stacked;
DROP TABLE vt_base;
