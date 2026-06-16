-- pcq_ms_vintage.sql
-- PCQ Modal Sales (MS) — cumulative converter curve by days since treatment start. Engine: Starburst / Trino.
-- One row per (tactic_id, ms_targeted, slicer_dim, slicer_value, metric, vintage_day). 0..90 day spine.
-- vintage_day = curated days_to_respond, clamped 0..90. ms_targeted REPLACES action/control.
-- Hop 1 (ms_clients) and curated column names copied verbatim from pcq_ms_vs_benchmark.sql.

WITH
ms_clients AS (
    SELECT DISTINCT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTRING(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTRING(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
),

base AS (
    SELECT
        r.clnt_no,
        r.tactic_id,
        CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS ms_targeted,
        r.model_score_decile,
        r.strtgy_seg_typ,
        r.test_group_latest,
        r.response_channel_grp,
        r.offer_prod_latest_name,
        r.app_completed,
        r.app_approved,
        r.days_to_respond
    FROM dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp r
    LEFT JOIN ms_clients m
           ON m.CLNT_NO = r.clnt_no
    WHERE r.mnemonic         = 'PCQ'
      AND r.decsn_year       = 2026
      AND r.treatmt_start_dt >= DATE '2026-06-01'
),

-- base_stacked: fan out to 6 rotating slicer blocks; tactic_id/ms_targeted carried as permanent columns
base_stacked AS (
    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('OVERALL' AS VARCHAR(50)) AS slicer_dim,
           CAST('ALL'     AS VARCHAR(50)) AS slicer_value
    FROM base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('decile' AS VARCHAR(50))                                AS slicer_dim,
           COALESCE(CAST(model_score_decile AS VARCHAR(50)), '(null)')  AS slicer_value
    FROM base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('strategy_seg' AS VARCHAR(50)) AS slicer_dim,
           COALESCE(strtgy_seg_typ, '(null)')  AS slicer_value
    FROM base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('test_group_latest' AS VARCHAR(50)) AS slicer_dim,
           COALESCE(test_group_latest, '(null)')    AS slicer_value
    FROM base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('response_channel' AS VARCHAR(50)) AS slicer_dim,
           COALESCE(response_channel_grp, '(null)') AS slicer_value
    FROM base

    UNION ALL

    SELECT clnt_no, tactic_id, ms_targeted, app_approved, app_completed, days_to_respond,
           CAST('offered_product' AS VARCHAR(50))     AS slicer_dim,
           COALESCE(offer_prod_latest_name, '(null)') AS slicer_value
    FROM base
),

-- metric_universe: hardcoded list of both metrics — ensures zero-event combos still appear
metric_universe AS (
    SELECT 'approved'  AS metric UNION ALL
    SELECT 'completed'
),

-- slicer_universe: every (tactic, ms_targeted, slicer) cell that exists in base_stacked
slicer_universe AS (
    SELECT DISTINCT tactic_id, ms_targeted, slicer_dim, slicer_value
    FROM base_stacked
),

-- scaffold: full day spine 0..90 for every (tactic x ms_targeted x slicer x metric) cell
scaffold AS (
    SELECT
        s.tactic_id,
        s.ms_targeted,
        s.slicer_dim,
        s.slicer_value,
        m.metric,
        d.vintage_day
    FROM slicer_universe s
    CROSS JOIN metric_universe m
    CROSS JOIN UNNEST(SEQUENCE(0, 90)) AS d(vintage_day)
),

-- client_first: per client per cell, the FIRST day they converted (min days_to_respond),
-- separately for approved and completed. Anchoring on first event prevents a multi-application
-- client from being counted on more than one vintage_day (which would inflate the cumulative curve).
client_first AS (
    SELECT
        tactic_id, ms_targeted, slicer_dim, slicer_value, clnt_no,
        MIN(CASE WHEN app_approved  = 1 THEN days_to_respond END) AS first_approved_day,
        MIN(CASE WHEN app_completed = 1 THEN days_to_respond END) AS first_completed_day
    FROM base_stacked
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, clnt_no
),

-- daily_counts: distinct converters booked on their FIRST conversion day, per metric
daily_counts AS (
    SELECT
        tactic_id, ms_targeted, slicer_dim, slicer_value,
        CAST('approved' AS VARCHAR(50))   AS metric,
        first_approved_day                AS vintage_day,
        COUNT(*)                          AS n_responders
    FROM client_first
    WHERE first_approved_day BETWEEN 0 AND 90
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, first_approved_day

    UNION ALL

    SELECT
        tactic_id, ms_targeted, slicer_dim, slicer_value,
        CAST('completed' AS VARCHAR(50))  AS metric,
        first_completed_day               AS vintage_day,
        COUNT(*)                          AS n_responders
    FROM client_first
    WHERE first_completed_day BETWEEN 0 AND 90
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value, first_completed_day
),

-- denominators: cohort_size per (tactic, ms_targeted, slicer) cell — constant across vintage_day/metric
denominators AS (
    SELECT
        tactic_id, ms_targeted, slicer_dim, slicer_value,
        COUNT(DISTINCT clnt_no) AS cohort_size
    FROM base_stacked
    GROUP BY tactic_id, ms_targeted, slicer_dim, slicer_value
),

-- cumulative: join daily counts onto scaffold, zero-fill, window cumulative sum
cumulative AS (
    SELECT
        s.tactic_id,
        s.ms_targeted,
        s.slicer_dim,
        s.slicer_value,
        s.metric,
        s.vintage_day,
        SUM(COALESCE(dc.n_responders, 0)) OVER (
            PARTITION BY s.tactic_id, s.ms_targeted, s.slicer_dim, s.slicer_value, s.metric
            ORDER BY s.vintage_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_responders
    FROM scaffold s
    LEFT JOIN daily_counts dc
        ON  dc.tactic_id    = s.tactic_id
        AND dc.ms_targeted  = s.ms_targeted
        AND dc.slicer_dim   = s.slicer_dim
        AND dc.slicer_value = s.slicer_value
        AND dc.metric       = s.metric
        AND dc.vintage_day  = s.vintage_day
)

-- final output
SELECT
    c.tactic_id,
    c.ms_targeted,
    c.slicer_dim,
    c.slicer_value,
    c.metric,
    c.vintage_day,
    c.cum_responders,
    d.cohort_size
FROM cumulative c
INNER JOIN denominators d
    ON  d.tactic_id    = c.tactic_id
    AND d.ms_targeted  = c.ms_targeted
    AND d.slicer_dim   = c.slicer_dim
    AND d.slicer_value = c.slicer_value
ORDER BY c.tactic_id, c.ms_targeted, c.slicer_dim, c.slicer_value, c.metric, c.vintage_day;
