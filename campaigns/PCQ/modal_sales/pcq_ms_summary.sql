-- pcq_ms_summary.sql
-- PCQ Modal Sales (MS) — summary totals (no vintage_day breakdown). Engine: Teradata-direct (no EDL/GA4 source — runs Teradata-direct; do NOT add a catalog prefix or it fails).
-- One row per (tactic_id, ms_targeted, slicer_dim, slicer_value). Counts only, no rates.
-- ms_targeted REPLACES action/control here — PCQ has no control arm for MS.
-- Hop 1 (ms_clients) and curated column names copied verbatim from pcq_ms_vs_benchmark.sql.

WITH
ms_clients AS (
    SELECT DISTINCT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'PCQ'
      AND TREATMT_STRT_DT >= DATE '2026-06-01'
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
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
        r.app_approved
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    LEFT JOIN ms_clients m
           ON m.CLNT_NO = r.clnt_no
    WHERE r.mnemonic         = 'PCQ'
      AND r.decsn_year       = 2026
      AND r.treatmt_start_dt >= DATE '2026-06-01'
)

-- OVERALL
SELECT
    tactic_id,
    ms_targeted,
    CAST('OVERALL' AS VARCHAR(50)) AS slicer_dim,
    CAST('ALL'     AS VARCHAR(50)) AS slicer_value,
    COUNT(DISTINCT clnt_no)                                          AS total_population,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)     AS responders,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)     AS responders_completed
FROM base
GROUP BY tactic_id, ms_targeted

UNION ALL

-- decile
SELECT
    tactic_id,
    ms_targeted,
    CAST('decile' AS VARCHAR(50))                       AS slicer_dim,
    COALESCE(CAST(model_score_decile AS VARCHAR(50)), '(null)') AS slicer_value,
    COUNT(DISTINCT clnt_no)                                          AS total_population,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)     AS responders,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)     AS responders_completed
FROM base
GROUP BY tactic_id, ms_targeted, model_score_decile

UNION ALL

-- strategy_seg
SELECT
    tactic_id,
    ms_targeted,
    CAST('strategy_seg' AS VARCHAR(50))         AS slicer_dim,
    COALESCE(strtgy_seg_typ, '(null)')          AS slicer_value,
    COUNT(DISTINCT clnt_no)                                          AS total_population,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)     AS responders,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)     AS responders_completed
FROM base
GROUP BY tactic_id, ms_targeted, strtgy_seg_typ

UNION ALL

-- test_group_latest
SELECT
    tactic_id,
    ms_targeted,
    CAST('test_group_latest' AS VARCHAR(50))    AS slicer_dim,
    COALESCE(test_group_latest, '(null)')       AS slicer_value,
    COUNT(DISTINCT clnt_no)                                          AS total_population,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)     AS responders,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)     AS responders_completed
FROM base
GROUP BY tactic_id, ms_targeted, test_group_latest

UNION ALL

-- response_channel
SELECT
    tactic_id,
    ms_targeted,
    CAST('response_channel' AS VARCHAR(50))     AS slicer_dim,
    COALESCE(response_channel_grp, '(null)')    AS slicer_value,
    COUNT(DISTINCT clnt_no)                                          AS total_population,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)     AS responders,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)     AS responders_completed
FROM base
GROUP BY tactic_id, ms_targeted, response_channel_grp

UNION ALL

-- offered_product
SELECT
    tactic_id,
    ms_targeted,
    CAST('offered_product' AS VARCHAR(50))      AS slicer_dim,
    COALESCE(offer_prod_latest_name, '(null)')  AS slicer_value,
    COUNT(DISTINCT clnt_no)                                          AS total_population,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)     AS responders,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)     AS responders_completed
FROM base
GROUP BY tactic_id, ms_targeted, offer_prod_latest_name

ORDER BY 1, 2, 3, 4;
