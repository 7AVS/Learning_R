-- pcq_ms_summary.sql
-- PCQ Modal Sales (MS) — summary. Engine: Teradata-direct (no EDL/GA4 source — do NOT add a catalog prefix or it fails).
-- ms_targeted (1/0) REPLACES action/control — PCQ has no control arm for MS. Counts only, no rates.
-- Hop 1 (ms_clients) and curated column names copied verbatim from pcq_ms_vs_benchmark.sql.
--
-- Two views (no slicer long-format — categories are kept as their own columns so you can cross-check):
--   QUERY 1  roll-up: MS vs benchmark totals per deployment + overall.
--   QUERY 2  wide category cube: every category as a column -> pivot/cross-tab any combination in Excel.
-- NOTE on QUERY 2: clients/approved/completed use COUNT(DISTINCT clnt_no). If a client spans
--   >1 value of a category (e.g. two offered products / response channels), summing those columns
--   across categories in a pivot can overcount that client. For TRUE distinct totals use QUERY 1.


-- ============================================================================
-- QUERY 1: roll-up — MS vs benchmark, per deployment and overall (GROUPING SETS).
-- ============================================================================
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
        r.app_completed,
        r.app_approved
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    LEFT JOIN ms_clients m
           ON m.CLNT_NO = r.clnt_no
    WHERE r.decsn_year       = 2026
      AND r.tpa_ita          = 'TPA'
      AND r.treatmt_start_dt >= DATE '2026-06-01'
)
SELECT
    ms_targeted,
    COALESCE(tactic_id, 'ALL DEPLOYMENTS')                         AS tactic_id,
    COUNT(DISTINCT clnt_no)                                        AS clients,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)   AS approved,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)   AS completed
FROM base
GROUP BY GROUPING SETS ((ms_targeted, tactic_id), (ms_targeted))
ORDER BY ms_targeted DESC, tactic_id;


-- ============================================================================
-- QUERY 2: wide category cube — one row per (ms_targeted x deployment x all categories).
--   Categories are columns, so pivot/cross-check any pair (e.g. decile x channel) in Excel.
-- ============================================================================
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
    WHERE r.decsn_year       = 2026
      AND r.tpa_ita          = 'TPA'
      AND r.treatmt_start_dt >= DATE '2026-06-01'
)
SELECT
    ms_targeted,
    tactic_id,
    model_score_decile,
    strtgy_seg_typ,
    test_group_latest,
    response_channel_grp,
    offer_prod_latest_name,
    COUNT(*)                                                       AS rows_acct_grain,
    COUNT(DISTINCT clnt_no)                                        AS clients,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)   AS approved,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)   AS completed
FROM base
GROUP BY
    ms_targeted,
    tactic_id,
    model_score_decile,
    strtgy_seg_typ,
    test_group_latest,
    response_channel_grp,
    offer_prod_latest_name
ORDER BY ms_targeted DESC, tactic_id, model_score_decile, strtgy_seg_typ;
