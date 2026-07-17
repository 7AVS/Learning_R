-- value_capture/blocks/pcq_ms_block.sql
-- NOTE: this file stays PER-COHORT / START-DATE windowed for granular presentation. The quarterly
--   partner-sheet rollup (value_capture_report.sql) uses a DIFFERENT window (treatment END date) and
--   a client-level first-touch dedup across the whole quarter -- do not sum this file's output across
--   cohort_month, it will double-count multi-cohort clients. See value_capture_report.sql's header.
-- Value-capture ASSIGNMENT contrast for PCQ Modal Sales.
-- MINIMAL RE-AGGREGATION of campaigns/sales_modal/pcq/pcq_ms_summary.sql QUERY 2 (same base table,
--   same filters -- NOT new measurement logic). Changes vs QUERY 2:
--   DROPPED: ms_clients CTE / LEFT JOIN and its ms_targeted output column, tactic_id, strtgy_seg_typ,
--     response_channel_grp, offer_prod_latest_name -- not part of the contract grain. ms_targeted is
--     the DELIVERY flag; this block is the ASSIGNMENT contrast (test_group_latest) per
--     campaigns/sales_modal/README.md Open Decision #2 -- chosen for exec reporting because the
--     delivery-flag comparison is self-selected (post-assignment) and the pooled assignment read is
--     decile-confounded, hence decile is kept as its own stratum column for stratified lift downstream.
--     Dropping the ms_clients join does not change any count: it was a 1:1 LEFT JOIN on clnt_no
--     (ms_clients is DISTINCT CLNT_NO) that only added a flag column, never duplicated or filtered rows.
--   ADDED: cohort_month (derived from treatmt_start_dt, 'YYYY-MM' cast pattern copied verbatim from
--     shared/ms_population_success_template.sql line 86 -- QUERY 2 had no cohort_month, only tactic_id).
--     *_asc columns gated TRIM(asc_on_app_source) = 'Period-ASC' (exact gate syntax copied verbatim
--     from pcq_ms_vintage.sql Step 3) ALONGSIDE the *_raw (ungated) columns QUERY 2 already had --
--     QUERY 2's approved/completed are exactly this block's approved_raw/completed_raw.
--   Gating choice = Open Decision #1 (see campaigns/sales_modal/README.md) -- both variants shipped,
--     workbook picks one via success_pick.
-- Reconciliation -- this is the SAME base filters/table as pcq_ms_summary.sql QUERY 2
--   (decsn_year=2026, tpa_ita='TPA', treatmt_start_dt >= DATE '2026-06-01', DL_MR_PROD.cards_tpa_pcq_
--   decision_resp): QUERY 2's clients/approved/completed, summed over its other grouping columns
--   (ms_targeted, tactic_id, strtgy_seg_typ, response_channel_grp, offer_prod_latest_name) for a fixed
--   (test_group_latest, model_score_decile) pair bucketed into the matching cohort_month, reproduce
--   this block's clients/approved_raw/completed_raw exactly (COUNT DISTINCT clnt_no partitions
--   disjointly across the dropped dimensions, so summing recombines them with no double-count).
--   *_asc columns have no QUERY 2 analog (QUERY 2 is ungated) -- they reconcile instead to
--   pcq_ms_vintage.sql's Step 3 first-event gate applied at day-0-or-later cumulative, collapsed to a
--   single ever-succeeded (any day) count per client.
-- Engine: Teradata-direct (DL_MR_PROD.*, no catalog prefix, Teradata SQL syntax -- do not run through
--   Starburst federation).
-- Arm codes NOT hardcoded/filtered (they drift across sources): raw test_group_latest carried as its
--   own column; the workbook maps codes to test/control (champion = NG3_CHMP per current data;
--   challenger = NG3_CHLN/NG3_CHLG per pcq_ms_vintage.sql's confirmed-code note, 2026-06-19 --
--   reverify before mapping).
-- Counts only. Grain: test_group_latest x decile x cohort_month -> clients, approved_asc,
--   completed_asc, approved_raw, completed_raw (all COUNT(DISTINCT clnt_no)).
-- cohort_month kept per repo hard rule; pooling across cohort_month happens downstream in the workbook.

-- EDIT POINT: treatment window start
SELECT
    TRIM(test_group_latest)                                                           AS test_group_latest,
    CAST(model_score_decile AS VARCHAR(10))                                           AS decile,
    CAST(CAST(treatmt_start_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7))                AS cohort_month,
    COUNT(DISTINCT clnt_no)                                                           AS clients,
    COUNT(DISTINCT CASE WHEN app_approved  = 1
                          AND TRIM(asc_on_app_source) = 'Period-ASC' THEN clnt_no END)  AS approved_asc,
    COUNT(DISTINCT CASE WHEN app_completed = 1
                          AND TRIM(asc_on_app_source) = 'Period-ASC' THEN clnt_no END)  AS completed_asc,
    COUNT(DISTINCT CASE WHEN app_approved  = 1 THEN clnt_no END)                       AS approved_raw,
    COUNT(DISTINCT CASE WHEN app_completed = 1 THEN clnt_no END)                       AS completed_raw
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE decsn_year       = 2026
  AND tpa_ita          = 'TPA'
  AND treatmt_start_dt >= DATE '2026-06-01'                                            -- EDIT POINT
GROUP BY
    TRIM(test_group_latest),
    CAST(model_score_decile AS VARCHAR(10)),
    CAST(CAST(treatmt_start_dt AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7))
ORDER BY cohort_month, test_group_latest, decile;
