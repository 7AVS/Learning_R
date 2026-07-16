-- ms_population_success_template.sql
-- SHARED PARAMETERIZED TEMPLATE — Modal Sales (MS) population + success, two-hop pattern.
-- Engine: TERADATA-DIRECT (source files below run Teradata-direct — bare table names, NO catalog
--   prefix, NO Starburst/Trino translation). Do not run this through Starburst/federation.
--
-- Generalized from: campaigns/sales_modal/pcq/pcq_ms_vs_benchmark.sql (OUTPUT B — counts-only rollup).
-- Cross-checked against: campaigns/sales_modal/pcq/pcq_ms_summary.sql (QUERY 1, near-duplicate of
--   vs_benchmark OUTPUT B), campaigns/sales_modal/pcq/pcq_ms_vintage.sql (Step 3 success-gating logic),
--   campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql (PCL curated column names: responder_cli,
--   dt_cl_change, treatmt_strt_dt — confirms PCL's curated table has NO asc_on_app_source-equivalent
--   field; schemas/nbo_vba_rbol_combined.md notes VBA's analog is visa_asc_on_app, nothing for PCL).
--
-- ============================================================================
-- PROPOSED CANON #1 — pending Andre sign-off
-- PCQ success numerators must ALWAYS be Period-ASC-gated: the numerator (SUM/COUNT of
-- app_approved / app_completed) is filtered to TRIM(asc_on_app_source) = 'Period-ASC'; the
-- denominator (clients / cohort_size) stays ALL ms_targeted clients in the window, ungated.
-- This is already memory canon (reference_pcq_measurement_filters.md) but AS READ in this repo,
-- pcq_ms_vs_benchmark.sql (OUTPUT A/B) and pcq_ms_summary.sql (QUERY 1/2) do NOT apply this gate —
-- both sum/count app_approved/app_completed raw, no asc_on_app_source filter anywhere in either file.
-- Only pcq_ms_vintage.sql (Step 3: first_approved_day/first_completed_day) applies it correctly.
-- This template applies the gate per <SUCCESS_ASC_GATE> below. asc_on_app_source is PCQ-ONLY —
-- leave <SUCCESS_ASC_GATE> blank for PCL (no equivalent field confirmed on cards_pli_decision_resp).
--
-- PROPOSED CANON #2 — pending Andre sign-off
-- Canonical PCQ population split = ms_targeted, the Hop-1 tactic-event flag (delivery truth: was this
-- client actually served the MS creative, per DG6V01.TACTIC_EVNT_IP_AR_HIST). test_group_latest
-- (NG3_CHMP / NG3_CHLN / NG3_CHLG) is carried as a DIMENSION only (a column in SELECT/GROUP BY),
-- NEVER as the population-defining split. AS READ, pcq_ms_vintage.sql uses test_group_latest
-- (champion/challenger) AS the arm definition instead of ms_targeted — that file does not apply this
-- rule. This template's Hop 1 (ms_targeted) is the corrected pattern; <POPULATION_DIMENSION> below
-- is where test_group_latest (or PCL's equivalent) gets carried, dimension-only.
-- ============================================================================
--
-- PARAMETERS (fill in before running — verify every column against the campaign's curated-table
-- source file before trusting it; do not guess):
--   <MNE>                    campaign mnemonic, TACTIC_ID position 8 length 3. 'PCQ' or 'PCL'.
--   <CURATED_TABLE>          curated decision/resp table:
--                              PCQ -> DL_MR_PROD.cards_tpa_pcq_decision_resp
--                              PCL -> DL_MR_PROD.cards_pli_decision_resp (schema prefix per source file)
--   <CURATED_ALIAS_TRT_STRT> curated table's treatment-start column name — NOT the same spelling as the
--                              tactic-event table's TREATMT_STRT_DT:
--                              PCQ -> treatmt_start_dt   PCL -> treatmt_strt_dt (verify per p9)
--   <TREATMENT_WINDOW_START> cohort start date, e.g. DATE '2026-06-01'
--   <MANDATORY_FILTERS>      campaign-specific WHERE clauses beyond the window:
--                              PCQ -> r.decsn_year = 2026 AND r.tpa_ita = 'TPA'
--                              PCL -> none confirmed in this repo (p9 filters only on
--                                     report_groups_period LIKE '%R____WMS%'/'%R____NMS%' + window —
--                                     if PCL curated needs an equivalent gate, confirm before running)
--   <SUCCESS_NUMERATOR_1>    first success column:
--                              PCQ -> r.app_approved     PCL -> r.responder_cli
--   <SUCCESS_NUMERATOR_2>    second success column (OPTIONAL — set to NULL literal if not needed):
--                              PCQ -> r.app_completed    PCL -> CASE WHEN r.dt_cl_change IS NOT NULL
--                                                                 AND r.dt_cl_change >= r.<CURATED_ALIAS_TRT_STRT>
--                                                                 THEN 1 ELSE 0 END (per p10's guard pattern)
--   <SUCCESS_ASC_GATE>       PCQ-only Period-ASC gate on the numerator, e.g.
--                              AND TRIM(r.asc_on_app_source) = 'Period-ASC'
--                            Leave as a no-op (1=1) for PCL — no equivalent field confirmed.
--   <POPULATION_DIMENSION>   curated-table column carried as a DIMENSION only (never the population
--                              split — see Canon #2): PCQ -> r.test_group_latest
--                              PCL -> derive an arm label from report_groups_period (see p9) if needed,
--                              or set to CAST(NULL AS VARCHAR(20)) if no dimension is wanted yet.
--
-- COLUMN NAMES NOT VERIFIED FOR PCL IN THIS REPO — do not guess, confirm against a live PCL curated
-- query before running:
--   - No PCL equivalent of decsn_year / tpa_ita was found; p9_vcl_full_measurement.sql does not filter
--     on either, so <MANDATORY_FILTERS> may legitimately be empty for PCL, but this is unconfirmed.
--   - No PCL equivalent of asc_on_app_source was found (see Canon #1 note above).
--
-- cohort_month: derived from <CURATED_ALIAS_TRT_STRT> per repo cohort convention (cohort grain =
-- treatment start date). Format pattern copied verbatim from pcq_ms_vintage.sql's wave_dt cast.

WITH ms_clients AS (
    SELECT DISTINCT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = '<MNE>'
      AND TREATMT_STRT_DT >= <TREATMENT_WINDOW_START>
      AND SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%MS%'
)
SELECT
    CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END               AS ms_targeted,
    CAST(CAST(r.<CURATED_ALIAS_TRT_STRT> AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)) AS cohort_month,
    r.tactic_id,
    <POPULATION_DIMENSION>                                          AS population_dimension,
    COUNT(*)                                                        AS rows_acct_grain,
    COUNT(DISTINCT r.clnt_no)                                       AS clients,
    SUM(CASE WHEN <SUCCESS_NUMERATOR_1> = 1 <SUCCESS_ASC_GATE> THEN 1 ELSE 0 END) AS success_1,
    SUM(CASE WHEN <SUCCESS_NUMERATOR_2> = 1 <SUCCESS_ASC_GATE> THEN 1 ELSE 0 END) AS success_2
FROM <CURATED_TABLE> r
LEFT JOIN ms_clients m
       ON m.CLNT_NO = r.clnt_no
WHERE r.<CURATED_ALIAS_TRT_STRT> >= <TREATMENT_WINDOW_START>
  AND <MANDATORY_FILTERS>
GROUP BY
    CASE WHEN m.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END,
    CAST(CAST(r.<CURATED_ALIAS_TRT_STRT> AS DATE FORMAT 'YYYY-MM') AS VARCHAR(7)),
    r.tactic_id,
    <POPULATION_DIMENSION>
ORDER BY ms_targeted DESC, cohort_month, r.tactic_id, population_dimension;
