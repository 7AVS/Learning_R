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
-- OPEN DECISION #1 — Period-ASC gating (two recorded rules CONFLICT — Andre picks per use)
--   (a) General PCQ measurement canon: success numerators gated to
--       TRIM(asc_on_app_source) = 'Period-ASC', NUMERATOR only; denominator stays all clients.
--   (b) Andre's instruction (2026-06, for the MS descriptive read specifically): do NOT gate —
--       app_approved/app_completed raw, asc_on_app_source kept as a visible column only.
-- AS READ in this repo: pcq_ms_vs_benchmark.sql and pcq_ms_summary.sql are ungated (rule b);
-- pcq_ms_vintage.sql gates (rule a). That split is deliberate, not an oversight — but every
-- filled-in copy of this template must state which rule it uses. <SUCCESS_ASC_GATE> expresses
-- either choice. asc_on_app_source is PCQ-ONLY — leave the gate as a no-op for PCL (no
-- equivalent field confirmed on cards_pli_decision_resp).
--
-- OPEN DECISION #2 — population split: TWO DIFFERENT ESTIMANDS, pick per question
--   (a) test_group_latest (NG3_CHMP champion vs NG3_CHLN/NG3_CHLD challengers) = design
--       ASSIGNMENT — closest to intent-to-treat; use for any lift-flavored read. Caveats:
--       deployment variance put both arms across all 10 deciles, so compare decile-matched,
--       never pooled; arm codes drift across sources (CHLG/CHLN/CHLD seen) — lock exact codes
--       from data before running.
--   (b) ms_targeted (Hop-1 tactic-event flag below) = DELIVERY truth (was the MS creative
--       actually served, per DG6V01.TACTIC_EVNT_IP_AR_HIST). Post-assignment — valid for a
--       descriptive population comparison, never for lift.
-- pcq_ms_vintage.sql splits by (a); pcq_ms_vs_benchmark/summary split by (b). Neither is wrong;
-- they answer different questions. Whichever is NOT the split goes in <POPULATION_DIMENSION>
-- as a carried dimension.
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
--   <POPULATION_DIMENSION>   curated-table column carried as a DIMENSION (whichever estimand is NOT
--                              the split — see Decision #2): PCQ -> r.test_group_latest
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
