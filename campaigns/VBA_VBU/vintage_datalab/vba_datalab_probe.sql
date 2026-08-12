-- ENGINE: Teradata-direct
-- VBA vintage off the curated Data Lab table — STEP 1 of 2: PROBE
-- Source: DL_MR_PROD.NBO_VBA_RBOL_COMBINED
-- Schema doc: schemas/nbo_vba_rbol_combined.md  (flagged "inference only unless confirmed")
--
-- WHY THIS FILE EXISTS: the schema doc never captured datatypes for response_dt,
-- net_response, gross_response, or the visa_*/rbol_* track fields. Repo rule
-- no_guessing_fields: confirm before building. Run P1..P5, paste results back,
-- THEN run vba_vintage_datalab.sql.
--
-- Every probe is <= 25 rows. Run them one at a time.


-------------------------------------------------------------------------------
-- P1. GRAIN. Is the table 1 row per (clnt_no, tactic_id)? Or does a client
--     repeat within a tactic (= denominator would double-count)?
-------------------------------------------------------------------------------
SELECT
    COUNT(*)                                        AS n_rows
  , COUNT(DISTINCT clnt_no)                         AS n_clients
  , COUNT(DISTINCT tactic_id)                       AS n_tactics
  , COUNT(DISTINCT clnt_no || '~' || TRIM(tactic_id)) AS n_clnt_tactic
FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
WHERE treatmt_strt_dt >= DATE '2024-01-01';
-- READ: if n_rows = n_clnt_tactic -> grain confirmed (clnt_no, tactic_id).
--       if n_rows > n_clnt_tactic -> dedup required, tell me the ratio.


-------------------------------------------------------------------------------
-- P2. ARM + MNEMONIC. Exact values of `control`, `test_group`, `mnc`.
--     No guessing arm codes (repo rule use_exact_codes_never_substrings).
-------------------------------------------------------------------------------
SELECT
    TRIM(mnc)                       AS mnc
  , TRIM(COALESCE(control,'<NULL>')) AS control_raw
  , COUNT(*)                        AS n_rows
  , COUNT(DISTINCT clnt_no)         AS n_clients
FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
WHERE treatmt_strt_dt >= DATE '2024-01-01'
GROUP BY 1,2
ORDER BY 1,2;
-- READ: confirm 'Action'/'Control' are the literal strings, and see which
--       mnemonics live in this table besides VBA.


-------------------------------------------------------------------------------
-- P3. SUCCESS FIELDS. What do net_response / gross_response actually hold,
--     and how often is response_dt populated when net_response > 0?
-------------------------------------------------------------------------------
SELECT
    CAST(COALESCE(gross_response,-1) AS INTEGER) AS gross_response_val
  , CAST(COALESCE(net_response,-1)   AS INTEGER) AS net_response_val
  , COUNT(*)                                    AS n_rows
  , SUM(CASE WHEN response_dt IS NULL THEN 1 ELSE 0 END) AS n_resp_dt_null
  , MIN(response_dt)                            AS min_resp_dt
  , MAX(response_dt)                            AS max_resp_dt
FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
WHERE treatmt_strt_dt >= DATE '2024-01-01'
GROUP BY 1,2
ORDER BY 1,2;
-- READ: expect 0/1 flags. If net_response is a count > 1, the vintage
--       numerator changes from "distinct clients" to "sum of responses" -- tell me.
-- READ: n_resp_dt_null on the net_response=1 row is the responders we CANNOT
--       place on a day axis. If that number is material, the curve understates.


-------------------------------------------------------------------------------
-- P4. DAY AXIS SANITY. Negative vintage days (response before treatment start)
--     and how far past treatmt_end_dt responses keep landing.
-------------------------------------------------------------------------------
SELECT
    CASE
        WHEN response_dt IS NULL                     THEN '0_no_response_dt'
        WHEN response_dt <  treatmt_strt_dt          THEN '1_negative_day'
        WHEN response_dt <= treatmt_end_dt           THEN '2_within_treat_window'
        WHEN response_dt <= treatmt_end_dt + 30      THEN '3_within_30d_after'
        WHEN response_dt <= treatmt_end_dt + 90      THEN '4_within_90d_after'
        ELSE                                              '5_beyond_90d_after'
    END                                              AS day_bucket
  , COUNT(*)                                         AS n_rows
  , MIN(response_dt - treatmt_strt_dt)                AS min_day
  , MAX(response_dt - treatmt_strt_dt)                AS max_day
FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
WHERE treatmt_strt_dt >= DATE '2024-01-01'
  AND COALESCE(net_response,0) > 0
GROUP BY 1
ORDER BY 1;
-- READ: bucket 1 (negative) gets clamped to day 0 in the vintage, never dropped
--       (references/vintage_datalab_method.md, the reconciliation fix).
-- READ: buckets 4+5 tell me the real response horizon -> sets HORIZON_DAYS.


-------------------------------------------------------------------------------
-- P5. RECONCILIATION TARGET. Flat summary with NO day axis. The terminal
--     cum_responders in the vintage MUST equal these numbers.
--     Keep this output -- it is the acceptance test for step 2.
-------------------------------------------------------------------------------
SELECT
    CAST(treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS DATE) AS cohort_month
  , TRIM(control)                                                  AS arm
  , COUNT(DISTINCT clnt_no)                                        AS cohort_size
  , COUNT(DISTINCT CASE WHEN COALESCE(net_response,0)  > 0 THEN clnt_no END) AS net_responders
  , COUNT(DISTINCT CASE WHEN COALESCE(gross_response,0)> 0 THEN clnt_no END) AS gross_responders
FROM DL_MR_PROD.NBO_VBA_RBOL_COMBINED
WHERE treatmt_strt_dt >= DATE '2024-01-01'
  AND treatmt_strt_dt IS NOT NULL
  AND TRIM(control) IN ('Action','Control')
GROUP BY 1,2
ORDER BY 1,2;
-- READ: if this returns > 25 rows, add   AND treatmt_strt_dt >= DATE '2025-01-01'
