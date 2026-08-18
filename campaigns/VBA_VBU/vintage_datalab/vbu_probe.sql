-- ENGINE: Teradata-direct
-- VBU vintage off the curated Data Lab table — STEP 1 of 2: PROBE
-- Source: DL_MR_PROD.cards_bizups_vbu_descresp_clnt
-- Schema doc: schemas/cards_bizups_vbu_descresp_clnt.md ("field meanings are
-- inference only unless explicitly confirmed" — do not assume, this pack exists
-- to confirm)
--
-- WHY THIS FILE EXISTS: VBU's methodology is product-change success, not a new
-- application, and its field names don't mirror VBA's 1:1 (see the naming-
-- conflicts table in the schema doc). Two things VBA had that VBU does NOT have
-- a confirmed equivalent for:
--   1. treatmt_strt_dt -- VBU has THREE candidates instead (fy_start,
--      year_mon_start, response_start), none confirmed as the real date-typed
--      treatment anchor. P1 decides which one to use.
--   2. treatmt_end_dt -- no equivalent at all. response_end is the closest
--      candidate by name; P5 checks whether it behaves like a window end.
-- This pack decides: grain / cohort-anchor field / arm field / success-flag
-- semantics (responder vs responder_anyproduct vs responder_targetproduct) /
-- conversion horizon / window length / product-pair concentration.
--
-- Every probe is <= 20 rows. Run one at a time, in order -- P2b onward assume
-- response_start as a TENTATIVE cohort anchor; if P1 says that's wrong, swap
-- the field before running the rest.
--
-- Paste results into campaigns/VBA_VBU/vintage_datalab/vbu_probe_results_2026-08-18.md


-------------------------------------------------------------------------------
-- P1: GRAIN + COHORT-ANCHOR RECON.
--     (a) Is this 1 row per (clnt_no, tactic_id)? Open question #8 in the
--         schema doc -- and the VBA table's answer to the same question was
--         NO (it was a reporting snapshot, ~4.1 rows per pair). Don't assume
--         VBU is different.
--     (b) Which of the four date-ish fields (fy_start, year_mon_start,
--         response_start, treatmt_mn) is an actual comparable date, and what's
--         its range? Whichever one is real becomes the cohort anchor for every
--         probe below and for the vintage build.
--     NO 2024 floor here on purpose -- a floor needs a confirmed date column,
--     which is exactly what this probe is deciding.
-------------------------------------------------------------------------------
SELECT
    COUNT(*)                                             AS n_rows
  , COUNT(DISTINCT clnt_no)                               AS n_clients
  , COUNT(DISTINCT clnt_no || '~' || TRIM(tactic_id))     AS n_clnt_tactic
  , MIN(fy_start)                                         AS min_fy_start
  , MAX(fy_start)                                         AS max_fy_start
  , MIN(response_start)                                   AS min_response_start
  , MAX(response_start)                                   AS max_response_start
  , MIN(year_mon_start)                                   AS min_year_mon_start
  , MAX(year_mon_start)                                   AS max_year_mon_start
  , MIN(treatmt_mn)                                       AS min_treatmt_mn
  , MAX(treatmt_mn)                                       AS max_treatmt_mn
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt;
-- READ: n_rows = n_clnt_tactic -> grain confirmed (clnt_no, tactic_id).
--       n_rows > n_clnt_tactic -> dedup required, same shape as VBA -- look for
--       a repeating dimension (report_date / comparison / segment equivalent).
-- READ: whichever of the four MIN/MAX pairs prints real calendar dates (not a
--       4-digit fiscal year, not a YYYYMM integer) is the cohort anchor. If a
--       column errors on MIN/MAX, it isn't a comparable date type -- drop it
--       and say so instead of guessing a cast.


-------------------------------------------------------------------------------
-- P2a: ARM FIELD RECON. VBA had ONE grouping column (`control`, literal values
--      'Action'/'Control'). VBU's schema doc lists test_group, control, AND
--      report_group as three separate identity/treatment fields -- decides
--      which one (if any) is the actual two-arm split, and whether the other
--      two are redundant labels or a different dimension entirely.
-------------------------------------------------------------------------------
SELECT
    TRIM(COALESCE(test_group,'<NULL>'))     AS test_group_raw
  , TRIM(COALESCE(control,'<NULL>'))        AS control_raw
  , TRIM(COALESCE(report_group,'<NULL>'))   AS report_group_raw
  , COUNT(*)                                AS n_rows
  , COUNT(DISTINCT clnt_no)                 AS n_clients
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
GROUP BY 1,2,3
ORDER BY 4 DESC;
-- READ: use_exact_codes rule -- don't assume control holds 'Action'/'Control'
--       like VBA. Read the literal strings back. If report_group ends up being
--       a wave/segment label rather than an arm, note that so it doesn't get
--       mistaken for one downstream.


-------------------------------------------------------------------------------
-- P2b: MONTHLY COHORT SIZES. Uses response_start as the TENTATIVE cohort
--      anchor pending P1 confirming it's the right field -- if P1 says a
--      different column is the real treatment-start date, rerun this with
--      that column before trusting it.
-------------------------------------------------------------------------------
SELECT
    EXTRACT(YEAR FROM response_start)*100 + EXTRACT(MONTH FROM response_start) AS cohort_month
  , COUNT(*)                 AS n_rows
  , COUNT(DISTINCT clnt_no)  AS n_clients
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2024-01-01'
GROUP BY 1
ORDER BY 1;
-- READ: monthly cadence and volume -- sanity check against known VBU deployment
--       frequency. If this statement errors, response_start isn't DATE-typed;
--       swap in whichever field P1 confirmed and rerun.


-------------------------------------------------------------------------------
-- P3: RESPONDER FLAG CROSS-TAB. Decides the relationship between responder /
--     responder_anyproduct / responder_targetproduct -- Daniel Chin's "any
--     change" vs "primary upgrade" definitions, open question #5 in the schema
--     doc -- and the NULL pattern on each. No cohort_month: crossing 3 flags x
--     ~20 months would blow the row cap, and this is a structural/definitional
--     read (does responder = a derived union of the other two?), not a trend
--     -- if the pattern turns out to shift by cohort, that's a follow-up probe.
-------------------------------------------------------------------------------
SELECT
    TRIM(COALESCE(CAST(responder AS VARCHAR(10)),'<NULL>'))              AS responder_raw
  , TRIM(COALESCE(CAST(responder_anyproduct AS VARCHAR(10)),'<NULL>'))   AS responder_anyproduct_raw
  , TRIM(COALESCE(CAST(responder_targetproduct AS VARCHAR(10)),'<NULL>')) AS responder_targetproduct_raw
  , COUNT(*)                AS n_rows
  , COUNT(DISTINCT clnt_no) AS n_clients
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2024-01-01'
GROUP BY 1,2,3
ORDER BY 4 DESC;
-- READ: if responder is always the max/union of the other two, use `responder`
--       directly as the single vintage numerator. If not, the two sub-flags
--       are genuinely different populations and the vintage needs its own
--       success_def column (anyproduct vs targetproduct), as planned.
-- CAUTION: type of responder/_anyproduct/_targetproduct is unconfirmed (CHAR
--          vs numeric). This CAST(...AS VARCHAR(10)) is defensive -- if it
--          errors, the column is neither castable that way; report the error
--          text instead of guessing a different cast.


-------------------------------------------------------------------------------
-- P4: CONVERSION LAG. For responders, distribution of
--     dt_prod_change_client - response_start (tentative anchor, see P1/P2b
--     caveat), bucketed. Decides the vintage horizon, same purpose as VBA's
--     probe P4. No cohort_month: crossing 8 buckets x ~20 months would blow
--     the cap, and this decides a fixed horizon constant, not a trend.
-------------------------------------------------------------------------------
SELECT
    CASE
        WHEN dt_prod_change_client IS NULL                  THEN '8_no_change_dt'
        WHEN dt_prod_change_client - response_start < 0      THEN '0_negative'
        WHEN dt_prod_change_client - response_start <= 30    THEN '1_0_30'
        WHEN dt_prod_change_client - response_start <= 60    THEN '2_31_60'
        WHEN dt_prod_change_client - response_start <= 90    THEN '3_61_90'
        WHEN dt_prod_change_client - response_start <= 120   THEN '4_91_120'
        WHEN dt_prod_change_client - response_start <= 180   THEN '5_121_180'
        ELSE                                                     '6_beyond_180'
    END                                                       AS lag_bucket
  , COUNT(*)                                                  AS n_rows
  , MIN(dt_prod_change_client - response_start)               AS min_lag_days
  , MAX(dt_prod_change_client - response_start)               AS max_lag_days
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2024-01-01'
  AND responder = 1
GROUP BY 1
ORDER BY 1;
-- READ: mirrors VBA P4 -- sets HORIZON_DAYS for the vintage curve. Negative-lag
--       rows get clamped to day 0 in the build, never dropped (same rule as
--       VBA, references/vintage_datalab_method.md).
-- CAUTION: `AND responder = 1` assumes responder is numeric 0/1. If P3 shows
--          it's CHAR, change to `responder = '1'` before running this.


-------------------------------------------------------------------------------
-- P5: WINDOW LENGTH. VBU has no treatmt_end_dt field at all -- response_end is
--     the closest-named candidate per the schema doc's naming-conflicts table,
--     unconfirmed. Decides whether VBU has a fixed deployment window like VBA
--     did, or whether response_end doesn't behave like a window end.
-------------------------------------------------------------------------------
SELECT
    CASE
        WHEN response_end IS NULL                       THEN '9_null_end'
        WHEN response_end - response_start < 0           THEN '0_negative'
        WHEN response_end - response_start <= 30          THEN '1_0_30'
        WHEN response_end - response_start <= 60          THEN '2_31_60'
        WHEN response_end - response_start <= 90          THEN '3_61_90'
        ELSE                                                   '4_beyond_90'
    END                                                   AS window_bucket
  , COUNT(*)                                              AS n_rows
  , COUNT(DISTINCT clnt_no)                               AS n_clients
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2024-01-01'
GROUP BY 1
ORDER BY 1;
-- READ: one bucket dominating = fixed window, same pattern as VBA's -- usable
--       as max_day in the build. A large NULL count means response_end isn't a
--       reliable window bound and the build should fall back to a fixed
--       horizon constant (like VBA's 60-day standard) instead.


-------------------------------------------------------------------------------
-- P6: TARGET PRODUCT PAIRS. Top from_product -> target_product/target_product_name
--     combos by row count. Decides whether the VBU vintage needs a product
--     dimension or can pool across products for a first cut. TOP N (not
--     SAMPLE) is correct here -- this is a ranked aggregate, not a raw-row
--     eyeball. No cohort_month -- would fragment an already-ranked top-20 into
--     noise; this is a concentration read, not a trend.
-------------------------------------------------------------------------------
SELECT TOP 20
    TRIM(COALESCE(from_product,'<NULL>'))          AS from_product
  , TRIM(COALESCE(target_product,'<NULL>'))        AS target_product
  , TRIM(COALESCE(target_product_name,'<NULL>'))   AS target_product_name
  , COUNT(*)                                        AS n_rows
  , COUNT(DISTINCT clnt_no)                          AS n_clients
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2024-01-01'
GROUP BY 1,2,3
ORDER BY 4 DESC;
-- READ: if 1-2 pairs dominate, pool across products for the first vintage cut.
--       If it's spread thin, target_product_grouping (not pulled here) may be
--       the right pooling level -- flag that as a follow-up, don't guess it.


-------------------------------------------------------------------------------
-- P7: RAW EYEBALL. Responders only, most recent first, random sample --
--     teradata_sample_probes rule: SAMPLE, not TOP, for a representative look
--     across the filtered set rather than just the load edge. Each row carries
--     its own response_start, which stands in for cohort_month here.
-------------------------------------------------------------------------------
SELECT
    clnt_no
  , tactic_id
  , TRIM(test_group)    AS test_group
  , TRIM(control)       AS control
  , TRIM(report_group)  AS report_group
  , response_start
  , response_end
  , responder
  , responder_anyproduct
  , responder_targetproduct
  , from_product
  , target_product
  , target_product_name
  , dt_prod_change_client
  , decile
  , model_score
FROM DL_MR_PROD.cards_bizups_vbu_descresp_clnt
WHERE response_start >= DATE '2024-01-01'
  AND responder = 1
ORDER BY response_start DESC
SAMPLE 10;
-- READ: does the row read like a coherent record (test_group/control/report_group
--       agree with each other, dt_prod_change_client falls after response_start,
--       target_product_name matches target_product)? Any row that doesn't is a
--       red flag for the grain/field assumptions above.
