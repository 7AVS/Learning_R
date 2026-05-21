-- ============================================================================
-- PCQ Q1 26 -- Monthly balance, spend, and past-due trajectory by cohort
-- ============================================================================
-- PURPOSE
--   Produce a (cohort dims x me_dt) aggregation answering:
--     - how does per-account spend evolve month-over-month?
--     - how does average daily balance evolve?
--     - how does the share of balance carried on past-due-flagged accounts
--       evolve, by aging band?
--
--   Output columns:
--     - cohort_accts            : total approved accts in the cohort
--                                 (constant across me_dt for a given cohort)
--     - active_accts            : approved accts with portfolio activity in month
--                                 (~ cohort_accts in practice; diverges only if
--                                  accts leave the portfolio)
--     - sum_purchases_in_month  : total $ purchases (flow)
--     - sum_bal_dollar_days     : sum of daily bal_current snapshots
--                                 (ADB numerator, unit: dollar-days)
--     - total_event_days        : count of observed event days (ADB denominator)
--     - sum_last_bal_current    : total month-end balance across accts
--     - sum_bal_pd_*            : 13 mutually-exclusive past-due bands +
--                                 a catch-all "unknown" bucket. Sum to
--                                 sum_last_bal_current by construction.
--
-- DATA SOURCES
--   - DL_MR_PROD.cards_tpa_pcq_decision_resp -- targeting + response
--   - D3CV12A.DLY_FULL_PORTFOLIO             -- per-account daily portfolio
--     (contains all calendar days; Sat/Sun reflect Friday values.
--      Index = SRVC_ID + ACCT_NO + VISA_PROD_CD + DT_RECORD_EXT.)
--
-- COMPANION FILE
--   pcq_q1_26_diagnostics.sql -- standalone queries verifying table-level
--   assumptions used here. Latest results in the DIAGNOSTIC VERIFICATION
--   LOG section below.
--
-- OUTPUT GRAIN
--   One row per (treatmt_start_dt, treatmt_end_dt, strtgy_seg_typ,
--                offer_prod_latest, offer_prod_latest_name, product_applied_name,
--                model_score_decile, response_channel_grp, me_dt,
--                months_since_treatment).
--
-- ============================================================================
-- KEY METHODOLOGY DECISIONS
-- ============================================================================
--
-- [1] TWO BALANCE VIEWS, TWO UNITS
--
--   - last_bal_current = bal_current on the latest event date in the month
--     (largest dt_record_ext per (acct, me_dt)). Unit: dollars. Used for
--     past-due bucket assignment and month-end snapshot totals.
--   - sum_bal_dollar_days = SUM(bal_current) across all observed days in
--     the month. Unit: dollar-days. Paired with event_days as ADB numerator.
--
--   They answer different questions:
--     last_bal_current  -> what is owed at month-end (stock)
--     ADB               -> typical balance carried over the month
--
--   For past-due ratios, snapshot is the right basis: cd_curr_pst_due is a
--   point-in-time field, so snapshot-to-snapshot keeps units consistent.
--   me_dt is the month-end calendar tag shared by every event row in that
--   calendar month.
--
-- [2] PAST-DUE FLAG CLASSIFIES THE ACCOUNT, NOT A DOLLAR PORTION
--
--   DLY_FULL_PORTFOLIO has cd_curr_pst_due (a code) but NO separate
--   past-due dollar field. Each acct is in exactly one band at month-end
--   (aging of the oldest unpaid amount, not the balance amount).
--
--   Code mapping:
--     NULL = no past-due
--     01 = 1-30 days       06 = 151-180 days     1A = 271-300 days
--     02 = 31-60 days      07 = 181-210 days     1B = 301-330 days
--     03 = 61-90 days      08 = 211-240 days     1C = 331+ days
--     04 = 91-120 days     09 = 241-270 days
--     05 = 121-150 days
--
--   Each acct contributes its full last_bal_current to the bucket matching
--   its code. sum_bal_pd_unknown catches any non-NULL code outside the 13
--   known categories (defensive). By construction:
--     SUM(sum_bal_pd_current ... sum_bal_pd_d331_plus + sum_bal_pd_unknown)
--     = SUM(sum_last_bal_current)
--
--   Resulting metric: "% of outstanding balance carried on accts flagged
--   at aging band X", NOT "% of balance that is itself overdue".
--
-- [3] active_accts AS PER-ACCOUNT DENOMINATOR
--
--   COUNT(DISTINCT acct_no) per (cohort x me_dt) = accts with >=1 event row
--   in DLY_FULL_PORTFOLIO that month, post-treatment.
--
--   Defensive dedup: pcq_q1_mb_approved deduplicates on acct_no so the
--   final INNER JOIN can't fan out. Earliest treatmt_start_dt wins.
--   Verified 2026-05-21 (D5) -- zero acct_no multi-wave in this population,
--   so the dedup is empirically a no-op. Kept as insurance against drift.
--
-- [4] event_days AS ADB DENOMINATOR
--
--   True ADB = SUM(sum_bal_dollar_days) / SUM(total_event_days) at cohort.
--   DLY_FULL_PORTFOLIO is daily so event_days approximates calendar days
--   the acct was observable that month.
--
-- ============================================================================
-- PAIN POINTS RESOLVED DURING BUILD
-- ============================================================================
--
-- [A] accum_dly_bal_mtd is a dollar-days accumulator, not a balance.
--     Resets to $0 on the 1st and accumulates daily. At constant $100
--     balance, day 28 reads $2,800. Aggregating it as a balance gave
--     numbers ~30x too large. Replaced with sum_bal_dollar_days +
--     event_days for true ADB.
--
-- [B] Past-due ratio unit mismatch.
--     Initial calc was sum_bal_pd_d1_30 / sum_bal_current_daily
--     (dollars over dollar-days, ~30x too small). Fixed by using
--     sum_last_bal_current (snapshot total) as the natural denominator;
--     PD buckets sum to it by construction.
--
-- [C] Data Dictionary entry for bal_current is 11 years old and shows a
--     derivation referencing ACCUM_DLY_BAL_MTD in some branches. Current
--     RBC usage treats bal_current as a point-in-time balance; behavior
--     in practice matches snapshot semantics.
--
-- [D] dt_record_ext usage.
--     One row per acct per calendar day in DLY_FULL_PORTFOLIO. Used here
--     as (i) a filter (dt_record_ext >= treatmt_start_dt) and (ii) a
--     ranking key in ROW_NUMBER to pick the latest event per (acct, me_dt).
--
-- [E] "Current" indicator may not be NULL.
--     The current bucket uses last_pst_due_cd IS NULL. If source ever uses
--     '' or another non-NULL "not past-due" value, those accts land in
--     sum_bal_pd_unknown rather than being silently dropped, so the 13+1
--     identity always holds. Verified 2026-05-19 (D3/D4): NULL is the only
--     not-past-due indicator; 13 distinct codes total, matching the
--     expected set.
--
-- ============================================================================
-- DIAGNOSTIC VERIFICATION LOG
-- ============================================================================
-- Diagnostics live in pcq_q1_26_diagnostics.sql. Re-run on any change to
-- the production SQL, source table semantics, or past-due code definitions.
--
-- 2026-05-19 -- Pre-submission run
--   D1 (me_dt convention)            PASS
--     0 rows where dt_record_ext month != me_dt month.
--   D2 (me_dt domain + daily check)  PASS
--     7 month-end values Nov 2025 - May 2026. event_rows / distinct_accts
--     ratios match days-in-month for complete months
--     (~12M distinct accts per month).
--   D3/D4 (cd_curr_pst_due coverage) PASS
--     13 distinct values: NULL + 01-09, 1A, 1B, 1C. No unexpected codes.
--
-- 2026-05-21 -- Dedup-impact check
--   D5 (multi-wave dedup impact)     PASS
--     - 5a acct_no grain: 669,405 accts, 100% wave_count = 1. The QUALIFY
--       ROW_NUMBER() in CTE 1 drops zero rows on current data.
--     - 5b clnt_no grain: 2 clients at wave_count = 2 (~0.0003%). Two
--       individuals re-acquired with a different acct_no across waves;
--       each acct sits cleanly in its own cohort row.
--     - 5c multi-wave acct sample: empty, consistent with 5a.
--     Conclusion: cohorts are clean. Dedup kept as defensive insurance.
--
-- ============================================================================
-- OUTPUT INTERPRETATION
-- ============================================================================
--   Avg purchases per cohort acct per month (fixed denominator):
--     SUM(sum_purchases_in_month) / SUM(cohort_accts)
--
--   Avg purchases per active acct per month:
--     SUM(sum_purchases_in_month) / SUM(active_accts)
--
--   True average daily balance (ADB) per month:
--     SUM(sum_bal_dollar_days) / SUM(total_event_days)
--
--   Past-due rate per month per aging band:
--     SUM(sum_bal_pd_d1_30) / SUM(sum_last_bal_current)
--     (same denominator for every band)
--
--   Bucket reconciliation:
--     SUM(sum_bal_pd_current + ... + sum_bal_pd_d331_plus + sum_bal_pd_unknown)
--     should equal SUM(sum_last_bal_current). Drift = new pd_cd at source.
-- ============================================================================


DATABASE DL_MR_PROD;


-- Drop volatiles in reverse creation order (first-run errors harmless)
DROP TABLE pcq_q1_mb_acct_month;
DROP TABLE pcq_q1_mb_events;
DROP TABLE pcq_q1_mb_approved;


-- ============================================================================
-- CTE 1: pcq_q1_mb_approved
-- ----------------------------------------------------------------------------
-- Approved PCQ TPA Period-ASC accts since Nov 2025, deduplicated on acct_no
-- so the downstream INNER JOIN can't fan out rows.
--
-- Filter shape:
--   - treatmt_start_dt >= '2025-11-01' : measurement window
--   - tpa_ita = 'TPA'                  : excludes IPC/IRI from same table
--   - asc_on_app_source = 'Period-ASC' : tightens scope so the event
--                                        volatile fits in spool
--   - app_approved = 1                 : portfolio data exists only for
--                                        approved accts
--   - acct_no IS NOT NULL              : safety against unjoinable rows
--
-- Dedup: without QUALIFY, an acct deployed in multiple waves would appear
-- multiple times here and inflate (acct, me_dt) aggregates at the final
-- INNER JOIN. Earliest treatmt_start_dt wins -- first wave keeps the acct.
-- Verified 2026-05-21 (D5): zero multi-wave acct_no in current data, so
-- the QUALIFY is empirically a no-op. Kept as defensive insurance.
--
-- cohort_accts: COUNT(*) OVER cohort dims, computed in the outer SELECT
-- on the deduped data so the count isn't inflated by pre-dedup duplicates.
-- Stamped on every acct row; carried through to the final output as a
-- fixed cohort-size denominator.
-- ============================================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_approved AS (
  SELECT
    deduped.*,
    COUNT(*) OVER (PARTITION BY treatmt_start_dt, treatmt_end_dt, strtgy_seg_typ,
                                offer_prod_latest, offer_prod_latest_name,
                                product_applied_name, model_score_decile,
                                response_channel_grp) AS cohort_accts
  FROM (
    SELECT
      acct_no,
      clnt_no,
      treatmt_start_dt,
      treatmt_end_dt,
      strtgy_seg_typ,
      model_score_decile,
      offer_prod_latest,
      offer_prod_latest_name,
      product_applied_name,
      response_channel_grp
    FROM cards_tpa_pcq_decision_resp
    WHERE treatmt_start_dt >= DATE '2025-11-01'
      AND tpa_ita = 'TPA'
      AND asc_on_app_source = 'Period-ASC'
      AND app_approved = 1
      AND acct_no IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY acct_no
                               ORDER BY treatmt_start_dt ASC) = 1
  ) deduped
) WITH DATA
PRIMARY INDEX (acct_no)
ON COMMIT PRESERVE ROWS;


-- ============================================================================
-- CTE 2: pcq_q1_mb_events
-- ----------------------------------------------------------------------------
-- Stages portfolio events for approved accounts after their treatment.
-- This is the ONLY scan of DLY_FULL_PORTFOLIO in the query -- Period-ASC
-- filtering at the approved CTE already cut the joined account pool, so
-- the staged volatile fits in spool.
--
-- Why dt_record_ext >= treatmt_start_dt: we only want post-treatment
-- behavior. Pre-treatment days would dilute the comparison and add rows.
--
-- Fields pulled (and why):
--   - acct_no, dt_record_ext, me_dt : grain + grouping keys
--   - net_prch_amt_dly : daily purchases (flow), summed over the month
--   - bal_current     : balance snapshot at this event date (used both as
--                       a daily contribution to ADB and as the source of
--                       the month-end snapshot via ROW_NUMBER in CTE 3)
--   - cd_curr_pst_due : past-due aging code (used at the month-end snapshot
--                       date to assign the account to one of the buckets)
-- ============================================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_events AS (
  SELECT
    p.acct_no,
    p.dt_record_ext,
    p.me_dt,
    p.net_prch_amt_dly,
    p.bal_current,
    p.cd_curr_pst_due
  FROM D3CV12A.DLY_FULL_PORTFOLIO p
  INNER JOIN pcq_q1_mb_approved a
    ON p.acct_no = a.acct_no
  WHERE p.dt_record_ext >= a.treatmt_start_dt
) WITH DATA
PRIMARY INDEX (acct_no, dt_record_ext)
ON COMMIT PRESERVE ROWS;


-- ============================================================================
-- CTE 3: pcq_q1_mb_acct_month
-- ----------------------------------------------------------------------------
-- Collapses per-day events to one row per (account, calendar month).
-- This is where the snapshot-vs-flow distinction is encoded.
--
-- The inner subquery ranks rows within each (acct_no, me_dt) by event date
-- descending. rn = 1 marks the LATEST observed event in the month.
--
-- The outer aggregation computes per (acct, me_dt):
--   - sum_purchases_in_month    : SUM(net_prch_amt_dly) = total spend (flow)
--   - sum_bal_current_in_month  : SUM(bal_current) across days = ADB numerator
--                                 (unit: dollar-days, exposed at output as
--                                 sum_bal_dollar_days)
--   - event_days_in_month       : COUNT(*) = observed days that month
--   - last_bal_current          : bal_current on the latest event date
--                                 (the month-end balance snapshot)
--   - last_pst_due_cd           : cd_curr_pst_due on that same latest date
--                                 (the month-end aging code)
--   - last_event_dt_in_month    : MAX(dt_record_ext) for reference
--
-- Why this CTE exists: we need both kinds of aggregation (sum across days
-- AND latest snapshot) for the same (acct, month). Doing them inline in
-- the final SELECT would force a second scan of pcq_q1_mb_events.
-- ============================================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_acct_month AS (
  SELECT
    acct_no,
    me_dt,
    SUM(net_prch_amt_dly)                                       AS sum_purchases_in_month,
    SUM(bal_current)                                            AS sum_bal_current_in_month,
    COUNT(*)                                                    AS event_days_in_month,
    MAX(CASE WHEN rn = 1 THEN bal_current END)                  AS last_bal_current,
    MAX(CASE WHEN rn = 1 THEN cd_curr_pst_due END)              AS last_pst_due_cd,
    MAX(dt_record_ext)                                          AS last_event_dt_in_month
  FROM (
    SELECT
      e.acct_no,
      e.me_dt,
      e.dt_record_ext,
      e.net_prch_amt_dly,
      e.bal_current,
      e.cd_curr_pst_due,
      ROW_NUMBER() OVER (PARTITION BY e.acct_no, e.me_dt
                         ORDER BY e.dt_record_ext DESC)         AS rn
    FROM pcq_q1_mb_events e
  ) ranked
  GROUP BY acct_no, me_dt
) WITH DATA
PRIMARY INDEX (acct_no, me_dt)
ON COMMIT PRESERVE ROWS;


-- ============================================================================
-- FINAL OUTPUT
-- ----------------------------------------------------------------------------
-- Joins per-account-month aggregates back to cohort dimensions and rolls
-- up to (cohort dims x me_dt). One row per pivot cell.
--
-- The 13 past-due bucket expressions + the unknown catch-all classify each
-- account by its month-end aging code and sum that account's full
-- last_bal_current into the matching bucket. Every account lands in
-- exactly one of the 14 buckets; sum of all 14 = sum_last_bal_current.
--
-- months_since_treatment is derived from EXTRACT(YEAR/MONTH) on me_dt vs
-- treatmt_start_dt. GREATEST(...,0) guards against negative values (the
-- dt_record_ext filter already prevents them, but the guard makes the
-- invariant explicit).
-- ============================================================================
SELECT
  a.treatmt_start_dt,
  a.treatmt_end_dt,
  a.strtgy_seg_typ,
  a.offer_prod_latest,
  a.offer_prod_latest_name,
  a.product_applied_name,
  a.model_score_decile,
  a.response_channel_grp,
  am.me_dt,
  GREATEST(
    (EXTRACT(YEAR FROM am.me_dt) - EXTRACT(YEAR FROM a.treatmt_start_dt)) * 12
    + (EXTRACT(MONTH FROM am.me_dt) - EXTRACT(MONTH FROM a.treatmt_start_dt)),
    0
  ) AS months_since_treatment,

  -- Cohort denominator (fixed cohort size, constant across me_dt)
  MAX(a.cohort_accts)                   AS cohort_accts,

  -- Per-month active denominator (accts with >=1 event row in DLY_FULL_PORTFOLIO
  -- that month). In practice ~= cohort_accts because the source is daily;
  -- diverges only if an acct leaves the portfolio (closure / charge-off).
  COUNT(DISTINCT am.acct_no)            AS active_accts,

  -- ADB numerator (dollar-days). Use SUM(sum_bal_dollar_days) /
  -- SUM(total_event_days) for true ADB.
  SUM(am.sum_bal_current_in_month)      AS sum_bal_dollar_days,

  -- Past-due ratio denominator: total outstanding balance at month-end
  SUM(am.last_bal_current)              AS sum_last_bal_current,

  -- Past-due aging buckets. Each account contributes its full
  -- last_bal_current to exactly one bucket. Sum of all 14 buckets =
  -- sum_last_bal_current by construction.
  -- Past-due ratio (Excel): SUM(sum_bal_pd_<band>) / SUM(sum_last_bal_current).
  SUM(CASE WHEN am.last_pst_due_cd IS NULL THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_current,
  SUM(CASE WHEN am.last_pst_due_cd = '01'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d1_30,
  SUM(CASE WHEN am.last_pst_due_cd = '02'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d31_60,
  SUM(CASE WHEN am.last_pst_due_cd = '03'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d61_90,
  SUM(CASE WHEN am.last_pst_due_cd = '04'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d91_120,
  SUM(CASE WHEN am.last_pst_due_cd = '05'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d121_150,
  SUM(CASE WHEN am.last_pst_due_cd = '06'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d151_180,
  SUM(CASE WHEN am.last_pst_due_cd = '07'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d181_210,
  SUM(CASE WHEN am.last_pst_due_cd = '08'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d211_240,
  SUM(CASE WHEN am.last_pst_due_cd = '09'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d241_270,
  SUM(CASE WHEN am.last_pst_due_cd = '1A'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d271_300,
  SUM(CASE WHEN am.last_pst_due_cd = '1B'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d301_330,
  SUM(CASE WHEN am.last_pst_due_cd = '1C'  THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_d331_plus,
  -- Catch-all for any unexpected non-NULL code value. Verified empirically
  -- 2026-05-19 to be 0 (no codes outside the 13 known across 75M+ rows).
  -- Stays in place as defensive insurance against future source-system
  -- changes. If non-zero in a future run, re-run pcq_q1_26_diagnostics.sql D3.
  SUM(CASE WHEN am.last_pst_due_cd IS NOT NULL
                AND am.last_pst_due_cd NOT IN ('01','02','03','04','05',
                                               '06','07','08','09',
                                               '1A','1B','1C')
           THEN am.last_bal_current ELSE 0 END) AS sum_bal_pd_unknown,

  -- Total purchases (flow) for the per-account spend numerator
  SUM(am.sum_purchases_in_month)        AS sum_purchases_in_month,

  -- ADB denominator
  SUM(am.event_days_in_month)           AS total_event_days
FROM pcq_q1_mb_acct_month am
INNER JOIN pcq_q1_mb_approved a
  ON am.acct_no = a.acct_no
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY 1, 9;
