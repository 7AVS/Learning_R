-- ============================================================================
-- PCQ Q1 26 -- Diagnostic queries for monthly balance pipeline
-- ============================================================================
-- PURPOSE
--   Verify the assumptions baked into pcq_q1_26_monthly_balance.sql before
--   trusting its output. Run on demand; not part of the production pipeline.
--
-- WHEN TO RUN
--   - After any change to the production SQL
--   - Before submitting output to GRM / Risk review
--   - When a downstream consumer reports unexpected numbers in the slide
--
-- PREREQUISITE
--   Run pcq_q1_26_monthly_balance.sql FIRST in the same session. These
--   queries reference the volatile tables (pcq_q1_mb_events,
--   pcq_q1_mb_acct_month, pcq_q1_mb_approved) created by that file.
--
-- HOW TO READ
--   Each diagnostic has:
--     - Purpose: what it checks
--     - Expected: the result that means "all good"
--     - On failure: what to investigate if you see otherwise
--   Run each block independently; they don't depend on each other.
-- ============================================================================


DATABASE DL_MR_PROD;


-- ============================================================================
-- DIAGNOSTIC 1: me_dt convention check
-- ----------------------------------------------------------------------------
-- Purpose
--   Confirm that me_dt always matches the calendar month of dt_record_ext.
--   The snapshot semantics in CTE 3 of the production SQL depend on this:
--   PARTITION BY (acct_no, me_dt) ORDER BY dt_record_ext DESC only returns
--   the latest event "in the month" if me_dt is the month-end tag for the
--   same calendar month as dt_record_ext.
--
-- Expected
--   0 rows. Every event row should have dt_record_ext in the same calendar
--   year-month as its me_dt.
--
-- On failure
--   If rows return, me_dt is a mid-period or shifted sentinel for those
--   rows -- the "last event per (acct, me_dt)" interpretation breaks for
--   them. Inspect the returned acct_no / dt_record_ext / me_dt combos and
--   talk to the source-system owner about how me_dt is assigned.
-- ============================================================================
SELECT
  acct_no,
  dt_record_ext,
  me_dt,
  EXTRACT(YEAR  FROM dt_record_ext) AS yr_event,
  EXTRACT(MONTH FROM dt_record_ext) AS mo_event,
  EXTRACT(YEAR  FROM me_dt)         AS yr_me,
  EXTRACT(MONTH FROM me_dt)         AS mo_me
FROM pcq_q1_mb_events
WHERE EXTRACT(YEAR  FROM dt_record_ext) <> EXTRACT(YEAR  FROM me_dt)
   OR EXTRACT(MONTH FROM dt_record_ext) <> EXTRACT(MONTH FROM me_dt)
SAMPLE 20;


-- ============================================================================
-- DIAGNOSTIC 2: me_dt domain survey
-- ----------------------------------------------------------------------------
-- Purpose
--   Show every distinct me_dt value in the staged events. Lets the analyst
--   eyeball the domain and verify only month-end-style dates appear.
--
-- Expected
--   One row per calendar month in the observation window. Dates are month-
--   end style (e.g., 2025-11-30, 2025-12-31, 2026-01-31, 2026-02-28,
--   2026-03-31, 2026-04-30, 2026-05-31).
--
-- On failure
--   - Mid-month dates (e.g., 2026-01-15) appearing: me_dt is not a true
--     month-end tag.
--   - Multiple distinct me_dt values for the same calendar month: me_dt
--     might be encoding something other than the month (e.g., billing
--     cycle anchored to account-specific dates).
-- ============================================================================
SELECT
  me_dt,
  COUNT(*) AS event_rows,
  COUNT(DISTINCT acct_no) AS distinct_accounts
FROM pcq_q1_mb_events
GROUP BY me_dt
ORDER BY me_dt;


-- ============================================================================
-- DIAGNOSTIC 3: Past-due bucket identity check (at acct-month grain)
-- ----------------------------------------------------------------------------
-- Purpose
--   Verify that for every (acct, me_dt), the sum of the 14 past-due bucket
--   conditions equals last_bal_current. If the bucket CASE expressions
--   collectively cover every possible value of last_pst_due_cd (which they
--   do via the 13 known codes + IS NULL "current" + the unknown catch-all),
--   every account's last_bal_current lands in exactly one bucket, and the
--   sum identity holds.
--
-- Expected
--   0 rows where ABS(drift) > 0.01. Identity holds for every account-month.
--
-- On failure
--   - drift > 0 (sum_buckets exceeds last_bal_current): an account is
--     contributing to more than one bucket (a CASE condition overlap). Audit
--     the CASE expressions in the production SQL.
--   - drift < 0 (sum_buckets short of last_bal_current): an account's
--     last_pst_due_cd value is matched by no CASE branch. Run Diagnostic 4
--     to identify what value escaped.
-- ============================================================================
SELECT
  acct_no,
  me_dt,
  last_bal_current,
  last_pst_due_cd,
  (
    CASE WHEN last_pst_due_cd IS NULL THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '01' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '02' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '03' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '04' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '05' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '06' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '07' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '08' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '09' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '1A' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '1B' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '1C' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd IS NOT NULL
                AND last_pst_due_cd NOT IN ('01','02','03','04','05',
                                            '06','07','08','09',
                                            '1A','1B','1C')
           THEN last_bal_current ELSE 0 END
  ) AS sum_buckets,
  (
    CASE WHEN last_pst_due_cd IS NULL THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '01' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '02' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '03' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '04' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '05' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '06' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '07' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '08' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '09' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '1A' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '1B' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd = '1C' THEN last_bal_current ELSE 0 END
    + CASE WHEN last_pst_due_cd IS NOT NULL
                AND last_pst_due_cd NOT IN ('01','02','03','04','05',
                                            '06','07','08','09',
                                            '1A','1B','1C')
           THEN last_bal_current ELSE 0 END
  ) - last_bal_current AS drift
FROM pcq_q1_mb_acct_month
WHERE last_bal_current IS NOT NULL
QUALIFY ABS(drift) > 0.01
SAMPLE 20;


-- ============================================================================
-- DIAGNOSTIC 4: cd_curr_pst_due value survey
-- ----------------------------------------------------------------------------
-- Purpose
--   Show every distinct cd_curr_pst_due value with row counts. Verifies
--   the "NULL = current" assumption and surfaces any non-NULL "current"
--   indicators (e.g., '', 'N', '0') that the production SQL would route
--   into sum_bal_pd_unknown instead of sum_bal_pd_current.
--
-- Expected
--   NULL appears with the highest row count (most accounts are current).
--   The 12 known past-due codes (01-09, 1A, 1B, 1C) appear with progressively
--   smaller counts.
--
-- On failure
--   - An empty string '', 'N', '0', or other non-NULL value with material
--     row count: that's a "current" indicator the IS NULL check misses.
--     Accounts with that value will land in sum_bal_pd_unknown. Update
--     the IS NULL check in the production SQL to OR in the new value.
--   - A new code not in (01-09, 1A, 1B, 1C) with non-trivial volume: a new
--     past-due bucket emerged at source. Add a CASE branch for it.
-- ============================================================================
SELECT
  cd_curr_pst_due,
  COUNT(*) AS rows,
  COUNT(DISTINCT acct_no) AS distinct_accounts
FROM pcq_q1_mb_events
GROUP BY cd_curr_pst_due
ORDER BY rows DESC;
