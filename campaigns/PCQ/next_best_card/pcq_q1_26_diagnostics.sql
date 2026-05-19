-- ============================================================================
-- PCQ Q1 26 -- Diagnostics for monthly balance pipeline
-- ============================================================================
-- PURPOSE
--   Verify the table-level assumptions baked into pcq_q1_26_monthly_balance.sql
--   before trusting its output. Each diagnostic runs independently against
--   the actual source tables -- no session-state prerequisites.
--
-- SOURCE TABLES
--   - D3CV12A.DLY_FULL_PORTFOLIO  : daily account-level portfolio table
--                                   (the field-semantics tests below all
--                                   rely on this table directly)
--
-- WHEN TO RUN
--   - After any change to the production SQL
--   - Before submitting output to GRM / Risk review
--   - When a downstream consumer reports unexpected numbers in the slide
--
-- HOW TO READ
--   Each diagnostic has:
--     - Purpose
--     - Expected (the result that means "all good")
--     - On failure (what to investigate if you see otherwise)
--   Run each block independently.
-- ============================================================================


-- ============================================================================
-- DIAGNOSTIC 1: me_dt convention check
-- ----------------------------------------------------------------------------
-- Purpose
--   Confirm that me_dt matches the calendar month of dt_record_ext for every
--   row in the observation window. The snapshot semantics in CTE 3 of the
--   production SQL depend on this:
--     PARTITION BY (acct_no, me_dt) ORDER BY dt_record_ext DESC
--   only returns the latest event "in the calendar month" if me_dt is the
--   month-end tag for the same calendar month as dt_record_ext.
--
-- Expected
--   0 rows. Every event row should have dt_record_ext in the same calendar
--   year-month as its me_dt.
--
-- On failure
--   If rows return, me_dt is a mid-period or shifted sentinel for those
--   rows -- "last event per (acct, me_dt)" no longer means month-end. Inspect
--   the returned acct_no / dt_record_ext / me_dt combos and consult the
--   source-system documentation.
-- ============================================================================
SELECT
  acct_no,
  dt_record_ext,
  me_dt,
  EXTRACT(YEAR  FROM dt_record_ext) AS yr_event,
  EXTRACT(MONTH FROM dt_record_ext) AS mo_event,
  EXTRACT(YEAR  FROM me_dt)         AS yr_me,
  EXTRACT(MONTH FROM me_dt)         AS mo_me
FROM D3CV12A.DLY_FULL_PORTFOLIO
WHERE dt_record_ext >= DATE '2025-11-01'
  AND ( EXTRACT(YEAR  FROM dt_record_ext) <> EXTRACT(YEAR  FROM me_dt)
     OR EXTRACT(MONTH FROM dt_record_ext) <> EXTRACT(MONTH FROM me_dt) )
SAMPLE 20;


-- ============================================================================
-- DIAGNOSTIC 2: me_dt domain survey
-- ----------------------------------------------------------------------------
-- Purpose
--   Show every distinct me_dt value in the observation window. Lets the
--   analyst eyeball the domain and verify only month-end-style dates appear.
--
-- Expected
--   One row per calendar month from Nov 2025 onward, with month-end-style
--   dates (e.g., 2025-11-30, 2025-12-31, 2026-01-31, 2026-02-28, 2026-03-31,
--   2026-04-30, 2026-05-31).
--
-- On failure
--   - Mid-month dates (e.g., 2026-01-15): me_dt is not a true month-end tag.
--   - Multiple distinct me_dt values per calendar month: me_dt might encode
--     something other than the calendar month (e.g., billing-cycle anchor).
-- ============================================================================
SELECT
  me_dt,
  COUNT(*) AS event_rows,
  COUNT(DISTINCT acct_no) AS distinct_accounts
FROM D3CV12A.DLY_FULL_PORTFOLIO
WHERE dt_record_ext >= DATE '2025-11-01'
GROUP BY me_dt
ORDER BY me_dt;


-- ============================================================================
-- DIAGNOSTIC 3: Past-due bucket coverage check
-- ----------------------------------------------------------------------------
-- Purpose
--   Verify that every cd_curr_pst_due value observed in the window is
--   either NULL (current) or one of the 12 known past-due codes. If any
--   other value exists, accounts with that code would land in the
--   sum_bal_pd_unknown catch-all bucket in the production SQL -- the
--   bucket sum identity still holds, but you'd want to know about the
--   new code so the bucket definitions can be updated.
--
-- Expected
--   0 rows. Every non-NULL value should match one of the 13 known codes.
--
-- On failure
--   If rows return, a new past-due code has appeared at source. Decide
--   whether it represents:
--     (a) A new aging band beyond 331+ -- add a new bucket
--     (b) A new "not past due" indicator -- update the IS NULL check in
--         the production SQL to OR in this value
--     (c) Data corruption -- raise with the source-system team
-- ============================================================================
SELECT
  cd_curr_pst_due,
  COUNT(*) AS rows,
  COUNT(DISTINCT acct_no) AS distinct_accounts
FROM D3CV12A.DLY_FULL_PORTFOLIO
WHERE dt_record_ext >= DATE '2025-11-01'
  AND cd_curr_pst_due IS NOT NULL
  AND cd_curr_pst_due NOT IN ('01','02','03','04','05',
                              '06','07','08','09',
                              '1A','1B','1C')
GROUP BY cd_curr_pst_due
ORDER BY rows DESC;


-- ============================================================================
-- DIAGNOSTIC 4: cd_curr_pst_due full value survey
-- ----------------------------------------------------------------------------
-- Purpose
--   Show every distinct cd_curr_pst_due value with row counts. Verifies
--   the "NULL = current" assumption and lets the analyst see the relative
--   distribution across aging bands.
--
-- Expected
--   NULL appears with the largest row count (most accounts are current).
--   The 12 known past-due codes (01-09, 1A, 1B, 1C) appear with
--   progressively smaller counts as severity increases.
--
-- On failure
--   - An empty string '', 'N', '0', or other non-NULL value with material
--     row count: that's a "current" indicator the production SQL would
--     route into sum_bal_pd_unknown instead of sum_bal_pd_current. Update
--     the IS NULL check to OR in the value.
--   - A non-trivial sum_bal_pd_unknown in the production output also
--     points back to this diagnostic to identify which code caused it.
-- ============================================================================
SELECT
  cd_curr_pst_due,
  COUNT(*) AS rows,
  COUNT(DISTINCT acct_no) AS distinct_accounts
FROM D3CV12A.DLY_FULL_PORTFOLIO
WHERE dt_record_ext >= DATE '2025-11-01'
GROUP BY cd_curr_pst_due
ORDER BY rows DESC;
