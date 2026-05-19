-- ============================================================================
-- PCQ Q1 26 -- Monthly balance, spend, and past-due trajectory by cohort
-- ============================================================================
-- PURPOSE
--   Produces a (cohort dims x strategy x me_dt) aggregation of:
--     - active_accts            : approved accounts with portfolio activity in month
--     - sum_purchases_in_month  : total $ purchases (flow)
--     - sum_bal_dollar_days     : sum of daily balance snapshots (ADB numerator,
--                                 unit: dollar-days -- NOT dollars)
--     - total_event_days        : count of observed event days (ADB denominator)
--     - sum_last_bal_current    : total month-end balance snapshot across accounts
--     - sum_bal_pd_*            : 13 mutually-exclusive past-due-band buckets +
--                                 a catch-all "unknown" bucket; together they
--                                 sum to sum_last_bal_current
--
--   Output drives slide 49 of the PCQ Power Pack: per-account spend and
--   past-due balance share across A2C and Rest of PCQ, Nov 2025 + Feb 2026 waves.
--
-- DATA SOURCES
--   - cards_tpa_pcq_decision_resp  (Teradata, DL_MR_PROD) -- targeting + response
--   - D3CV12A.DLY_FULL_PORTFOLIO   (Teradata) -- per-account daily portfolio
--     Note: contains ALL calendar days (Sat/Sun reflect Friday values).
--     Index = SRVC_ID + ACCT_NO + VISA_PROD_CD + DT_RECORD_EXT.
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
-- [1] TWO BALANCE VIEWS, TWO DIFFERENT UNITS
--
--   - last_bal_current = bal_current on the latest event date in the month
--     (i.e., the row with the largest dt_record_ext within a given me_dt).
--     One value per (account, me_dt). Used for past-due bucket calcs and
--     month-end snapshot totals. Unit: dollars.
--
--   - sum_bal_current_in_month = SUM(bal_current) across all observed days
--     in the month. In dollar-days. Paired with event_days as ADB denominator.
--     Exposed at the output level as sum_bal_dollar_days to make the unit
--     explicit (NOT a daily balance figure).
--
--   These answer different questions:
--     - last_bal_current  -> what is owed at month-end (stock)
--     - ADB               -> typical balance carried over the month (time-weighted avg)
--
--   For past-due ratios, snapshot is the right basis: cd_curr_pst_due is
--   itself a point-in-time field, so matching snapshot-to-snapshot keeps
--   units consistent.
--
--   Note on me_dt: per RBC convention, me_dt is the month-end calendar tag
--   shared by every event row in that calendar month. So
--   PARTITION BY (acct_no, me_dt) ORDER BY dt_record_ext DESC LIMIT 1
--   genuinely returns the latest observed event in that calendar month.
--   Sanity check before reading any output: SELECT DISTINCT me_dt -- should
--   be one value per calendar month, always a month-end-style date.
--
-- [2] PAST-DUE SEMANTICS: FLAG CLASSIFIES THE ACCOUNT, NOT A DOLLAR PORTION
--
--   DLY_FULL_PORTFOLIO has cd_curr_pst_due (a code) but NO separate
--   "past-due dollar amount" field. Each account is in exactly one band at
--   month-end (the aging of the oldest unpaid amount, not the balance amount).
--
--   Code mapping:
--     NULL = no past-due
--     01   = 1-30 days       06   = 151-180 days     1A   = 271-300 days
--     02   = 31-60 days      07   = 181-210 days     1B   = 301-330 days
--     03   = 61-90 days      08   = 211-240 days     1C   = 331+ days
--     04   = 91-120 days     09   = 241-270 days
--     05   = 121-150 days
--
--   The buckets sum the FULL last_bal_current of each account into the
--   bucket matching that account's code. Plus a sum_bal_pd_unknown catch-all
--   for any code value not in the 13 known categories (defensive: should be 0
--   in normal data; non-zero indicates a new code emerged at source).
--   By construction:
--       sum_bal_pd_current + sum_bal_pd_d1_30 + ... + sum_bal_pd_d331_plus
--       + sum_bal_pd_unknown  =  sum_last_bal_current
--
--   Resulting metric: "% of outstanding balance carried on accounts flagged
--   at aging band X". NOT "% of balance that is itself overdue".
--
-- [3] active_accts AS DENOMINATOR FOR PER-ACCOUNT METRICS
--
--   active_accts = COUNT(DISTINCT acct_no) in the (cohort x me_dt) bucket
--                = accounts with at least one event row in DLY_FULL_PORTFOLIO
--                  in that month, post-treatment.
--
--   Defensive dedup: pcq_q1_mb_approved deduplicates on acct_no so the final
--   INNER JOIN cannot fan out rows. If a customer was deployed in multiple
--   waves (e.g., Nov AND Feb), the earliest approval is kept.
--
-- [4] event_days AS ADB DENOMINATOR
--
--   True ADB = sum_bal_dollar_days / total_event_days at the cohort level.
--   DLY_FULL_PORTFOLIO is daily so event_days approximates calendar days
--   the account was observable that month.
--
-- ============================================================================
-- PAIN POINTS RESOLVED DURING BUILD
-- ============================================================================
--
-- [A] accum_dly_bal_mtd is a dollar-days accumulator, not a balance.
--     Original version used sum_mtd_avg_bal derived from accum_dly_bal_mtd.
--     That field resets to $0 on the 1st of each month and accumulates the
--     day's balance daily. At constant $100 balance, day 28 reads $2,800,
--     not $100. Aggregating it as a balance gave numbers ~30x too large.
--     Replaced with sum_bal_dollar_days and event_days for true ADB.
--     accum_dly_bal_mtd dropped entirely.
--
-- [B] Past-due ratio unit mismatch.
--     Initial Excel calc used sum_bal_pd_d1_30 / sum_bal_current_daily.
--     Numerator was dollars (snapshot); denominator was dollar-days.
--     Ratio was ~30x too small. Fixed by adding sum_last_bal_current
--     (the snapshot total) as the natural denominator. The PD buckets
--     sum to sum_last_bal_current by construction, so units match.
--     The dollar-days column has now been renamed sum_bal_dollar_days
--     to prevent the original misread from happening again.
--
-- [C] Data Dictionary scare about bal_current being derived.
--     RBC's internal Data Dictionary (last reviewed July 2015) shows a
--     derivation rule for bal_current that uses ACCUM_DLY_BAL_MTD as a base
--     in some branches. This raised concern that bal_current itself was an
--     accumulator. Resolution: the dictionary entry is 11 years old; current
--     usage at RBC treats bal_current as a point-in-time balance, consistent
--     with the field's business description. Behavior in practice matches
--     snapshot semantics.
--
-- [D] dt_record_ext usage.
--     The Data Dictionary warns "use Date Record Extracted to avoid pulling
--     multiple records for the same accounts". The table holds one row per
--     account per calendar day. Without a date filter you pull every day in
--     retention. This file uses dt_record_ext two correct ways:
--       (i) as a filter: dt_record_ext >= treatmt_start_dt
--       (ii) as a ranking key in ROW_NUMBER to pick the latest event per
--            (account, me_dt) for snapshot purposes.
--
-- [E] cd_curr_pst_due "current" indicator may not be NULL.
--     The "current" bucket uses last_pst_due_cd IS NULL. If the source uses
--     an empty string ('') or another non-NULL value for "not past-due",
--     this check misses those accounts. They now land in sum_bal_pd_unknown
--     (catch-all bucket) rather than being silently dropped from the
--     totals -- so the 13+1 identity always holds.
--
--     Verification query (run once to confirm):
--         SELECT cd_curr_pst_due, COUNT(*) AS rows
--         FROM pcq_q1_mb_events
--         GROUP BY cd_curr_pst_due
--         ORDER BY rows DESC;
--
--     If sum_bal_pd_unknown > 0 in output, update the IS NULL check below
--     to OR in the additional "current" indicator value.
--
-- ============================================================================
-- EXCEL PIVOT RECIPES
-- ============================================================================
--   Avg purchases per active account, per month:
--     Values = SUM(sum_purchases_in_month) / SUM(active_accts)
--
--   True average daily balance (ADB), per month:
--     Values = SUM(sum_bal_dollar_days) / SUM(total_event_days)
--
--   Past-due rate, per month, per aging band:
--     Values = SUM(sum_bal_pd_d1_30) / SUM(sum_last_bal_current)
--     (and likewise for the other PD bands -- all share the same denominator)
--
--   Sanity check that buckets reconcile:
--     SUM(sum_bal_pd_current) + SUM(sum_bal_pd_d1_30) + ... +
--     SUM(sum_bal_pd_d331_plus) + SUM(sum_bal_pd_unknown)
--     should equal SUM(sum_last_bal_current). Any drift indicates a new
--     pd_cd value emerged at source.
-- ============================================================================


DATABASE DL_MR_PROD;


-- Drop volatiles in reverse creation order (first-run errors harmless)
DROP TABLE pcq_q1_mb_acct_month;
DROP TABLE pcq_q1_mb_events;
DROP TABLE pcq_q1_mb_approved;


-- ============================================================================
-- CTE 1: pcq_q1_mb_approved
-- ----------------------------------------------------------------------------
-- Approved PCQ TPA Period-ASC accounts since Nov 2025, deduplicated on
-- acct_no so the downstream INNER JOIN can't fan out rows.
--
-- Why this filter shape:
--   - treatmt_start_dt >= '2025-11-01' : current measurement window
--   - tpa_ita = 'TPA'                  : excludes IPC/IRI from same table
--   - asc_on_app_source = 'Period-ASC' : tightens scope so the joined event
--                                        volatile fits in spool
--   - app_approved = 1                 : portfolio data only exists for
--                                        approved accounts
--   - acct_no IS NOT NULL              : safety against unjoinable rows
--
-- Why dedup: a customer can be deployed in more than one wave. Without
-- QUALIFY, the same acct_no appears multiple times here -> at the final
-- INNER JOIN it would multiply (acct, me_dt) aggregates. We keep the
-- earliest treatmt_start_dt so the customer is attributed to the first
-- wave they entered.
--
-- This is the cohort-equivalent CTE: there is no separate "all targeted"
-- volatile because nothing downstream needs non-approved clients. The
-- companion ASC file does maintain a separate cohort CTE because it
-- computes deployed/responded counts.
-- ============================================================================
CREATE MULTISET VOLATILE TABLE pcq_q1_mb_approved AS (
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

  -- Denominator for per-account spend metrics
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
  -- Catch-all for any unexpected non-NULL code value. Should be 0 in normal
  -- data. Non-zero signals a new code emerged at source and the bucket
  -- definitions need to be updated.
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
