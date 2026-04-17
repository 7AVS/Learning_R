-- ============================================================================
-- PCQ Next Best Card Test — Analytical File
-- ============================================================================
-- Source tables:
--   DL_MR_PROD.cards_tpa_pcq_decision_resp    (Teradata — PCQ campaign side)
--   D3CV12A.DLY_FULL_PORTFOLIO                (Teradata — daily portfolio events)
--
-- Test groups:
--   NG3_1ST  = control, 1st recommended card
--   NG3_2ND  = test,    2nd recommended card
--
-- Purpose:
--   Answer: does recommending the 2nd-best card produce materially different
--   conversion, spend, balance, fee, and loyalty outcomes vs the 1st-best?
--
-- File structure (run top to bottom, or jump by section):
--
--   SECTION A — CONVERSION SUMMARY
--     Q1  Rollup: deployed → approved by test_group × wave × product × ASC
--
--   SECTION B — MASTER ANALYTICAL (account grain) — THE EXCEL FILE
--     Q2  Per-account enriched dataset. All flags, balances, fees, classifications.
--         This is the main deliverable. Pivot in Excel for every question.
--
--   SECTION C — MONTHLY CURVES (me_dt grain)
--     Q3  Monthly balance/spend/fees/loyalty curves, sliced by classification.
--         Use for cohort curves and time-series charts.
--
--   SECTION D — VALIDATION APPENDIX (sanity checks, kept for audit)
--     V1  Client overlap across test groups
--     V2  Approved clients across both waves
--     V3  Approved clients across multiple ASC categories
--     V4  Approved clients with multiple approval rows
--     V5  Portfolio row distribution per acct × me_dt (grain check)
--     V6  Offer product vs portfolio visa_prod_cd cross-tab (all ASCs)
--     V7  Same cross-tab split by asc_on_app_source
--     V8  asc_on_app_source label validation (raw ACQ_STRATEGY_CODE vs ASC_ON_APP)
--     V9  Sample accounts per classification quadrant
--     V10 Portfolio timelines for those sample accounts
--
-- Key findings from validation (as of 2026-04-13):
--   • Period-ASC match rate ≈ 90%, not 100% as originally assumed. The ~10%
--     gap is real fulfillment drift, not a query bug — mismatches scatter
--     across many visa codes with no clean 1:1 taxonomy mapping.
--   • IAV (~85% match) and IOP (~87%) drive ~64% of all Period-ASC mismatches.
--     MCP is a separate anomaly (~3% match on NG3_1ST, small volume).
--   • Reclass (product change on same acct_no over time) affects ~3% of accounts.
--   • For clean measurement, filter Q2 to booking_status='match' + Period-ASC.
--     For richer analysis, use booking_status + lifetime_status together.
-- ============================================================================



-- ============================================================================
-- SECTION A — CONVERSION SUMMARY
-- ============================================================================

-- ==========================================================================
-- Q1: Conversion rollup.
-- One row per test_group × wave × offered product. Columns include:
--   deployed          — total clients sent the offer
--   responded         — clients with a non-null asc_on_app_source
--   approved_*        — approvals split by ASC category (three buckets)
--   rate_*            — percentages against the full deployed denominator
-- Denominator is always the deployed population (not responders only), so
-- rates comparable across groups. Pivot in Excel by any dimension.
-- ==========================================================================
SELECT
    test_group_latest,
    treatmt_start_dt,
    offer_prod_latest,
    offer_prod_latest_name,
    COUNT(*)                                                                   AS deployed,
    SUM(CASE WHEN asc_on_app_source IS NOT NULL THEN 1 ELSE 0 END)             AS responded,
    SUM(app_approved)                                                          AS approved_total,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS approved_period_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC'  THEN 1 ELSE 0 END) AS approved_other_asc,
    SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC'     THEN 1 ELSE 0 END) AS approved_no_asc,
    ROUND(100.0 * SUM(app_approved) / NULLIFZERO(COUNT(*)), 2)                 AS rate_approved_total_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) / NULLIFZERO(COUNT(*)), 2) AS rate_period_asc_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'Other ASC'  THEN 1 ELSE 0 END) / NULLIFZERO(COUNT(*)), 2) AS rate_other_asc_pct,
    ROUND(100.0 * SUM(CASE WHEN app_approved = 1 AND asc_on_app_source = 'NO ASC'     THEN 1 ELSE 0 END) / NULLIFZERO(COUNT(*)), 2) AS rate_no_asc_pct
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    test_group_latest,
    treatmt_start_dt,
    offer_prod_latest,
    offer_prod_latest_name
ORDER BY
    test_group_latest,
    treatmt_start_dt,
    offer_prod_latest;



-- ============================================================================
-- SECTION B — MASTER ANALYTICAL (account grain)
-- ============================================================================

-- ==========================================================================
-- Q2: Full per-account enriched dataset.
-- One row per approved NG3_1ST/NG3_2ND account. Includes all ASCs so the
-- reader can filter in Excel. The "true measurement" slice is typically:
--   asc_on_app_source = 'Period-ASC' AND booking_status = 'match'
--     AND lifetime_status = 'stable'
-- That gives clean TPA conversions on their offered product with no reclass.
--
-- Column groups:
--   Identity         test_group, clnt_no, acct_no
--   Offer side       offer_prod, asc_on_app_source, treatmt_start_dt, response_dt
--   Account timing   acct_open_dt, acct_cls_dt, days_offer_to_open,
--                    account_existed_pre_offer flag, first/last extract dates,
--                    months_with_activity
--   Classification   booked_visa_prod_cd (anchor = earliest post-offer row)
--                    last_visa_prod_cd, n_distinct_visa
--                    booking_status  = match / mismatch vs offer_prod_latest
--                    lifetime_status = stable / reclassed (n_distinct_visa > 1)
--   Balances         last_balance, last_avg_daily_bal_mtd,
--                    total_net_purchases, avg_monthly_purchases
--   Fees             annual_fee_last (list),  annual_fee_last_dt,
--                    ever_charged_annual_fee, total_fees_charged (revenue),
--                    months_with_fees (incidence)
--   Loyalty          last_loyalty_balance (point-in-time),
--                    max_loyalty_balance (peak accumulation)
--   Risk             ever_overlimit, last_overlimit_cd,
--                    ever_past_due, last_past_due_cd
--   Status lifecycle last_status, st_{bkpt,coll,frd,inv,open,vol,woff},
--                    first_non_open_dt, days_to_status_change
--
-- Anchor rule for booked_visa_prod_cd: earliest dt_record_ext on/after
-- treatmt_start_dt. At monthly grain this equals the acct_open_dt row.
--
-- Fee aggregation: NET_ALL_FEES_AMT_MTD resets each month, so total_fees_charged
-- sums the MAX per me_dt across all months (= the end-of-month running total).
--
-- Risk flags: cd_curr_ovrlmt / cd_curr_pst_due are treated as non-trivial
-- whenever the value is not NULL, 'N', '0', or empty. If the encoding turns
-- out to differ (e.g., days-past-due integers), adjust the CASE here.
-- ==========================================================================
SELECT
    -- === Identity ===
    r.test_group_latest,
    r.clnt_no,
    r.acct_no,

    -- === Offer side ===
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    r.asc_on_app_source,
    r.treatmt_start_dt,
    r.response_dt,

    -- === Account timing ===
    pa.acct_open_dt,
    pa.acct_cls_dt,
    CAST(pa.acct_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)          AS days_offer_to_open,
    CASE WHEN pa.acct_open_dt < r.treatmt_start_dt THEN 1 ELSE 0 END           AS account_existed_pre_offer,
    pa.first_extract_dt,
    pa.last_extract_dt,
    pa.months_with_activity,

    -- === Classification ===
    bk.booked_visa_prod_cd,
    ls.last_visa_prod_cd,
    pa.n_distinct_visa,
    CASE WHEN bk.booked_visa_prod_cd = r.offer_prod_latest
         THEN 'match' ELSE 'mismatch' END                                      AS booking_status,
    CASE WHEN pa.n_distinct_visa > 1
         THEN 'reclassed' ELSE 'stable' END                                    AS lifetime_status,

    -- === Balances ===
    ls.last_balance,
    ls.last_avg_daily_bal_mtd,
    pa.total_net_purchases,
    pa.total_net_purchases / NULLIFZERO(pa.months_with_activity)               AS avg_monthly_purchases,

    -- === Fees ===
    ls.annual_fee_last,
    ls.annual_fee_last_dt,
    CASE WHEN ls.annual_fee_last > 0 THEN 1 ELSE 0 END                         AS ever_charged_annual_fee,
    fa.total_fees_charged,
    fa.months_with_fees,

    -- === Loyalty ===
    ls.last_loyalty_balance,
    pa.max_loyalty_balance,

    -- === Risk ===
    pa.ever_overlimit,
    ls.last_overlimit_cd,
    pa.ever_past_due,
    ls.last_past_due_cd,

    -- === Status lifecycle ===
    ls.last_status,
    pa.st_bkpt, pa.st_coll, pa.st_frd, pa.st_inv, pa.st_open, pa.st_vol, pa.st_woff,
    pa.first_non_open_dt,
    CAST(pa.first_non_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)      AS days_to_status_change

FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r

-- ---- Per-account aggregates over the post-offer window ----
LEFT JOIN (
    SELECT
        r2.acct_no,
        MIN(p.dt_record_ext)            AS first_extract_dt,
        MAX(p.dt_record_ext)            AS last_extract_dt,
        MAX(p.acct_open_dt)             AS acct_open_dt,
        MAX(p.acct_cls_dt)              AS acct_cls_dt,
        COUNT(DISTINCT p.me_dt)         AS months_with_activity,
        COUNT(DISTINCT p.visa_prod_cd)  AS n_distinct_visa,
        SUM(p.net_prch_amt_dly)         AS total_net_purchases,
        MAX(p.lylty_bal_amt)            AS max_loyalty_balance,
        MAX(CASE WHEN p.status = 'BKPT' THEN 1 ELSE 0 END) AS st_bkpt,
        MAX(CASE WHEN p.status = 'COLL' THEN 1 ELSE 0 END) AS st_coll,
        MAX(CASE WHEN p.status = 'FRD'  THEN 1 ELSE 0 END) AS st_frd,
        MAX(CASE WHEN p.status = 'INV'  THEN 1 ELSE 0 END) AS st_inv,
        MAX(CASE WHEN p.status = 'OPEN' THEN 1 ELSE 0 END) AS st_open,
        MAX(CASE WHEN p.status = 'VOL'  THEN 1 ELSE 0 END) AS st_vol,
        MAX(CASE WHEN p.status = 'WOFF' THEN 1 ELSE 0 END) AS st_woff,
        MIN(CASE WHEN p.status <> 'OPEN' THEN p.dt_record_ext END) AS first_non_open_dt,
        MAX(CASE WHEN p.cd_curr_ovrlmt IS NOT NULL
                  AND p.cd_curr_ovrlmt NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END) AS ever_overlimit,
        MAX(CASE WHEN p.cd_curr_pst_due IS NOT NULL
                  AND p.cd_curr_pst_due NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END) AS ever_past_due
    FROM (
        SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
          AND app_approved = 1
        GROUP BY acct_no
    ) r2
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON p.acct_no       = r2.acct_no
        AND p.dt_record_ext >= r2.treatmt_start_dt
    GROUP BY r2.acct_no
) pa ON pa.acct_no = r.acct_no

-- ---- Latest-row snapshot: point-in-time values from the most recent row ----
LEFT JOIN (
    SELECT
        p.acct_no,
        p.visa_prod_cd         AS last_visa_prod_cd,
        p.bal_current          AS last_balance,
        p.accum_dly_bal_mtd    AS last_avg_daily_bal_mtd,
        p.status               AS last_status,
        p.cd_curr_ovrlmt       AS last_overlimit_cd,
        p.cd_curr_pst_due      AS last_past_due_cd,
        p.lylty_bal_amt        AS last_loyalty_balance,
        p.lst_ann_fee_chrg_amt AS annual_fee_last,
        p.lst_ann_fee_dt       AS annual_fee_last_dt
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN (
        SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
          AND app_approved = 1
        GROUP BY acct_no
    ) r2 ON r2.acct_no = p.acct_no
         AND p.dt_record_ext >= r2.treatmt_start_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext DESC) = 1
) ls ON ls.acct_no = r.acct_no

-- ---- Booked-row anchor: visa_prod_cd at the earliest post-offer row ----
LEFT JOIN (
    SELECT
        p.acct_no,
        p.visa_prod_cd AS booked_visa_prod_cd
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN (
        SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
          AND app_approved = 1
        GROUP BY acct_no
    ) r2 ON r2.acct_no = p.acct_no
         AND p.dt_record_ext >= r2.treatmt_start_dt
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext ASC) = 1
) bk ON bk.acct_no = r.acct_no

-- ---- Fee aggregation: sum of end-of-month totals across the window ----
LEFT JOIN (
    SELECT
        mf.acct_no,
        SUM(mf.month_fee_total)                                   AS total_fees_charged,
        SUM(CASE WHEN mf.month_fee_total > 0 THEN 1 ELSE 0 END)   AS months_with_fees
    FROM (
        SELECT
            p.acct_no,
            p.me_dt,
            MAX(p.net_all_fees_amt_mtd) AS month_fee_total
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        INNER JOIN (
            SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
            FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
            WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
              AND app_approved = 1
            GROUP BY acct_no
        ) r2 ON r2.acct_no = p.acct_no
             AND p.dt_record_ext >= r2.treatmt_start_dt
        GROUP BY p.acct_no, p.me_dt
    ) mf
    GROUP BY mf.acct_no
) fa ON fa.acct_no = r.acct_no

WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
ORDER BY
    r.test_group_latest,
    r.offer_prod_latest,
    pa.total_net_purchases DESC;



-- SECTION C — MONTHLY CURVES (me_dt grain)
-- ============================================================================

-- ==========================================================================
-- Q3: Monthly purchase + balance vintage curves since account opening.
--
-- Spool-friendly: three-step volatile table approach.
-- Grain: test_group × offer_prod × asc_source × visa_prod_cd × months_since_open
--
-- HOW TO RUN: highlight ALL THREE statements and run together.
--   1. CREATE VOLATILE TABLE pcq_curve_base  (one row per approved account)
--   2. CREATE VOLATILE TABLE pcq_curve_pw    (month-end portfolio slice, ~6k accts only)
--   3. SELECT ... (monthly + cumulative curves)
-- ==========================================================================


-- Step 1: one row per approved account with the fields we need for grouping.
-- ASC collapsed here: Period-ASC = Offered Card, everything else = Alternate Card.
-- This flows through to both outputs — no re-categorization needed in Excel.
DROP TABLE pcq_curve_base;
CREATE VOLATILE TABLE pcq_curve_base AS (
    SELECT
        r.acct_no,
        r.test_group_latest,
        r.offer_prod_latest,
        r.offer_prod_latest_name,
        CASE WHEN r.asc_on_app_source = 'NO ASC' THEN 'Alternate Card'
             ELSE 'Offered Card' END                       AS card_type,
        MIN(r.treatmt_start_dt) AS treatmt_start_dt
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND r.app_approved = 1
    GROUP BY r.acct_no, r.test_group_latest, r.offer_prod_latest,
             r.offer_prod_latest_name,
             CASE WHEN r.asc_on_app_source = 'NO ASC' THEN 'Alternate Card'
                  ELSE 'Offered Card' END
) WITH DATA
  PRIMARY INDEX (acct_no)
  ON COMMIT PRESERVE ROWS;


-- Step 2: portfolio slice — one row per acct × me_dt.
-- Joins to pcq_curve_base FIRST so Teradata only reads ~6k accounts.
-- Purchases: SUM(net_prch_amt_dly) per month — same logic as Q2.
-- Balance/fees/loyalty: month-end snapshot (last dt_record_ext row).
-- This ensures cumulative purchases here match Q2's total_net_purchases.
DROP TABLE pcq_curve_pw;
CREATE VOLATILE TABLE pcq_curve_pw AS (
    SELECT
        p.acct_no,
        p.me_dt,
        MAX(CASE WHEN p.rn_me = 1 THEN p.visa_prod_cd END)                 AS visa_prod_cd,
        MAX(CASE WHEN p.rn_me = 1 THEN
            (EXTRACT(YEAR FROM p.me_dt)  - EXTRACT(YEAR FROM p.acct_open_dt)) * 12
            + (EXTRACT(MONTH FROM p.me_dt) - EXTRACT(MONTH FROM p.acct_open_dt))
        END)                                                                AS months_since_open,
        MAX(CASE WHEN p.rn_me = 1 THEN p.bal_current END)                  AS bal_current,
        SUM(p.net_prch_amt_dly)                                             AS net_prch_mtd,
        MAX(CASE WHEN p.rn_me = 1 THEN p.net_all_fees_amt_mtd END)         AS net_all_fees_amt_mtd,
        MAX(CASE WHEN p.rn_me = 1 THEN p.lylty_bal_amt END)                AS lylty_bal_amt
    FROM (
        SELECT
            p.acct_no,
            p.me_dt,
            p.dt_record_ext,
            p.visa_prod_cd,
            p.acct_open_dt,
            p.bal_current,
            p.net_prch_amt_dly,
            p.net_all_fees_amt_mtd,
            p.lylty_bal_amt,
            ROW_NUMBER() OVER (PARTITION BY p.acct_no, p.me_dt
                               ORDER BY p.dt_record_ext DESC)              AS rn_me
        FROM D3CV12A.DLY_FULL_PORTFOLIO p
        INNER JOIN pcq_curve_base cb
            ON cb.acct_no = p.acct_no
            AND p.dt_record_ext >= cb.treatmt_start_dt
        WHERE p.dt_record_ext >= DATE '2025-01-01'
    ) p
    GROUP BY p.acct_no, p.me_dt
) WITH DATA
  PRIMARY INDEX (acct_no)
  ON COMMIT PRESERVE ROWS;


-- Step 3A: OUTPUT A — Product-level curves.
-- Grain: test_group × card_type × visa_prod_cd × months_since_open.
-- Use this to compare spending curves by card product.
SELECT
    cb.test_group_latest,
    cb.card_type,
    pw.visa_prod_cd,
    pw.months_since_open,
    COUNT(DISTINCT pw.acct_no)                                              AS accounts,
    AVG(pw.net_prch_mtd)                                                    AS avg_purchases_mtd,
    SUM(pw.net_prch_mtd)                                                    AS sum_purchases_mtd,
    AVG(pw.cumul_purchases)                                                 AS avg_cumul_purchases,
    SUM(pw.cumul_purchases)                                                 AS sum_cumul_purchases,
    AVG(pw.bal_current)                                                     AS avg_balance,
    AVG(pw.net_all_fees_amt_mtd)                                            AS avg_fees_mtd,
    AVG(pw.lylty_bal_amt)                                                   AS avg_loyalty
FROM (
    SELECT
        acct_no,
        visa_prod_cd,
        months_since_open,
        bal_current,
        net_prch_mtd,
        net_all_fees_amt_mtd,
        lylty_bal_amt,
        SUM(net_prch_mtd) OVER (PARTITION BY acct_no ORDER BY months_since_open
                                 ROWS UNBOUNDED PRECEDING)                  AS cumul_purchases
    FROM pcq_curve_pw
) pw
INNER JOIN pcq_curve_base cb
    ON cb.acct_no = pw.acct_no
GROUP BY
    cb.test_group_latest,
    cb.card_type,
    pw.visa_prod_cd,
    pw.months_since_open
ORDER BY
    cb.test_group_latest,
    cb.card_type,
    pw.visa_prod_cd,
    pw.months_since_open;


-- Step 3B: OUTPUT B — Group-level curves.
-- Grain: test_group × card_type × months_since_open.
-- Aggregated directly from account data — no averaging of averages.
-- Use this for the top-line test vs. control comparison.
SELECT
    cb.test_group_latest,
    cb.card_type,
    pw.months_since_open,
    COUNT(DISTINCT pw.acct_no)                                              AS accounts,
    AVG(pw.net_prch_mtd)                                                    AS avg_purchases_mtd,
    SUM(pw.net_prch_mtd)                                                    AS sum_purchases_mtd,
    AVG(pw.cumul_purchases)                                                 AS avg_cumul_purchases,
    SUM(pw.cumul_purchases)                                                 AS sum_cumul_purchases,
    AVG(pw.bal_current)                                                     AS avg_balance,
    AVG(pw.net_all_fees_amt_mtd)                                            AS avg_fees_mtd,
    AVG(pw.lylty_bal_amt)                                                   AS avg_loyalty
FROM (
    SELECT
        acct_no,
        months_since_open,
        bal_current,
        net_prch_mtd,
        net_all_fees_amt_mtd,
        lylty_bal_amt,
        SUM(net_prch_mtd) OVER (PARTITION BY acct_no ORDER BY months_since_open
                                 ROWS UNBOUNDED PRECEDING)                  AS cumul_purchases
    FROM pcq_curve_pw
) pw
INNER JOIN pcq_curve_base cb
    ON cb.acct_no = pw.acct_no
GROUP BY
    cb.test_group_latest,
    cb.card_type,
    pw.months_since_open
ORDER BY
    cb.test_group_latest,
    cb.card_type,
    pw.months_since_open;



-- ============================================================================
-- SECTION D — VALIDATION APPENDIX
-- ============================================================================
-- These are sanity checks run during investigation. They don't feed the
-- analytical file but are kept for audit and re-validation if the data
-- refreshes. Each is independent and safe to run in isolation.
-- ============================================================================


-- ==========================================================================
-- V1: Client overlap across test groups.
-- Are any clients in BOTH NG3_1ST and NG3_2ND?
-- If overlap_count = 0, groups are clean.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest = 'NG3_1ST'
    GROUP BY clnt_no
) a
INNER JOIN (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest = 'NG3_2ND'
    GROUP BY clnt_no
) b
ON a.clnt_no = b.clnt_no;


-- ==========================================================================
-- V2: Approved clients across BOTH waves.
-- Same client approved in Jan AND Feb deployment?
-- If overlap_count = 0, no cross-wave approvals.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
      AND treatmt_start_dt = DATE '2025-01-09'
    GROUP BY clnt_no
) jan
INNER JOIN (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
      AND treatmt_start_dt = DATE '2025-02-06'
    GROUP BY clnt_no
) feb
ON jan.clnt_no = feb.clnt_no;


-- ==========================================================================
-- V3: Approved clients across multiple ASC categories.
-- Same client approved through more than one ASC source?
-- If overlap_count = 0, each approved client has exactly one ASC source.
-- ==========================================================================
SELECT
    COUNT(*) AS overlap_count
FROM (
    SELECT clnt_no
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
    GROUP BY clnt_no
    HAVING COUNT(DISTINCT asc_on_app_source) > 1
) multi_asc;


-- ==========================================================================
-- V4: Approved clients with multiple approval rows.
-- If empty, every approved client has exactly one approval row.
-- ==========================================================================
SELECT
    clnt_no,
    COUNT(*)                          AS total_approvals,
    COUNT(DISTINCT offer_prod_latest) AS distinct_products,
    COUNT(DISTINCT treatmt_start_dt)  AS distinct_waves,
    COUNT(DISTINCT asc_on_app_source) AS distinct_asc
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND app_approved = 1
GROUP BY clnt_no
HAVING COUNT(*) > 1
ORDER BY total_approvals DESC;


-- ==========================================================================
-- V5: Portfolio row distribution per account × me_dt.
-- Distribution of rows-per-acct-per-month in the post-offer window.
-- Max ≈ 31 confirmed acct_no is account-grain (one card per acct per day).
-- ==========================================================================
SELECT
    rows_per_acct_month,
    COUNT(*) AS occurrences
FROM (
    SELECT
        p.acct_no,
        p.me_dt,
        COUNT(*) AS rows_per_acct_month
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON p.acct_no = r.acct_no
        AND p.me_dt >= r.treatmt_start_dt
    WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND r.app_approved = 1
    GROUP BY p.acct_no, p.me_dt
) grain_dist
GROUP BY rows_per_acct_month
ORDER BY rows_per_acct_month;


-- ==========================================================================
-- V6: Offer product vs portfolio visa_prod_cd cross-tab (pooled across ASCs).
-- Superseded by V7 (split by ASC). Kept for historical reference.
-- ==========================================================================
SELECT
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.visa_prod_cd,
    COUNT(DISTINCT r.acct_no) AS accounts
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
    ON p.acct_no = r.acct_no
    AND p.me_dt >= r.treatmt_start_dt
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
GROUP BY
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.visa_prod_cd
ORDER BY
    r.offer_prod_latest,
    accounts DESC;


-- ==========================================================================
-- V7: V6 split by asc_on_app_source.
-- Period-ASC block should have the dominant diagonal (booked = offered).
-- Other ASC / NO ASC blocks are organic and expected to scatter.
-- Note: this does NOT anchor at the booking row (unlike Q2), so reclassed
-- accounts contribute to multiple cells. Use Q2's booking_status for the
-- anchored version.
-- ==========================================================================
SELECT
    r.asc_on_app_source,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.visa_prod_cd,
    COUNT(DISTINCT r.acct_no) AS accounts
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r
INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
    ON p.acct_no = r.acct_no
    AND p.me_dt >= r.treatmt_start_dt
WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
GROUP BY
    r.asc_on_app_source,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    p.visa_prod_cd
ORDER BY
    r.asc_on_app_source,
    r.offer_prod_latest,
    accounts DESC;


-- ==========================================================================
-- V8: Validate asc_on_app_source label against raw ACQ_STRATEGY_CODE vs
-- ASC_ON_APP comparison. Expected to collapse to three diagonal cells:
--   raw_null      × NO ASC
--   raw_match     × Period-ASC
--   raw_mismatch  × Other ASC
-- Any off-diagonal cell = label is derived differently than we assume.
-- ==========================================================================
SELECT
    CASE
        WHEN asc_on_app IS NULL THEN 'raw_null'
        WHEN acq_strategy_code = asc_on_app THEN 'raw_match'
        ELSE 'raw_mismatch'
    END AS raw_comparison,
    asc_on_app_source,
    COUNT(*)                  AS n_rows,
    COUNT(DISTINCT clnt_no)   AS clients,
    COUNT(DISTINCT acct_no)   AS accounts
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND app_approved = 1
GROUP BY 1, 2
ORDER BY 1, 2;


-- ==========================================================================
-- V9: Sample accounts per (booking_status × lifetime_status) quadrant.
-- Returns 2 accounts per quadrant (8 total) with TPA-side details.
-- Pair with V10 to see the portfolio-side timeline for the same accounts.
-- ==========================================================================
WITH approved_period_asc AS (
    SELECT
        clnt_no, acct_no,
        offer_prod_latest, offer_prod_latest_name,
        treatmt_start_dt, test_group_latest,
        acq_strategy_code, asc_on_app, asc_on_app_source
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
      AND asc_on_app_source = 'Period-ASC'
),
pp AS (
    SELECT r.acct_no, p.dt_record_ext, p.visa_prod_cd
    FROM approved_period_asc r
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON p.acct_no = r.acct_no
        AND p.dt_record_ext >= r.treatmt_start_dt
),
booked AS (
    SELECT acct_no, visa_prod_cd AS booked_visa_prod_cd
    FROM pp
    QUALIFY ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY dt_record_ext) = 1
),
lifetime AS (
    SELECT acct_no, COUNT(DISTINCT visa_prod_cd) AS n_distinct_visa
    FROM pp GROUP BY acct_no
),
classified AS (
    SELECT
        r.clnt_no, r.acct_no, r.test_group_latest,
        r.offer_prod_latest, r.offer_prod_latest_name, r.treatmt_start_dt,
        r.acq_strategy_code, r.asc_on_app, r.asc_on_app_source,
        b.booked_visa_prod_cd, l.n_distinct_visa,
        CASE WHEN b.booked_visa_prod_cd = r.offer_prod_latest
             THEN 'match' ELSE 'mismatch' END AS booking_status,
        CASE WHEN l.n_distinct_visa > 1
             THEN 'reclassed' ELSE 'stable' END AS lifetime_status
    FROM approved_period_asc r
    INNER JOIN booked b ON b.acct_no = r.acct_no
    INNER JOIN lifetime l ON l.acct_no = r.acct_no
)
SELECT
    booking_status, lifetime_status,
    test_group_latest, clnt_no, acct_no,
    offer_prod_latest, offer_prod_latest_name,
    booked_visa_prod_cd, n_distinct_visa,
    treatmt_start_dt,
    acq_strategy_code, asc_on_app, asc_on_app_source
FROM classified
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY booking_status, lifetime_status
    ORDER BY acct_no
) <= 2
ORDER BY booking_status, lifetime_status, acct_no;


-- ==========================================================================
-- V10: Full portfolio timeline for the same sample accounts used in V9.
-- Self-contained — re-runs the classification CTE to pick the same 8 accounts,
-- then dumps every post-offer portfolio row for each, ordered chronologically.
-- Run V9 and V10 together, compare the two result tabs side by side.
-- ==========================================================================
WITH approved_period_asc AS (
    SELECT clnt_no, acct_no, offer_prod_latest, treatmt_start_dt, test_group_latest
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
      AND asc_on_app_source = 'Period-ASC'
),
pp AS (
    SELECT r.acct_no, p.dt_record_ext, p.visa_prod_cd
    FROM approved_period_asc r
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON p.acct_no = r.acct_no
        AND p.dt_record_ext >= r.treatmt_start_dt
),
booked AS (
    SELECT acct_no, visa_prod_cd AS booked_visa_prod_cd
    FROM pp
    QUALIFY ROW_NUMBER() OVER (PARTITION BY acct_no ORDER BY dt_record_ext) = 1
),
lifetime AS (
    SELECT acct_no, COUNT(DISTINCT visa_prod_cd) AS n_distinct_visa
    FROM pp GROUP BY acct_no
),
classified AS (
    SELECT
        r.acct_no, r.offer_prod_latest, r.treatmt_start_dt,
        CASE WHEN b.booked_visa_prod_cd = r.offer_prod_latest
             THEN 'match' ELSE 'mismatch' END AS booking_status,
        CASE WHEN l.n_distinct_visa > 1
             THEN 'reclassed' ELSE 'stable' END AS lifetime_status
    FROM approved_period_asc r
    INNER JOIN booked b ON b.acct_no = r.acct_no
    INNER JOIN lifetime l ON l.acct_no = r.acct_no
),
samples AS (
    SELECT acct_no, offer_prod_latest, booking_status, lifetime_status, treatmt_start_dt
    FROM classified
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY booking_status, lifetime_status
        ORDER BY acct_no
    ) <= 2
)
SELECT
    s.booking_status,
    s.lifetime_status,
    s.offer_prod_latest,
    s.acct_no,
    s.treatmt_start_dt,
    p.dt_record_ext,
    p.me_dt,
    p.visa_prod_cd
FROM samples s
INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
    ON p.acct_no = s.acct_no
    AND p.dt_record_ext >= s.treatmt_start_dt
ORDER BY s.booking_status, s.lifetime_status, s.acct_no, p.dt_record_ext;
