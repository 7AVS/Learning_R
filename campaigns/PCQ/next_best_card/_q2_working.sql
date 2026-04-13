-- ============================================================================
-- Q2 + Q1 SUMMARY — two-volatile-table version.
--
-- HOW TO RUN: highlight ALL FOUR STATEMENTS below and run them together.
--   1. CREATE VOLATILE TABLE pcq_accts       (tiny — ~6k accounts)
--   2. CREATE VOLATILE TABLE pcq_pw          (filtered portfolio slice)
--   3. SELECT ... (Q2 account-level analytical output)
--   4. SELECT ... (Q1 summary — full deployed denominator + all numerators)
--
-- Why this works:
--   • pcq_accts is indexed on acct_no, so Teradata distributes the join by
--     acct_no and only reads DLY_FULL_PORTFOLIO rows for accounts we care
--     about (not a full scan).
--   • `p.dt_record_ext >= DATE '2025-01-01'` prunes partitions aggressively
--     (portfolio is partitioned by date).
--   • pcq_pw holds ONE pre-filtered + ranked portfolio slice. All four
--     downstream aggregates hit it instead of re-scanning the base table.
-- ============================================================================


-- Step 1: tiny account list (one row per approved NG3 account).
CREATE VOLATILE TABLE pcq_accts AS (
    SELECT
        acct_no,
        MIN(treatmt_start_dt) AS treatmt_start_dt
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
    GROUP BY acct_no
) WITH DATA
  PRIMARY INDEX (acct_no)
  ON COMMIT PRESERVE ROWS;


-- Step 2: portfolio slice for those accounts, with first/last row ranks.
-- Hard date floor ('2025-01-01') prunes date-partitioned portfolio pages.
CREATE VOLATILE TABLE pcq_pw AS (
    SELECT
        p.acct_no,
        p.dt_record_ext,
        p.me_dt,
        p.visa_prod_cd,
        p.acct_open_dt,
        p.acct_cls_dt,
        p.status AS acct_status,
        p.bal_current,
        p.accum_dly_bal_mtd,
        p.net_prch_amt_dly,
        p.lylty_bal_amt,
        p.lst_ann_fee_chrg_amt,
        p.lst_ann_fee_dt,
        p.net_all_fees_amt_mtd,
        p.cd_curr_ovrlmt,
        p.cd_curr_pst_due,
        ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext ASC)  AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext DESC) AS rn_last
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN pcq_accts a
        ON a.acct_no = p.acct_no
        AND p.dt_record_ext >= a.treatmt_start_dt
    WHERE p.dt_record_ext >= DATE '2025-01-01'
) WITH DATA
  PRIMARY INDEX (acct_no)
  ON COMMIT PRESERVE ROWS;


-- Step 3: the analytical query.
SELECT
    r.test_group_latest,
    r.clnt_no,
    r.acct_no,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    r.asc_on_app_source,
    r.treatmt_start_dt,
    r.response_dt,
    pa.acct_open_dt,
    pa.acct_cls_dt,
    CAST(pa.acct_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)       AS days_offer_to_open,
    pa.first_extract_dt,
    pa.last_extract_dt,
    pa.months_with_activity,
    pa.booked_visa_prod_cd,
    pa.last_visa_prod_cd,
    pa.n_uniq_visa,
    CASE WHEN pa.booked_visa_prod_cd = r.offer_prod_latest
         THEN 'match' ELSE 'mismatch' END                                   AS booking_status,
    CASE WHEN pa.n_uniq_visa > 1
         THEN 'reclassed' ELSE 'stable' END                                 AS lifetime_status,
    pa.last_balance,
    pa.last_avg_daily_bal_mtd,
    pa.total_net_purchases,
    pa.total_net_purchases / NULLIFZERO(pa.months_with_activity)            AS avg_monthly_purchases,
    pa.annual_fee_last,
    pa.annual_fee_last_dt,
    CASE WHEN pa.annual_fee_last > 0 THEN 1 ELSE 0 END                      AS ever_charged_annual_fee,
    fa.total_fees_charged,
    fa.months_with_fees,
    pa.last_loyalty_balance,
    pa.max_loyalty_balance,
    pa.ever_overlimit,
    pa.last_overlimit_cd,
    pa.ever_past_due,
    pa.last_past_due_cd,
    pa.last_status,
    pa.st_bkpt, pa.st_coll, pa.st_frd, pa.st_inv, pa.st_open, pa.st_vol, pa.st_woff,
    pa.first_non_open_dt,
    CAST(pa.first_non_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)   AS days_to_status_change
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r

-- Single aggregate pass against pcq_pw — all per-account values computed via
-- MAX(CASE WHEN rn_first/rn_last = 1 THEN ... END) instead of separate joins.
LEFT JOIN (
    SELECT
        acct_no,
        MIN(dt_record_ext)              AS first_extract_dt,
        MAX(dt_record_ext)              AS last_extract_dt,
        MAX(acct_open_dt)               AS acct_open_dt,
        MAX(acct_cls_dt)                AS acct_cls_dt,
        COUNT(DISTINCT me_dt)           AS months_with_activity,
        COUNT(DISTINCT visa_prod_cd)    AS n_uniq_visa,
        SUM(net_prch_amt_dly)           AS total_net_purchases,
        MAX(lylty_bal_amt)              AS max_loyalty_balance,

        -- "First row" (booked anchor)
        MAX(CASE WHEN rn_first = 1 THEN visa_prod_cd END)         AS booked_visa_prod_cd,

        -- "Last row" (point-in-time snapshot)
        MAX(CASE WHEN rn_last = 1 THEN visa_prod_cd END)          AS last_visa_prod_cd,
        MAX(CASE WHEN rn_last = 1 THEN bal_current END)           AS last_balance,
        MAX(CASE WHEN rn_last = 1 THEN accum_dly_bal_mtd END)     AS last_avg_daily_bal_mtd,
        MAX(CASE WHEN rn_last = 1 THEN acct_status END)           AS last_status,
        MAX(CASE WHEN rn_last = 1 THEN cd_curr_ovrlmt END)        AS last_overlimit_cd,
        MAX(CASE WHEN rn_last = 1 THEN cd_curr_pst_due END)       AS last_past_due_cd,
        MAX(CASE WHEN rn_last = 1 THEN lylty_bal_amt END)         AS last_loyalty_balance,
        MAX(CASE WHEN rn_last = 1 THEN lst_ann_fee_chrg_amt END)  AS annual_fee_last,
        MAX(CASE WHEN rn_last = 1 THEN lst_ann_fee_dt END)        AS annual_fee_last_dt,

        -- Status ever-flags
        MAX(CASE WHEN acct_status = 'BKPT' THEN 1 ELSE 0 END) AS st_bkpt,
        MAX(CASE WHEN acct_status = 'COLL' THEN 1 ELSE 0 END) AS st_coll,
        MAX(CASE WHEN acct_status = 'FRD'  THEN 1 ELSE 0 END) AS st_frd,
        MAX(CASE WHEN acct_status = 'INV'  THEN 1 ELSE 0 END) AS st_inv,
        MAX(CASE WHEN acct_status = 'OPEN' THEN 1 ELSE 0 END) AS st_open,
        MAX(CASE WHEN acct_status = 'VOL'  THEN 1 ELSE 0 END) AS st_vol,
        MAX(CASE WHEN acct_status = 'WOFF' THEN 1 ELSE 0 END) AS st_woff,
        MIN(CASE WHEN acct_status <> 'OPEN' THEN dt_record_ext END) AS first_non_open_dt,

        -- Risk ever-flags
        MAX(CASE WHEN cd_curr_ovrlmt IS NOT NULL
                  AND cd_curr_ovrlmt NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END)                               AS ever_overlimit,
        MAX(CASE WHEN cd_curr_pst_due IS NOT NULL
                  AND cd_curr_pst_due NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END)                               AS ever_past_due
    FROM pcq_pw
    GROUP BY acct_no
) pa ON pa.acct_no = r.acct_no

-- Fees: separate because needs per-me_dt rollup first
LEFT JOIN (
    SELECT
        acct_no,
        SUM(month_fee_total)                                AS total_fees_charged,
        SUM(CASE WHEN month_fee_total > 0 THEN 1 ELSE 0 END) AS months_with_fees
    FROM (
        SELECT
            acct_no,
            me_dt,
            MAX(net_all_fees_amt_mtd) AS month_fee_total
        FROM pcq_pw
        GROUP BY acct_no, me_dt
    ) mf
    GROUP BY acct_no
) fa ON fa.acct_no = r.acct_no

WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
ORDER BY
    r.test_group_latest,
    r.offer_prod_latest,
    pa.total_net_purchases DESC;


-- ============================================================================
-- Step 4 — Q1 SUMMARY
-- Grain: test_group × wave × offered product.
-- Denominator = full deployed population (NO app_approved filter).
--
-- Contains three layers of information:
--
-- 1. FUNNEL COUNTS — nested conversion definitions (each stricter than prior):
--      deployed → approved_any → approved_period_asc → approved_period_asc_match
--      → approved_clean (= match AND stable)
--
-- 2. FUNNEL RATES — each numerator over the same deployed denominator.
--
-- 3. $ ROLLUPS — totals AND per-account averages, computed over TWO
--    populations side by side so you can compare without re-running:
--      pasc_*   = all Period-ASC approved accounts (the standard measure)
--      clean_*  = Period-ASC + booking_status=match + lifetime_status=stable
--
--    Metrics rolled up: total_net_purchases, last_balance, last_avg_daily_bal,
--    max_loyalty_balance, last_loyalty_balance, total_fees_charged, and risk
--    incidence counts (ever_overlimit, ever_past_due, st_woff, st_bkpt).
-- ============================================================================
SELECT
    r.test_group_latest,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    COALESCE(r.asc_on_app_source, 'No response')                      AS asc_category,

    -- Deployed (denominator) is the count for the whole bucket before ASC split.
    -- Same value repeats across the 3-4 ASC rows within a (group × wave × product).
    grp_tot.deployed,

    -- Approved count within this ASC bucket
    SUM(r.app_approved)                                               AS approved,

    -- $ rollups over approved accounts in this ASC bucket
    SUM(CASE WHEN r.app_approved = 1 THEN cls.total_net_purchases END) AS sum_total_purchases,
    AVG(CASE WHEN r.app_approved = 1 THEN cls.total_net_purchases END) AS avg_total_purchases,
    SUM(CASE WHEN r.app_approved = 1 THEN cls.last_balance END)        AS sum_last_balance,
    AVG(CASE WHEN r.app_approved = 1 THEN cls.last_balance END)        AS avg_last_balance,
    AVG(CASE WHEN r.app_approved = 1 THEN cls.last_avg_daily_bal END)  AS avg_last_avg_daily_bal,
    SUM(CASE WHEN r.app_approved = 1 THEN cls.total_fees_charged END)  AS sum_total_fees,
    AVG(CASE WHEN r.app_approved = 1 THEN cls.total_fees_charged END)  AS avg_total_fees,
    SUM(CASE WHEN r.app_approved = 1 THEN cls.max_loyalty_balance END) AS sum_max_loyalty,
    AVG(CASE WHEN r.app_approved = 1 THEN cls.max_loyalty_balance END) AS avg_max_loyalty,
    AVG(CASE WHEN r.app_approved = 1 THEN cls.last_loyalty_balance END) AS avg_last_loyalty,

    -- Risk incidence counts in this ASC bucket
    SUM(CASE WHEN r.app_approved = 1 AND cls.ever_overlimit = 1 THEN 1 ELSE 0 END) AS cnt_ever_overlimit,
    SUM(CASE WHEN r.app_approved = 1 AND cls.ever_past_due  = 1 THEN 1 ELSE 0 END) AS cnt_ever_past_due,
    SUM(CASE WHEN r.app_approved = 1 AND cls.st_woff = 1 THEN 1 ELSE 0 END)        AS cnt_writeoff,
    SUM(CASE WHEN r.app_approved = 1 AND cls.st_bkpt = 1 THEN 1 ELSE 0 END)        AS cnt_bankruptcy
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r

-- Deployed denominator: total count per (group × wave × product), regardless of ASC.
-- Joined in so it appears as a column repeated across each ASC row of the same bucket.
LEFT JOIN (
    SELECT
        test_group_latest,
        treatmt_start_dt,
        offer_prod_latest,
        COUNT(*) AS deployed
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
    GROUP BY test_group_latest, treatmt_start_dt, offer_prod_latest
) grp_tot
    ON grp_tot.test_group_latest = r.test_group_latest
    AND grp_tot.treatmt_start_dt = r.treatmt_start_dt
    AND grp_tot.offer_prod_latest = r.offer_prod_latest

-- Per-account $ metrics from pcq_pw (LEFT JOIN — non-approved rows get NULLs)
LEFT JOIN (
    SELECT
        a.acct_no,
        a.total_net_purchases,
        a.last_balance,
        a.last_avg_daily_bal,
        a.max_loyalty_balance,
        a.last_loyalty_balance,
        a.ever_overlimit,
        a.ever_past_due,
        a.st_woff,
        a.st_bkpt,
        COALESCE(f.total_fees_charged, 0) AS total_fees_charged
    FROM (
        SELECT
            acct_no,
            SUM(net_prch_amt_dly)                                   AS total_net_purchases,
            MAX(CASE WHEN rn_last = 1 THEN bal_current END)         AS last_balance,
            MAX(CASE WHEN rn_last = 1 THEN accum_dly_bal_mtd END)   AS last_avg_daily_bal,
            MAX(lylty_bal_amt)                                      AS max_loyalty_balance,
            MAX(CASE WHEN rn_last = 1 THEN lylty_bal_amt END)       AS last_loyalty_balance,
            MAX(CASE WHEN cd_curr_ovrlmt IS NOT NULL
                      AND cd_curr_ovrlmt NOT IN ('', 'N', '0')
                     THEN 1 ELSE 0 END)                             AS ever_overlimit,
            MAX(CASE WHEN cd_curr_pst_due IS NOT NULL
                      AND cd_curr_pst_due NOT IN ('', 'N', '0')
                     THEN 1 ELSE 0 END)                             AS ever_past_due,
            MAX(CASE WHEN acct_status = 'WOFF' THEN 1 ELSE 0 END)   AS st_woff,
            MAX(CASE WHEN acct_status = 'BKPT' THEN 1 ELSE 0 END)   AS st_bkpt
        FROM pcq_pw
        GROUP BY acct_no
    ) a
    LEFT JOIN (
        SELECT acct_no, SUM(month_fee_total) AS total_fees_charged
        FROM (
            SELECT acct_no, me_dt, MAX(net_all_fees_amt_mtd) AS month_fee_total
            FROM pcq_pw
            GROUP BY acct_no, me_dt
        ) mf
        GROUP BY acct_no
    ) f ON f.acct_no = a.acct_no
) cls ON cls.acct_no = r.acct_no

WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    r.test_group_latest,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    COALESCE(r.asc_on_app_source, 'No response'),
    grp_tot.deployed
ORDER BY
    r.test_group_latest,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    asc_category;
