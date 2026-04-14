-- ============================================================================
-- Q2 v2 — adds halo / cross-sell tracking.
--
-- HOW TO RUN: highlight ALL FIVE STATEMENTS and run together.
--   1. CREATE VOLATILE TABLE pcq_accts    (tiny — one row per approved account)
--   2. CREATE VOLATILE TABLE pcq_pw       (in-campaign portfolio slice)
--   3. CREATE VOLATILE TABLE pcq_cross    (post-campaign halo slice, by clnt_no)
--   4. SELECT ... (Q2 analytical — per-account + per-client cross-sell rollup)
--   5. SELECT ... (Summary — one row per (group × wave × product),
--                  pasc_* / other_* / cross_* metric blocks as columns)
--
-- THREE POPULATIONS rolled up side by side in the summary:
--   pasc_*   = app_approved = 1 AND asc_on_app_source = 'Period-ASC'
--   other_*  = app_approved = 1 AND asc_on_app_source IN ('Other ASC','NO ASC')
--   cross_*  = any acct_no in portfolio for an in-campaign-approved clnt_no,
--              where acct_open_dt > treatmt_end_dt and the acct_no is NOT
--              one of the in-campaign approved accounts. This is the "halo":
--              post-campaign acquisitions potentially triggered by the offer.
--
-- Classification flags (booking_status, lifetime_status) apply only to the
-- in-campaign account in Q2 — they don't apply to cross-sell cards because
-- those cards weren't "offered" anything.
--
-- Known limitation: if a client has multiple in-campaign approved rows (rare,
-- V4 sanity check shows <1%), their cross-sell metrics are counted once per
-- approved row in the summary. Inflates only marginally.
-- ============================================================================


-- Step 1: account + client list with campaign window dates.
CREATE VOLATILE TABLE pcq_accts AS (
    SELECT
        acct_no,
        MAX(clnt_no)          AS clnt_no,
        MIN(treatmt_start_dt) AS treatmt_start_dt,
        MAX(treatmt_end_dt)   AS treatmt_end_dt
    FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
    WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
      AND app_approved = 1
    GROUP BY acct_no
) WITH DATA
  PRIMARY INDEX (acct_no)
  ON COMMIT PRESERVE ROWS;


-- Step 2: in-campaign portfolio slice (same accts as pcq_accts).
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


-- Step 3: cross-sell (halo) portfolio slice.
-- Joins portfolio on clnt_no. Filters to accounts opened AFTER treatmt_end_dt.
-- Excludes in-campaign approved acct_nos via LEFT JOIN anti-join (safer than
-- NOT IN for NULLs).
CREATE VOLATILE TABLE pcq_cross AS (
    SELECT
        p.clnt_no,
        p.acct_no,
        p.dt_record_ext,
        p.me_dt,
        p.visa_prod_cd,
        p.acct_open_dt,
        p.status AS acct_status,
        p.bal_current,
        p.accum_dly_bal_mtd,
        p.net_prch_amt_dly,
        p.lylty_bal_amt,
        p.net_all_fees_amt_mtd,
        p.cd_curr_ovrlmt,
        p.cd_curr_pst_due,
        ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.dt_record_ext DESC) AS rn_last
    FROM D3CV12A.DLY_FULL_PORTFOLIO p
    INNER JOIN (
        SELECT clnt_no, MAX(treatmt_end_dt) AS treatmt_end_dt
        FROM pcq_accts
        GROUP BY clnt_no
    ) c
        ON c.clnt_no = p.clnt_no
        AND p.acct_open_dt > c.treatmt_end_dt
    LEFT JOIN pcq_accts excl
        ON excl.acct_no = p.acct_no
    WHERE excl.acct_no IS NULL
      AND p.dt_record_ext >= DATE '2025-01-01'
) WITH DATA
  PRIMARY INDEX (clnt_no)
  ON COMMIT PRESERVE ROWS;


-- ============================================================================
-- Step 4: Q2 — per-account analytical output.
-- One row per in-campaign approved account + per-client cross-sell rollup
-- pinned alongside (same cross_* values repeat for clients with multiple
-- in-campaign approved accounts).
-- ============================================================================
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
    r.treatmt_end_dt,
    r.response_dt,

    -- === Account timing (in-campaign account) ===
    pa.acct_open_dt,
    pa.acct_cls_dt,
    CAST(pa.acct_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)       AS days_offer_to_open,
    pa.first_extract_dt,
    pa.last_extract_dt,
    pa.months_with_activity,

    -- === Classification (in-campaign account) ===
    pa.booked_visa_prod_cd,
    pa.last_visa_prod_cd,
    pa.n_uniq_visa,
    CASE WHEN pa.booked_visa_prod_cd = r.offer_prod_latest
         THEN 'match' ELSE 'mismatch' END                                   AS booking_status,
    CASE WHEN pa.n_uniq_visa > 1
         THEN 'reclassed' ELSE 'stable' END                                 AS lifetime_status,

    -- === Balances (in-campaign account) ===
    pa.last_balance,
    pa.last_avg_daily_bal_mtd,
    pa.total_net_purchases,
    pa.total_net_purchases / NULLIFZERO(pa.months_with_activity)            AS avg_monthly_purchases,

    -- === Fees (in-campaign account) ===
    pa.annual_fee_last,
    pa.annual_fee_last_dt,
    CASE WHEN pa.annual_fee_last > 0 THEN 1 ELSE 0 END                      AS ever_charged_annual_fee,
    fa.total_fees_charged,
    fa.months_with_fees,

    -- === Loyalty (in-campaign account) ===
    pa.last_loyalty_balance,
    pa.max_loyalty_balance,

    -- === Risk (in-campaign account) ===
    pa.ever_overlimit,
    pa.last_overlimit_cd,
    pa.ever_past_due,
    pa.last_past_due_cd,

    -- === Status lifecycle (in-campaign account) ===
    pa.last_status,
    pa.st_bkpt, pa.st_coll, pa.st_frd, pa.st_inv, pa.st_open, pa.st_vol, pa.st_woff,
    pa.first_non_open_dt,
    CAST(pa.first_non_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)   AS days_to_status_change,

    -- === Cross-sell / halo (per-client rollup — same value repeats for client's
    --     multi-approved rows in this file) ===
    COALESCE(cx.n_cross_accts, 0)    AS cross_n_accounts,
    cx.first_cross_open_dt,
    cx.sum_total_purchases           AS cross_total_purchases,
    cx.sum_last_balance              AS cross_sum_last_balance,
    cx.sum_total_fees_charged        AS cross_total_fees_charged,
    COALESCE(cx.any_woff, 0)         AS cross_any_woff,
    COALESCE(cx.any_bkpt, 0)         AS cross_any_bkpt
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r

-- In-campaign per-account aggregates (single-pass GROUP BY acct_no)
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
        MAX(CASE WHEN rn_first = 1 THEN visa_prod_cd END)         AS booked_visa_prod_cd,
        MAX(CASE WHEN rn_last = 1 THEN visa_prod_cd END)          AS last_visa_prod_cd,
        MAX(CASE WHEN rn_last = 1 THEN bal_current END)           AS last_balance,
        MAX(CASE WHEN rn_last = 1 THEN accum_dly_bal_mtd END)     AS last_avg_daily_bal_mtd,
        MAX(CASE WHEN rn_last = 1 THEN acct_status END)           AS last_status,
        MAX(CASE WHEN rn_last = 1 THEN cd_curr_ovrlmt END)        AS last_overlimit_cd,
        MAX(CASE WHEN rn_last = 1 THEN cd_curr_pst_due END)       AS last_past_due_cd,
        MAX(CASE WHEN rn_last = 1 THEN lylty_bal_amt END)         AS last_loyalty_balance,
        MAX(CASE WHEN rn_last = 1 THEN lst_ann_fee_chrg_amt END)  AS annual_fee_last,
        MAX(CASE WHEN rn_last = 1 THEN lst_ann_fee_dt END)        AS annual_fee_last_dt,
        MAX(CASE WHEN acct_status = 'BKPT' THEN 1 ELSE 0 END) AS st_bkpt,
        MAX(CASE WHEN acct_status = 'COLL' THEN 1 ELSE 0 END) AS st_coll,
        MAX(CASE WHEN acct_status = 'FRD'  THEN 1 ELSE 0 END) AS st_frd,
        MAX(CASE WHEN acct_status = 'INV'  THEN 1 ELSE 0 END) AS st_inv,
        MAX(CASE WHEN acct_status = 'OPEN' THEN 1 ELSE 0 END) AS st_open,
        MAX(CASE WHEN acct_status = 'VOL'  THEN 1 ELSE 0 END) AS st_vol,
        MAX(CASE WHEN acct_status = 'WOFF' THEN 1 ELSE 0 END) AS st_woff,
        MIN(CASE WHEN acct_status <> 'OPEN' THEN dt_record_ext END) AS first_non_open_dt,
        MAX(CASE WHEN cd_curr_ovrlmt IS NOT NULL
                  AND cd_curr_ovrlmt NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END) AS ever_overlimit,
        MAX(CASE WHEN cd_curr_pst_due IS NOT NULL
                  AND cd_curr_pst_due NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END) AS ever_past_due
    FROM pcq_pw
    GROUP BY acct_no
) pa ON pa.acct_no = r.acct_no

-- In-campaign fees rollup (per-me_dt max → sum)
LEFT JOIN (
    SELECT
        acct_no,
        SUM(month_fee_total)                                 AS total_fees_charged,
        SUM(CASE WHEN month_fee_total > 0 THEN 1 ELSE 0 END) AS months_with_fees
    FROM (
        SELECT acct_no, me_dt, MAX(net_all_fees_amt_mtd) AS month_fee_total
        FROM pcq_pw
        GROUP BY acct_no, me_dt
    ) mf
    GROUP BY acct_no
) fa ON fa.acct_no = r.acct_no

-- Cross-sell rollup per client (one row per clnt_no)
LEFT JOIN (
    SELECT
        cx_acct.clnt_no,
        COUNT(DISTINCT cx_acct.acct_no) AS n_cross_accts,
        MIN(cx_acct.acct_open_dt)       AS first_cross_open_dt,
        SUM(cx_acct.total_net_purchases) AS sum_total_purchases,
        SUM(cx_acct.last_balance)        AS sum_last_balance,
        SUM(cx_acct.total_fees_charged)  AS sum_total_fees_charged,
        MAX(cx_acct.st_woff)             AS any_woff,
        MAX(cx_acct.st_bkpt)             AS any_bkpt
    FROM (
        -- Per cross-sell-acct aggregates
        SELECT
            acct_no,
            MAX(clnt_no)                                            AS clnt_no,
            MAX(acct_open_dt)                                       AS acct_open_dt,
            SUM(net_prch_amt_dly)                                   AS total_net_purchases,
            MAX(CASE WHEN rn_last = 1 THEN bal_current END)         AS last_balance,
            COALESCE(SUM(monthly_fee.mf), 0)                        AS total_fees_charged,
            MAX(CASE WHEN acct_status = 'WOFF' THEN 1 ELSE 0 END)   AS st_woff,
            MAX(CASE WHEN acct_status = 'BKPT' THEN 1 ELSE 0 END)   AS st_bkpt
        FROM pcq_cross
        LEFT JOIN (
            SELECT acct_no AS mf_acct_no, me_dt AS mf_me_dt, MAX(net_all_fees_amt_mtd) AS mf
            FROM pcq_cross
            GROUP BY acct_no, me_dt
        ) monthly_fee ON monthly_fee.mf_acct_no = pcq_cross.acct_no
                      AND monthly_fee.mf_me_dt = pcq_cross.me_dt
        GROUP BY acct_no
    ) cx_acct
    GROUP BY cx_acct.clnt_no
) cx ON cx.clnt_no = r.clnt_no

WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
  AND r.app_approved = 1
ORDER BY
    r.test_group_latest,
    r.offer_prod_latest,
    pa.total_net_purchases DESC;


-- ============================================================================
-- Step 5: SUMMARY — one row per (test_group × wave × product).
-- Three metric blocks: pasc_* / other_* / cross_* as column prefixes.
-- Deployed appears once (no duplication).
-- ============================================================================
SELECT
    r.test_group_latest,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    grp_tot.deployed,

    -- ============ pasc_* — Period-ASC approved ============
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN 1 ELSE 0 END) AS pasc_approved,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.total_net_purchases END) AS pasc_sum_total_purchases,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.total_net_purchases END) AS pasc_avg_total_purchases,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.last_balance END)        AS pasc_sum_last_balance,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.last_balance END)        AS pasc_avg_last_balance,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.last_avg_daily_bal END)  AS pasc_avg_last_avg_daily_bal,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN fa.total_fees_charged END)  AS pasc_sum_total_fees,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN fa.total_fees_charged END)  AS pasc_avg_total_fees,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.max_loyalty_balance END) AS pasc_avg_max_loyalty,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' THEN pa.last_loyalty_balance END) AS pasc_avg_last_loyalty,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.ever_overlimit = 1 THEN 1 ELSE 0 END) AS pasc_cnt_ever_overlimit,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.ever_past_due  = 1 THEN 1 ELSE 0 END) AS pasc_cnt_ever_past_due,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_bkpt = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_bkpt,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_coll = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_coll,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_frd  = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_frd,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_inv  = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_inv,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_open = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_open,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_vol  = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_vol,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source = 'Period-ASC' AND pa.st_woff = 1 THEN 1 ELSE 0 END) AS pasc_cnt_st_woff,

    -- ============ other_* — Other ASC + NO ASC approved ============
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN 1 ELSE 0 END) AS other_approved,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.total_net_purchases END) AS other_sum_total_purchases,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.total_net_purchases END) AS other_avg_total_purchases,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.last_balance END)        AS other_sum_last_balance,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.last_balance END)        AS other_avg_last_balance,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.last_avg_daily_bal END)  AS other_avg_last_avg_daily_bal,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN fa.total_fees_charged END)  AS other_sum_total_fees,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN fa.total_fees_charged END)  AS other_avg_total_fees,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.max_loyalty_balance END) AS other_avg_max_loyalty,
    AVG(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') THEN pa.last_loyalty_balance END) AS other_avg_last_loyalty,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.ever_overlimit = 1 THEN 1 ELSE 0 END) AS other_cnt_ever_overlimit,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.ever_past_due  = 1 THEN 1 ELSE 0 END) AS other_cnt_ever_past_due,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_bkpt = 1 THEN 1 ELSE 0 END) AS other_cnt_st_bkpt,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_coll = 1 THEN 1 ELSE 0 END) AS other_cnt_st_coll,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_frd  = 1 THEN 1 ELSE 0 END) AS other_cnt_st_frd,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_inv  = 1 THEN 1 ELSE 0 END) AS other_cnt_st_inv,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_open = 1 THEN 1 ELSE 0 END) AS other_cnt_st_open,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_vol  = 1 THEN 1 ELSE 0 END) AS other_cnt_st_vol,
    SUM(CASE WHEN r.app_approved = 1 AND r.asc_on_app_source IN ('Other ASC','NO ASC') AND pa.st_woff = 1 THEN 1 ELSE 0 END) AS other_cnt_st_woff,

    -- ============ cross_* — halo (post-campaign new cards for approved clients) ============
    SUM(CASE WHEN r.app_approved = 1 THEN COALESCE(cx.n_cross_accts, 0)   END) AS cross_n_accounts,
    SUM(CASE WHEN r.app_approved = 1 THEN cx.sum_total_purchases          END) AS cross_sum_total_purchases,
    AVG(CASE WHEN r.app_approved = 1 THEN cx.avg_total_purchases          END) AS cross_avg_total_purchases,
    SUM(CASE WHEN r.app_approved = 1 THEN cx.sum_last_balance             END) AS cross_sum_last_balance,
    AVG(CASE WHEN r.app_approved = 1 THEN cx.avg_last_balance             END) AS cross_avg_last_balance,
    SUM(CASE WHEN r.app_approved = 1 THEN cx.sum_total_fees_charged       END) AS cross_sum_total_fees,
    AVG(CASE WHEN r.app_approved = 1 THEN cx.avg_total_fees_charged       END) AS cross_avg_total_fees,
    AVG(CASE WHEN r.app_approved = 1 THEN cx.avg_max_loyalty              END) AS cross_avg_max_loyalty,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_ever_overlimit = 1 THEN 1 ELSE 0 END) AS cross_cnt_ever_overlimit,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_ever_past_due  = 1 THEN 1 ELSE 0 END) AS cross_cnt_ever_past_due,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_bkpt = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_bkpt,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_coll = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_coll,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_frd  = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_frd,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_inv  = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_inv,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_open = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_open,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_vol  = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_vol,
    SUM(CASE WHEN r.app_approved = 1 AND cx.any_woff = 1 THEN 1 ELSE 0 END) AS cross_cnt_any_woff
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r

-- Deployed denominator
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

-- In-campaign per-account aggregates (reused from Q2)
LEFT JOIN (
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
        MAX(CASE WHEN acct_status = 'BKPT' THEN 1 ELSE 0 END)   AS st_bkpt,
        MAX(CASE WHEN acct_status = 'COLL' THEN 1 ELSE 0 END)   AS st_coll,
        MAX(CASE WHEN acct_status = 'FRD'  THEN 1 ELSE 0 END)   AS st_frd,
        MAX(CASE WHEN acct_status = 'INV'  THEN 1 ELSE 0 END)   AS st_inv,
        MAX(CASE WHEN acct_status = 'OPEN' THEN 1 ELSE 0 END)   AS st_open,
        MAX(CASE WHEN acct_status = 'VOL'  THEN 1 ELSE 0 END)   AS st_vol,
        MAX(CASE WHEN acct_status = 'WOFF' THEN 1 ELSE 0 END)   AS st_woff
    FROM pcq_pw
    GROUP BY acct_no
) pa ON pa.acct_no = r.acct_no

-- In-campaign fees rollup
LEFT JOIN (
    SELECT
        acct_no,
        SUM(month_fee_total) AS total_fees_charged
    FROM (
        SELECT acct_no, me_dt, MAX(net_all_fees_amt_mtd) AS month_fee_total
        FROM pcq_pw
        GROUP BY acct_no, me_dt
    ) mf
    GROUP BY acct_no
) fa ON fa.acct_no = r.acct_no

-- Cross-sell per-client rollup
LEFT JOIN (
    SELECT
        cx_acct.clnt_no,
        COUNT(DISTINCT cx_acct.acct_no)     AS n_cross_accts,
        SUM(cx_acct.total_net_purchases)    AS sum_total_purchases,
        AVG(cx_acct.total_net_purchases)    AS avg_total_purchases,
        SUM(cx_acct.last_balance)           AS sum_last_balance,
        AVG(cx_acct.last_balance)           AS avg_last_balance,
        SUM(cx_acct.total_fees_charged)     AS sum_total_fees_charged,
        AVG(cx_acct.total_fees_charged)     AS avg_total_fees_charged,
        AVG(cx_acct.max_loyalty_balance)    AS avg_max_loyalty,
        MAX(cx_acct.ever_overlimit)         AS any_ever_overlimit,
        MAX(cx_acct.ever_past_due)          AS any_ever_past_due,
        MAX(cx_acct.st_bkpt)                AS any_bkpt,
        MAX(cx_acct.st_coll)                AS any_coll,
        MAX(cx_acct.st_frd)                 AS any_frd,
        MAX(cx_acct.st_inv)                 AS any_inv,
        MAX(cx_acct.st_open)                AS any_open,
        MAX(cx_acct.st_vol)                 AS any_vol,
        MAX(cx_acct.st_woff)                AS any_woff
    FROM (
        SELECT
            acct_no,
            MAX(clnt_no)                                            AS clnt_no,
            SUM(net_prch_amt_dly)                                   AS total_net_purchases,
            MAX(CASE WHEN rn_last = 1 THEN bal_current END)         AS last_balance,
            MAX(lylty_bal_amt)                                      AS max_loyalty_balance,
            COALESCE((SELECT SUM(mf) FROM (
                SELECT me_dt, MAX(net_all_fees_amt_mtd) AS mf
                FROM pcq_cross cx_inner
                WHERE cx_inner.acct_no = pcq_cross.acct_no
                GROUP BY me_dt
            ) fee_sub), 0)                                          AS total_fees_charged,
            MAX(CASE WHEN cd_curr_ovrlmt IS NOT NULL
                      AND cd_curr_ovrlmt NOT IN ('', 'N', '0')
                     THEN 1 ELSE 0 END)                             AS ever_overlimit,
            MAX(CASE WHEN cd_curr_pst_due IS NOT NULL
                      AND cd_curr_pst_due NOT IN ('', 'N', '0')
                     THEN 1 ELSE 0 END)                             AS ever_past_due,
            MAX(CASE WHEN acct_status = 'BKPT' THEN 1 ELSE 0 END)   AS st_bkpt,
            MAX(CASE WHEN acct_status = 'COLL' THEN 1 ELSE 0 END)   AS st_coll,
            MAX(CASE WHEN acct_status = 'FRD'  THEN 1 ELSE 0 END)   AS st_frd,
            MAX(CASE WHEN acct_status = 'INV'  THEN 1 ELSE 0 END)   AS st_inv,
            MAX(CASE WHEN acct_status = 'OPEN' THEN 1 ELSE 0 END)   AS st_open,
            MAX(CASE WHEN acct_status = 'VOL'  THEN 1 ELSE 0 END)   AS st_vol,
            MAX(CASE WHEN acct_status = 'WOFF' THEN 1 ELSE 0 END)   AS st_woff
        FROM pcq_cross
        GROUP BY acct_no
    ) cx_acct
    GROUP BY cx_acct.clnt_no
) cx ON cx.clnt_no = r.clnt_no

WHERE r.test_group_latest IN ('NG3_1ST', 'NG3_2ND')
GROUP BY
    r.test_group_latest,
    r.treatmt_start_dt,
    r.offer_prod_latest,
    r.offer_prod_latest_name,
    grp_tot.deployed
ORDER BY
    r.test_group_latest,
    r.treatmt_start_dt,
    r.offer_prod_latest;
