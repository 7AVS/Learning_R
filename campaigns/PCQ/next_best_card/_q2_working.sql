-- ============================================================================
-- Q2 — volatile-table version. Fixes spool-space errors.
--
-- HOW TO RUN: highlight BOTH statements (CREATE VOLATILE + the final SELECT)
-- and run them together in one shot. The volatile table lives only for the
-- current session; once the session ends it disappears.
-- ============================================================================

-- Step 1: Pre-filter portfolio ONCE into a volatile table.
-- Scans DLY_FULL_PORTFOLIO exactly once. All downstream aggregates then hit
-- this small intermediate instead of re-scanning the base table four times.
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
    INNER JOIN (
        SELECT acct_no, MIN(treatmt_start_dt) AS treatmt_start_dt
        FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
        WHERE test_group_latest IN ('NG3_1ST', 'NG3_2ND')
          AND app_approved = 1
        GROUP BY acct_no
    ) r2 ON r2.acct_no = p.acct_no
         AND p.dt_record_ext >= r2.treatmt_start_dt
) WITH DATA
  PRIMARY INDEX (acct_no)
  ON COMMIT PRESERVE ROWS;


-- Step 2: The analytical query. Joins TPA to the volatile table's aggregates.
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
    CASE WHEN pa.acct_open_dt < r.treatmt_start_dt THEN 1 ELSE 0 END        AS account_existed_pre_offer,
    pa.first_extract_dt,
    pa.last_extract_dt,
    pa.months_with_activity,
    bk.booked_visa_prod_cd,
    snp.last_visa_prod_cd,
    pa.n_uniq_visa,
    CASE WHEN bk.booked_visa_prod_cd = r.offer_prod_latest
         THEN 'match' ELSE 'mismatch' END                                   AS booking_status,
    CASE WHEN pa.n_uniq_visa > 1
         THEN 'reclassed' ELSE 'stable' END                                 AS lifetime_status,
    snp.last_balance,
    snp.last_avg_daily_bal_mtd,
    pa.total_net_purchases,
    pa.total_net_purchases / NULLIFZERO(pa.months_with_activity)            AS avg_monthly_purchases,
    snp.annual_fee_last,
    snp.annual_fee_last_dt,
    CASE WHEN snp.annual_fee_last > 0 THEN 1 ELSE 0 END                     AS ever_charged_annual_fee,
    fa.total_fees_charged,
    fa.months_with_fees,
    snp.last_loyalty_balance,
    pa.max_loyalty_balance,
    pa.ever_overlimit,
    snp.last_overlimit_cd,
    pa.ever_past_due,
    snp.last_past_due_cd,
    snp.last_status,
    pa.st_bkpt, pa.st_coll, pa.st_frd, pa.st_inv, pa.st_open, pa.st_vol, pa.st_woff,
    pa.first_non_open_dt,
    CAST(pa.first_non_open_dt AS DATE) - CAST(r.treatmt_start_dt AS DATE)   AS days_to_status_change
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp r

LEFT JOIN (
    SELECT
        acct_no,
        MIN(dt_record_ext)                                  AS first_extract_dt,
        MAX(dt_record_ext)                                  AS last_extract_dt,
        MAX(acct_open_dt)                                   AS acct_open_dt,
        MAX(acct_cls_dt)                                    AS acct_cls_dt,
        COUNT(DISTINCT me_dt)                               AS months_with_activity,
        COUNT(DISTINCT visa_prod_cd)                        AS n_uniq_visa,
        SUM(net_prch_amt_dly)                               AS total_net_purchases,
        MAX(lylty_bal_amt)                                  AS max_loyalty_balance,
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
                 THEN 1 ELSE 0 END)                         AS ever_overlimit,
        MAX(CASE WHEN cd_curr_pst_due IS NOT NULL
                  AND cd_curr_pst_due NOT IN ('', 'N', '0')
                 THEN 1 ELSE 0 END)                         AS ever_past_due
    FROM pcq_pw
    GROUP BY acct_no
) pa ON pa.acct_no = r.acct_no

LEFT JOIN (
    SELECT
        acct_no,
        visa_prod_cd         AS last_visa_prod_cd,
        bal_current          AS last_balance,
        accum_dly_bal_mtd    AS last_avg_daily_bal_mtd,
        acct_status          AS last_status,
        cd_curr_ovrlmt       AS last_overlimit_cd,
        cd_curr_pst_due      AS last_past_due_cd,
        lylty_bal_amt        AS last_loyalty_balance,
        lst_ann_fee_chrg_amt AS annual_fee_last,
        lst_ann_fee_dt       AS annual_fee_last_dt
    FROM pcq_pw
    WHERE rn_last = 1
) snp ON snp.acct_no = r.acct_no

LEFT JOIN (
    SELECT
        acct_no,
        visa_prod_cd AS booked_visa_prod_cd
    FROM pcq_pw
    WHERE rn_first = 1
) bk ON bk.acct_no = r.acct_no

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
