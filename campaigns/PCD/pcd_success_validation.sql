-- PCD success validation
-- Validates the campaign-conversion logic against the async-eligible PCD cohort
-- (same eligibility used in async_banner_vintage_tracker.sql block 1).
-- Source SAS logic (PCD Campaign Success) re-implemented in Starburst (Trino) syntax,
-- with DTZV01.TACTIC_EVNT_IP_AR_H60M swapped for DG6V01.TACTIC_EVNT_IP_AR_HIST so we
-- stay on the same tactic event table the vintage tracker uses.
--
-- Success event: first product change within (treatmt_strt_dt - 1, treatmt_end_dt]
-- where the new visa_prod_cd is NOT equal to the FROM product code carried in the
-- packed tactic_decisn_vrb_info string at character positions 42-44.
--
-- ---------------------------------------------------------------------------
-- TODO — primary_success placeholder (suppressed until target_product is sourced)
-- ---------------------------------------------------------------------------
-- The SAS comment block defines two tiers:
--   primary_success   = new_product == upgrade_path_product (from an NBO-supplied
--                       upgrade-path lookup table — not in our environment)
--   secondary_success = new_product != FROM product (any change in window)
-- This validation emits secondary_responders only. When the NBO upgrade-path lookup
-- is sourced, re-enable primary in three steps:
--   1. Add target_product to the cohort CTE (joined from the upgrade-path lookup).
--   2. Add a primary_success CASE to the responders CTE:
--        CASE WHEN target_product IS NOT NULL AND target_product = new_product
--             THEN 1 ELSE 0 END AS primary_success
--   3. Add primary_responders to the seg_counts:
--        COUNT(DISTINCT CASE WHEN primary_success = 1 THEN clnt_no END) AS primary_responders
-- ---------------------------------------------------------------------------

WITH
cohort AS (
    SELECT
        clnt_no,
        visa_acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 4) AS product_mnemonic,
        CASE
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%C' THEN 'CONTROL'
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%T' THEN 'TEST'
        END                                                                                AS test_control_flag,
        SUBSTR(tactic_decisn_vrb_info, 42, 3)                                              AS from_product_code
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD','2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 3)
          IN ('MBC8YU53','MA02BC35','MA02ED01','MFB8L6X6','MF88UJPY','MF89BX97','MF89HY07')
      AND (trim(coalesce(tst_grp_cd, '')) LIKE '%T'
           OR trim(coalesce(tst_grp_cd, '')) LIKE '%C')
),

-- Product change events from dly_full_portfolio, attributed to cohort recipients
-- via visa_acct_no within the (treatmt_strt_dt - 1, treatmt_end_dt] window.
-- Single scan of DFP (per the cards-pod rule against multi-scan volatile builds).
product_changes AS (
    SELECT
        c.clnt_no,
        c.visa_acct_no,
        c.treatmt_strt_dt,
        c.product_mnemonic,
        c.test_control_flag,
        dfp.visa_prod_cd                                       AS new_product,
        dfp.dt_record_ext                                      AS change_dt
    FROM cohort c
    INNER JOIN D3CV12A.dly_full_portfolio dfp
        ON  dfp.acct_no       = c.visa_acct_no
        AND dfp.dt_record_ext BETWEEN date_add('day', -1, c.treatmt_strt_dt)
                                  AND c.treatmt_end_dt
        AND dfp.visa_prod_cd <> c.from_product_code
),

-- One row per (acct, parent tactic, wave): first qualifying product change
responders AS (
    SELECT
        clnt_no,
        visa_acct_no,
        treatmt_strt_dt,
        product_mnemonic,
        test_control_flag,
        MIN(change_dt) AS first_change_dt
    FROM product_changes
    GROUP BY 1,2,3,4,5
),

seg_counts AS (
    -- ALL grain (seg_counts across products)
    SELECT
        CAST('ALL'     AS VARCHAR(50)) AS segment,
        CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
        c.test_control_flag,
        COUNT(DISTINCT c.clnt_no) AS cohort_size,
        COUNT(DISTINCT r.clnt_no) AS secondary_responders
    FROM cohort c
    LEFT JOIN responders r
        ON  r.clnt_no           = c.clnt_no
        AND r.visa_acct_no      = c.visa_acct_no
        AND r.treatmt_strt_dt   = c.treatmt_strt_dt
        AND r.product_mnemonic  = c.product_mnemonic
        AND r.test_control_flag = c.test_control_flag
    GROUP BY c.test_control_flag

    UNION ALL

    -- PRODUCT grain (per product_mnemonic)
    SELECT
        'PRODUCT'          AS segment,
        c.product_mnemonic AS segment_level,
        c.test_control_flag,
        COUNT(DISTINCT c.clnt_no),
        COUNT(DISTINCT r.clnt_no)
    FROM cohort c
    LEFT JOIN responders r
        ON  r.clnt_no           = c.clnt_no
        AND r.visa_acct_no      = c.visa_acct_no
        AND r.treatmt_strt_dt   = c.treatmt_strt_dt
        AND r.product_mnemonic  = c.product_mnemonic
        AND r.test_control_flag = c.test_control_flag
    GROUP BY c.product_mnemonic, c.test_control_flag
)

SELECT
    segment,
    segment_level,
    test_control_flag,
    cohort_size,
    secondary_responders
FROM seg_counts
ORDER BY segment, segment_level, test_control_flag
;
