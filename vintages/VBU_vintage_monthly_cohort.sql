-- Campaign : VBU (Visa Benefit Upgrade)
-- Source   : DG6V01.TACTIC_EVNT_IP_AR_HIST (population) + d3cv12a.cr_crd_rpts_acct (ME snapshot)
--            + D3CV12A.dly_full_portfolio (product changes) + D3CV12A.dly_full_portfolio (prior-AIB check)
-- Success  : Primary = first product change to 'AIB', excluding clients already holding AIB before
--            treatmt_strt_dt, no flip-backs, excluding no-ops.
--            (visa_prod_cd = 'AIB' AND new_product != prod_me_before_launch AND
--             new_product != from_product_code AND NOT prior AIB holder)
--
-- PROVENANCE: Logic adapted from Daniel Chin's reference in campaigns/VBA_VBU/vbu_vintage_original.sql
--   (Teradata-original; converted to Trino syntax here). The curated table
--   cards_bizups_vbu_descresp_clnt also exists and carries dt_prod_change_client +
--   responder_targetproduct pre-built, but uses response_start (not treatmt_strt_dt) as the
--   treatment date and does not expose the monthly-cohort grain needed here. Raw-table approach
--   is the correct choice for this file. vba_vbu_vintage.sql is a PLACEHOLDER — not an
--   authoritative source.
--
-- Anchor   : treatmt_strt_dt
-- Arm      : tst_grp_cd (raw)
-- Engine   : Starburst/Trino (federation)
-- Range    : treatmt_strt_dt >= 2026-01-01

WITH pop AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))                              AS tactic_id,
        E.treatmt_strt_dt,
        E.treatmt_end_dt,
        E.addnl_data_dt,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE SUBSTR(CAST(E.tactic_id AS VARCHAR(50)), 8, 3) = 'VBU'
      AND SUBSTR(CAST(E.tactic_id AS VARCHAR(50)), 8, 1) <> 'J'
      AND E.treatmt_strt_dt >= DATE '2026-01-01'
),

cohort_months AS (
    SELECT
        clnt_no,
        tactic_id,
        tst_grp_cd,
        treatmt_strt_dt,
        treatmt_end_dt,
        addnl_data_dt,
        date_trunc('month', treatmt_strt_dt)                          AS cohort_month,
        SUBSTR(tst_grp_cd, 6, 3)                                      AS from_product_code
    FROM pop
),

-- ME snapshot: client's product at the month-end before launch
me_snap AS (
    SELECT
        cm.clnt_no,
        cm.tactic_id,
        cm.tst_grp_cd,
        cm.treatmt_strt_dt,
        cm.treatmt_end_dt,
        cm.cohort_month,
        cm.from_product_code,
        A.acct_no,
        A.prod_cd_current                                             AS prod_me_before_launch
    FROM cohort_months cm
    INNER JOIN d3cv12a.cr_crd_rpts_acct A
        ON  A.clnt_no = cm.clnt_no
        -- Trino date_add for month-end before addnl_data_dt
        AND A.ME_dt   = date_add('day', -1, date_trunc('month', cm.addnl_data_dt))
        AND A.status  = 'OPEN'
        AND (
            (A.prod_cd_current = cm.from_product_code AND cm.tst_grp_cd <> 'XX')
            OR (A.prod_cd_current IN ('C00', 'C01', 'C02') AND cm.tst_grp_cd = 'XX')
        )
),

-- Product changes in-window (no no-ops, no flip-backs)
acct_changes AS (
    SELECT
        e.clnt_no,
        e.tactic_id,
        e.tst_grp_cd,
        e.cohort_month,
        e.acct_no,
        e.prod_me_before_launch,
        e.from_product_code,
        e.treatmt_strt_dt,
        d.visa_prod_cd                                                AS new_product,
        d.DT_record_ext                                               AS dt_prod_change
    FROM me_snap e
    INNER JOIN D3CV12A.dly_full_portfolio d
        ON  d.acct_no       = e.acct_no
        AND d.DT_record_ext >= e.treatmt_strt_dt - INTERVAL '1' DAY
        AND d.DT_record_ext >= DATE '2026-01-01'
        AND d.DT_record_ext <= e.treatmt_end_dt + INTERVAL '5' DAY
        AND d.visa_prod_cd <> e.prod_me_before_launch
        AND d.visa_prod_cd <> e.from_product_code
),

-- Prior AIB holders (exclude from primary)
prior_aib AS (
    SELECT DISTINCT
        e.clnt_no,
        e.tactic_id,
        e.acct_no
    FROM me_snap e
    INNER JOIN D3CV12A.dly_full_portfolio d
        ON  d.acct_no       = e.acct_no
        AND d.visa_prod_cd  = 'AIB'
        AND d.dt_record_ext < e.treatmt_strt_dt
        AND d.dt_record_ext >= DATE '2025-01-01'
),

-- First AIB change per client, excluding prior AIB holders
first_success AS (
    SELECT
        a.clnt_no,
        a.cohort_month,
        a.tst_grp_cd                                                  AS arm,
        MIN(a.dt_prod_change)                                         AS first_success_dt,
        a.treatmt_strt_dt
    FROM acct_changes a
    LEFT JOIN prior_aib p
        ON  p.clnt_no   = a.clnt_no
        AND p.tactic_id = a.tactic_id
        AND p.acct_no   = a.acct_no
    WHERE a.new_product = 'AIB'
      AND p.acct_no IS NULL
    GROUP BY a.clnt_no, a.cohort_month, a.tst_grp_cd, a.treatmt_strt_dt
),

cohort_size AS (
    SELECT
        cohort_month,
        tst_grp_cd                                                    AS arm,
        COUNT(DISTINCT clnt_no)                                       AS cohort_size
    FROM cohort_months
    GROUP BY 1, 2
),

vintage_days_raw AS (
    SELECT
        cohort_month,
        arm,
        date_diff('day', treatmt_strt_dt, first_success_dt)          AS vintage_day,
        COUNT(DISTINCT clnt_no)                                       AS new_events
    FROM first_success
    WHERE date_diff('day', treatmt_strt_dt, first_success_dt) BETWEEN 0 AND 180
    GROUP BY 1, 2, 3
),

spine AS (
    SELECT
        cs.cohort_month,
        cs.arm,
        cs.cohort_size,
        t.vintage_day
    FROM cohort_size cs
    CROSS JOIN UNNEST(sequence(0, 180)) AS t(vintage_day)
),

joined AS (
    SELECT
        s.cohort_month,
        s.arm,
        s.vintage_day,
        s.cohort_size,
        COALESCE(r.new_events, 0)                                     AS new_events
    FROM spine s
    LEFT JOIN vintage_days_raw r
        ON  r.cohort_month = s.cohort_month
        AND r.arm          = s.arm
        AND r.vintage_day  = s.vintage_day
)

SELECT
    CAST('VBU' AS VARCHAR(10))                                        AS campaign,
    cohort_month,
    arm,
    vintage_day,
    cohort_size,
    SUM(new_events) OVER (
        PARTITION BY cohort_month, arm
        ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                                                 AS cum_events
FROM joined
ORDER BY cohort_month, arm, vintage_day
;
