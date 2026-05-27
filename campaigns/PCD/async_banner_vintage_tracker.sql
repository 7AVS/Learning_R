-- Async banner vintage tracker — PCD, CTU, O2P
-- Engine: Starburst (Trino).
-- Three independent blocks. Each one builds its own vintage curve and can be run on its own.
-- Output schema (PCD):
--   campaign | cohort | segment | segment_level | test_control_flag | vintage_day
--   | total_population | view_users | click_users | leads_p | leads_n
--   | view_users_cum  | click_users_cum  | leads_p_cum | leads_n_cum
-- CTU and O2P additionally carry mobile_population (channel-served count).


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD                                                              ║
-- ║ Eligibility: position-3 of tactic_decisn_vrb_info IN (async allowlist).    ║
-- ║ Scope: tst_grp_cd ends in T or C (in experimental design).                 ║
-- ║ Channel intentionally NOT used — campaign-ID + arm fully define the cohort. ║
-- ║ Emits ALL rollup + PRODUCT-level grain.                                    ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT seq AS vintage_day FROM UNNEST(SEQUENCE(0, 60)) AS t(seq)
),

cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 4) AS product_mnemonic,
        CASE
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%C' THEN 'CONTROL'
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%T' THEN 'TEST'
        END AS test_control_flag
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD','2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 3)
          IN ('MBC8YU53','MA02BC35','MA02ED01','MFB8L6X6','MF88UJPY','MF89BX97','MF89HY07')
      AND (trim(coalesce(tst_grp_cd, '')) LIKE '%T'
           OR trim(coalesce(tst_grp_cd, '')) LIKE '%C')
),

cohort AS (
    SELECT DISTINCT
        clnt_no, treatmt_strt_dt, cohort_month, product_mnemonic, test_control_flag
    FROM cohort_raw
),

population AS (
    SELECT
        cohort_month, product_mnemonic, test_control_flag,
        COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3
),

events AS (
    SELECT
        event_date,
        event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END AS lead_class,
        COALESCE(TRY_CAST(up_srf_id2_value AS BIGINT), TRY_CAST(ep_srf_id2 AS BIGINT)) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-04-01'
      AND year = '2026' AND month IN ('04','05','06')
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

attributed AS (
    SELECT
        c.cohort_month, c.product_mnemonic, c.test_control_flag, c.clnt_no,
        e.event_name, e.lead_class,
        date_diff('day', c.treatmt_strt_dt, e.event_date) AS vintage_day
    FROM cohort c
    INNER JOIN events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt
                             AND date_add('day', 60, c.treatmt_strt_dt)
),

daily_metrics AS (
    SELECT
        cohort_month, product_mnemonic, test_control_flag, vintage_day,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'view_promotion'   THEN clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'select_promotion' THEN clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_p'                 THEN clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_n'                 THEN clnt_no END) AS leads_n
    FROM attributed
    GROUP BY 1,2,3,4
),

spine AS (
    SELECT
        p.cohort_month, p.product_mnemonic, p.test_control_flag,
        v.vintage_day,
        p.total_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.product_mnemonic, s.test_control_flag, s.vintage_day,
        s.total_population,
        COALESCE(d.view_users,  0) AS view_users,
        COALESCE(d.click_users, 0) AS click_users,
        COALESCE(d.leads_p,     0) AS leads_p,
        COALESCE(d.leads_n,     0) AS leads_n
    FROM spine s
    LEFT JOIN daily_metrics d
        ON  d.cohort_month      = s.cohort_month
        AND d.product_mnemonic  = s.product_mnemonic
        AND d.test_control_flag = s.test_control_flag
        AND d.vintage_day       = s.vintage_day
),

final_grain AS (
    -- ALL rollup across products
    SELECT
        cohort_month                   AS cohort,
        CAST('ALL'     AS VARCHAR)     AS segment,
        CAST('OVERALL' AS VARCHAR)     AS segment_level,
        test_control_flag, vintage_day,
        SUM(total_population)          AS total_population,
        SUM(view_users)                AS view_users,
        SUM(click_users)               AS click_users,
        SUM(leads_p)                   AS leads_p,
        SUM(leads_n)                   AS leads_n
    FROM base
    GROUP BY cohort_month, test_control_flag, vintage_day

    UNION ALL

    -- Per-product
    SELECT
        cohort_month                   AS cohort,
        'PRODUCT'                      AS segment,
        product_mnemonic               AS segment_level,
        test_control_flag, vintage_day,
        total_population,
        view_users, click_users, leads_p, leads_n
    FROM base
)

SELECT
    CAST('PCD' AS VARCHAR) AS campaign,
    cohort, segment, segment_level, test_control_flag, vintage_day,
    total_population,
    view_users, click_users, leads_p, leads_n,
    SUM(view_users)  OVER w AS view_users_cum,
    SUM(click_users) OVER w AS click_users_cum,
    SUM(leads_p)     OVER w AS leads_p_cum,
    SUM(leads_n)     OVER w AS leads_n_cum
FROM final_grain
WINDOW w AS (
    PARTITION BY cohort, segment, segment_level, test_control_flag
    ORDER BY vintage_day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY cohort, segment, segment_level, test_control_flag, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU                                                              ║
-- ║ Eligibility: single tactic_id 2026098CTU.                                  ║
-- ║ No test/control split available (channel-only deployment). test_control='ALL'. ║
-- ║ is_mobile derived from TACTIC_DECISN_VRB_INFO substring(121,30) LIKE '%MB%'. ║
-- ║ Emits ALL/OVERALL grain only.                                              ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT seq AS vintage_day FROM UNNEST(SEQUENCE(0, 60)) AS t(seq)
),

cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        CASE WHEN SUBSTRING(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'
             THEN 1 ELSE 0 END AS is_mobile
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

cohort AS (
    SELECT
        clnt_no, treatmt_strt_dt, cohort_month,
        MAX(is_mobile) AS is_mobile
    FROM cohort_raw
    GROUP BY 1,2,3
),

population AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT clnt_no)                                  AS total_population,
        COUNT(DISTINCT CASE WHEN is_mobile = 1 THEN clnt_no END) AS mobile_population
    FROM cohort
    GROUP BY 1
),

events AS (
    SELECT
        event_date,
        event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END AS lead_class,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

attributed AS (
    SELECT
        c.cohort_month, c.clnt_no, e.event_name, e.lead_class,
        date_diff('day', c.treatmt_strt_dt, e.event_date) AS vintage_day
    FROM cohort c
    INNER JOIN events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt
                             AND date_add('day', 60, c.treatmt_strt_dt)
    WHERE c.is_mobile = 1
),

daily_metrics AS (
    SELECT
        cohort_month, vintage_day,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'view_promotion'   THEN clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'select_promotion' THEN clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_p'                 THEN clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_n'                 THEN clnt_no END) AS leads_n
    FROM attributed
    GROUP BY 1,2
),

spine AS (
    SELECT
        p.cohort_month, v.vintage_day,
        p.total_population, p.mobile_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.vintage_day,
        s.total_population, s.mobile_population,
        COALESCE(d.view_users,  0) AS view_users,
        COALESCE(d.click_users, 0) AS click_users,
        COALESCE(d.leads_p,     0) AS leads_p,
        COALESCE(d.leads_n,     0) AS leads_n
    FROM spine s
    LEFT JOIN daily_metrics d
        ON  d.cohort_month = s.cohort_month
        AND d.vintage_day  = s.vintage_day
)

SELECT
    CAST('CTU'     AS VARCHAR) AS campaign,
    cohort_month               AS cohort,
    CAST('ALL'     AS VARCHAR) AS segment,
    CAST('OVERALL' AS VARCHAR) AS segment_level,
    CAST('ALL'     AS VARCHAR) AS test_control_flag,
    vintage_day,
    total_population, mobile_population,
    view_users, click_users, leads_p, leads_n,
    SUM(view_users)  OVER w AS view_users_cum,
    SUM(click_users) OVER w AS click_users_cum,
    SUM(leads_p)     OVER w AS leads_p_cum,
    SUM(leads_n)     OVER w AS leads_n_cum
FROM base
WINDOW w AS (
    PARTITION BY cohort_month
    ORDER BY vintage_day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY cohort_month, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P                                                              ║
-- ║ Eligibility: RPT_GRP_CD IN (9 PO2P reporting groups), tactic_id IN (3).    ║
-- ║ tactic_ids: 2026099O2P, 2026126O2P, 2026132O2P (suffix is letter O).       ║
-- ║ Scope: TST_GRP_CD IN ('TG4','TG7'); TG4=TEST, TG7=CONTROL.                 ║
-- ║ is_mobile derived from TACTIC_CELL_CD LIKE '%MB%' (not a filter).          ║
-- ║ Holdout confirmed as designed 5.00% across all 9 reporting groups.         ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
vintage_days AS (
    SELECT seq AS vintage_day FROM UNNEST(SEQUENCE(0, 60)) AS t(seq)
),

cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        TRIM(rpt_grp_cd) AS rpt_grp_cd,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag,
        CASE WHEN TRIM(tactic_cell_cd) LIKE '%MB%' THEN 1 ELSE 0 END AS is_mobile
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026099O2P','2026126O2P','2026132O2P')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(rpt_grp_cd) IN (
            'PO2PNL01','PO2PNL03','PO2PNL07',
            'PO2POT01','PO2POT03','PO2POT07',
            'PO2PPR01','PO2PPR03','PO2PPR07'
          )
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

cohort AS (
    SELECT
        clnt_no, treatmt_strt_dt, cohort_month, rpt_grp_cd, test_control_flag,
        MAX(is_mobile) AS is_mobile
    FROM cohort_raw
    GROUP BY 1,2,3,4,5
),

population AS (
    SELECT
        cohort_month, rpt_grp_cd, test_control_flag,
        COUNT(DISTINCT clnt_no)                                  AS total_population,
        COUNT(DISTINCT CASE WHEN is_mobile = 1 THEN clnt_no END) AS mobile_population
    FROM cohort
    GROUP BY 1,2,3
),

events AS (
    SELECT
        event_date,
        event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END AS lead_class,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

attributed AS (
    SELECT
        c.cohort_month, c.rpt_grp_cd, c.test_control_flag,
        c.clnt_no, e.event_name, e.lead_class,
        date_diff('day', c.treatmt_strt_dt, e.event_date) AS vintage_day
    FROM cohort c
    INNER JOIN events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt
                             AND date_add('day', 60, c.treatmt_strt_dt)
),

daily_metrics AS (
    SELECT
        cohort_month, rpt_grp_cd, test_control_flag, vintage_day,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'view_promotion'   THEN clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'select_promotion' THEN clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_p'                 THEN clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_n'                 THEN clnt_no END) AS leads_n
    FROM attributed
    GROUP BY 1,2,3,4
),

spine AS (
    SELECT
        p.cohort_month, p.rpt_grp_cd, p.test_control_flag,
        v.vintage_day,
        p.total_population, p.mobile_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.cohort_month, s.rpt_grp_cd, s.test_control_flag, s.vintage_day,
        s.total_population, s.mobile_population,
        COALESCE(d.view_users,  0) AS view_users,
        COALESCE(d.click_users, 0) AS click_users,
        COALESCE(d.leads_p,     0) AS leads_p,
        COALESCE(d.leads_n,     0) AS leads_n
    FROM spine s
    LEFT JOIN daily_metrics d
        ON  d.cohort_month      = s.cohort_month
        AND d.rpt_grp_cd        = s.rpt_grp_cd
        AND d.test_control_flag = s.test_control_flag
        AND d.vintage_day       = s.vintage_day
),

final_grain AS (
    -- ALL rollup across reporting groups
    SELECT
        cohort_month                   AS cohort,
        CAST('ALL'     AS VARCHAR)     AS segment,
        CAST('OVERALL' AS VARCHAR)     AS segment_level,
        test_control_flag, vintage_day,
        SUM(total_population)          AS total_population,
        SUM(mobile_population)         AS mobile_population,
        SUM(view_users)                AS view_users,
        SUM(click_users)               AS click_users,
        SUM(leads_p)                   AS leads_p,
        SUM(leads_n)                   AS leads_n
    FROM base
    GROUP BY cohort_month, test_control_flag, vintage_day

    UNION ALL

    -- Per reporting group
    SELECT
        cohort_month                   AS cohort,
        'REPORT_GROUP'                 AS segment,
        rpt_grp_cd                     AS segment_level,
        test_control_flag, vintage_day,
        total_population, mobile_population,
        view_users, click_users, leads_p, leads_n
    FROM base
)

SELECT
    CAST('O2P' AS VARCHAR) AS campaign,
    cohort, segment, segment_level, test_control_flag, vintage_day,
    total_population, mobile_population,
    view_users, click_users, leads_p, leads_n,
    SUM(view_users)  OVER w AS view_users_cum,
    SUM(click_users) OVER w AS click_users_cum,
    SUM(leads_p)     OVER w AS leads_p_cum,
    SUM(leads_n)     OVER w AS leads_n_cum
FROM final_grain
WINDOW w AS (
    PARTITION BY cohort, segment, segment_level, test_control_flag
    ORDER BY vintage_day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY cohort, segment, segment_level, test_control_flag, vintage_day
;
