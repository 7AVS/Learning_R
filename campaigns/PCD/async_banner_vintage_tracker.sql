-- Async banner vintage tracker — CTU, O2P, PCD (one unified pipeline)
-- Engine: Starburst (Trino).
-- Output grain: campaign × cohort_month × segment × segment_level × test_control_flag × vintage_day.
--   segment='ALL'/segment_level='OVERALL'  → rollup across products (all 3 campaigns)
--   segment='PRODUCT'/segment_level=<mne>   → per-product (PCD only — CTU/O2P have no product split)
-- CTU/O2P have no test/control split → test_control_flag = 'ALL'.

WITH
vintage_days AS (
    SELECT seq AS vintage_day
    FROM UNNEST(SEQUENCE(0, 60)) AS t(seq)
),

-- ── Recipient cohorts ──────────────────────────────────────────────────────────
-- One row per (campaign, tactic_id, clnt_no, treatmt_strt_dt) raw event;
-- is_mobile flags whether this client was a mobile-channel recipient.
-- Note: PCD restricts to mobile recipients in the source filter (preserved from
-- original), so PCD total_pop == mobile_pop. CTU/O2P keep them separate.

cohort_raw AS (
    -- CTU
    SELECT
        'CTU'                                                                      AS campaign,
        clnt_no,
        treatmt_strt_dt,
        CAST('OVERALL' AS VARCHAR)                                                 AS product_mnemonic,
        CAST('ALL'     AS VARCHAR)                                                 AS test_control_flag,
        CASE WHEN SUBSTRING(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'
             THEN 1 ELSE 0 END                                                     AS is_mobile
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'

    UNION ALL

    -- O2P
    SELECT
        'O2P',
        clnt_no,
        treatmt_strt_dt,
        CAST('OVERALL' AS VARCHAR),
        CAST('ALL'     AS VARCHAR),
        CASE WHEN TRIM(tactic_cell_cd) LIKE '%IMN%' THEN 1 ELSE 0 END
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('202609902P','202612602P','202613202P')
      AND treatmt_strt_dt >= DATE '2026-04-01'

    UNION ALL

    -- PCD  (source filter restricts to mobile recipients only — preserved from original)
    SELECT
        'PCD',
        clnt_no,
        treatmt_strt_dt,
        element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 4)
                                                                                   AS product_mnemonic,
        CASE
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%C' THEN 'CONTROL'
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%T' THEN 'TEST'
            ELSE 'OTHER'
        END                                                                        AS test_control_flag,
        1                                                                          AS is_mobile
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD','2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 3)
          IN ('MBC8YU53','MA02BC35','MA02ED01','MFB8L6X6','MF88UJPY','MF89BX97','MF89HY07')
      AND element_at(
            split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '),
            cardinality(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '))
          ) LIKE '%MB%'
),

-- One row per (campaign, clnt_no, treatmt_strt_dt) within the cohort key
cohort AS (
    SELECT
        campaign,
        clnt_no,
        treatmt_strt_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        product_mnemonic,
        test_control_flag,
        MAX(is_mobile) AS is_mobile
    FROM cohort_raw
    GROUP BY 1,2,3,4,5,6
),

population AS (
    SELECT
        campaign, cohort_month, product_mnemonic, test_control_flag,
        COUNT(DISTINCT clnt_no)                                          AS total_population,
        COUNT(DISTINCT CASE WHEN is_mobile = 1 THEN clnt_no END)         AS mobile_population
    FROM cohort
    GROUP BY 1,2,3,4
),

-- ── GA4 events ────────────────────────────────────────────────────────────────
-- NOTE: PCD uses it_item_name (legacy); CTU/O2P use it_item_id (preferred). Open item.

events AS (
    -- CTU
    SELECT
        'CTU'                                                            AS campaign,
        event_date,
        event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END                                                              AS lead_class,
        TRY_CAST(up_srf_id2_value AS BIGINT)                             AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion','select_promotion')

    UNION ALL

    -- O2P
    SELECT
        'O2P',
        event_date,
        event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END,
        TRY_CAST(up_srf_id2_value AS BIGINT)
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion','select_promotion')

    UNION ALL

    -- PCD  (item filter on it_item_name; COALESCE up_srf_id2_value + ep_srf_id2 — both preserved from original)
    SELECT
        'PCD',
        event_date,
        event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END,
        COALESCE(TRY_CAST(up_srf_id2_value AS BIGINT), TRY_CAST(ep_srf_id2 AS BIGINT))
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

-- ── Attribution ───────────────────────────────────────────────────────────────
-- Inner-join mobile recipients to events within 0–60 day window from their wave start.
-- vintage_day is computed wave-relative; cohort_month rolls waves up to deployment month.
-- COUNT(DISTINCT clnt_no) at month grain naturally dedups clients spanning multiple waves.

attributed AS (
    SELECT
        c.campaign,
        c.cohort_month,
        c.product_mnemonic,
        c.test_control_flag,
        c.clnt_no,
        e.event_name,
        e.lead_class,
        date_diff('day', c.treatmt_strt_dt, e.event_date) AS vintage_day
    FROM cohort c
    INNER JOIN events e
        ON  e.campaign    = c.campaign
        AND e.clnt_no     = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt
                             AND date_add('day', 60, c.treatmt_strt_dt)
    WHERE c.is_mobile = 1
),

daily_metrics AS (
    SELECT
        campaign, cohort_month, product_mnemonic, test_control_flag, vintage_day,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'view_promotion'   THEN clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(event_name) = 'select_promotion' THEN clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_p'                 THEN clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN lead_class = 'click_n'                 THEN clnt_no END) AS leads_n
    FROM attributed
    GROUP BY 1,2,3,4,5
),

-- ── Vintage spine ─────────────────────────────────────────────────────────────
spine AS (
    SELECT
        p.campaign, p.cohort_month, p.product_mnemonic, p.test_control_flag,
        v.vintage_day,
        p.total_population,
        p.mobile_population
    FROM population p
    CROSS JOIN vintage_days v
),

base AS (
    SELECT
        s.campaign,
        s.cohort_month,
        s.product_mnemonic,
        s.test_control_flag,
        s.vintage_day,
        s.total_population,
        s.mobile_population,
        COALESCE(d.view_users,  0) AS view_users,
        COALESCE(d.click_users, 0) AS click_users,
        COALESCE(d.leads_p,     0) AS leads_p,
        COALESCE(d.leads_n,     0) AS leads_n
    FROM spine s
    LEFT JOIN daily_metrics d
        ON  d.campaign          = s.campaign
        AND d.cohort_month      = s.cohort_month
        AND d.product_mnemonic  = s.product_mnemonic
        AND d.test_control_flag = s.test_control_flag
        AND d.vintage_day       = s.vintage_day
),

-- ── Two output grains ─────────────────────────────────────────────────────────
--   ALL rollup (across products) — always emitted
--   PRODUCT (per product)        — emitted only for PCD (CTU/O2P have a single product='OVERALL')

final_grain AS (
    SELECT
        campaign,
        cohort_month                   AS cohort,
        'ALL'                          AS segment,
        CAST('OVERALL' AS VARCHAR)     AS segment_level,
        test_control_flag,
        vintage_day,
        SUM(total_population)          AS total_population,
        SUM(mobile_population)         AS mobile_population,
        SUM(view_users)                AS view_users,
        SUM(click_users)               AS click_users,
        SUM(leads_p)                   AS leads_p,
        SUM(leads_n)                   AS leads_n
    FROM base
    GROUP BY campaign, cohort_month, test_control_flag, vintage_day

    UNION ALL

    SELECT
        campaign,
        cohort_month                   AS cohort,
        'PRODUCT'                      AS segment,
        product_mnemonic               AS segment_level,
        test_control_flag,
        vintage_day,
        total_population,
        mobile_population,
        view_users,
        click_users,
        leads_p,
        leads_n
    FROM base
    WHERE campaign = 'PCD'
)

SELECT
    campaign,
    cohort,
    segment,
    segment_level,
    test_control_flag,
    vintage_day,
    total_population,
    mobile_population,
    view_users,
    click_users,
    leads_p,
    leads_n,
    SUM(view_users)  OVER w AS view_users_cum,
    SUM(click_users) OVER w AS click_users_cum,
    SUM(leads_p)     OVER w AS leads_p_cum,
    SUM(leads_n)     OVER w AS leads_n_cum
FROM final_grain
WINDOW w AS (
    PARTITION BY campaign, cohort, segment, segment_level, test_control_flag
    ORDER BY vintage_day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY campaign, cohort, segment, segment_level, test_control_flag, vintage_day;
