-- PCD deployment 2026111PCD — vintage curves dataset
-- Output: one row per (tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value, metric, vintage_day)
-- Metrics: conversion | mobile_view | mobile_click_p | mobile_click_n. Denominator: cohort_size (all metrics).
-- act_ctl_seg is a permanent column, not a slicer. Run in Starburst (Trino).
-- Action/Control derived from tst_grp_cd suffix (%T/%C); see action_control reference memory for mapping caveat.

-- DIAGNOSTIC (run separately): GA4 it_item_name distribution for PCD 2026111 mobile-deployed clients.
-- Confirms whether the 4 known PCD promo names exhaust mobile traffic or if there are other creatives.
/*
SELECT g.it_item_name, COUNT(DISTINCT TRY_CAST(g.up_srf_id2_value AS BIGINT)) AS unique_clients, COUNT(*) AS events
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp c
INNER JOIN edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    ON TRY_CAST(g.up_srf_id2_value AS BIGINT) = c.clnt_no
WHERE c.tactic_id_parent = '2026111PCD'
  AND c.channel_deploy_mb = 'Y'
  AND g.year = '2026' AND g.month IN ('04','05','06')
  AND g.event_name IN ('view_promotion','select_promotion')
GROUP BY g.it_item_name
ORDER BY unique_clients DESC;
*/

-- DIAGNOSTIC (run separately): distinct tst_grp_cd values for the cohort.
-- Reveals what falls into the 'OTHER' bucket (codes that don't suffix in T/C).
-- A (campaign_id, tst_grp_cd) -> action_control mapping exists for the non-T/C codes.
-- Once that mapping is in hand, replace the 'OTHER' branch above with a CASE list.
/*
SELECT
    t.tst_grp_cd,
    CASE
        WHEN TRIM(t.tst_grp_cd) LIKE '%T' THEN 'TEST'
        WHEN TRIM(t.tst_grp_cd) LIKE '%C' THEN 'CONTROL'
        WHEN t.tst_grp_cd IS NULL          THEN '(no_match)'
        ELSE 'OTHER'
    END AS derived_act_ctl,
    COUNT(DISTINCT t.clnt_no) AS clients,
    COUNT(*)                  AS n_rows
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.tactic_id = '2026111PCD'
  AND t.treatmt_strt_dt >= DATE '2026-04-01'
GROUP BY t.tst_grp_cd
ORDER BY clients DESC;
*/

-- tactic_tst_grp: one tst_grp_cd per client from the tactic event table
WITH tactic_tst_grp AS (
    SELECT
        clnt_no,
        MAX(tst_grp_cd) AS tst_grp_cd          -- if a client appears multiple times for the tactic, MAX picks deterministically
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026111PCD'
      AND treatmt_strt_dt >= DATE '2026-04-01'
    GROUP BY clnt_no
),

-- cohort: one row per acct in deployment 2026111PCD
cohort AS (
    SELECT
        c.acct_no,
        c.clnt_no,
        '2026111PCD'                                        AS tactic_id,
        DATE_TRUNC('month', c.response_start)               AS cohort_yyyymm,
        c.response_start,
        c.channel_deploy_mb,
        CASE
            WHEN TRIM(t.tst_grp_cd) LIKE '%T' THEN 'TEST'
            WHEN TRIM(t.tst_grp_cd) LIKE '%C' THEN 'CONTROL'
            WHEN t.tst_grp_cd IS NULL          THEN '(no_match)'
            ELSE 'OTHER'
        END                                                 AS act_ctl_seg,
        c.product_at_decision,
        c.target_product,
        c.responder_anyproduct,
        c.success_dt_1
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp c
    LEFT JOIN tactic_tst_grp t ON t.clnt_no = c.clnt_no
    WHERE c.tactic_id_parent = '2026111PCD'
),

-- cohort_stacked: fan out to 4 rotating slicer blocks; act_ctl_seg carried as permanent column
cohort_stacked AS (
    SELECT acct_no, clnt_no, tactic_id, cohort_yyyymm, act_ctl_seg, response_start, channel_deploy_mb,
           responder_anyproduct, success_dt_1,
           'overall'              AS slicer_dim,
           'ALL'                  AS slicer_value
    FROM cohort

    UNION ALL

    SELECT acct_no, clnt_no, tactic_id, cohort_yyyymm, act_ctl_seg, response_start, channel_deploy_mb,
           responder_anyproduct, success_dt_1,
           'product_at_decision'  AS slicer_dim,
           COALESCE(product_at_decision, '(null)') AS slicer_value
    FROM cohort

    UNION ALL

    SELECT acct_no, clnt_no, tactic_id, cohort_yyyymm, act_ctl_seg, response_start, channel_deploy_mb,
           responder_anyproduct, success_dt_1,
           'target_product'       AS slicer_dim,
           COALESCE(target_product, '(null)') AS slicer_value
    FROM cohort

    UNION ALL

    SELECT acct_no, clnt_no, tactic_id, cohort_yyyymm, act_ctl_seg, response_start, channel_deploy_mb,
           responder_anyproduct, success_dt_1,
           'channel_deploy_mb'    AS slicer_dim,
           COALESCE(channel_deploy_mb, '(null)') AS slicer_value
    FROM cohort
),

-- conversion_events: first conversion date per acct (from curated flags)
conversion_events AS (
    SELECT
        acct_no,
        tactic_id,
        cohort_yyyymm,
        act_ctl_seg,
        slicer_dim,
        slicer_value,
        DATE_DIFF('day', response_start, success_dt_1) AS event_day,
        'conversion'                                    AS metric
    FROM cohort_stacked
    WHERE responder_anyproduct = 1
      AND success_dt_1 IS NOT NULL
      AND DATE_DIFF('day', response_start, success_dt_1) BETWEEN 0 AND 60
),

-- ga4_raw: pull GA4 banner events for PCD mobile cohort, both event types + creative name
-- mobile_click_p/_n split by it_creative_name _m0 suffix; pattern sourced from production tracker query
ga4_raw AS (
    SELECT
        TRY_CAST(g.up_srf_id2_value AS BIGINT) AS clnt_no,
        g.event_name,
        g.event_date,
        g.it_creative_name
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    WHERE g.year  = '2026'
      AND g.month IN ('04','05','06')
      AND g.event_name IN ('view_promotion','select_promotion')
      AND g.it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND g.up_srf_id2_value IS NOT NULL
),

-- ga4_first: first view, first positive click, first negative click per clnt_no
ga4_first AS (
    SELECT
        clnt_no,
        MIN(CASE WHEN event_name = 'view_promotion'                                           THEN event_date END) AS first_view_dt,
        MIN(CASE WHEN event_name = 'select_promotion' AND LOWER(it_creative_name) NOT LIKE '%_m0%' THEN event_date END) AS first_click_p_dt,
        MIN(CASE WHEN event_name = 'select_promotion' AND LOWER(it_creative_name)     LIKE '%_m0%' THEN event_date END) AS first_click_n_dt
    FROM ga4_raw
    GROUP BY clnt_no
),

-- mobile_events: join first GA4 events back to cohort_stacked (mobile-deployed only)
mobile_events AS (
    SELECT
        cs.acct_no,
        cs.tactic_id,
        cs.cohort_yyyymm,
        cs.act_ctl_seg,
        cs.slicer_dim,
        cs.slicer_value,
        cs.response_start,
        gf.first_view_dt,
        gf.first_click_p_dt,
        gf.first_click_n_dt
    FROM cohort_stacked cs
    INNER JOIN ga4_first gf ON gf.clnt_no = cs.clnt_no
    WHERE cs.channel_deploy_mb = 'Y'
),

-- mobile_view_events: vintage_day for first banner view
mobile_view_events AS (
    SELECT
        acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value,
        DATE_DIFF('day', response_start, first_view_dt) AS event_day,
        'mobile_view'                                    AS metric
    FROM mobile_events
    WHERE first_view_dt IS NOT NULL
      AND first_view_dt >= response_start
      AND DATE_DIFF('day', response_start, first_view_dt) BETWEEN 0 AND 60
),

-- mobile_click_p_events: vintage_day for first positive click (creative NOT _m0)
mobile_click_p_events AS (
    SELECT
        acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value,
        DATE_DIFF('day', response_start, first_click_p_dt) AS event_day,
        'mobile_click_p'                                    AS metric
    FROM mobile_events
    WHERE first_click_p_dt IS NOT NULL
      AND first_click_p_dt >= response_start
      AND DATE_DIFF('day', response_start, first_click_p_dt) BETWEEN 0 AND 60
),

-- mobile_click_n_events: vintage_day for first negative click (creative LIKE %_m0%)
mobile_click_n_events AS (
    SELECT
        acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value,
        DATE_DIFF('day', response_start, first_click_n_dt) AS event_day,
        'mobile_click_n'                                    AS metric
    FROM mobile_events
    WHERE first_click_n_dt IS NOT NULL
      AND first_click_n_dt >= response_start
      AND DATE_DIFF('day', response_start, first_click_n_dt) BETWEEN 0 AND 60
),

-- all_events: union all four metric sources
all_events AS (
    SELECT acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value, event_day, metric
    FROM conversion_events
    UNION ALL
    SELECT acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value, event_day, metric
    FROM mobile_view_events
    UNION ALL
    SELECT acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value, event_day, metric
    FROM mobile_click_p_events
    UNION ALL
    SELECT acct_no, tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value, event_day, metric
    FROM mobile_click_n_events
),

-- denominators: one cohort_size per (tactic, cohort, act_ctl_seg, slicer) cell
denominators AS (
    SELECT
        tactic_id,
        cohort_yyyymm,
        act_ctl_seg,
        slicer_dim,
        slicer_value,
        COUNT(DISTINCT acct_no) AS cohort_size
    FROM cohort_stacked
    GROUP BY tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value
),

-- daily_counts: accts whose first event for this metric landed on this exact vintage_day
daily_counts AS (
    SELECT
        tactic_id,
        cohort_yyyymm,
        act_ctl_seg,
        slicer_dim,
        slicer_value,
        metric,
        event_day                       AS vintage_day,
        COUNT(DISTINCT acct_no)         AS n_first_events
    FROM all_events
    GROUP BY tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value, metric, event_day
),

-- metric_universe: hardcoded list of all 4 metrics — ensures zero-event combos still appear
metric_universe AS (
    SELECT 'conversion'     AS metric UNION ALL
    SELECT 'mobile_view'              UNION ALL
    SELECT 'mobile_click_p'           UNION ALL
    SELECT 'mobile_click_n'
),

-- slicer_universe: every (tactic, cohort, act_ctl, slicer) cell that exists in cohort, regardless of events
slicer_universe AS (
    SELECT DISTINCT tactic_id, cohort_yyyymm, act_ctl_seg, slicer_dim, slicer_value
    FROM cohort_stacked
),

-- scaffold: full day spine 0..60 for every (cohort × act_ctl × slicer × metric) cell
scaffold AS (
    SELECT
        s.tactic_id,
        s.cohort_yyyymm,
        s.act_ctl_seg,
        s.slicer_dim,
        s.slicer_value,
        m.metric,
        d.vintage_day
    FROM slicer_universe s
    CROSS JOIN metric_universe m
    CROSS JOIN UNNEST(SEQUENCE(0, 60)) AS d(vintage_day)
),

-- cumulative: join daily counts onto scaffold, zero-fill, window cumulative sum
cumulative AS (
    SELECT
        s.tactic_id,
        s.cohort_yyyymm,
        s.act_ctl_seg,
        s.slicer_dim,
        s.slicer_value,
        s.metric,
        s.vintage_day,
        SUM(COALESCE(dc.n_first_events, 0)) OVER (
            PARTITION BY s.tactic_id, s.cohort_yyyymm, s.act_ctl_seg, s.slicer_dim, s.slicer_value, s.metric
            ORDER BY s.vintage_day
            ROWS UNBOUNDED PRECEDING
        )                                    AS cum_responders
    FROM scaffold s
    LEFT JOIN daily_counts dc
        ON  dc.tactic_id     = s.tactic_id
        AND dc.cohort_yyyymm = s.cohort_yyyymm
        AND dc.act_ctl_seg   = s.act_ctl_seg
        AND dc.slicer_dim    = s.slicer_dim
        AND dc.slicer_value  = s.slicer_value
        AND dc.metric        = s.metric
        AND dc.vintage_day   = s.vintage_day
)

-- final output
SELECT
    c.tactic_id,
    c.cohort_yyyymm,
    c.act_ctl_seg,
    c.slicer_dim,
    c.slicer_value,
    c.metric,
    c.vintage_day,
    c.cum_responders,
    d.cohort_size
FROM cumulative c
INNER JOIN denominators d
    ON  d.tactic_id     = c.tactic_id
    AND d.cohort_yyyymm = c.cohort_yyyymm
    AND d.act_ctl_seg   = c.act_ctl_seg
    AND d.slicer_dim    = c.slicer_dim
    AND d.slicer_value  = c.slicer_value
ORDER BY c.tactic_id, c.cohort_yyyymm, c.act_ctl_seg, c.slicer_dim, c.slicer_value, c.metric, c.vintage_day;
