-- AUH base + success + engagement + usage (Starburst/Trino)
-- One BASE (both phases, full targeted population = denominator) + satellites:
--   success    = AU-add EVENT (CR_CRD_ACCT_EVNT_DLY 191/3), first add inside treatment window
--   engagement = GA4 OLB banner (Phase 2 only; Phase 1 was email-only so cols read 0)
--   usage      = DLY_FULL_PORTFOLIO monthly spend/balance post-add (converters only)
-- it_item_id list assumes 'i_' || Salesforce offer id — confirm via auh_ga4_banner_discovery.sql.
-- click_p for AUH is PENDING: positive labels are campaign-specific. Run Q4 diagnostic to find
-- AUH's it_creative_name values, then lock the IN-list. click_n labels are generic and reused.
-- Phase 2 arm IN-list spellings inferred from pattern (prefix NR/RN/RO + 3rd char R/M/W).


-- Q1: aggregated crosstab — phase x arm x model x test_group x offered product
WITH base AS (
    SELECT
        clnt_no,
        TRY_CAST(TRIM(TACTIC_EVNT_ID) AS BIGINT) AS acct_no,
        tactic_id,
        CASE tactic_id WHEN '2026042AUH' THEN 'Phase1' WHEN '2026119AUH' THEN 'Phase2' END AS phase,
        treatmt_strt_dt, treatmt_end_dt,
        CASE WHEN RIGHT(TRIM(tst_grp_cd),2)='_C' THEN 'Control' ELSE 'Test' END AS test_group,
        CASE
            WHEN tactic_id='2026042AUH' THEN
                CASE WHEN TRIM(tst_grp_cd) IN ('NRGA','NRGA_C','NRR','NRR_C','NRS','NRS_C')
                     THEN 'NonReward' ELSE 'Unknown' END
            WHEN tactic_id='2026119AUH' THEN
                CASE WHEN SUBSTR(tst_grp_cd,1,3) IN ('NRR','NRM','NRW') THEN 'NonReward'
                     WHEN SUBSTR(tst_grp_cd,1,3) IN ('RNR','RNM','RNW') THEN 'Rewards_No_Offer'
                     WHEN SUBSTR(tst_grp_cd,1,3) IN ('ROR','ROM','ROW') THEN 'Rewards_Offer'
                     ELSE 'Unknown' END
            ELSE 'Unknown'
        END AS strategy_arm,
        CASE
            WHEN tactic_id='2026042AUH' THEN
                CASE WHEN TRIM(tst_grp_cd) IN ('NRGA','NRGA_C') THEN 'Web'
                     WHEN TRIM(tst_grp_cd) IN ('NRR','NRR_C') THEN 'Random'
                     WHEN TRIM(tst_grp_cd) IN ('NRS','NRS_C') THEN 'Model'
                     ELSE 'Unknown' END
            WHEN tactic_id='2026119AUH' THEN
                CASE WHEN SUBSTR(tst_grp_cd,3,1)='R' THEN 'Random'
                     WHEN SUBSTR(tst_grp_cd,3,1)='M' THEN 'Model'
                     WHEN SUBSTR(tst_grp_cd,3,1)='W' THEN 'Web'
                     ELSE 'Unknown' END
            ELSE 'Unknown'
        END AS model_arm,
        SUBSTR(tactic_decisn_vrb_info,21,3) AS prod_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH','2026119AUH')
),
au_event AS (
    SELECT a.acct_no, c.visa_prod_cd AS prod_cd, a.evnt_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
        ON  a.clnt_no = c.clnt_no
        AND a.evnt_dt = c.dt_record_ext
        AND a.acct_no = c.acct_no
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD = 3
      AND a.evnt_dt >= DATE '2026-01-01'
),
success AS (
    SELECT b.acct_no, b.treatmt_strt_dt,
           MIN(e.evnt_dt)                                                          AS first_add_dt,
           COUNT(*)                                                                AS add_events,
           COUNT(DISTINCT e.prod_cd)                                               AS add_products,
           MIN(CASE WHEN TRIM(e.prod_cd) = TRIM(b.prod_cd) THEN e.evnt_dt END)     AS first_target_add_dt
    FROM base b
    INNER JOIN au_event e
        ON  e.acct_no = b.acct_no
        AND e.evnt_dt BETWEEN b.treatmt_strt_dt AND b.treatmt_end_dt
    GROUP BY 1, 2
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN lower(event_name) = 'view_promotion'   THEN 1 ELSE 0 END AS view_e,
        CASE WHEN lower(event_name) = 'select_promotion' THEN 1 ELSE 0 END AS click_e,
        CASE WHEN lower(event_name) = 'select_promotion'
              AND it_creative_name IN ('n_Non intéressé','n_Not interested','n_Not now','n_Pas maintenant',
                                       'Not now','Pas maintenant','n_close','Close')
             THEN 1 ELSE 0 END AS click_n_e
        -- click_p_e: pending AUH-specific positive labels (Q4 diagnostic)
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND event_date >= DATE '2026-04-30'
      AND it_item_id IN ('i_300108','i_308317','i_308314','i_308315',
                         'i_308333','i_308334','i_308335','i_308336')
),
dep AS (
    SELECT
        b.clnt_no, b.acct_no, b.phase, b.strategy_arm, b.model_arm, b.test_group, b.prod_cd,
        MAX(CASE WHEN s.acct_no IS NOT NULL THEN 1 ELSE 0 END)            AS converted,
        MAX(CASE WHEN s.first_target_add_dt IS NOT NULL THEN 1 ELSE 0 END) AS converted_target,
        MAX(COALESCE(s.add_events, 0))                                    AS add_events,
        COALESCE(MAX(g.view_e),   0)                                      AS viewed,
        COALESCE(MAX(g.click_e),  0)                                      AS clicked,
        COALESCE(MAX(g.click_n_e),0)                                      AS clicked_neg
    FROM base b
    LEFT JOIN success s
        ON s.acct_no = b.acct_no AND s.treatmt_strt_dt = b.treatmt_strt_dt
    LEFT JOIN ga4 g
        ON g.clnt_no = b.clnt_no
       AND g.event_date BETWEEN b.treatmt_strt_dt AND b.treatmt_end_dt
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
    phase, strategy_arm, model_arm, test_group, prod_cd,
    COUNT(*)                                                       AS population,
    SUM(viewed)                                                    AS view_users,
    SUM(clicked)                                                   AS click_users,
    SUM(clicked_neg)                                               AS click_n_users,
    SUM(converted)                                                 AS converters,
    SUM(converted_target)                                          AS converters_target,
    SUM(add_events)                                                AS au_add_events,
    SUM(CASE WHEN converted = 1 AND viewed = 1 THEN 1 ELSE 0 END)  AS conv_viewed,
    SUM(CASE WHEN converted = 1 AND viewed = 0 THEN 1 ELSE 0 END)  AS conv_not_viewed,
    SUM(CASE WHEN converted = 0 AND viewed = 1 THEN 1 ELSE 0 END)  AS viewed_not_conv,
    SUM(CASE WHEN converted = 1 AND clicked = 1 THEN 1 ELSE 0 END) AS conv_clicked
FROM dep
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;


-- Q2: converter-level detail (small — converters only). Analytical extract.
WITH base AS (
    SELECT
        clnt_no,
        TRY_CAST(TRIM(TACTIC_EVNT_ID) AS BIGINT) AS acct_no,
        CASE tactic_id WHEN '2026042AUH' THEN 'Phase1' WHEN '2026119AUH' THEN 'Phase2' END AS phase,
        treatmt_strt_dt, treatmt_end_dt,
        CASE WHEN RIGHT(TRIM(tst_grp_cd),2)='_C' THEN 'Control' ELSE 'Test' END AS test_group,
        TRIM(tst_grp_cd) AS tst_grp_cd,
        SUBSTR(tactic_decisn_vrb_info,21,3) AS prod_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH','2026119AUH')
),
au_event AS (
    SELECT a.acct_no, c.visa_prod_cd AS prod_cd, a.evnt_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
        ON  a.clnt_no = c.clnt_no
        AND a.evnt_dt = c.dt_record_ext
        AND a.acct_no = c.acct_no
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD = 3
      AND a.evnt_dt >= DATE '2026-01-01'
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN lower(event_name) = 'view_promotion'   THEN 1 ELSE 0 END AS view_e,
        CASE WHEN lower(event_name) = 'select_promotion' THEN 1 ELSE 0 END AS click_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND event_date >= DATE '2026-04-30'
      AND it_item_id IN ('i_300108','i_308317','i_308314','i_308315',
                         'i_308333','i_308334','i_308335','i_308336')
)
SELECT
    b.phase, b.tst_grp_cd, b.test_group, b.prod_cd                AS offered_prod,
    b.clnt_no, b.acct_no, b.treatmt_strt_dt,
    MIN(e.evnt_dt)                                                AS first_add_dt,
    COUNT(*)                                                      AS add_events,
    COUNT(DISTINCT e.prod_cd)                                     AS add_products,
    MIN(e.prod_cd)                                                AS acquired_prod_first,
    MAX(CASE WHEN TRIM(e.prod_cd) = TRIM(b.prod_cd) THEN 1 ELSE 0 END) AS acquired_target,
    COALESCE(MAX(g.view_e),  0)                                   AS viewed,
    COALESCE(MAX(g.click_e), 0)                                   AS clicked
FROM base b
INNER JOIN au_event e
    ON  e.acct_no = b.acct_no
    AND e.evnt_dt BETWEEN b.treatmt_strt_dt AND b.treatmt_end_dt
LEFT JOIN ga4 g
    ON g.clnt_no = b.clnt_no
   AND g.event_date BETWEEN b.treatmt_strt_dt AND b.treatmt_end_dt
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 7;


-- Q3: converter monthly usage — DLY_FULL_PORTFOLIO, converters only (small join key set).
-- AU card shares the primary's account, so post-add usage shows on the same acct_no.
-- Monthly grain mirrors pcq_q1_26_monthly_balance.sql: spend = SUM(net_prch_amt_dly),
-- balance = month-end bal_current via ranked rn=1 (no QUALIFY). NEVER use accum_dly_bal_mtd as balance.
WITH base AS (
    SELECT
        TRY_CAST(TRIM(TACTIC_EVNT_ID) AS BIGINT) AS acct_no,
        CASE tactic_id WHEN '2026042AUH' THEN 'Phase1' WHEN '2026119AUH' THEN 'Phase2' END AS phase,
        treatmt_strt_dt, treatmt_end_dt,
        CASE WHEN RIGHT(TRIM(tst_grp_cd),2)='_C' THEN 'Control' ELSE 'Test' END AS test_group
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH','2026119AUH')
),
converters AS (
    SELECT b.acct_no, b.phase, b.test_group, MIN(a.evnt_dt) AS first_add_dt
    FROM base b
    INNER JOIN D3CV12A.CR_CRD_ACCT_EVNT_DLY a
        ON  a.acct_no = b.acct_no
        AND a.evnt_dt BETWEEN b.treatmt_strt_dt AND b.treatmt_end_dt
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD = 3
      AND a.evnt_dt >= DATE '2026-01-01'
    GROUP BY 1, 2, 3
),
dfp AS (
    SELECT
        v.acct_no, v.phase, v.test_group, v.first_add_dt,
        p.me_dt, p.dt_record_ext, p.net_prch_amt_dly, p.bal_current, p.status
    FROM converters v
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO p
        ON  p.acct_no = v.acct_no
        AND p.dt_record_ext >= v.first_add_dt - INTERVAL '30' DAY   -- one pre-add month for contrast
        AND p.dt_record_ext <  v.first_add_dt + INTERVAL '180' DAY
),
ranked AS (
    SELECT dfp.*,
           ROW_NUMBER() OVER (PARTITION BY acct_no, me_dt ORDER BY dt_record_ext DESC) AS rn
    FROM dfp
)
SELECT
    phase, test_group, acct_no, first_add_dt, me_dt,
    SUM(net_prch_amt_dly)                       AS monthly_spend,
    MAX(CASE WHEN rn = 1 THEN bal_current END)  AS month_end_balance,
    MAX(CASE WHEN rn = 1 THEN status END)       AS month_end_status
FROM ranked
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 5;


-- Q4: click-classification diagnostic — surface AUH's actual it_creative_name values
-- so the AUH click_p IN-list can be locked from real output (mirror of PCD diagnostic).
SELECT
    it_creative_name,
    event_name,
    COUNT(*)                          AS events,
    COUNT(DISTINCT up_srf_id2_value)  AS users
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026'
  AND event_date >= DATE '2026-04-30'
  AND it_item_id IN ('i_300108','i_308317','i_308314','i_308315',
                     'i_308333','i_308334','i_308335','i_308336')
  AND lower(event_name) IN ('view_promotion','select_promotion')
GROUP BY 1, 2
ORDER BY 2, 3 DESC;
