-- Async banner summary — PCD, CTU, O2P
-- Engine: Starburst (Trino).
-- Sibling of async_banner_vintage_tracker.sql with the vintage_day dimension
-- collapsed. Each row is the total-to-date for one cohort × segment × arm,
-- equivalent to the cumulative value at the latest vintage_day available in the
-- data. No daily breakdown, no cumulative window functions — just totals.
--
-- Cohort, engagement, and success definitions are identical to the tracker.
-- Engagement window = 0-60 days from treatmt_strt_dt. Success window = each
-- recipient's (treatmt_strt_dt, treatmt_end_dt].
--
-- Output columns:
--   campaign | cohort | segment | segment_level | test_control_flag | cohort_arm
--   | total_population [| mobile_population for O2P]
--   | view_users | click_users | leads_p | leads_n | responders


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD                                                              ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
cohort_raw AS (
    SELECT
        clnt_no,
        visa_acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 4) AS product_mnemonic,
        CASE
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%C' THEN 'CONTROL'
            WHEN trim(coalesce(tst_grp_cd, '')) LIKE '%T' THEN 'TEST'
        END AS test_control_flag,
        CASE
            WHEN element_at(split(regexp_replace(trim(tactic_decisn_vrb_info), ' +', ' '), ' '), 3)
                IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
            THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm,
        SUBSTR(tactic_decisn_vrb_info, 42, 3) AS from_product_code
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD','2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND (trim(coalesce(tst_grp_cd, '')) LIKE '%T'
           OR trim(coalesce(tst_grp_cd, '')) LIKE '%C')
),

cohort AS (
    SELECT DISTINCT
        clnt_no, visa_acct_no, treatmt_strt_dt, treatmt_end_dt,
        cohort_month, product_mnemonic, test_control_flag, cohort_arm, from_product_code
    FROM cohort_raw
),

population AS (
    SELECT cohort_month, product_mnemonic, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2,3,4
),

engagement_events AS (
    SELECT
        event_date, event_name,
        CASE
            WHEN lower(event_name) = 'view_promotion'                                              THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name) NOT LIKE 'n_no%' THEN 'click_p'
            WHEN lower(event_name) = 'select_promotion' AND lower(it_creative_name)     LIKE 'n_no%' THEN 'click_n'
            ELSE 'OTH'
        END AS lead_class,
        COALESCE(TRY_CAST(up_srf_id2_value AS BIGINT), TRY_CAST(ep_srf_id2 AS BIGINT)) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND lower(event_name) IN ('view_promotion','select_promotion')
),

engagement_total AS (
    SELECT
        c.cohort_month, c.product_mnemonic, c.test_control_flag, c.cohort_arm,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'view_promotion'   THEN c.clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'select_promotion' THEN c.clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_p'                 THEN c.clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_n'                 THEN c.clnt_no END) AS leads_n
    FROM cohort c
    INNER JOIN engagement_events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY 1,2,3,4
),

success_total AS (
    SELECT
        c.cohort_month, c.product_mnemonic, c.test_control_flag,
        COUNT(DISTINCT c.clnt_no) AS responders
    FROM cohort c
    INNER JOIN D3CV12A.dly_full_portfolio dfp
        ON  dfp.acct_no = c.visa_acct_no
        AND dfp.dt_record_ext BETWEEN date_add('day', -1, c.treatmt_strt_dt) AND c.treatmt_end_dt
        AND dfp.visa_prod_cd <> c.from_product_code
    WHERE c.cohort_arm = 'ASYNC'
      AND dfp.dt_record_ext >= DATE '2026-04-01'
      AND dfp.dt_record_ext <= DATE '2026-07-31'
    GROUP BY 1,2,3
),

base AS (
    SELECT
        p.cohort_month, p.product_mnemonic, p.test_control_flag, p.cohort_arm,
        p.total_population,
        COALESCE(e.view_users,  0) AS view_users,
        COALESCE(e.click_users, 0) AS click_users,
        COALESCE(e.leads_p,     0) AS leads_p,
        COALESCE(e.leads_n,     0) AS leads_n,
        CASE WHEN p.cohort_arm = 'ASYNC' THEN COALESCE(r.responders, 0)
             ELSE CAST(NULL AS BIGINT) END AS responders
    FROM population p
    LEFT JOIN engagement_total e
        ON  e.cohort_month      = p.cohort_month
        AND e.product_mnemonic  = p.product_mnemonic
        AND e.test_control_flag = p.test_control_flag
        AND e.cohort_arm        = p.cohort_arm
    LEFT JOIN success_total r
        ON  r.cohort_month      = p.cohort_month
        AND r.product_mnemonic  = p.product_mnemonic
        AND r.test_control_flag = p.test_control_flag
)

SELECT
    CAST('PCD' AS VARCHAR) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR)     AS segment,
    CAST('OVERALL' AS VARCHAR)     AS segment_level,
    test_control_flag, cohort_arm,
    SUM(total_population) AS total_population,
    SUM(view_users)       AS view_users,
    SUM(click_users)      AS click_users,
    SUM(leads_p)          AS leads_p,
    SUM(leads_n)          AS leads_n,
    SUM(responders)       AS responders
FROM base
GROUP BY cohort_month, test_control_flag, cohort_arm

UNION ALL

SELECT
    CAST('PCD' AS VARCHAR) AS campaign,
    cohort_month     AS cohort,
    'PRODUCT'        AS segment,
    product_mnemonic AS segment_level,
    test_control_flag, cohort_arm,
    total_population,
    view_users, click_users, leads_p, leads_n, responders
FROM base
ORDER BY cohort, segment, segment_level, test_control_flag, cohort_arm
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU                                                              ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        CASE WHEN substring(tactic_decisn_vrb_info, 121, 30) LIKE '%MB%'
             THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),

cohort AS (
    SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_end_dt, cohort_month, cohort_arm
    FROM cohort_raw
),

population AS (
    SELECT cohort_month, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
    FROM cohort
    GROUP BY 1,2
),

engagement_events AS (
    SELECT
        event_date, event_name,
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

engagement_total AS (
    SELECT
        c.cohort_month, c.cohort_arm,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'view_promotion'   THEN c.clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'select_promotion' THEN c.clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_p'                 THEN c.clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_n'                 THEN c.clnt_no END) AS leads_n
    FROM cohort c
    INNER JOIN engagement_events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY 1,2
),

cohort_snap_dts AS (
    SELECT DISTINCT date_add('day', -1, treatmt_strt_dt) AS snap_dt FROM cohort
),

cohort_window AS (
    SELECT MIN(treatmt_strt_dt) AS min_dt, MAX(treatmt_end_dt) AS max_dt FROM cohort
),

pba_lkup_curr AS (
    SELECT acct_typ_cd, acct_clss_cd, srvc_fee_opt_cd, prod_en_nm
    FROM (
        SELECT acct_typ_cd, acct_clss_cd, srvc_fee_opt_cd, prod_en_nm, snap_dt,
               MAX(snap_dt) OVER () AS max_snap_dt
        FROM ddwv01.pba_acct_lkup
        WHERE pda_typ_cd = 'C'
          AND snap_dt BETWEEN (SELECT min_dt FROM cohort_window)
                          AND (SELECT max_dt FROM cohort_window)
    ) sub
    WHERE snap_dt = max_snap_dt
),

precamp_product AS (
    SELECT
        c.clnt_no, c.cohort_month, c.cohort_arm, c.treatmt_strt_dt, c.treatmt_end_dt,
        b.ar_id,
        CASE
            WHEN s.acct_typ = 13 AND s.acct_cls = 10    AND d.flt_pr_tm_trnsctn = 3 THEN 'RBC Student Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls = 10    AND d.flt_pr_tm_trnsctn = 4 THEN 'RBC No Limit Banking for Students'
            WHEN s.acct_typ = 13 AND s.acct_cls = 0     AND d.flt_pr_tm_trnsctn = 2 THEN 'RBC Day to Day Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls = 0     AND d.flt_pr_tm_trnsctn = 4 THEN 'RBC No Limit Banking'
            WHEN s.acct_typ = 13 AND s.acct_cls IN (8,9) AND d.flt_pr_tm_trnsctn = 0 THEN 'RBC Signature No Limit Banking'
        END AS from_product
    FROM cohort c
    INNER JOIN ddwv01.clnt_ar_reltn_dly b
        ON  b.clnt_no    = c.clnt_no
        AND b.dw_srvc_id = 1
        AND b.snap_dt    = date_add('day', -1, c.treatmt_strt_dt)
    INNER JOIN ddwv01.ar_static_dly s
        ON  s.ar_id          = b.ar_id
        AND s.snap_dt        = b.snap_dt
        AND s.srvc_id        = 1
        AND s.open_cls_sts   = 'O'
        AND s.acct_typ       = 13
        AND s.acct_cls IN (0,8,9,10)
    INNER JOIN ddwv01.deposit_account_dly d
        ON  d.ar_id      = b.ar_id
        AND d.snap_dt    = b.snap_dt
        AND d.dw_srvc_id = 1
    WHERE b.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
      AND s.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
      AND d.snap_dt IN (SELECT snap_dt FROM cohort_snap_dts)
),

success_total AS (
    SELECT
        p.cohort_month, p.cohort_arm,
        COUNT(DISTINCT p.clnt_no) AS responders
    FROM precamp_product p
    INNER JOIN ddwv01.dep_acct_sw_dly sw
        ON  sw.ar_id            = p.ar_id
        AND sw.acct_sw_proc_dt BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
    INNER JOIN pba_lkup_curr tl
        ON  tl.acct_typ_cd     = sw.to_acct_typ
        AND tl.acct_clss_cd    = sw.to_acct_clss
        AND tl.srvc_fee_opt_cd = sw.to_fee_opt
    WHERE sw.acct_sw_proc_dt BETWEEN (SELECT min_dt FROM cohort_window)
                                 AND (SELECT max_dt FROM cohort_window)
      AND (
        (p.from_product = 'RBC Student Banking'
         AND tl.prod_en_nm IN ('RBC No Limit Banking for Students','RBC Day to Day Banking','RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking'))
        OR (p.from_product = 'RBC No Limit Banking for Students'
         AND tl.prod_en_nm IN ('RBC Student Banking','RBC Day to Day Banking','RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking'))
        OR (p.from_product = 'RBC Day to Day Banking'
         AND tl.prod_en_nm IN ('RBC No Limit Banking','RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking'))
        OR (p.from_product = 'RBC No Limit Banking'
         AND tl.prod_en_nm IN ('RBC Signature No Limit Banking','RBC VIP Banking','RBC Advantage Banking'))
        OR (p.from_product = 'RBC Signature No Limit Banking'
         AND tl.prod_en_nm = 'RBC VIP Banking')
      )
    GROUP BY 1,2
)

SELECT
    CAST('CTU'     AS VARCHAR) AS campaign,
    p.cohort_month             AS cohort,
    CAST('ALL'     AS VARCHAR) AS segment,
    CAST('OVERALL' AS VARCHAR) AS segment_level,
    CAST('ALL'     AS VARCHAR) AS test_control_flag,
    p.cohort_arm,
    p.total_population,
    COALESCE(e.view_users,  0) AS view_users,
    COALESCE(e.click_users, 0) AS click_users,
    COALESCE(e.leads_p,     0) AS leads_p,
    COALESCE(e.leads_n,     0) AS leads_n,
    COALESCE(r.responders,  0) AS responders
FROM population p
LEFT JOIN engagement_total e
    ON  e.cohort_month = p.cohort_month
    AND e.cohort_arm   = p.cohort_arm
LEFT JOIN success_total r
    ON  r.cohort_month = p.cohort_month
    AND r.cohort_arm   = p.cohort_arm
ORDER BY cohort, cohort_arm
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P                                                              ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

WITH
cohort_raw AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        date_trunc('month', treatmt_strt_dt) AS cohort_month,
        TRIM(rpt_grp_cd) AS rpt_grp_cd,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag,
        CASE
            WHEN TRIM(rpt_grp_cd) IN (
                'PO2PNL01','PO2PNL03','PO2PNL07',
                'PO2POT01','PO2POT03','PO2POT07',
                'PO2PPR01','PO2PPR03','PO2PPR07'
            ) THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm,
        CASE WHEN TRIM(tactic_cell_cd) LIKE '%MB%' THEN 1 ELSE 0 END AS is_mobile
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026099O2P','2026126O2P','2026132O2P')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

cohort AS (
    SELECT clnt_no, treatmt_strt_dt, treatmt_end_dt,
           cohort_month, rpt_grp_cd, test_control_flag, cohort_arm,
           MAX(is_mobile) AS is_mobile
    FROM cohort_raw
    GROUP BY 1,2,3,4,5,6,7
),

population AS (
    SELECT cohort_month, rpt_grp_cd, test_control_flag, cohort_arm,
           COUNT(DISTINCT clnt_no)                                  AS total_population,
           COUNT(DISTINCT CASE WHEN is_mobile = 1 THEN clnt_no END) AS mobile_population
    FROM cohort
    GROUP BY 1,2,3,4
),

engagement_events AS (
    SELECT
        event_date, event_name,
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

engagement_total AS (
    SELECT
        c.cohort_month, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'view_promotion'   THEN c.clnt_no END) AS view_users,
        COUNT(DISTINCT CASE WHEN lower(e.event_name) = 'select_promotion' THEN c.clnt_no END) AS click_users,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_p'                 THEN c.clnt_no END) AS leads_p,
        COUNT(DISTINCT CASE WHEN e.lead_class = 'click_n'                 THEN c.clnt_no END) AS leads_n
    FROM cohort c
    INNER JOIN engagement_events e
        ON  e.clnt_no = c.clnt_no
        AND e.event_date BETWEEN c.treatmt_strt_dt AND date_add('day', 60, c.treatmt_strt_dt)
    GROUP BY 1,2,3,4
),

applications AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN     AS a
    JOIN DDWV01.OVRL_CR_APP            AS b
        ON  b.cr_app_id  = a.cr_app_id
        AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN AS c
        ON  c.cr_app_id          = a.cr_app_id
        AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id         = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD            AS d
        ON  d.cr_app_id          = c.cr_app_id
        AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id         = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN (32,37,45,47,51,56,62)
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),

success_total AS (
    SELECT
        c.cohort_month, c.rpt_grp_cd, c.test_control_flag, c.cohort_arm,
        COUNT(DISTINCT c.clnt_no) AS responders
    FROM cohort c
    INNER JOIN applications a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    GROUP BY 1,2,3,4
),

base AS (
    SELECT
        p.cohort_month, p.rpt_grp_cd, p.test_control_flag, p.cohort_arm,
        p.total_population, p.mobile_population,
        COALESCE(e.view_users,  0) AS view_users,
        COALESCE(e.click_users, 0) AS click_users,
        COALESCE(e.leads_p,     0) AS leads_p,
        COALESCE(e.leads_n,     0) AS leads_n,
        COALESCE(r.responders,  0) AS responders
    FROM population p
    LEFT JOIN engagement_total e
        ON  e.cohort_month      = p.cohort_month
        AND e.rpt_grp_cd        = p.rpt_grp_cd
        AND e.test_control_flag = p.test_control_flag
        AND e.cohort_arm        = p.cohort_arm
    LEFT JOIN success_total r
        ON  r.cohort_month      = p.cohort_month
        AND r.rpt_grp_cd        = p.rpt_grp_cd
        AND r.test_control_flag = p.test_control_flag
        AND r.cohort_arm        = p.cohort_arm
)

SELECT
    CAST('O2P' AS VARCHAR) AS campaign,
    cohort_month                   AS cohort,
    CAST('ALL'     AS VARCHAR)     AS segment,
    CAST('OVERALL' AS VARCHAR)     AS segment_level,
    test_control_flag, cohort_arm,
    SUM(total_population)  AS total_population,
    SUM(mobile_population) AS mobile_population,
    SUM(view_users)        AS view_users,
    SUM(click_users)       AS click_users,
    SUM(leads_p)           AS leads_p,
    SUM(leads_n)           AS leads_n,
    SUM(responders)        AS responders
FROM base
GROUP BY cohort_month, test_control_flag, cohort_arm

UNION ALL

SELECT
    CAST('O2P' AS VARCHAR) AS campaign,
    cohort_month   AS cohort,
    'REPORT_GROUP' AS segment,
    rpt_grp_cd     AS segment_level,
    test_control_flag, cohort_arm,
    total_population, mobile_population,
    view_users, click_users, leads_p, leads_n, responders
FROM base
ORDER BY cohort, segment, segment_level, test_control_flag, cohort_arm
;
