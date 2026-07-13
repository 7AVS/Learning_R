-- c3: CRV mobile-banner reach — FULL CRV population (NOT the PCL-overlap slice of q19/q28),
-- 2026 deployments, per cohort month x arm: leads, banner viewers, banner clickers.
-- Rates computed downstream: view rate = viewers/leads, interaction rate = clickers/viewers (CTR
-- doctrine: clicks over VIEWERS, never over renders).
-- ENGINE: Starburst federated (GA4 is Trino-only). Trino syntax; COUNTS ONLY — no division
-- (Teradata pushdown wraps rate math in ROUND -> 9881).
-- Exposure window per lead: [offer_start_date, LEAST(offer_end_date, 2026-06-30)].
-- Grain note: banner assignment is acct-level but GA4 identity is client-level — exposure is a
-- CLIENT flag applied to each lead (same convention as all prior CRV GA4 work).
-- Banner detection canon (s5/s9): it_item_id 8-value allowlist (both 'i_' and bare formats);
-- select_promotion falls back on it_item_name '%instalment%' (s4).
-- Control rows double as a contamination check: Control should show ~no viewers.

WITH crv AS (
    SELECT
        acct_no,
        offer_start_date,
        year_mth_offer_start,
        action_control,
        LEAST(COALESCE(offer_end_date, DATE '2026-06-30'), DATE '2026-06-30') AS window_end
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-01-01'
      AND offer_start_date <  DATE '2026-06-01'
),

lead_keys AS (
    SELECT DISTINCT acct_no FROM crv
),

bridge AS (
    SELECT acct_no, clnt_no
    FROM (
        SELECT r.acct_no, r.clnt_no,
               ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.me_dt DESC) AS rn
        FROM d3cv12a.cr_crd_rpts_acct r
        WHERE r.acct_no IN (SELECT CAST(acct_no AS DECIMAL(38,0)) FROM lead_keys)
    )
    WHERE rn = 1
),

pop AS (
    SELECT c.*, CAST(b.clnt_no AS DECIMAL(38,0)) AS clnt_no
    FROM crv c
    LEFT JOIN bridge b ON b.acct_no = CAST(c.acct_no AS DECIMAL(38,0))
),

-- banner views per client-day (pre-aggregated)
view_days AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt_no,
        CAST(from_unixtime(event_timestamp / 1000000) AS DATE) AS event_dt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('01','02','03','04','05','06')
      AND up_srf_id2_value IS NOT NULL
      AND event_name = 'view_promotion'
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                         '87340','87342','87343','87344')
    GROUP BY 1, 2
),

-- banner clicks per client-day
click_days AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt_no,
        CAST(from_unixtime(event_timestamp / 1000000) AS DATE) AS event_dt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('01','02','03','04','05','06')
      AND up_srf_id2_value IS NOT NULL
      AND event_name = 'select_promotion'
      AND (it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                          '87340','87342','87343','87344')
           OR LOWER(it_item_name) LIKE '%instalment%')
    GROUP BY 1, 2
),

viewed AS (
    SELECT DISTINCT p.acct_no, p.offer_start_date
    FROM pop p
    JOIN view_days v
      ON v.clnt_no = p.clnt_no
     AND v.event_dt BETWEEN p.offer_start_date AND p.window_end
),

clicked AS (
    SELECT DISTINCT p.acct_no, p.offer_start_date
    FROM pop p
    JOIN click_days c
      ON c.clnt_no = p.clnt_no
     AND c.event_dt BETWEEN p.offer_start_date AND p.window_end
)

SELECT
    p.year_mth_offer_start AS cohort_month,
    p.action_control,
    COUNT(*)                                                   AS leads,
    COUNT(DISTINCT p.clnt_no)                                  AS clients,
    SUM(CASE WHEN v.acct_no IS NOT NULL THEN 1 ELSE 0 END)     AS viewed_leads,
    SUM(CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END)     AS clicked_leads,
    SUM(CASE WHEN p.clnt_no IS NULL THEN 1 ELSE 0 END)         AS unbridged_leads
FROM pop p
LEFT JOIN viewed  v ON v.acct_no = p.acct_no AND v.offer_start_date = p.offer_start_date
LEFT JOIN clicked c ON c.acct_no = p.acct_no AND c.offer_start_date = p.offer_start_date
GROUP BY 1, 2
ORDER BY 1, 2
;
