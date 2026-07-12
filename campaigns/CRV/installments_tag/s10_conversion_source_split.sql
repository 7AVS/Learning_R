-- s10: CRV conversion source split — which surface did converters touch before converting?
-- ONE statement, one summary table. DESCRIPTIVE flow map (tag is organic/non-randomized) — not causal lift.
-- ENGINE: Starburst federated, Trino syntax (no QUALIFY / no division — 9881 pushdown ROUND; counts only).
--
-- Segments per lead (acct x wave):
--   tag_only / banner_only / both_last_tag / both_last_banner (last touch by max event_timestamp)
--   neither_app_active        = bridged + some app activity in window, but no CRV touch (closest to true neither)
--   neither_no_app_activity   = bridged but zero GA4 narrow events in window (non-app-user OR GA4 identity miss)
--   neither_bridge_unmatched  = acct not found in CR_CRD_RPTS_ACCT (can't reach GA4 at all)
-- Converters with first_response_date < offer_start_date are split out as cohort 'converter_pre_offer'
-- (their touch window is empty by construction — known pre-offer-responder population).
--
-- Touch definitions (surface-SPECIFIC only — downstream setup taps choose/review/start plan are SHARED
-- by both journeys and are excluded; activation-success is the conversion itself, excluded):
--   banner = view_promotion on it_item_id allowlist; select_promotion on allowlist OR item_name '%instalment%' (s4/s5)
--   tag    = green view 'eligible transaction' + entry taps 'view rbc installment plans' / 'transaction details - learn more'
--
-- PARAMS to edit: wave window (offer_start_date), GA4 partitions (year/month), data cutoff 2026-06-30.
-- Fallbacks: CRV catalog dw00_im -> dw00_jm if it errors (s9 note). Bridge = d3cv12a.cr_crd_rpts_acct
-- (prefix with your session catalog if needed). If ga4_narrow lacks Mar'26+ history, swap to ..._narrow_reduced.

WITH crv AS (
    SELECT
        acct_no,
        offer_start_date,
        action_control,
        channels_deployed,
        responder,
        first_response_date,
        CASE WHEN responder = 1 AND first_response_date IS NOT NULL
             THEN first_response_date
             ELSE LEAST(offer_end_date, DATE '2026-06-30')
        END AS touch_cutoff
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2026-03-01'
      AND offer_start_date <  DATE '2026-06-01'
),

-- acct -> clnt bridge, latest month-end row per acct (ranked CTE: Trino has no QUALIFY)
bridge AS (
    SELECT acct_no, clnt_no
    FROM (
        SELECT r.acct_no, r.clnt_no,
               ROW_NUMBER() OVER (PARTITION BY r.acct_no ORDER BY r.me_dt DESC) AS rn
        FROM d3cv12a.cr_crd_rpts_acct r
        WHERE r.me_dt >= DATE '2026-01-31'
          AND r.acct_no IN (SELECT CAST(acct_no AS DECIMAL(38,0)) FROM crv)
    )
    WHERE rn = 1
),

pop AS (
    SELECT c.*, CAST(b.clnt_no AS DECIMAL(38,0)) AS clnt_no
    FROM crv c
    LEFT JOIN bridge b ON b.acct_no = CAST(c.acct_no AS DECIMAL(38,0))
),

-- GA4 banner touches, per client-day (pre-aggregated so the per-lead join stays small)
banner_days AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt_no,
        CAST(from_unixtime(event_timestamp / 1000000) AS DATE) AS event_dt,
        MAX(event_timestamp) AS last_ts
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('03','04','05','06')
      AND up_srf_id2_value IS NOT NULL
      AND (
            (event_name = 'view_promotion'
             AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                                '87340','87342','87343','87344'))
         OR (event_name = 'select_promotion'
             AND (it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                                 '87340','87342','87343','87344')
                  OR LOWER(it_item_name) LIKE '%instalment%'))
      )
    GROUP BY 1, 2
),

-- GA4 green-tag touches (surface-specific signatures only), per client-day
tag_days AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt_no,
        CAST(from_unixtime(event_timestamp / 1000000) AS DATE) AS event_dt,
        MAX(event_timestamp) AS last_ts
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE year = '2026' AND month IN ('03','04','05','06')
      AND up_srf_id2_value IS NOT NULL
      AND (
            (event_name = 'view'
             AND LOWER(ep_details) = 'view - credit card installments - eligible transaction')
         OR (event_name = 'tap'
             AND LOWER(ep_details) IN (
                 'tap - credit card installments - view rbc installment plans',
                 'tap - credit card installments - transaction details - learn more'))
      )
    GROUP BY 1, 2
),

-- any app activity in the window (validation split for the neither bucket)
app_active AS (
    SELECT DISTINCT TRY_CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE year = '2026' AND month IN ('03','04','05','06')
      AND up_srf_id2_value IS NOT NULL
),

-- collapse touches to one row per lead (separate CTEs: two LEFT JOINs on days would cross-multiply)
tag_per_lead AS (
    SELECT p.acct_no, p.offer_start_date, MAX(t.last_ts) AS tag_last_ts
    FROM pop p
    JOIN tag_days t
      ON t.clnt_no = p.clnt_no
     AND t.event_dt BETWEEN p.offer_start_date AND p.touch_cutoff
    GROUP BY 1, 2
),
banner_per_lead AS (
    SELECT p.acct_no, p.offer_start_date, MAX(b.last_ts) AS banner_last_ts
    FROM pop p
    JOIN banner_days b
      ON b.clnt_no = p.clnt_no
     AND b.event_dt BETWEEN p.offer_start_date AND p.touch_cutoff
    GROUP BY 1, 2
),

leads AS (
    SELECT
        p.acct_no,
        p.clnt_no,
        p.offer_start_date,
        p.action_control,
        p.channels_deployed,
        CASE
            WHEN p.responder = 1 AND p.first_response_date < p.offer_start_date THEN 'converter_pre_offer'
            WHEN p.responder = 1 THEN 'converter'
            ELSE 'non_converter'
        END AS cohort,
        CASE
            WHEN p.clnt_no IS NULL THEN 'neither_bridge_unmatched'
            WHEN t.tag_last_ts IS NOT NULL AND b.banner_last_ts IS NULL THEN 'tag_only'
            WHEN b.banner_last_ts IS NOT NULL AND t.tag_last_ts IS NULL THEN 'banner_only'
            WHEN t.tag_last_ts IS NOT NULL AND b.banner_last_ts IS NOT NULL
                 THEN CASE WHEN t.tag_last_ts >= b.banner_last_ts
                           THEN 'both_last_tag' ELSE 'both_last_banner' END
            WHEN a.clnt_no IS NOT NULL THEN 'neither_app_active'
            ELSE 'neither_no_app_activity'
        END AS segment
    FROM pop p
    LEFT JOIN tag_per_lead    t ON t.acct_no = p.acct_no AND t.offer_start_date = p.offer_start_date
    LEFT JOIN banner_per_lead b ON b.acct_no = p.acct_no AND b.offer_start_date = p.offer_start_date
    LEFT JOIN app_active      a ON a.clnt_no = p.clnt_no
)

SELECT
    offer_start_date,
    cohort,
    action_control,
    channels_deployed,
    segment,
    COUNT(*)                 AS leads,
    COUNT(DISTINCT acct_no)  AS accts,
    COUNT(DISTINCT clnt_no)  AS clients
FROM leads
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
;
