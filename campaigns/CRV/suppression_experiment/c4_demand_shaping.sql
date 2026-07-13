-- c4 v2: demand-shaping — pre/post purchase behavior with converter + exposure overlays.
-- LAYERS (Andre's framing, 2026-07-13):
--   * action_control  = the RANDOMIZED split -> causal. (v1 result: overall clean null,
--     A 63.44% vs C 63.49% post-elig incidence; txns/lead 3.755 vs 3.779; $/lead 3,148 vs 3,156.)
--   * converter (responder) and exposed (banner viewed, GA4) = POST-TREATMENT overlays ->
--     DESCRIPTIVE/directional only. Converters are 1.3% of leads, so the v1 aggregate null does
--     NOT rule out converter-level behavior change — that's what v2 looks at.
-- ENGINE: Starburst federated (exposure needs GA4/Trino). Trino syntax; COUNTS/SUMS ONLY (9881).
-- Output: cohort_month x converter x exposed (rows) x arm (wide columns) with pre-30d and
-- post-90d eligible-txn behavior. Control-exposed rows double as contamination check (~0 expected).
-- Near-causal read inside: Action-converters vs Control-converters (banner-driven vs organic).

WITH crv AS (
    SELECT
        acct_no,
        offer_start_date,
        year_mth_offer_start,
        action_control,
        responder
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2025-09-01'
      AND offer_start_date <  DATE '2026-04-01'
),

lead_keys AS (
    SELECT DISTINCT acct_no, offer_start_date FROM crv
),

-- eligible-txn pool (single-source subquery -> filters push down to Teradata)
txn_pool AS (
    SELECT CAST(t.acct_no AS DECIMAL(38,0)) AS acct_no, t.txn_dt, t.DR_TXN_AMT
    FROM d3cv12a.visa_txn_dly t
    JOIN d3cv12a.lkup_txn_cd_catg k
      ON k.txn_cd = t.txn_cd
    WHERE t.DR_TXN_AMT >= 250
      AND t.txn_catg_cd <> 5001
      AND k.TXN_CATG_LVL_ID = 2
      AND k.catg_lvl_desc IN ('CAPR_OCRG_DB','PRCH_TRF_DB')
      AND t.txn_dt >= DATE '2025-08-01'
      AND t.txn_dt <  DATE '2026-07-01'
),

pre_beh AS (
    SELECT k.acct_no, k.offer_start_date,
           COUNT(*) AS pre_cnt, SUM(t.DR_TXN_AMT) AS pre_amt
    FROM lead_keys k
    JOIN txn_pool t
      ON t.acct_no = CAST(k.acct_no AS DECIMAL(38,0))
     AND t.txn_dt >= k.offer_start_date - INTERVAL '30' DAY
     AND t.txn_dt <  k.offer_start_date
    GROUP BY 1, 2
),

post_beh AS (
    SELECT k.acct_no, k.offer_start_date,
           COUNT(*) AS post_cnt, SUM(t.DR_TXN_AMT) AS post_amt
    FROM lead_keys k
    JOIN txn_pool t
      ON t.acct_no = CAST(k.acct_no AS DECIMAL(38,0))
     AND t.txn_dt >  k.offer_start_date
     AND t.txn_dt <= k.offer_start_date + INTERVAL '90' DAY
    GROUP BY 1, 2
),

-- acct -> clnt bridge (for GA4 exposure only)
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

-- banner views per client-day (GA4, canon 8-id allowlist)
view_days AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt_no,
        CAST(from_unixtime(event_timestamp / 1000000) AS DATE) AS event_dt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE ((year = '2025' AND month IN ('09','10','11','12'))
        OR (year = '2026' AND month IN ('01','02','03','04','05','06')))
      AND up_srf_id2_value IS NOT NULL
      AND event_name = 'view_promotion'
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344',
                         '87340','87342','87343','87344')
    GROUP BY 1, 2
),

exposed AS (
    SELECT DISTINCT c.acct_no, c.offer_start_date
    FROM crv c
    JOIN bridge b ON b.acct_no = CAST(c.acct_no AS DECIMAL(38,0))
    JOIN view_days v
      ON v.clnt_no = CAST(b.clnt_no AS DECIMAL(38,0))
     AND v.event_dt >= c.offer_start_date
     AND v.event_dt <= c.offer_start_date + INTERVAL '90' DAY
)

SELECT
    l.year_mth_offer_start AS cohort_month,
    CASE WHEN l.responder = 1 THEN 'converter' ELSE 'non_converter' END AS converter_seg,
    CASE WHEN e.acct_no IS NOT NULL THEN 'exposed' ELSE 'not_exposed' END AS exposure_seg,
    /* wide: arms as columns */
    SUM(CASE WHEN l.action_control = 'Action'  THEN 1 ELSE 0 END) AS leads_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN 1 ELSE 0 END) AS leads_control,
    /* PRE (30d before deployment) */
    SUM(CASE WHEN l.action_control = 'Action'  AND p.pre_cnt >= 1 THEN 1 ELSE 0 END) AS pre_elig_leads_action,
    SUM(CASE WHEN l.action_control = 'Control' AND p.pre_cnt >= 1 THEN 1 ELSE 0 END) AS pre_elig_leads_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN COALESCE(p.pre_cnt, 0) ELSE 0 END) AS pre_txns_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN COALESCE(p.pre_cnt, 0) ELSE 0 END) AS pre_txns_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN COALESCE(p.pre_amt, 0) ELSE 0 END) AS pre_amt_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN COALESCE(p.pre_amt, 0) ELSE 0 END) AS pre_amt_control,
    /* POST (90d after deployment) */
    SUM(CASE WHEN l.action_control = 'Action'  AND q.post_cnt >= 1 THEN 1 ELSE 0 END) AS post_elig_leads_action,
    SUM(CASE WHEN l.action_control = 'Control' AND q.post_cnt >= 1 THEN 1 ELSE 0 END) AS post_elig_leads_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN COALESCE(q.post_cnt, 0) ELSE 0 END) AS post_txns_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN COALESCE(q.post_cnt, 0) ELSE 0 END) AS post_txns_control,
    SUM(CASE WHEN l.action_control = 'Action'  THEN COALESCE(q.post_amt, 0) ELSE 0 END) AS post_amt_action,
    SUM(CASE WHEN l.action_control = 'Control' THEN COALESCE(q.post_amt, 0) ELSE 0 END) AS post_amt_control
FROM crv l
LEFT JOIN pre_beh  p ON p.acct_no = l.acct_no AND p.offer_start_date = l.offer_start_date
LEFT JOIN post_beh q ON q.acct_no = l.acct_no AND q.offer_start_date = l.offer_start_date
LEFT JOIN exposed  e ON e.acct_no = l.acct_no AND e.offer_start_date = l.offer_start_date
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;
