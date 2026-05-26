-- =============================================================================
-- PCD Deployment 2026111 — Curated Table EDA
-- =============================================================================
-- Deployment: tactic_id_parent = '2026111PCD'
-- Table:      DL_MR_PROD.cards_pcd_ongoing_decis_resp  (Teradata)
-- GA4 cross-ref: edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce (Trino)
-- Run Sections A-H in Teradata. Section I in Starburst/Trino.
-- =============================================================================


-- =============================================================================
-- SECTION A — Cohort sizing & grain
-- =============================================================================

-- A1: Total rows vs distinct acct_no vs distinct clnt_no.
-- If rows = distinct acct_no → account grain. Gaps reveal duplicates.

SELECT
    COUNT(*)                AS total_rows,
    COUNT(DISTINCT acct_no) AS distinct_acct_no,
    COUNT(DISTINCT clnt_no) AS distinct_clnt_no
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- ---
-- A2: Offer window dates and distinct treatment months.

SELECT
    MIN(response_start)    AS earliest_response_start,
    MAX(response_start)    AS latest_response_start,
    MIN(response_end)      AS earliest_response_end,
    MAX(response_end)      AS latest_response_end,
    treatmt_mn,
    COUNT(*)               AS n_rows_in_wave,
    COUNT(DISTINCT acct_no) AS accts_in_wave
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY treatmt_mn
ORDER BY treatmt_mn;


-- ---
-- A3: Row count by response_start — wave deployment shape.

SELECT
    response_start,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY response_start
ORDER BY response_start;


-- =============================================================================
-- SECTION B — Experiment configuration
-- =============================================================================

-- B1: act_ctl_seg x test_groups_period cell sizes.

SELECT
    act_ctl_seg,
    test_groups_period,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts,
    COUNT(DISTINCT clnt_no) AS clients
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY act_ctl_seg, test_groups_period
ORDER BY act_ctl_seg, test_groups_period;


-- ---
-- B2: strategy_seg_cd x strtgy_seg_desc — raw codes, no decoding.

SELECT
    strategy_seg_cd,
    strtgy_seg_desc,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY strategy_seg_cd, strtgy_seg_desc
ORDER BY accts DESC;


-- ---
-- B3: test_description x test_value — raw values, no interpretation.

SELECT
    test_description,
    test_value,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY test_description, test_value
ORDER BY accts DESC;


-- ---
-- B4: cmpgn_seg distribution.

SELECT
    cmpgn_seg,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY cmpgn_seg
ORDER BY accts DESC;


-- =============================================================================
-- SECTION C — Audience product mix at decision
-- =============================================================================

-- C1: Current product at decision (3-way label).

SELECT
    product_at_decision,
    product_grouping_at_decision,
    product_name_at_decision,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY
    product_at_decision,
    product_grouping_at_decision,
    product_name_at_decision
ORDER BY accts DESC;


-- ---
-- C2: Target product distribution.

SELECT
    target_product,
    target_product_name,
    target_product_grouping,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY
    target_product,
    target_product_name,
    target_product_grouping
ORDER BY accts DESC;


-- ---
-- C3: Upgrade path matrix — from product to target product.

SELECT
    product_at_decision,
    target_product,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY product_at_decision, target_product
ORDER BY accts DESC;


-- ---
-- C4: invitation_to_upgrade distribution.

SELECT
    invitation_to_upgrade,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY invitation_to_upgrade
ORDER BY accts DESC;


-- =============================================================================
-- SECTION D — Channels deployed
-- =============================================================================

-- D1: Count of accounts with each channel_deploy_* flag set.
-- Raw flag values — do not assume '1' = active until confirmed.

SELECT
    'channel_deploy_cc' AS channel_flag,
    channel_deploy_cc   AS flag_value,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_cc

UNION ALL

SELECT
    'channel_deploy_dm',
    channel_deploy_dm,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_dm

UNION ALL

SELECT
    'channel_deploy_do',
    channel_deploy_do,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_do

UNION ALL

SELECT
    'channel_deploy_im',
    channel_deploy_im,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_im

UNION ALL

SELECT
    'channel_deploy_em',
    channel_deploy_em,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_em

UNION ALL

SELECT
    'channel_deploy_rd',
    channel_deploy_rd,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_rd

UNION ALL

SELECT
    'channel_deploy_iv',
    channel_deploy_iv,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_iv

UNION ALL

SELECT
    'channel_deploy_mb',
    channel_deploy_mb,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_deploy_mb

UNION ALL

SELECT
    'channel_em_reminder',
    channel_em_reminder,
    COUNT(DISTINCT acct_no)
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channel_em_reminder

ORDER BY channel_flag, accts DESC;


-- ---
-- D2: channels (varchar summary) — top distinct values by account count.

SELECT
    channels,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY channels
ORDER BY accts DESC;


-- ---
-- D3: Multi-channel exposure — how many flag columns are active per account.
-- Uses channel_deploy_* flags; adjusts for whatever the active indicator is.

WITH channel_counts AS (
    SELECT
        acct_no,
        (CASE WHEN channel_deploy_cc = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_dm = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_do = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_im = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_em = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_rd = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_iv = 'Y' THEN 1 ELSE 0 END
       + CASE WHEN channel_deploy_mb = 'Y' THEN 1 ELSE 0 END) AS active_channels
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
)
SELECT
    active_channels,
    COUNT(DISTINCT acct_no) AS accts
FROM channel_counts
GROUP BY active_channels
ORDER BY active_channels;


-- =============================================================================
-- SECTION E — Offer economics
-- =============================================================================

-- E1: offer_bonus_cash distinct values and account counts.

SELECT
    offer_bonus_cash,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY offer_bonus_cash
ORDER BY offer_bonus_cash;


-- ---
-- E2: offer_bonus_points distinct values and account counts.

SELECT
    offer_bonus_points,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY offer_bonus_points
ORDER BY offer_bonus_points;


-- ---
-- E3: nibt_expected_value summary — min, max, percentiles, null count.

SELECT
    COUNT(*)                                          AS n_rows,
    COUNT(nibt_expected_value)                        AS n_non_null,
    SUM(CASE WHEN nibt_expected_value IS NULL THEN 1 ELSE 0 END) AS n_null,
    MIN(nibt_expected_value)                          AS min_val,
    MAX(nibt_expected_value)                          AS max_val,
    AVG(nibt_expected_value)                          AS avg_val,
    STDDEV_POP(nibt_expected_value)                   AS stddev_val
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- ---
-- E4: nibt_expec_value_upgradepath summary — same shape as E3.

SELECT
    COUNT(*)                                                        AS n_rows,
    COUNT(nibt_expec_value_upgradepath)                             AS n_non_null,
    SUM(CASE WHEN nibt_expec_value_upgradepath IS NULL THEN 1 ELSE 0 END) AS n_null,
    MIN(nibt_expec_value_upgradepath)                               AS min_val,
    MAX(nibt_expec_value_upgradepath)                               AS max_val,
    AVG(nibt_expec_value_upgradepath)                               AS avg_val,
    STDDEV_POP(nibt_expec_value_upgradepath)                        AS stddev_val
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- =============================================================================
-- SECTION F — Conversion outcomes
-- =============================================================================

-- F1: Raw conversion flag counts — numerators side by side.

SELECT
    COUNT(DISTINCT CASE WHEN responder_anyproduct    = 1 THEN acct_no END) AS accts_anyproduct,
    COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN acct_no END) AS accts_targetproduct,
    COUNT(DISTINCT CASE WHEN responder_upgrade_path  = 1 THEN acct_no END) AS accts_upgrade_path,
    COUNT(DISTINCT CASE WHEN responder IS NOT NULL AND responder <> '' THEN acct_no END) AS accts_responder_notnull,
    COUNT(DISTINCT acct_no)                                                 AS total_accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- ---
-- F2: success_cd_1 x success_cd_2 joint distribution.

SELECT
    success_cd_1,
    success_cd_2,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY success_cd_1, success_cd_2
ORDER BY accts DESC;


-- ---
-- F3: Days from response_start to success_dt_1, bucketed.

WITH days AS (
    SELECT
        acct_no,
        (success_dt_1 - response_start) AS days_to_success
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND success_dt_1 IS NOT NULL
)
SELECT
    CASE
        WHEN days_to_success = 0            THEN '0'
        WHEN days_to_success BETWEEN 1  AND  3 THEN '1-3'
        WHEN days_to_success BETWEEN 4  AND  7 THEN '4-7'
        WHEN days_to_success BETWEEN 8  AND 14 THEN '8-14'
        WHEN days_to_success BETWEEN 15 AND 30 THEN '15-30'
        WHEN days_to_success BETWEEN 31 AND 60 THEN '31-60'
        WHEN days_to_success > 60              THEN '60+'
        ELSE 'negative'
    END AS days_bucket,
    COUNT(DISTINCT acct_no) AS accts
FROM days
GROUP BY
    CASE
        WHEN days_to_success = 0            THEN '0'
        WHEN days_to_success BETWEEN 1  AND  3 THEN '1-3'
        WHEN days_to_success BETWEEN 4  AND  7 THEN '4-7'
        WHEN days_to_success BETWEEN 8  AND 14 THEN '8-14'
        WHEN days_to_success BETWEEN 15 AND 30 THEN '15-30'
        WHEN days_to_success BETWEEN 31 AND 60 THEN '31-60'
        WHEN days_to_success > 60              THEN '60+'
        ELSE 'negative'
    END
ORDER BY
    MIN(days_to_success);


-- ---
-- F4: weeknum_response distribution.

SELECT
    weeknum_response,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY weeknum_response
ORDER BY weeknum_response;


-- ---
-- F5: new_product distribution among accounts with a positive responder_anyproduct.

SELECT
    new_product,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
  AND responder_anyproduct = 1
GROUP BY new_product
ORDER BY accts DESC;


-- ---
-- F6: Vintage curve — cumulative conversion count by dt_prod_change date.

SELECT
    dt_prod_change,
    COUNT(DISTINCT acct_no) AS accts_converted_on_date
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
  AND dt_prod_change IS NOT NULL
GROUP BY dt_prod_change
ORDER BY dt_prod_change;


-- =============================================================================
-- SECTION G — Engagement funnel
-- =============================================================================

-- G1: tactic_email x email_disposition x email_status joint counts.

SELECT
    tactic_email,
    email_disposition,
    email_status,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY tactic_email, email_disposition, email_status
ORDER BY accts DESC;


-- ---
-- G2: OandO funnel counts — accounts with each flag > 0.

SELECT
    COUNT(DISTINCT CASE WHEN oando          > 0 THEN acct_no END) AS accts_oando,
    COUNT(DISTINCT CASE WHEN oando_actioned > 0 THEN acct_no END) AS accts_oando_actioned,
    COUNT(DISTINCT CASE WHEN oando_pending  > 0 THEN acct_no END) AS accts_oando_pending,
    COUNT(DISTINCT CASE WHEN oando_declined > 0 THEN acct_no END) AS accts_oando_declined,
    COUNT(DISTINCT CASE WHEN oando_approved > 0 THEN acct_no END) AS accts_oando_approved,
    COUNT(DISTINCT acct_no)                                        AS total_accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- ---
-- G3: fulfillment_channel distribution.

SELECT
    fulfillment_channel,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY fulfillment_channel
ORDER BY accts DESC;


-- ---
-- G4: Digital impression/click totals — sums + clients with any activity.

SELECT
    SUM(impression_olb)                                                     AS total_impression_olb,
    SUM(clicked_olb)                                                        AS total_clicked_olb,
    SUM(impression_mb)                                                      AS total_impression_mb,
    SUM(clicked_mb)                                                         AS total_clicked_mb,
    COUNT(DISTINCT CASE WHEN impression_olb > 0 THEN acct_no END)          AS accts_with_impression_olb,
    COUNT(DISTINCT CASE WHEN clicked_olb    > 0 THEN acct_no END)          AS accts_with_clicked_olb,
    COUNT(DISTINCT CASE WHEN impression_mb  > 0 THEN acct_no END)          AS accts_with_impression_mb,
    COUNT(DISTINCT CASE WHEN clicked_mb     > 0 THEN acct_no END)          AS accts_with_clicked_mb
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- =============================================================================
-- SECTION H — Client segmentation overlay
-- =============================================================================

-- H1: life_stage x age_band joint counts.

SELECT
    life_stage,
    age_band,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY life_stage, age_band
ORDER BY accts DESC;


-- ---
-- H2: bi_clnt_seg — top 20 values.

SELECT TOP 20
    bi_clnt_seg,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY bi_clnt_seg
ORDER BY accts DESC;


-- ---
-- H3: credit_phase x wallet_band joint counts.

SELECT
    credit_phase,
    wallet_band,
    COUNT(*)               AS n_rows,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY credit_phase, wallet_band
ORDER BY accts DESC;


-- ---
-- H4: gu, active, opn_prod_cnt, actv_prod_cnt summary.

SELECT
    gu,
    active,
    MIN(opn_prod_cnt)                              AS min_opn_prod_cnt,
    MAX(opn_prod_cnt)                              AS max_opn_prod_cnt,
    MIN(actv_prod_cnt)                             AS min_actv_prod_cnt,
    MAX(actv_prod_cnt)                             AS max_actv_prod_cnt,
    COUNT(*)                                       AS n_rows,
    COUNT(DISTINCT acct_no)                        AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY gu, active
ORDER BY accts DESC;


-- ---
-- H5: rbc_tenure x avg_yrs_rbc summary.

SELECT
    rbc_tenure,
    MIN(avg_yrs_rbc)       AS min_avg_yrs,
    MAX(avg_yrs_rbc)       AS max_avg_yrs,
    COUNT(DISTINCT acct_no) AS accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
GROUP BY rbc_tenure
ORDER BY accts DESC;


-- ---
-- H6: mb, olb, dor digital flag counts — clients with flag = 1.

SELECT
    COUNT(DISTINCT CASE WHEN mb  = 1 THEN acct_no END) AS accts_mb,
    COUNT(DISTINCT CASE WHEN olb = 1 THEN acct_no END) AS accts_olb,
    COUNT(DISTINCT CASE WHEN dor = 1 THEN acct_no END) AS accts_dor,
    COUNT(DISTINCT acct_no)                            AS total_accts
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD';


-- =============================================================================
-- SECTION I — GA4 cross-reference (Trino / Starburst)
-- =============================================================================
-- Cohort: mobile-deployed accounts from deployment 2026111PCD.
-- Join key: clnt_no (Teradata) → up_srf_id2_value (GA4, bigint-compatible).
-- Promo names confirmed by Rajani Singineedi (2026-03-18).
-- GA4 partition filter: year='2026', month IN ('04','05','06').
-- =============================================================================

-- I1: Mobile-deployed cohort size from deployment 2026111PCD.
-- Run in Teradata to get the denominator before cross-referencing GA4.

SELECT
    COUNT(DISTINCT acct_no) AS mobile_deployed_accts,
    COUNT(DISTINCT clnt_no) AS mobile_deployed_clients
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
WHERE tactic_id_parent = '2026111PCD'
  AND channel_deploy_mb = 'Y';


-- ---
-- I2: Of mobile-deployed cohort, count with ≥1 view_promotion and ≥1 select_promotion.
-- Run in Trino.

WITH cohort AS (
    SELECT DISTINCT clnt_no
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND channel_deploy_mb = 'Y'
),
ga4_pcd AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_name,
        it_item_name
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE year  = '2026'
      AND month IN ('04', '05', '06')
      AND event_name IN ('view_promotion', 'select_promotion')
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND up_srf_id2_value IS NOT NULL
),
joined AS (
    SELECT
        c.clnt_no,
        g.event_name
    FROM cohort c
    INNER JOIN ga4_pcd g ON g.clnt_no = c.clnt_no
)
SELECT
    COUNT(DISTINCT CASE WHEN event_name = 'view_promotion'   THEN clnt_no END) AS clients_with_view,
    COUNT(DISTINCT CASE WHEN event_name = 'select_promotion' THEN clnt_no END) AS clients_with_click
FROM joined;


-- ---
-- I3: Banner views and clicks by it_item_name — which promo got the most activity.
-- Run in Trino.

WITH cohort AS (
    SELECT DISTINCT clnt_no
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND channel_deploy_mb = 'Y'
)
SELECT
    g.it_item_name,
    g.event_name,
    COUNT(*)                                           AS total_events,
    COUNT(DISTINCT TRY_CAST(g.up_srf_id2_value AS BIGINT)) AS unique_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
INNER JOIN cohort c
    ON TRY_CAST(g.up_srf_id2_value AS BIGINT) = c.clnt_no
WHERE g.year  = '2026'
  AND g.month IN ('04', '05', '06')
  AND g.event_name IN ('view_promotion', 'select_promotion')
  AND g.it_item_name IN (
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
        'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
        'PB_CC_ALL_26_02_RBC_PCD_PPCN',
        'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
      )
  AND g.up_srf_id2_value IS NOT NULL
GROUP BY g.it_item_name, g.event_name
ORDER BY g.it_item_name, g.event_name;


-- ---
-- I4: Days from response_start to first banner view, bucketed.
-- Run in Trino.

WITH cohort AS (
    SELECT DISTINCT clnt_no, response_start
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND channel_deploy_mb = 'Y'
),
first_views AS (
    SELECT
        TRY_CAST(g.up_srf_id2_value AS BIGINT) AS clnt_no,
        MIN(DATE_PARSE(g.event_date, '%Y%m%d')) AS first_view_dt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    WHERE g.year  = '2026'
      AND g.month IN ('04', '05', '06')
      AND g.event_name = 'view_promotion'
      AND g.it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND g.up_srf_id2_value IS NOT NULL
    GROUP BY TRY_CAST(g.up_srf_id2_value AS BIGINT)
),
joined AS (
    SELECT
        c.clnt_no,
        DATE_DIFF('day', c.response_start, fv.first_view_dt) AS days_to_view
    FROM cohort c
    INNER JOIN first_views fv ON fv.clnt_no = c.clnt_no
)
SELECT
    CASE
        WHEN days_to_view < 0               THEN 'before_response_start'
        WHEN days_to_view = 0              THEN '0'
        WHEN days_to_view BETWEEN 1  AND  3 THEN '1-3'
        WHEN days_to_view BETWEEN 4  AND  7 THEN '4-7'
        WHEN days_to_view BETWEEN 8  AND 14 THEN '8-14'
        WHEN days_to_view BETWEEN 15 AND 30 THEN '15-30'
        WHEN days_to_view BETWEEN 31 AND 60 THEN '31-60'
        WHEN days_to_view > 60              THEN '60+'
    END AS days_bucket,
    COUNT(DISTINCT clnt_no) AS clients
FROM joined
GROUP BY
    CASE
        WHEN days_to_view < 0               THEN 'before_response_start'
        WHEN days_to_view = 0              THEN '0'
        WHEN days_to_view BETWEEN 1  AND  3 THEN '1-3'
        WHEN days_to_view BETWEEN 4  AND  7 THEN '4-7'
        WHEN days_to_view BETWEEN 8  AND 14 THEN '8-14'
        WHEN days_to_view BETWEEN 15 AND 30 THEN '15-30'
        WHEN days_to_view BETWEEN 31 AND 60 THEN '31-60'
        WHEN days_to_view > 60              THEN '60+'
    END
ORDER BY MIN(days_to_view);


-- ---
-- I5: Days from response_start to first click (select_promotion), bucketed.
-- Run in Trino.

WITH cohort AS (
    SELECT DISTINCT clnt_no, response_start
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND channel_deploy_mb = 'Y'
),
first_clicks AS (
    SELECT
        TRY_CAST(g.up_srf_id2_value AS BIGINT) AS clnt_no,
        MIN(DATE_PARSE(g.event_date, '%Y%m%d')) AS first_click_dt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce g
    WHERE g.year  = '2026'
      AND g.month IN ('04', '05', '06')
      AND g.event_name = 'select_promotion'
      AND g.it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND g.up_srf_id2_value IS NOT NULL
    GROUP BY TRY_CAST(g.up_srf_id2_value AS BIGINT)
),
joined AS (
    SELECT
        c.clnt_no,
        DATE_DIFF('day', c.response_start, fc.first_click_dt) AS days_to_click
    FROM cohort c
    INNER JOIN first_clicks fc ON fc.clnt_no = c.clnt_no
)
SELECT
    CASE
        WHEN days_to_click < 0               THEN 'before_response_start'
        WHEN days_to_click = 0              THEN '0'
        WHEN days_to_click BETWEEN 1  AND  3 THEN '1-3'
        WHEN days_to_click BETWEEN 4  AND  7 THEN '4-7'
        WHEN days_to_click BETWEEN 8  AND 14 THEN '8-14'
        WHEN days_to_click BETWEEN 15 AND 30 THEN '15-30'
        WHEN days_to_click BETWEEN 31 AND 60 THEN '31-60'
        WHEN days_to_click > 60              THEN '60+'
    END AS days_bucket,
    COUNT(DISTINCT clnt_no) AS clients
FROM joined
GROUP BY
    CASE
        WHEN days_to_click < 0               THEN 'before_response_start'
        WHEN days_to_click = 0              THEN '0'
        WHEN days_to_click BETWEEN 1  AND  3 THEN '1-3'
        WHEN days_to_click BETWEEN 4  AND  7 THEN '4-7'
        WHEN days_to_click BETWEEN 8  AND 14 THEN '8-14'
        WHEN days_to_click BETWEEN 15 AND 30 THEN '15-30'
        WHEN days_to_click BETWEEN 31 AND 60 THEN '31-60'
        WHEN days_to_click > 60              THEN '60+'
    END
ORDER BY MIN(days_to_click);


-- ---
-- I6: Among converters (responder_anyproduct = 1), how many had a prior banner view?
-- Run in Trino.

WITH converters AS (
    SELECT DISTINCT clnt_no, response_start
    FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND responder_anyproduct = 1
),
banner_viewers AS (
    SELECT DISTINCT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce
    WHERE year  = '2026'
      AND month IN ('04', '05', '06')
      AND event_name = 'view_promotion'
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND up_srf_id2_value IS NOT NULL
)
SELECT
    COUNT(DISTINCT c.clnt_no)                                              AS total_converters,
    COUNT(DISTINCT CASE WHEN bv.clnt_no IS NOT NULL THEN c.clnt_no END)   AS converters_with_prior_view
FROM converters c
LEFT JOIN banner_viewers bv ON bv.clnt_no = c.clnt_no;


-- =============================================================================
-- Open questions / things to verify after running
-- =============================================================================
-- 1. channel_deploy_* active indicator = 'Y' (confirmed for channel_deploy_mb on
--    cards_pcd_ongoing_decis_resp by Andre 2026-05-26). D3 + Section I assume the
--    same convention for the other channel_deploy_* columns — verify via D1 output.
-- 2. E3/E4 now use MIN/MAX/AVG/STDDEV_POP instead of APPROX_PERCENTILE (Trino-only).
--    Compute percentiles in Excel from raw distribution if needed.
-- 3. Section I cohort subquery uses DL_MR_PROD in Trino cross-ref CTEs — I1 is
--    Teradata-only; I2–I6 require the cohort to be materialized or queried via a
--    Starburst-accessible federated view. Confirm whether cross-engine inline CTEs
--    are supported in the work env or if clnt_no list must be exported first.
-- 4. tibc vs fibc (col 87): PCD schema notes this may be tibc; PCL uses fibc —
--    verify exact column name via HELP TABLE before using in any join or filter.
-- 5. response_start grain vs treatmt_mn: A2 and A3 may show different counts if
--    multiple treatmt_mn values share a response_start — cross-check totals.
-- =============================================================================
