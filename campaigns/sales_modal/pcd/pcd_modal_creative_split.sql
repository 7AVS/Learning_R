-- pcd_modal_creative_split.sql
-- Engine: Starburst (Trino). Federated: Teradata cohort/conversion table + edl0_im GA4.
-- Purpose: PCD's production async-banner engagement tracker
--   (campaigns/PCD/async_banner_responder_engagement.sql, BLOCK 1 — PCD) pools all 4 PCD GA4 mobile
--   creatives into one IN-list before counting engagement, so SalesModal's two variants are invisible,
--   mixed in with PPCN and Offer_Hub_Banner. This query adds it_item_name as its own grouping dimension
--   so SalesModal gets its OWN read, split from PPCN/Offer_Hub_Banner, alongside the other banners.
-- Population, window, and click-classification logic copied VERBATIM from the production tracker —
--   this does NOT modify campaigns/PCD/async_banner_responder_engagement.sql, it is an additional read.
-- Counts only, no rates. Cohort dimension = wave_dt (response_start, treatment-start grain).

WITH

pcd_cohort AS (
    SELECT
        clnt_no,
        response_start,
        response_start                   AS wave_dt,
        responder_anyproduct,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
        END AS test_control_flag,
        CASE
            WHEN strategy_seg_cd IN (
                'MSC8YUS3','MAO28CJ5','MAO2EDB1',
                'MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7'
            ) THEN 'ASYNC' ELSE 'NON_ASYNC'
        END AS cohort_arm
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start >= DATE '2026-04-01'
),

-- one row per converter; population + filters identical to the production tracker's pcd_converter_events
pcd_converters AS (
    SELECT DISTINCT wave_dt, clnt_no, test_control_flag, cohort_arm
    FROM pcd_cohort
    WHERE responder_anyproduct = 1
      AND test_control_flag IS NOT NULL
),

-- the 4 known PCD mobile creatives (verbatim from async_banner_responder_engagement.sql /
-- PCD_2026111_README.md) — SalesModal's 2 variants get their own rows here, split from PPCN and
-- Offer_Hub_Banner instead of being pooled into one IN-list bucket.
creative_universe (it_item_name) AS (
    VALUES
        ('PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP'),
        ('PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV'),
        ('PB_CC_ALL_26_02_RBC_PCD_PPCN'),
        ('PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner')
),

-- click-classification IN-lists copied verbatim from async_banner_responder_engagement.sql /
-- click_classification_diagnostic.sql — do not edit these lists here; edit the source, not this copy.
pcd_ga4_raw AS (
    SELECT
        event_date,
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        it_item_name,
        CASE
            WHEN lower(event_name) = 'view_promotion' THEN 'view'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'n_Non intéressé','n_Not interested','n_Not now','n_Pas maintenant',
                'Not now','Pas maintenant','n_close','Close'
            ) THEN 'click_n'
            WHEN lower(event_name) = 'select_promotion' AND it_creative_name IN (
                'p_Chat to learn more','p_Chat with us','p_Clavarder avec nous',
                'p_Clavardez pour en savoir plus','Chat to learn more',
                'Clavardez pour en savoir plus','VSA_OFFER_SF'
            ) THEN 'click_p'
        END AS lead_class
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

-- cross join converters x every creative: every creative row gets the SAME converter denominator;
-- the numerator (had_view / had_click_p / had_click_n) is scoped to THAT creative's own events.
converters_x_creative AS (
    SELECT c.wave_dt, c.clnt_no, c.test_control_flag, c.cohort_arm, u.it_item_name
    FROM pcd_converters c
    CROSS JOIN creative_universe u
),

creative_events AS (
    SELECT
        x.wave_dt,
        x.it_item_name,
        x.test_control_flag,
        x.cohort_arm,
        x.clnt_no,
        MAX(CASE WHEN g.lead_class = 'view'    THEN 1 ELSE 0 END) AS had_view,
        MAX(CASE WHEN g.lead_class = 'click_p' THEN 1 ELSE 0 END) AS had_click_p,
        MAX(CASE WHEN g.lead_class = 'click_n' THEN 1 ELSE 0 END) AS had_click_n
    FROM converters_x_creative x
    LEFT JOIN pcd_ga4_raw g
        ON  g.clnt_no      = x.clnt_no
        AND g.it_item_name = x.it_item_name
        AND g.event_date BETWEEN x.wave_dt AND date_add('day', 60, x.wave_dt)
    GROUP BY x.wave_dt, x.it_item_name, x.test_control_flag, x.cohort_arm, x.clnt_no
)

SELECT
    wave_dt AS cohort,
    it_item_name,
    test_control_flag,
    cohort_arm,
    COUNT(DISTINCT clnt_no)                                     AS converters,
    COUNT(DISTINCT CASE WHEN had_view    = 1 THEN clnt_no END)  AS view_clients,
    COUNT(DISTINCT CASE WHEN had_click_p = 1 THEN clnt_no END)  AS click_p_clients,
    COUNT(DISTINCT CASE WHEN had_click_n = 1 THEN clnt_no END)  AS click_n_clients
FROM creative_events
GROUP BY wave_dt, it_item_name, test_control_flag, cohort_arm
ORDER BY wave_dt, it_item_name, test_control_flag, cohort_arm
;
