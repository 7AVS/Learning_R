-- AUH Phase 2 OLB banner engagement (Starburst/Trino)
-- Cohort: tactic 2026119AUH (deployed 2026-04-30).
-- Success carried TWO ways so the gap is measurable:
--   converters_event    = AU-add EVENT in treatment window (AUH_vintage.sql definition:
--                         CR_CRD_ACCT_EVNT_DLY 191/3 — true adds only). PRIMARY.
--   converters_snapshot = ownership snapshot, CAPTR_DT > start (auh_interim_measurement.sql
--                         definition — vintage author found it counts long-time holders; expect it higher).
-- GA4 it_item_id list assumes 'i_' || Salesforce offer id — CONFIRM via auh_ga4_banner_discovery.sql
-- before trusting output; swap IN-list if discovery lands on it_promotion_id / it_item_name instead.
-- Both view_promotion and view_item carried until discovery settles which event is the impression.
-- ac_temp (_C suffix = Control) is TEMP — unconfirmed by Robin for Phase 2.


-- Q1: client-grain engagement x success by arm
WITH cohort AS (
    SELECT
        CLNT_NO                                 AS clnt_no,
        TRY_CAST(TRIM(TACTIC_EVNT_ID) AS DECIMAL(20,0))   AS acct_no,
        TREATMT_STRT_DT                         AS treatmt_strt_dt,
        TREATMT_END_DT                          AS treatmt_end_dt,
        CASE SUBSTR(TRIM(TST_GRP_CD), 1, 2)
            WHEN 'NR' THEN 'NonReward'
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE 'Unknown'
        END                                     AS arm_label,
        CASE SUBSTR(TRIM(TST_GRP_CD), 3, 1)
            WHEN 'R' THEN 'Random'
            WHEN 'M' THEN 'Model'
            WHEN 'W' THEN 'Web'
            ELSE 'Unknown'
        END                                     AS model_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\'
             THEN 'Control' ELSE 'Action'
        END                                     AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026119AUH'
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT)    AS clnt_no,
        event_date,
        CASE WHEN event_name = 'view_promotion'   THEN 1 ELSE 0 END AS view_promo_e,
        CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END AS view_item_e,
        CASE WHEN event_name = 'select_promotion' THEN 1 ELSE 0 END AS click_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND event_date >= DATE '2026-04-30'
      AND it_item_id IN ('i_300108','i_308317','i_308314','i_308315',
                         'i_308333','i_308334','i_308335','i_308336')
),
au_first AS (   -- first true AU-add per account (AUH_vintage.sql semantics, any product)
    SELECT acct_no, MIN(evnt_dt) AS first_add_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY
    WHERE dtl_evnt_typ_cd = 191
      AND ADD_RELTN_CD = 3
      AND evnt_dt >= DATE '2026-04-30'
    GROUP BY acct_no
),
dep AS (
    SELECT
        c.clnt_no, c.acct_no, c.treatmt_strt_dt, c.treatmt_end_dt,
        c.arm_label, c.model_label, c.ac_temp,
        MAX(CASE WHEN e.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS au_add_event,
        MAX(CASE WHEN s.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS au_add_snapshot,
        COALESCE(MAX(g.view_promo_e), 0)                       AS view_promo,
        COALESCE(MAX(g.view_item_e),  0)                       AS view_item,
        COALESCE(MAX(g.click_e),      0)                       AS click
    FROM cohort c
    LEFT JOIN au_first e
        ON  e.acct_no = c.acct_no
        AND e.first_add_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    LEFT JOIN D3CV12A.ACCT_CRD_OWN_DLY_DELTA s
        ON  s.acct_no         = c.acct_no
        AND s.CHG_DT          = DATE '9999-12-31'
        AND s.RELATIONSHIP_CD = '2'
        AND s.card_sts IN ('A', '')
        AND s.CAPTR_DT        > c.treatmt_strt_dt
    LEFT JOIN ga4 g
        ON  g.clnt_no = c.clnt_no
        AND g.event_date BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
    GROUP BY c.clnt_no, c.acct_no, c.treatmt_strt_dt, c.treatmt_end_dt,
             c.arm_label, c.model_label, c.ac_temp
),
client_roll AS (
    SELECT
        clnt_no, arm_label, model_label, ac_temp,
        MAX(au_add_event)    AS au_add_event,
        MAX(au_add_snapshot) AS au_add_snapshot,
        MAX(view_promo)      AS view_promo,
        MAX(view_item)       AS view_item,
        MAX(click)           AS click
    FROM dep
    GROUP BY 1, 2, 3, 4
)
SELECT
    arm_label,
    model_label,
    ac_temp,
    COUNT(*)                                            AS population,
    SUM(view_promo)                                     AS view_promo_users,
    SUM(view_item)                                      AS view_item_users,
    SUM(click)                                          AS click_users,
    SUM(CASE WHEN view_promo = 1 OR view_item = 1
             THEN au_add_event ELSE 0 END)              AS converters_event_viewed,
    SUM(au_add_event)                                   AS converters_event,
    SUM(au_add_snapshot)                                AS converters_snapshot
FROM client_roll
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- Q2: creative x arm alignment check — distinct clients with >=1 event inside own treatment window.
-- Validates that arm-named creatives (NonRewards / RewardsNonOffer / Offer*) serve their matching TST_GRP_CD arms.
WITH cohort AS (
    SELECT
        CLNT_NO                                 AS clnt_no,
        TREATMT_STRT_DT                         AS treatmt_strt_dt,
        TREATMT_END_DT                          AS treatmt_end_dt,
        CASE SUBSTR(TRIM(TST_GRP_CD), 1, 2)
            WHEN 'NR' THEN 'NonReward'
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE 'Unknown'
        END                                     AS arm_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\'
             THEN 'Control' ELSE 'Action'
        END                                     AS ac_temp,
        SUBSTR(TACTIC_DECISN_VRB_INFO, 21, 3)   AS offered_prod
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026119AUH'
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT)    AS clnt_no,
        event_date,
        it_item_id,
        event_name
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND event_date >= DATE '2026-04-30'
      AND it_item_id IN ('i_300108','i_308317','i_308314','i_308315',
                         'i_308333','i_308334','i_308335','i_308336')
      AND event_name IN ('view_promotion', 'view_item', 'select_promotion')
)
-- creative IDs encode product (IAV/GCP/MC4/MC2/AVP/GPR) — crossing them against
-- offered_prod from the base confirms serving alignment, banner vs decision record
SELECT
    g.it_item_id,
    c.offered_prod,
    c.arm_label,
    c.ac_temp,
    COUNT(DISTINCT CASE WHEN g.event_name = 'view_promotion'   THEN c.clnt_no END) AS view_promo_users,
    COUNT(DISTINCT CASE WHEN g.event_name = 'view_item'        THEN c.clnt_no END) AS view_item_users,
    COUNT(DISTINCT CASE WHEN g.event_name = 'select_promotion' THEN c.clnt_no END) AS click_users
FROM cohort c
INNER JOIN ga4 g
    ON  g.clnt_no = c.clnt_no
    AND g.event_date BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;
