-- 19_pcl_crv_engagement_ga4.sql
--
-- GA4 rebuild of Q18 (18_pcl_clicks_cannibalization): PCL banner IMPRESSIONS + CLICKS by
-- CRV arm, now sourced from GA4 instead of curated clicked_mb/impression_mb (which Andre
-- distrusts — downward trend), and ADDING the CRV engagement side alongside PCL.
--
-- Banner set = the locked, on-list banners validated by crv_pcl_banner_exposure.sql #1
-- (pics 20260608_215605/215642):
--   PCL  = vcl-limitincrease + vcl-joint   (trace to the digital team's CLI + Joint codes)
--   CRV  = cc-instalments
--   finoffershub (not on list) and lending (separate product) are EXCLUDED.
-- GA4: view_promotion = impression, select_promotion = click.
--
-- Roster + randomised ARM still come from curated (GA4 carries no arm). Engagement = GA4.
-- Join CLNT_NO (curated) = up_srf_id2_value (GA4). Cast ONLY the GA4 side (federation ROUND
-- gotcha, err 9981). Starburst/Trino. Counts only — no rates/CTR.
--
-- Clean causal comparison = overlap_action vs overlap_control (randomised CRV assignment).
-- no_overlap = continuity only, NOT causal (those clients were never CRV-eligible).
--
-- WINDOW NOTE: PCL leads pinned to >= 2025-02-01 to match GA4 _reduced coverage (Feb-2025+).
-- Q18 used 2024-10-01; pre-Feb-2025 leads can't have GA4 events, so they'd dilute. Editable.
-- ENGAGEMENT NOTE: GA4 events are counted over 2025-2026, NOT windowed to each client's PCL
-- treatment dates (roster is deduped to clnt_no). This is "ever saw the banner", same as the
-- block test. Tighten to per-treatment-window if a precise in-flight read is needed.

-- =============================================================================
-- #1  PER-ARM TOTALS — the clean causal read (Action vs Control).
-- =============================================================================
WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
pli AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
),
pli_flagged AS (
    SELECT p.clnt_no,
           MAX(CASE WHEN a.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_action,
           MAX(CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_control
    FROM pli p
    LEFT JOIN crv_action  a ON a.acct_no = p.acct_no
                           AND a.offer_start_date <= p.treatmt_end_dt AND a.offer_end_date >= p.treatmt_strt_dt
    LEFT JOIN crv_control c ON c.acct_no = p.acct_no
                           AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.clnt_no
),
roster AS (
    SELECT clnt_no,
           CASE WHEN any_action  = 1 THEN 'overlap_action'
                WHEN any_control = 1 THEN 'overlap_control'
                ELSE                      'no_overlap' END AS grp
    FROM pli_flagged
),
-- GA4 per client: impression + click counts for PCL and CRV (on-list banners only)
ga4 AS (
    SELECT clnt_no,
           SUM(CASE WHEN is_pli = 1 AND is_click = 0 THEN 1 ELSE 0 END) AS pli_impr,
           SUM(CASE WHEN is_pli = 1 AND is_click = 1 THEN 1 ELSE 0 END) AS pli_click,
           SUM(CASE WHEN is_crv = 1 AND is_click = 0 THEN 1 ELSE 0 END) AS crv_impr,
           SUM(CASE WHEN is_crv = 1 AND is_click = 1 THEN 1 ELSE 0 END) AS crv_click,
           MAX(CASE WHEN is_pli = 1 AND is_click = 0 THEN 1 ELSE 0 END) AS any_pli_impr,
           MAX(CASE WHEN is_pli = 1 AND is_click = 1 THEN 1 ELSE 0 END) AS any_pli_click,
           MAX(CASE WHEN is_crv = 1 AND is_click = 0 THEN 1 ELSE 0 END) AS any_crv_impr,
           MAX(CASE WHEN is_crv = 1 AND is_click = 1 THEN 1 ELSE 0 END) AS any_crv_click
    FROM (
        SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
               CASE WHEN lower(it_item_name) LIKE '%vcl-limitincrease%'
                     OR  lower(it_item_name) LIKE '%vcl-joint%'    THEN 1 ELSE 0 END AS is_pli,
               CASE WHEN lower(it_item_name) LIKE '%cc-instalments%' THEN 1 ELSE 0 END AS is_crv,
               CASE WHEN lower(event_name) = 'select_promotion'      THEN 1 ELSE 0 END AS is_click
        FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
        WHERE year IN ('2025', '2026')
          AND lower(event_name) IN ('view_promotion', 'select_promotion')
          AND platform IN ('IOS', 'ANDROID')
          AND ( lower(it_item_name) LIKE '%cc-instalments%'
             OR lower(it_item_name) LIKE '%vcl-limitincrease%'
             OR lower(it_item_name) LIKE '%vcl-joint%' )
          AND lower(it_item_name) NOT LIKE '%finoffershub%'
    ) e
    GROUP BY clnt_no
)
SELECT
    r.grp,
    COUNT(*)                  AS n_clients,
    -- PCL engagement (GA4) — clients reached, then total events
    SUM(g.any_pli_impr)       AS n_clients_pli_impr,
    SUM(g.any_pli_click)      AS n_clients_pli_click,
    SUM(g.pli_impr)           AS pli_impressions,
    SUM(g.pli_click)          AS pli_clicks,
    -- CRV engagement (GA4) — the added CRV side
    SUM(g.any_crv_impr)       AS n_clients_crv_impr,
    SUM(g.any_crv_click)      AS n_clients_crv_click,
    SUM(g.crv_impr)           AS crv_impressions,
    SUM(g.crv_click)          AS crv_clicks
FROM roster r
LEFT JOIN ga4 g ON g.clnt_no = r.clnt_no
GROUP BY r.grp
ORDER BY r.grp
;


-- =============================================================================
-- #2  PER-ARM x GA4 EVENT-MONTH — the trend (faithful to Q18's monthly grain).
--     Lets us read GA4's PCL impression/click trend against curated's downward trend.
--     Volume counts only (no per-month distinct-client denominators).
-- =============================================================================
WITH
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
pli AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
),
pli_flagged AS (
    SELECT p.clnt_no,
           MAX(CASE WHEN a.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_action,
           MAX(CASE WHEN c.acct_no IS NOT NULL THEN 1 ELSE 0 END) AS any_control
    FROM pli p
    LEFT JOIN crv_action  a ON a.acct_no = p.acct_no
                           AND a.offer_start_date <= p.treatmt_end_dt AND a.offer_end_date >= p.treatmt_strt_dt
    LEFT JOIN crv_control c ON c.acct_no = p.acct_no
                           AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.clnt_no
),
roster AS (
    SELECT clnt_no,
           CASE WHEN any_action  = 1 THEN 'overlap_action'
                WHEN any_control = 1 THEN 'overlap_control'
                ELSE                      'no_overlap' END AS grp
    FROM pli_flagged
),
ga4_evt AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
           date_trunc('month', event_date) AS evt_month,
           CASE WHEN lower(it_item_name) LIKE '%vcl-limitincrease%'
                 OR  lower(it_item_name) LIKE '%vcl-joint%'    THEN 1 ELSE 0 END AS is_pli,
           CASE WHEN lower(it_item_name) LIKE '%cc-instalments%' THEN 1 ELSE 0 END AS is_crv,
           CASE WHEN lower(event_name) = 'select_promotion'      THEN 1 ELSE 0 END AS is_click
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year IN ('2025', '2026')
      AND lower(event_name) IN ('view_promotion', 'select_promotion')
      AND platform IN ('IOS', 'ANDROID')
      AND ( lower(it_item_name) LIKE '%cc-instalments%'
         OR lower(it_item_name) LIKE '%vcl-limitincrease%'
         OR lower(it_item_name) LIKE '%vcl-joint%' )
      AND lower(it_item_name) NOT LIKE '%finoffershub%'
)
SELECT
    r.grp,
    e.evt_month,
    SUM(CASE WHEN e.is_pli = 1 AND e.is_click = 0 THEN 1 ELSE 0 END) AS pli_impressions,
    SUM(CASE WHEN e.is_pli = 1 AND e.is_click = 1 THEN 1 ELSE 0 END) AS pli_clicks,
    SUM(CASE WHEN e.is_crv = 1 AND e.is_click = 0 THEN 1 ELSE 0 END) AS crv_impressions,
    SUM(CASE WHEN e.is_crv = 1 AND e.is_click = 1 THEN 1 ELSE 0 END) AS crv_clicks
FROM roster r
INNER JOIN ga4_evt e ON e.clnt_no = r.clnt_no
GROUP BY r.grp, e.evt_month
ORDER BY r.grp, e.evt_month
;
