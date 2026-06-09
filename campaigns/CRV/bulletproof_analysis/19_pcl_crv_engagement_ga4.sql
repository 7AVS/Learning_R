-- 19_pcl_crv_engagement_ga4.sql
--
-- GA4 rebuild of Q18 (18_pcl_clicks_cannibalization) — SAME SHAPE as Q18:
-- ONE output, GROUP BY pcl_month (PCL deployment / treatment-start month), arm splits.
-- The only changes vs Q18:
--   1. PCL clicks/impressions come from GA4 (locked on-list banners) instead of curated
--      clicked_mb / impression_mb (which Andre distrusts — downward trend).
--   2. CRV clicks/impressions ADDED alongside PCL.
--   3. GA4 engagement is WINDOWED to each PCL lead's treatment dates
--      (event_date BETWEEN treatmt_strt_dt AND treatmt_end_dt) — the PCL deployment / overlap
--      window. Engagement is attributed to the lead it happened during, same spirit as Q18's
--      per-lead clicked_mb/impression_mb.
--
-- Banner set (locked, validated by crv_pcl_banner_exposure.sql #1, pics 215605/215642):
--   PCL = vcl-limitincrease + vcl-joint  (trace to digital team's CLI + Joint codes)
--   CRV = cc-instalments
--   finoffershub (not on list) + lending (separate product) EXCLUDED.
-- GA4: view_promotion = impression, select_promotion = click.
--
-- Roster + randomised ARM from curated; engagement from GA4. Join CLNT_NO = up_srf_id2_value
-- (cast GA4 side only — federation ROUND gotcha). Starburst/Trino. Counts only — no rates.
-- Causal contrast = overlap_action vs overlap_control (randomised). no_overlap = continuity only.
-- WINDOW: PCL leads >= 2025-02-01 to match GA4 _reduced coverage (Feb-2025+). Editable.

WITH
pcl_universe AS (
    SELECT
        clnt_no,
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli,
        date_trunc('month', treatmt_strt_dt) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01'
      AND channel LIKE '%MB%'
),
crv_im_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control_pool AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
overlap_action_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_im_action c
      ON c.acct_no           = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
),
overlap_control_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_control_pool c
      ON c.acct_no           = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
),
pcl_flagged AS (
    SELECT
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt,
        p.responder_cli, p.pcl_month,
        CASE WHEN oa.acct_no IS NOT NULL
             THEN 1 ELSE 0 END AS overlap_action_flag,
        CASE WHEN oa.acct_no IS NULL AND oc.acct_no IS NOT NULL
             THEN 1 ELSE 0 END AS overlap_control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no         = p.acct_no
     AND oa.treatmt_strt_dt = p.treatmt_strt_dt
     AND oa.treatmt_end_dt  = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no         = p.acct_no
     AND oc.treatmt_strt_dt = p.treatmt_strt_dt
     AND oc.treatmt_end_dt  = p.treatmt_end_dt
),
ga4_events AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
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
),
-- one row per PCL lead. BINARY per-lead flags (MAX, not SUM): did this lead see / click the
-- banner AT LEAST ONCE inside its treatment window. Same 0/1 unit as Q18's impression_mb /
-- clicked_mb -> when summed per arm below, every number = NUMBER OF LEADS (<= leads), so
-- "exposed < targeted" is visible. NOT event volume.
lead_eng AS (
    SELECT
        f.pcl_month,
        f.overlap_action_flag,
        f.overlap_control_flag,
        f.responder_cli,
        f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt,
        MAX(CASE WHEN g.is_pli = 1 AND g.is_click = 0 THEN 1 ELSE 0 END) AS pli_impr,
        MAX(CASE WHEN g.is_pli = 1 AND g.is_click = 1 THEN 1 ELSE 0 END) AS pli_click,
        MAX(CASE WHEN g.is_crv = 1 AND g.is_click = 0 THEN 1 ELSE 0 END) AS crv_impr,
        MAX(CASE WHEN g.is_crv = 1 AND g.is_click = 1 THEN 1 ELSE 0 END) AS crv_click
    FROM pcl_flagged f
    LEFT JOIN ga4_events g
      ON g.clnt_no    = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY
        f.pcl_month, f.overlap_action_flag, f.overlap_control_flag,
        f.responder_cli, f.acct_no, f.treatmt_strt_dt, f.treatmt_end_dt
)
SELECT
    pcl_month,
    -- leads (denominators)
    COUNT(*)                                                                                  AS total_pcl_leads,
    SUM(overlap_action_flag)                                                                  AS overlap_action_leads,
    SUM(overlap_control_flag)                                                                 AS overlap_control_leads,
    SUM(CASE WHEN overlap_action_flag = 0 AND overlap_control_flag = 0 THEN 1 ELSE 0 END)     AS no_overlap_leads,
    -- responders (conversion twin — kept for comparability with H1)
    SUM(CASE WHEN overlap_action_flag  = 1 THEN responder_cli ELSE 0 END)                     AS overlap_action_responders,
    SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)                     AS overlap_control_responders,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0 THEN responder_cli ELSE 0 END) AS no_overlap_responders,
    -- LEADS WHO CLICKED PLI (GA4, >=1 in window) — primary outcome
    SUM(CASE WHEN overlap_action_flag  = 1 THEN pli_click ELSE 0 END)                         AS overlap_action_pli_clicks,
    SUM(CASE WHEN overlap_control_flag = 1 THEN pli_click ELSE 0 END)                         AS overlap_control_pli_clicks,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0 THEN pli_click ELSE 0 END) AS no_overlap_pli_clicks,
    -- LEADS WHO SAW PLI (GA4, >=1 impression in window)
    SUM(CASE WHEN overlap_action_flag  = 1 THEN pli_impr ELSE 0 END)                          AS overlap_action_pli_impr,
    SUM(CASE WHEN overlap_control_flag = 1 THEN pli_impr ELSE 0 END)                          AS overlap_control_pli_impr,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0 THEN pli_impr ELSE 0 END) AS no_overlap_pli_impr,
    -- LEADS WHO CLICKED CRV (GA4, >=1 in window) — added
    SUM(CASE WHEN overlap_action_flag  = 1 THEN crv_click ELSE 0 END)                         AS overlap_action_crv_clicks,
    SUM(CASE WHEN overlap_control_flag = 1 THEN crv_click ELSE 0 END)                         AS overlap_control_crv_clicks,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0 THEN crv_click ELSE 0 END) AS no_overlap_crv_clicks,
    -- LEADS WHO SAW CRV (GA4, >=1 impression in window) — added
    SUM(CASE WHEN overlap_action_flag  = 1 THEN crv_impr ELSE 0 END)                          AS overlap_action_crv_impr,
    SUM(CASE WHEN overlap_control_flag = 1 THEN crv_impr ELSE 0 END)                          AS overlap_control_crv_impr,
    SUM(CASE WHEN overlap_action_flag  = 0 AND overlap_control_flag = 0 THEN crv_impr ELSE 0 END) AS no_overlap_crv_impr
FROM lead_eng
GROUP BY pcl_month
ORDER BY pcl_month
;
