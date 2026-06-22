-- s9_cannibalization_by_green_exposure.sql
-- ENGINE: Starburst/Trino (FEDERATED — joins GA4 `edl0_im` + curated EDW). Trino syntax.
-- GOAL (Tracy): reuse Q04 cannibalization logic EXACTLY, add a green-banner-exposure split.
--   Q04 grain = PCL "lead" (acct_no x PCL-MB deployment). Outcome = responder_cli.
--   gap = p_control - p_action  (positive = control higher = cannibalization). Published = 1.08pp.
--
-- RUN IN ORDER — each statement validates the next:
--   STMT 1  validate GA4 flags reproduce known counts (green ~1.54M @ Jun-2026; M1 87342).
--   STMT 2  REPRODUCE the published 1.08pp gap (curated only, 2024-10+). GATE: if this != ~1.08pp, STOP.
--   STMT 3  the deliverable: same gap split by entry segment (green/banner). 2025-02+ window.
--   STMT 4  profile cpc_dni (we don't know what it is — learn its values; NOT used as a definition).
--
-- CAVEATS (read):
--  * CATALOG: PLI confirmed `dw00_im.dl_mr_prod`. CRV catalog NOT confirmed — if STMT 2/3 errors on the
--    CRV table, switch `dw00_im` -> `dw00_jm` for cards_crv_install_decis_resp.
--  * WINDOW: GA4 history starts ~Feb-2025; PCL starts Oct-2024. Green exposure is only OBSERVABLE 2025-02+.
--    So STMT 2 (2024-10+) reproduces 1.08pp; STMT 3 runs on 2025-02+ and its overall gap need NOT equal
--    1.08pp (shorter window) — that is expected, not a bug.
--  * "green_only" = green view AND no M1 view_promotion (BY EXCLUSION). NOT asserted as CPC-suppressed.
--  * Join keys cast to BIGINT (CRV acct integer; PCL acct/clnt decimal; GA4 clnt integer).
--  * green/M1 are ORGANIC/non-randomized splits → descriptive overlay; the clean RCT is the overall gap.

-- ============================================================
-- STMT 1 — validate the GA4 exposure flags
-- ============================================================
SELECT 'green_banner_Jun2026' AS flag,
       COUNT(DISTINCT CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
WHERE year = '2026' AND month = '06'
  AND event_name = 'view'
  AND LOWER(ep_details) = 'view - credit card installments - eligible transaction'
UNION ALL
SELECT 'm1_banner_Jun2026' AS flag,
       COUNT(DISTINCT CAST(up_srf_id2_value AS BIGINT)) AS n_clients
FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
WHERE year = '2026' AND month = '06'
  AND event_name = 'view_promotion'
  AND TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) = 87342  -- iOS '87342' + Android '87342.0'
;

-- ============================================================
-- STMT 2 — REPRODUCTION GATE: published 1.08pp gap (curated only, no green). Must match before STMT 3.
-- ============================================================
WITH pcl_universe AS (
    SELECT CAST(acct_no AS BIGINT) AS acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli
    FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT CAST(acct_no AS BIGINT) AS acct_no, offer_start_date, offer_end_date
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT CAST(acct_no AS BIGINT) AS acct_no, offer_start_date, offer_end_date
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
flagged AS (
    SELECT p.responder_cli,
        CASE WHEN EXISTS (SELECT 1 FROM crv_action ca WHERE ca.acct_no = p.acct_no
                          AND ca.offer_start_date <= p.treatmt_end_dt AND ca.offer_end_date >= p.treatmt_strt_dt)
             THEN 1 ELSE 0 END AS a_flag,
        CASE WHEN EXISTS (SELECT 1 FROM crv_control cc WHERE cc.acct_no = p.acct_no
                          AND cc.offer_start_date <= p.treatmt_end_dt AND cc.offer_end_date >= p.treatmt_strt_dt)
             THEN 1 ELSE 0 END AS c_flag
    FROM pcl_universe p
),
agg AS (
    -- keep SUMs as exact numerics (NOT DOUBLE): STMT 2 is single-source (all Teradata),
    -- so Starburst pushes the whole statement down to Teradata. Teradata ROUND/arithmetic
    -- rejects FLOAT (=Trino DOUBLE) with error 9881 — use DECIMAL division instead.
    SELECT SUM(a_flag)                                            AS n_action,
           SUM(CASE WHEN a_flag = 1 THEN responder_cli ELSE 0 END) AS resp_action,
           SUM(c_flag)                                            AS n_control,
           SUM(CASE WHEN c_flag = 1 THEN responder_cli ELSE 0 END) AS resp_control
    FROM flagged
)
SELECT 'overall_2024-10+' AS slice, n_action, resp_action, n_control, resp_control,
       CAST(resp_action  AS DECIMAL(18,6)) / NULLIF(n_action,0)                  AS p_action,
       CAST(resp_control AS DECIMAL(18,6)) / NULLIF(n_control,0)                 AS p_control,
       CAST(resp_control AS DECIMAL(18,6)) / NULLIF(n_control,0)
     - CAST(resp_action  AS DECIMAL(18,6)) / NULLIF(n_action,0)                  AS gap
FROM agg
;

-- ============================================================
-- STMT 3 — DELIVERABLE: the gap split by entry segment (green / M1 banner). 2025-02+ (GA4-observable).
-- ============================================================
WITH green_clients AS (
    SELECT DISTINCT CAST(up_srf_id2_value AS BIGINT) AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE year IN ('2025','2026') AND event_name = 'view'
      AND LOWER(ep_details) = 'view - credit card installments - eligible transaction'
),
m1_clients AS (
    SELECT DISTINCT CAST(up_srf_id2_value AS BIGINT) AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year IN ('2025','2026') AND event_name = 'view_promotion'
      AND TRY_CAST(TRY_CAST(it_promotion_id AS DOUBLE) AS BIGINT) = 87342  -- iOS '87342' + Android '87342.0'
),
pcl_universe AS (
    SELECT CAST(acct_no AS BIGINT) AS acct_no, CAST(clnt_no AS BIGINT) AS clnt_no,
           treatmt_strt_dt, treatmt_end_dt, responder_cli
    FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT CAST(acct_no AS BIGINT) AS acct_no, offer_start_date, offer_end_date
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT CAST(acct_no AS BIGINT) AS acct_no, offer_start_date, offer_end_date
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
flagged AS (
    SELECT p.responder_cli,
        CASE WHEN EXISTS (SELECT 1 FROM crv_action ca WHERE ca.acct_no = p.acct_no
                          AND ca.offer_start_date <= p.treatmt_end_dt AND ca.offer_end_date >= p.treatmt_strt_dt)
             THEN 1 ELSE 0 END AS a_flag,
        CASE WHEN EXISTS (SELECT 1 FROM crv_control cc WHERE cc.acct_no = p.acct_no
                          AND cc.offer_start_date <= p.treatmt_end_dt AND cc.offer_end_date >= p.treatmt_strt_dt)
             THEN 1 ELSE 0 END AS c_flag,
        CASE WHEN p.clnt_no IN (SELECT clnt FROM m1_clients)    THEN 1 ELSE 0 END AS m1_flag,
        CASE WHEN p.clnt_no IN (SELECT clnt FROM green_clients) THEN 1 ELSE 0 END AS green_flag
    FROM pcl_universe p
),
seg AS (
    SELECT responder_cli, a_flag, c_flag,
        CASE WHEN m1_flag = 1 AND green_flag = 1 THEN 'both'
             WHEN m1_flag = 1 AND green_flag = 0 THEN 'banner_only'
             WHEN m1_flag = 0 AND green_flag = 1 THEN 'green_only'
             ELSE 'neither' END AS entry_segment
    FROM flagged
),
agg AS (
    SELECT COALESCE(entry_segment, 'overall')                                     AS segment,
           CAST(SUM(a_flag) AS DOUBLE)                                            AS n_action,
           CAST(SUM(CASE WHEN a_flag = 1 THEN responder_cli ELSE 0 END) AS DOUBLE) AS resp_action,
           CAST(SUM(c_flag) AS DOUBLE)                                            AS n_control,
           CAST(SUM(CASE WHEN c_flag = 1 THEN responder_cli ELSE 0 END) AS DOUBLE) AS resp_control
    FROM seg
    GROUP BY GROUPING SETS ((entry_segment), ())
),
stats AS (
    SELECT segment, n_action, resp_action, n_control, resp_control,
           resp_action / NULLIF(n_action,0)   AS p_action,
           resp_control / NULLIF(n_control,0) AS p_control
    FROM agg
)
SELECT segment, n_action, resp_action, n_control, resp_control, p_action, p_control,
       p_control - p_action AS gap,
       SQRT( p_action*(1-p_action)/NULLIF(n_action,0) + p_control*(1-p_control)/NULLIF(n_control,0) ) AS se,
       (p_control - p_action) - 1.96 * SQRT( p_action*(1-p_action)/NULLIF(n_action,0) + p_control*(1-p_control)/NULLIF(n_control,0) ) AS ci_lower,
       (p_control - p_action) + 1.96 * SQRT( p_action*(1-p_action)/NULLIF(n_action,0) + p_control*(1-p_control)/NULLIF(n_control,0) ) AS ci_upper
FROM stats
ORDER BY segment
;

-- ============================================================
-- STMT 4 — profile cpc_dni (learn what it is — values + volume). NOT used as a definition yet.
-- ============================================================
SELECT cpc_dni,
       COUNT(*)                         AS n_leads,
       COUNT(DISTINCT acct_no)          AS n_accts
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
GROUP BY cpc_dni
ORDER BY n_leads DESC
;
