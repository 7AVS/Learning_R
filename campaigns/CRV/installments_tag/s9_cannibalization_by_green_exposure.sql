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
  AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344')  -- CRV 4-code allowlist (s2/q28 contract; only 87342 live in Dec'25-Feb'26; it_item_id format-stable, no Android '.0')
;

-- ============================================================
-- STMT 2 — REPRODUCTION GATE: published 1.08pp gap (curated only, no green). Must match before STMT 3.
-- ============================================================
WITH pcl_universe AS (
    SELECT CAST(acct_no AS DECIMAL(38,0)) AS acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli
    FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT CAST(acct_no AS DECIMAL(38,0)) AS acct_no, offer_start_date, offer_end_date
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT CAST(acct_no AS DECIMAL(38,0)) AS acct_no, offer_start_date, offer_end_date
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
    SELECT SUM(a_flag)                                            AS n_action,
           SUM(CASE WHEN a_flag = 1 THEN responder_cli ELSE 0 END) AS resp_action,
           SUM(c_flag)                                            AS n_control,
           SUM(CASE WHEN c_flag = 1 THEN responder_cli ELSE 0 END) AS resp_control
    FROM flagged
)
-- COUNTS ONLY — do NOT divide here. STMT 2 is single-source (all Teradata), so Starburst pushes
-- the whole statement down to Teradata. The connector wraps ANY pushed-down rate division in a
-- ROUND() (to preserve Trino result-type semantics), and Teradata rejects that with error 9881 —
-- DOUBLE and DECIMAL both fail. SUM pushes down fine. Compute the gate by hand from the 4 counts:
--   p_action  = resp_action  / n_action
--   p_control = resp_control / n_control
--   gap = p_control - p_action   -- EXPECT ~ +0.0108 (1.08pp). If so, STMT 3 is good to run.
SELECT 'overall_2024-10+' AS slice, n_action, resp_action, n_control, resp_control
FROM agg
;

-- ============================================================
-- STMT 3 — DELIVERABLE: the gap split by entry segment (green / M1 banner). 2025-02+ (GA4-observable).
-- ============================================================
WITH green_clients AS (
    SELECT DISTINCT CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE ( (year = '2025' AND month = '12') OR (year = '2026' AND month IN ('01','02')) )  -- Dec'25–Feb'26
      AND event_name = 'view'
      AND LOWER(ep_details) = 'view - credit card installments - eligible transaction'
),
m1_clients AS (
    SELECT DISTINCT CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE ( (year = '2025' AND month = '12') OR (year = '2026' AND month IN ('01','02')) )  -- Dec'25–Feb'26
      AND event_name = 'view_promotion'
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344')  -- CRV 4-code allowlist (s2/q28 contract; only 87342 live in Dec'25-Feb'26; it_item_id format-stable, no Android '.0')
),
pcl_universe AS (
    SELECT CAST(acct_no AS DECIMAL(38,0)) AS acct_no, CAST(clnt_no AS DECIMAL(38,0)) AS clnt_no,
           treatmt_strt_dt, treatmt_end_dt, responder_cli
    FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-12-01' AND treatmt_strt_dt < DATE '2026-03-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT CAST(acct_no AS DECIMAL(38,0)) AS acct_no, offer_start_date, offer_end_date
    FROM dw00_im.dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT CAST(acct_no AS DECIMAL(38,0)) AS acct_no, offer_start_date, offer_end_date
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
-- STMT 4 — profile cpc_dni values + volume.
-- KNOWN (Andre, 2026-06-22): 3-way categorical = 'Not CPC/DNI' | 'CPC' | 'DNI' (mutually exclusive).
--   CPC = Customer Preference Contact = OPTED OUT of marketing contact (repo: cpc_*_eligible).
--   DNI = reportedly "Do Not Increase" (unconfirmed / doesn't cohere as a contact sibling) — we don't use it.
-- ============================================================
SELECT cpc_dni,
       COUNT(*)                         AS n_leads,
       COUNT(DISTINCT acct_no)          AS n_accts
FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2025-02-01' AND channel LIKE '%MB%'
GROUP BY cpc_dni
ORDER BY n_leads DESC
;

-- ============================================================
-- STMT 5 — Is green-banner exposure present specifically where CPC (contact opt-out) is flagged?
--   LOGIC: CPC opt-out => M1 installments banner is SUPPRESSED (it's a marketing contact).
--   The green per-transaction tag is NOT CPC-capped, so it still shows. Expectation among
--   CPC-flagged leads: n_m1_exposed ~ 0 (suppressed) while n_green_exposed > 0 — i.e. green is
--   the independent channel that survives the opt-out. This is the curated identification of the
--   "M1-suppressed" population (cleaner than the by-exclusion green_only of STMT 3).
--   Grain = PCL-MB lead, Dec'25–Feb'26. COUNTS ONLY. Mixed-source (GA4+EDW) → runs in Trino.
--   NOTE: column is `cpc_dni` (the STMT 4 column). If your env names it differently, swap it.
-- ============================================================
WITH green_clients AS (
    SELECT DISTINCT CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
    WHERE ( (year = '2025' AND month = '12') OR (year = '2026' AND month IN ('01','02')) )  -- Dec'25–Feb'26
      AND event_name = 'view'
      AND LOWER(ep_details) = 'view - credit card installments - eligible transaction'
),
m1_clients AS (
    SELECT DISTINCT CAST(up_srf_id2_value AS DECIMAL(38,0)) AS clnt
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE ( (year = '2025' AND month = '12') OR (year = '2026' AND month IN ('01','02')) )  -- Dec'25–Feb'26
      AND event_name = 'view_promotion'
      AND it_item_id IN ('i_87340','i_87342','i_87343','i_87344')  -- CRV 4-code allowlist (s2/q28 contract; only 87342 live in Dec'25-Feb'26; it_item_id format-stable, no Android '.0')
),
pcl_universe AS (
    SELECT CAST(clnt_no AS DECIMAL(38,0)) AS clnt_no,
           CASE WHEN cpc_dni = 'CPC' THEN 'CPC_optout' ELSE 'not_CPC' END AS cpc_flag
    FROM dw00_im.dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2025-12-01' AND treatmt_strt_dt < DATE '2026-03-01' AND channel LIKE '%MB%'
),
flagged AS (
    SELECT p.cpc_flag,
        CASE WHEN p.clnt_no IN (SELECT clnt FROM green_clients) THEN 1 ELSE 0 END AS green_flag,
        CASE WHEN p.clnt_no IN (SELECT clnt FROM m1_clients)    THEN 1 ELSE 0 END AS m1_flag
    FROM pcl_universe p
)
SELECT cpc_flag,
       COUNT(*)                                                        AS n_leads,
       SUM(green_flag)                                                 AS n_green_exposed,
       SUM(m1_flag)                                                    AS n_m1_exposed,
       SUM(CASE WHEN green_flag = 1 AND m1_flag = 0 THEN 1 ELSE 0 END) AS n_green_only,
       SUM(CASE WHEN green_flag = 1 AND m1_flag = 1 THEN 1 ELSE 0 END) AS n_both,
       SUM(CASE WHEN green_flag = 0 AND m1_flag = 0 THEN 1 ELSE 0 END) AS n_neither
FROM flagged
GROUP BY cpc_flag
ORDER BY cpc_flag
;
