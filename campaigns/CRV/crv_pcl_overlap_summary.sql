-- CRV vs PCL — overlap summary
-- Two source variants of the same question: how many clients/accounts were
-- exposed to both CRV and PCL with overlapping windows, per wave-pair?
--
-- Overlap = any intersection of windows.
-- Date floor 2024-10-01 — change as needed.

------------------------------------------------------------------------------
-- A) Tactic event history (client-grain via CLNT_NO)
------------------------------------------------------------------------------
WITH crv AS (
    SELECT
        clnt_no,
        treatmt_strt_dt  AS crv_strt_dt,
        treatmt_end_dt   AS crv_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2024-10-01'
),
pcl AS (
    SELECT
        clnt_no,
        treatmt_strt_dt  AS pcl_strt_dt,
        treatmt_end_dt   AS pcl_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'PCL'
      AND treatmt_strt_dt >= DATE '2024-10-01'
)
SELECT
    c.crv_strt_dt,
    p.pcl_strt_dt,
    COUNT(DISTINCT c.clnt_no) AS overlapping_clients
FROM crv c
INNER JOIN pcl p
  ON c.clnt_no      = p.clnt_no
 AND c.crv_strt_dt <= p.pcl_end_dt
 AND c.crv_end_dt  >= p.pcl_strt_dt
GROUP BY c.crv_strt_dt, p.pcl_strt_dt
ORDER BY c.crv_strt_dt, p.pcl_strt_dt
;

------------------------------------------------------------------------------
-- B) Curated decision/response tables (account-grain via ACCT_NO)
-- CRV: cards_crv_install_decis_resp     (offer_start_date / offer_end_date)
-- PCL: cards_pli_decision_resp          (treatmt_strt_dt  / treatmt_end_dt)
-- clnt_no not surfaced on CRV decis_resp in current schema view — using acct_no.
------------------------------------------------------------------------------
WITH crv AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
),
pcl AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
)
SELECT
    c.crv_strt_dt,
    p.pcl_strt_dt,
    COUNT(DISTINCT c.acct_no) AS overlapping_accounts
FROM crv c
INNER JOIN pcl p
  ON c.acct_no      = p.acct_no
 AND c.crv_strt_dt <= p.pcl_end_dt
 AND c.crv_end_dt  >= p.pcl_strt_dt
GROUP BY c.crv_strt_dt, p.pcl_strt_dt
ORDER BY c.crv_strt_dt, p.pcl_strt_dt
;
