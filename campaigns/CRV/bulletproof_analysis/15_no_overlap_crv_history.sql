-- ============================================================================
-- Q15 — Who are the no_overlap PCL clients?  CRV history class x current PCL model
-- no_overlap = PCL leads (mobile banner) with NO CRV offer active during the PCL window.
-- crv_hist_class: prior_crv (CRV ended before window), later_crv (CRV starts after window),
--   never_crv (no CRV decision in window at all), other_crv (overlapping/edge).
-- Adds new_decile (cv_score = current PCL model) to profile WHO the non-overlap clients are:
--   if never_crv concentrates in certain deciles, that concentration is the de-facto
--   eligibility boundary CRV applies and PCL does not.
-- Counts only (response_rate / days-since dropped: cosmetic + don't aggregate in a pivot).
-- ============================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt,
           responder_cli, new_decile
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_im_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
oa_keys AS (
    SELECT DISTINCT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt
    FROM pcl_universe p JOIN crv_im_action c
      ON c.acct_no=p.acct_no AND c.offer_start_date<=p.pcl_end_dt AND c.offer_end_date>=p.pcl_strt_dt
),
oc_keys AS (
    SELECT DISTINCT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt
    FROM pcl_universe p JOIN crv_control c
      ON c.acct_no=p.acct_no AND c.offer_start_date<=p.pcl_end_dt AND c.offer_end_date>=p.pcl_strt_dt
),
no_overlap AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.pcl_end_dt, p.responder_cli, p.new_decile
    FROM pcl_universe p
    LEFT JOIN oa_keys oa ON oa.acct_no=p.acct_no AND oa.pcl_strt_dt=p.pcl_strt_dt AND oa.pcl_end_dt=p.pcl_end_dt
    LEFT JOIN oc_keys oc ON oc.acct_no=p.acct_no AND oc.pcl_strt_dt=p.pcl_strt_dt AND oc.pcl_end_dt=p.pcl_end_dt
    WHERE oa.acct_no IS NULL AND oc.acct_no IS NULL
),
-- ONE row per account: collapses CRV history so the join below is 1-to-1 (kills fanout)
crv_summary AS (
    SELECT acct_no,
           MIN(offer_start_date) AS min_start,
           MIN(offer_end_date)   AS min_end,
           MAX(responder)        AS ever_conv
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-01-01'
    GROUP BY acct_no
),
classified AS (
    SELECT
        n.new_decile, n.responder_cli, s.ever_conv,
        CASE WHEN s.acct_no IS NULL            THEN 'never_crv'
             WHEN s.min_end   < n.pcl_strt_dt  THEN 'prior_crv'
             WHEN s.min_start > n.pcl_end_dt   THEN 'later_crv'
             ELSE 'other_crv' END AS crv_hist_class
    FROM no_overlap n
    LEFT JOIN crv_summary s ON s.acct_no = n.acct_no
)
SELECT
    crv_hist_class,
    new_decile,
    COUNT(*)            AS n_leads,
    SUM(responder_cli)  AS n_responders,
    SUM(ever_conv)      AS n_ever_converted_crv
FROM classified
GROUP BY crv_hist_class, new_decile
ORDER BY 1, 2
;
