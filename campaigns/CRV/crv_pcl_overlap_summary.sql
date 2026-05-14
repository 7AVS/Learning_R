-- CRV vs PCL — overlap summary
-- Two source variants of the same question: how many clients/accounts were
-- exposed to both CRV and PCL with overlapping windows, per wave-pair?
--
-- Overlap = any intersection of windows.
-- Date floor 2024-10-01 — change as needed.

------------------------------------------------------------------------------
-- A) Tactic event history (client-grain via CLNT_NO)
--    Filtered: channel contains IM on both sides; CRV Control (TG8) excluded.
--    PCL Control exclusion TBD.
------------------------------------------------------------------------------
WITH crv AS (
    SELECT
        clnt_no,
        treatmt_strt_dt  AS crv_strt_dt,
        treatmt_end_dt   AS crv_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'CRV'
      AND treatmt_strt_dt >= DATE '2024-10-01'
      AND substr(tactic_decisn_vrb_info, 121, 8) LIKE '%IM%'
      AND tst_grp_cd <> 'TG8'  -- exclude CRV Control
),
pcl AS (
    SELECT
        clnt_no,
        treatmt_strt_dt  AS pcl_strt_dt,
        treatmt_end_dt   AS pcl_end_dt
    FROM dg6v01.tactic_evnt_ip_ar_hist
    WHERE substr(tactic_id, 8, 3) = 'PCL'
      AND treatmt_strt_dt >= DATE '2024-10-01'
      AND substr(tactic_decisn_vrb_info, 121, 14) LIKE '%IM%'
      -- TODO: exclude PCL Control once tst_grp_cd code is confirmed
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
--    Filtered: channel contains IM on both sides.
--    Control exclusions TBD (need curated-table Control codes).
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
      AND channels_deployed LIKE '%IM%'
      -- TODO: exclude CRV Control via action_control / test_group once curated-table Control code is confirmed
),
pcl AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
      -- TODO: exclude PCL Control via tst_grp_cd once code is confirmed
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

------------------------------------------------------------------------------
-- C) Discovery — distinct channel x test_group per campaign × source
-- Run these first to map what literals exist before adding the
-- same-channel + treated filter to sections A/B.
------------------------------------------------------------------------------

-- C1) Tactic event hist — CRV: channel x test_group
SELECT
    substr(tactic_decisn_vrb_info, 121, 8) AS crv_channel,
    tst_grp_cd,
    COUNT(DISTINCT clnt_no) AS clients
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'CRV'
  AND treatmt_strt_dt >= DATE '2024-10-01'
GROUP BY substr(tactic_decisn_vrb_info, 121, 8), tst_grp_cd
ORDER BY clients DESC
;

-- C2) Tactic event hist — PCL: channel x test_group
SELECT
    substr(tactic_decisn_vrb_info, 121, 14) AS pcl_channel,
    tst_grp_cd,
    COUNT(DISTINCT clnt_no) AS clients
FROM dg6v01.tactic_evnt_ip_ar_hist
WHERE substr(tactic_id, 8, 3) = 'PCL'
  AND treatmt_strt_dt >= DATE '2024-10-01'
GROUP BY substr(tactic_decisn_vrb_info, 121, 14), tst_grp_cd
ORDER BY clients DESC
;

-- C3) Curated CRV decis_resp — channels_deployed x test_group x action_control
SELECT
    channels_deployed,
    test_group,
    action_control,
    COUNT(DISTINCT acct_no) AS accounts
FROM dl_mr_prod.cards_crv_install_decis_resp
WHERE offer_start_date >= DATE '2024-10-01'
GROUP BY channels_deployed, test_group, action_control
ORDER BY accounts DESC
;

-- C4) Curated PCL decis_resp — channel x test_group x action_code
SELECT
    channel,
    tst_grp_cd,
    action_code,
    COUNT(DISTINCT acct_no) AS accounts
FROM dl_mr_prod.cards_pli_decision_resp
WHERE treatmt_strt_dt >= DATE '2024-10-01'
GROUP BY channel, tst_grp_cd, action_code
ORDER BY accounts DESC
;

------------------------------------------------------------------------------
-- D) Headline cannibalization test — monthly pivot
--    Row grain: PCL treatmt_strt_dt rolled to month-start date.
--    Both groups (overlap / no_overlap) side-by-side as columns so the
--    response rates are directly comparable per month.
--    Control filters TBD on both sides.
------------------------------------------------------------------------------
WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%IM%'
      -- TODO: exclude PCL Control once tst_grp_cd code is confirmed
),
crv_im AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      -- TODO: exclude CRV Control via action_control once code is confirmed
),
overlap_keys AS (
    SELECT DISTINCT
        p.acct_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_im c
      ON c.acct_no           = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt
     AND c.offer_end_date   >= p.treatmt_strt_dt
),
pcl_flagged AS (
    SELECT
        p.pcl_month,
        p.responder_cli,
        CASE WHEN o.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_flag
    FROM pcl_universe p
    LEFT JOIN overlap_keys o
      ON o.acct_no         = p.acct_no
     AND o.treatmt_strt_dt = p.treatmt_strt_dt
     AND o.treatmt_end_dt  = p.treatmt_end_dt
)
SELECT
    pcl_month,
    COUNT(*)                                                       AS total_pcl_leads,
    SUM(overlap_flag)                                              AS overlap_leads,
    SUM(CASE WHEN overlap_flag = 1 THEN responder_cli ELSE 0 END)  AS overlap_responders,
    SUM(CASE WHEN overlap_flag = 0 THEN 1 ELSE 0 END)              AS no_overlap_leads,
    SUM(CASE WHEN overlap_flag = 0 THEN responder_cli ELSE 0 END)  AS no_overlap_responders
FROM pcl_flagged
GROUP BY pcl_month
ORDER BY pcl_month
;
