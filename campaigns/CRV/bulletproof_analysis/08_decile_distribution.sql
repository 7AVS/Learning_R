-- PCL + CRV conversion by PCL decile, split by overlap arm (action / control / no-overlap).
-- Q06 aggregation idiom only (SUM of 0/1 flags, SUM(CASE WHEN flag=1 THEN responder_cli ELSE 0)).
-- No COUNT(*), no cohort string, no UNION -- the constructs that caused the 2616/22003 overflow.
-- TWO separate statements: (1) by new_decile (cv_score model), (2) by decile (model_score).
-- Run each; compare the two PCL models. Arm columns give the cannibalization gap per decile.

-- =====================================================================
-- STATEMENT 1 -- by NEW_DECILE (cv_score model)
-- =====================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt, new_decile, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date, responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date, responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
action_match AS (
    SELECT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, MAX(ca.responder) AS crv_action_responder_max
    FROM pcl_universe p JOIN crv_action ca
      ON ca.acct_no = p.acct_no AND ca.offer_start_date <= p.treatmt_end_dt AND ca.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
control_match AS (
    SELECT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, MAX(cc.responder) AS crv_control_responder_max
    FROM pcl_universe p JOIN crv_control cc
      ON cc.acct_no = p.acct_no AND cc.offer_start_date <= p.treatmt_end_dt AND cc.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
pcl_flagged AS (
    SELECT p.new_decile, p.responder_cli,
        CASE WHEN am1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_action_flag,
        CASE WHEN am1.crv_action_responder_max  = 1 THEN 1 ELSE 0 END AS crv_responded_action_flag,
        CASE WHEN cm1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_control_flag,
        CASE WHEN cm1.crv_control_responder_max = 1 THEN 1 ELSE 0 END AS crv_responded_control_flag
    FROM pcl_universe p
    LEFT JOIN action_match  am1 ON am1.acct_no=p.acct_no AND am1.treatmt_strt_dt=p.treatmt_strt_dt AND am1.treatmt_end_dt=p.treatmt_end_dt
    LEFT JOIN control_match cm1 ON cm1.acct_no=p.acct_no AND cm1.treatmt_strt_dt=p.treatmt_strt_dt AND cm1.treatmt_end_dt=p.treatmt_end_dt
)
SELECT
    new_decile,
    SUM(overlap_action_flag)                                                       AS n_action_overlap,
    SUM(CASE WHEN overlap_action_flag = 1 THEN responder_cli ELSE 0 END)           AS pcl_resp_action,
    SUM(CASE WHEN overlap_action_flag = 1 THEN crv_responded_action_flag ELSE 0 END)  AS crv_resp_action,
    SUM(overlap_control_flag)                                                      AS n_control_overlap,
    SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)          AS pcl_resp_control,
    SUM(CASE WHEN overlap_control_flag = 1 THEN crv_responded_control_flag ELSE 0 END) AS crv_resp_control,
    SUM(CASE WHEN overlap_action_flag = 0 AND overlap_control_flag = 0 THEN 1 ELSE 0 END)             AS n_no_overlap,
    SUM(CASE WHEN overlap_action_flag = 0 AND overlap_control_flag = 0 THEN responder_cli ELSE 0 END) AS pcl_resp_no_overlap
FROM pcl_flagged
GROUP BY new_decile
ORDER BY 1
;

-- =====================================================================
-- STATEMENT 2 -- by DECILE (model_score model)
-- =====================================================================
WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt, decile, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date, responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date, responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
action_match AS (
    SELECT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, MAX(ca.responder) AS crv_action_responder_max
    FROM pcl_universe p JOIN crv_action ca
      ON ca.acct_no = p.acct_no AND ca.offer_start_date <= p.treatmt_end_dt AND ca.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
control_match AS (
    SELECT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, MAX(cc.responder) AS crv_control_responder_max
    FROM pcl_universe p JOIN crv_control cc
      ON cc.acct_no = p.acct_no AND cc.offer_start_date <= p.treatmt_end_dt AND cc.offer_end_date >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
pcl_flagged AS (
    SELECT p.decile, p.responder_cli,
        CASE WHEN am1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_action_flag,
        CASE WHEN am1.crv_action_responder_max  = 1 THEN 1 ELSE 0 END AS crv_responded_action_flag,
        CASE WHEN cm1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS overlap_control_flag,
        CASE WHEN cm1.crv_control_responder_max = 1 THEN 1 ELSE 0 END AS crv_responded_control_flag
    FROM pcl_universe p
    LEFT JOIN action_match  am1 ON am1.acct_no=p.acct_no AND am1.treatmt_strt_dt=p.treatmt_strt_dt AND am1.treatmt_end_dt=p.treatmt_end_dt
    LEFT JOIN control_match cm1 ON cm1.acct_no=p.acct_no AND cm1.treatmt_strt_dt=p.treatmt_strt_dt AND cm1.treatmt_end_dt=p.treatmt_end_dt
)
SELECT
    decile,
    SUM(overlap_action_flag)                                                       AS n_action_overlap,
    SUM(CASE WHEN overlap_action_flag = 1 THEN responder_cli ELSE 0 END)           AS pcl_resp_action,
    SUM(CASE WHEN overlap_action_flag = 1 THEN crv_responded_action_flag ELSE 0 END)  AS crv_resp_action,
    SUM(overlap_control_flag)                                                      AS n_control_overlap,
    SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)          AS pcl_resp_control,
    SUM(CASE WHEN overlap_control_flag = 1 THEN crv_responded_control_flag ELSE 0 END) AS crv_resp_control,
    SUM(CASE WHEN overlap_action_flag = 0 AND overlap_control_flag = 0 THEN 1 ELSE 0 END)             AS n_no_overlap,
    SUM(CASE WHEN overlap_action_flag = 0 AND overlap_control_flag = 0 THEN responder_cli ELSE 0 END) AS pcl_resp_no_overlap
FROM pcl_flagged
GROUP BY decile
ORDER BY 1
;
