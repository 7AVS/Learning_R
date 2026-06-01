-- Cannibalization concentration: PCL conversion + CRV conversion by PCL propensity decile, per cohort.
-- Cohort = the CRV dimension (action_overlap / control_overlap / no_overlap).
-- Decile  = the PCL dimension (decile from model_score, new_decile from cv_score; both labeled pli_*).
-- Responder counts use SUM(CASE ... THEN 1 ELSE 0 END) integer flags -- the SAME proven pattern as Q06
-- (which runs clean). Avoids the decimal/BIGINT SUM that triggered the numeric overflow.
-- NOTE: the curated CRV tables have NO decile/score column, so we cannot slice by CRV propensity here.
-- PCL-LEAD CENTRIC: one PCL-mobile deployment per account. Match CTEs replace EXISTS-in-CASE.
-- Long format (slicer_dim x slicer_value); counts only, rates/shares computed in Excel.

WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt, decile, new_decile, responder_cli
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
    SELECT p.decile, p.new_decile, p.responder_cli,
        CASE WHEN am1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_action_overlap,
        CASE WHEN cm1.acct_no IS NOT NULL THEN 1 ELSE 0 END AS has_control_overlap,
        am1.crv_action_responder_max,
        cm1.crv_control_responder_max
    FROM pcl_universe p
    LEFT JOIN action_match  am1 ON am1.acct_no=p.acct_no AND am1.treatmt_strt_dt=p.treatmt_strt_dt AND am1.treatmt_end_dt=p.treatmt_end_dt
    LEFT JOIN control_match cm1 ON cm1.acct_no=p.acct_no AND cm1.treatmt_strt_dt=p.treatmt_strt_dt AND cm1.treatmt_end_dt=p.treatmt_end_dt
),
labeled AS (
    SELECT decile, new_decile,
        CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END AS pcl_resp,
        CASE WHEN has_action_overlap  = 1 THEN CAST('action_overlap'  AS VARCHAR(20))
             WHEN has_control_overlap = 1 THEN CAST('control_overlap' AS VARCHAR(20))
             ELSE                              CAST('no_overlap'       AS VARCHAR(20)) END AS cohort,
        CASE WHEN has_action_overlap  = 1 THEN (CASE WHEN crv_action_responder_max  = 1 THEN 1 ELSE 0 END)
             WHEN has_control_overlap = 1 THEN (CASE WHEN crv_control_responder_max = 1 THEN 1 ELSE 0 END)
             ELSE 0 END AS crv_resp
    FROM pcl_flagged
)
SELECT CAST('pli_decile' AS VARCHAR(20)) AS slicer_dim, decile AS slicer_value, cohort,
       COUNT(*)        AS n_leads,
       SUM(pcl_resp)   AS pcl_responders,
       SUM(crv_resp)   AS crv_responders
FROM labeled
GROUP BY cohort, decile
UNION ALL
SELECT CAST('pli_new_decile' AS VARCHAR(20)), new_decile, cohort,
       COUNT(*), SUM(pcl_resp), SUM(crv_resp)
FROM labeled
GROUP BY cohort, new_decile
ORDER BY 1, 3, 2
;
