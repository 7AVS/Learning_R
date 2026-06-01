-- DIAGNOSTIC for the Q8 numeric-overflow. Full join structure + PCL responders,
-- but ZERO CRV-responder math (no MAX(responder), no second SUM) -- the one piece
-- Q8 has that Q6 (which runs fine) does not.
--   Works    -> overflow is in the CRV-responder piece; rebuild only that.
--   Overflow -> overflow is in the join or the PCL sum; structural.

WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt, treatmt_end_dt, decile, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
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
action_match AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p JOIN crv_action ca
      ON ca.acct_no = p.acct_no AND ca.offer_start_date <= p.treatmt_end_dt AND ca.offer_end_date >= p.treatmt_strt_dt
),
control_match AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p JOIN crv_control cc
      ON cc.acct_no = p.acct_no AND cc.offer_start_date <= p.treatmt_end_dt AND cc.offer_end_date >= p.treatmt_strt_dt
),
labeled AS (
    SELECT p.decile, p.responder_cli,
        CASE WHEN am.acct_no IS NOT NULL THEN 'action_overlap'
             WHEN cm.acct_no IS NOT NULL THEN 'control_overlap'
             ELSE 'no_overlap' END AS cohort
    FROM pcl_universe p
    LEFT JOIN action_match am ON am.acct_no=p.acct_no AND am.treatmt_strt_dt=p.treatmt_strt_dt AND am.treatmt_end_dt=p.treatmt_end_dt
    LEFT JOIN control_match cm ON cm.acct_no=p.acct_no AND cm.treatmt_strt_dt=p.treatmt_strt_dt AND cm.treatmt_end_dt=p.treatmt_end_dt
)
SELECT cohort, decile,
       CAST(COUNT(*) AS BIGINT) AS n_leads,
       SUM(CAST(responder_cli AS BIGINT)) AS pcl_responders
FROM labeled
GROUP BY cohort, decile
;
