-- Net economics: counts of PCL leads, PCL responders, and CRV responders in the overlap cohort.
-- PCL-LEAD CENTRIC: unit = one PCL-mobile deployment per account.
-- Counts only. No rates, no $ math. Andre computes derived metrics in Excel.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        responder_cli,
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date,
        offer_end_date,
        responder
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- Teradata-safe: EXISTS is illegal inside CASE. Each arm's overlap is resolved in a
-- match CTE (non-equi join on acct + window intersection); LEFT JOIN + IS NOT NULL gives
-- the overlap flag. MAX(responder) over the overlapping waves = the CRV-conversion flag.
-- Match CTEs are unique on (acct_no, treatmt_strt_dt, treatmt_end_dt) so the join-back
-- to pcl_universe does not multiply rows. Counts are identical to the EXISTS version.
action_match AS (
    SELECT
        p.acct_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt,
        MAX(ca.responder) AS crv_action_responder_max
    FROM pcl_universe p
    JOIN crv_action ca
      ON ca.acct_no           = p.acct_no
     AND ca.offer_start_date <= p.treatmt_end_dt
     AND ca.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
control_match AS (
    SELECT
        p.acct_no,
        p.treatmt_strt_dt,
        p.treatmt_end_dt,
        MAX(cc.responder) AS crv_control_responder_max
    FROM pcl_universe p
    JOIN crv_control cc
      ON cc.acct_no           = p.acct_no
     AND cc.offer_start_date <= p.treatmt_end_dt
     AND cc.offer_end_date   >= p.treatmt_strt_dt
    GROUP BY p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
),
pcl_flagged AS (
    SELECT
        p.pcl_month,
        p.responder_cli,
        CASE WHEN am.acct_no IS NOT NULL              THEN 1 ELSE 0 END AS overlap_action_flag,
        CASE WHEN am.crv_action_responder_max  = 1    THEN 1 ELSE 0 END AS crv_responded_action_flag,
        CASE WHEN cm1.acct_no IS NOT NULL              THEN 1 ELSE 0 END AS overlap_control_flag,
        CASE WHEN cm1.crv_control_responder_max = 1    THEN 1 ELSE 0 END AS crv_responded_control_flag
    FROM pcl_universe p
    LEFT JOIN action_match am
      ON am.acct_no         = p.acct_no
     AND am.treatmt_strt_dt = p.treatmt_strt_dt
     AND am.treatmt_end_dt  = p.treatmt_end_dt
    LEFT JOIN control_match cm1
      ON cm1.acct_no         = p.acct_no
     AND cm1.treatmt_strt_dt = p.treatmt_strt_dt
     AND cm1.treatmt_end_dt  = p.treatmt_end_dt
),
agg_overall AS (
    SELECT
        CAST('overall' AS VARCHAR(20))                                                                    AS pcl_month,
        SUM(overlap_action_flag)                                                                          AS n_pcl_leads_action_overlap,
        SUM(CASE WHEN overlap_action_flag  = 1 THEN responder_cli ELSE 0 END)                             AS pcl_responders_action_overlap,
        SUM(CASE WHEN overlap_action_flag  = 1 THEN crv_responded_action_flag  ELSE 0 END)               AS crv_responders_in_action_overlap,
        SUM(overlap_control_flag)                                                                         AS n_pcl_leads_control_overlap,
        SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)                             AS pcl_responders_control_overlap,
        SUM(CASE WHEN overlap_control_flag = 1 THEN crv_responded_control_flag ELSE 0 END)               AS crv_responders_in_control_overlap
    FROM pcl_flagged
),
agg_monthly AS (
    SELECT
        CAST(pcl_month AS VARCHAR(20))                                                                    AS pcl_month,
        SUM(overlap_action_flag)                                                                          AS n_pcl_leads_action_overlap,
        SUM(CASE WHEN overlap_action_flag  = 1 THEN responder_cli ELSE 0 END)                             AS pcl_responders_action_overlap,
        SUM(CASE WHEN overlap_action_flag  = 1 THEN crv_responded_action_flag  ELSE 0 END)               AS crv_responders_in_action_overlap,
        SUM(overlap_control_flag)                                                                         AS n_pcl_leads_control_overlap,
        SUM(CASE WHEN overlap_control_flag = 1 THEN responder_cli ELSE 0 END)                             AS pcl_responders_control_overlap,
        SUM(CASE WHEN overlap_control_flag = 1 THEN crv_responded_control_flag ELSE 0 END)               AS crv_responders_in_control_overlap
    FROM pcl_flagged
    GROUP BY pcl_month
)
SELECT * FROM agg_overall
UNION ALL
SELECT * FROM agg_monthly
ORDER BY 1
;
