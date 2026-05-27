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
-- Single scan over pcl_universe with EXISTS flags per arm.
-- Extra EXISTS checks responder=1 on the matching CRV wave(s) for CRV-conversion count.
pcl_flagged AS (
    SELECT
        p.pcl_month,
        p.responder_cli,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM crv_action ca
                WHERE ca.acct_no           = p.acct_no
                  AND ca.offer_start_date <= p.treatmt_end_dt
                  AND ca.offer_end_date   >= p.treatmt_strt_dt
            ) THEN 1 ELSE 0
        END AS overlap_action_flag,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM crv_action ca
                WHERE ca.acct_no           = p.acct_no
                  AND ca.offer_start_date <= p.treatmt_end_dt
                  AND ca.offer_end_date   >= p.treatmt_strt_dt
                  AND ca.responder        = 1
            ) THEN 1 ELSE 0
        END AS crv_responded_action_flag,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM crv_control cc
                WHERE cc.acct_no           = p.acct_no
                  AND cc.offer_start_date <= p.treatmt_end_dt
                  AND cc.offer_end_date   >= p.treatmt_strt_dt
            ) THEN 1 ELSE 0
        END AS overlap_control_flag,
        CASE
            WHEN EXISTS (
                SELECT 1 FROM crv_control cc
                WHERE cc.acct_no           = p.acct_no
                  AND cc.offer_start_date <= p.treatmt_end_dt
                  AND cc.offer_end_date   >= p.treatmt_strt_dt
                  AND cc.responder        = 1
            ) THEN 1 ELSE 0
        END AS crv_responded_control_flag
    FROM pcl_universe p
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
