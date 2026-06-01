-- Dose-response: PCL conversion by HOW MANY DAYS the CRV and PCL windows overlapped, split by arm.
-- Tests the "more CRV overlap -> less PCL" hypothesis.
-- overlap_days = calendar intersection of the two deployment windows (a PROXY for mobile exposure,
-- NOT a count of impressions). Per PCL lead, take MAX overlap_days across its overlapping CRV waves
-- (longest concurrent exposure). Bucket it, then count PCL responders per bucket x arm.
-- Read: Action PCL rate should DECLINE across buckets while Control stays flatter -> dose effect.
-- Counts only (Q06 idiom: SUM of CASE integer flags); rates computed in Excel.

WITH pcl_universe AS (
    SELECT acct_no, treatmt_strt_dt AS pcl_strt_dt, treatmt_end_dt AS pcl_end_dt, responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01' AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT acct_no, offer_start_date AS crv_strt_dt, offer_end_date AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date AS crv_strt_dt, offer_end_date AS crv_end_dt
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01' AND action_control = 'Control'
),
-- Per PCL lead: longest overlap (in days) with any CRV-Action wave. Inner window via CASE (no LEAST/GREATEST).
action_days AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.responder_cli,
        MAX(
            (CASE WHEN c.crv_end_dt  <= p.pcl_end_dt  THEN c.crv_end_dt  ELSE p.pcl_end_dt  END)
          - (CASE WHEN c.crv_strt_dt >= p.pcl_strt_dt THEN c.crv_strt_dt ELSE p.pcl_strt_dt END)
          + 1
        ) AS overlap_days
    FROM pcl_universe p JOIN crv_action c
      ON c.acct_no = p.acct_no AND c.crv_strt_dt <= p.pcl_end_dt AND c.crv_end_dt >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.responder_cli
),
control_days AS (
    SELECT p.acct_no, p.pcl_strt_dt, p.responder_cli,
        MAX(
            (CASE WHEN c.crv_end_dt  <= p.pcl_end_dt  THEN c.crv_end_dt  ELSE p.pcl_end_dt  END)
          - (CASE WHEN c.crv_strt_dt >= p.pcl_strt_dt THEN c.crv_strt_dt ELSE p.pcl_strt_dt END)
          + 1
        ) AS overlap_days
    FROM pcl_universe p JOIN crv_control c
      ON c.acct_no = p.acct_no AND c.crv_strt_dt <= p.pcl_end_dt AND c.crv_end_dt >= p.pcl_strt_dt
    GROUP BY p.acct_no, p.pcl_strt_dt, p.responder_cli
),
bucketed AS (
    SELECT CAST('Action' AS VARCHAR(10)) AS arm, responder_cli,
        CASE WHEN overlap_days <= 3  THEN CAST('01: 1-3'   AS VARCHAR(12))
             WHEN overlap_days <= 7  THEN CAST('02: 4-7'   AS VARCHAR(12))
             WHEN overlap_days <= 14 THEN CAST('03: 8-14'  AS VARCHAR(12))
             WHEN overlap_days <= 21 THEN CAST('04: 15-21' AS VARCHAR(12))
             WHEN overlap_days <= 30 THEN CAST('05: 22-30' AS VARCHAR(12))
             WHEN overlap_days <= 45 THEN CAST('06: 31-45' AS VARCHAR(12))
             WHEN overlap_days <= 60 THEN CAST('07: 46-60' AS VARCHAR(12))
             ELSE                         CAST('08: 61-90' AS VARCHAR(12)) END AS day_bucket
    FROM action_days
    UNION ALL
    SELECT CAST('Control' AS VARCHAR(10)), responder_cli,
        CASE WHEN overlap_days <= 3  THEN CAST('01: 1-3'   AS VARCHAR(12))
             WHEN overlap_days <= 7  THEN CAST('02: 4-7'   AS VARCHAR(12))
             WHEN overlap_days <= 14 THEN CAST('03: 8-14'  AS VARCHAR(12))
             WHEN overlap_days <= 21 THEN CAST('04: 15-21' AS VARCHAR(12))
             WHEN overlap_days <= 30 THEN CAST('05: 22-30' AS VARCHAR(12))
             WHEN overlap_days <= 45 THEN CAST('06: 31-45' AS VARCHAR(12))
             WHEN overlap_days <= 60 THEN CAST('07: 46-60' AS VARCHAR(12))
             ELSE                         CAST('08: 61-90' AS VARCHAR(12)) END AS day_bucket
    FROM control_days
)
SELECT
    arm,
    day_bucket,
    COUNT(*)                                              AS n_leads,
    SUM(CASE WHEN responder_cli = 1 THEN 1 ELSE 0 END)    AS pcl_responders
FROM bucketed
GROUP BY arm, day_bucket
ORDER BY 1, 2
;
