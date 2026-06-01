-- Substitution test: for each PCL-mobile lead with CRV overlap, did the account convert CRV only,
-- PCL only, both, or neither? For both-converters, classify by conversion order.
-- PCL-LEAD CENTRIC: unit = one PCL-mobile deployment per account. Split by Action vs Control overlap.
-- Output grain: overall (sentinel) + one row per (pcl_month × arm), via UNION ALL.

WITH pcl_universe AS (
    SELECT
        acct_no,
        treatmt_strt_dt AS pcl_strt_dt,
        treatmt_end_dt  AS pcl_end_dt,
        responder_cli,
        dt_cl_change,
        -- Month-start of the PCL lead wave, same derivation as Q06
        treatmt_strt_dt - (EXTRACT(DAY FROM treatmt_strt_dt) - 1) AS pcl_month1
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt >= DATE '2024-10-01'
      AND channel LIKE '%MB%'
),
crv_action AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        responder,
        first_response_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT
        acct_no,
        offer_start_date AS crv_strt_dt,
        offer_end_date   AS crv_end_dt,
        responder,
        first_response_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_start_date >= DATE '2024-10-01'
      AND action_control = 'Control'
),
-- Pre-aggregate CRV-Action per (acct_no, pcl_strt_dt, pcl_end_dt):
--   crv_resp = 1 if ANY overlapping Action wave converted.
--   crv_first_resp_dt = earliest first_response_date among converting Action waves.
crv_action_agg AS (
    SELECT
        c.acct_no,
        p.pcl_strt_dt,
        p.pcl_end_dt,
        MAX(c.responder)                                             AS crv_resp,
        MIN(CASE WHEN c.responder = 1 THEN c.first_response_date END) AS crv_first_resp_dt
    FROM crv_action c
    INNER JOIN pcl_universe p
      ON p.acct_no       = c.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    GROUP BY c.acct_no, p.pcl_strt_dt, p.pcl_end_dt
),
-- Same pre-aggregation for CRV-Control.
crv_control_agg AS (
    SELECT
        c.acct_no,
        p.pcl_strt_dt,
        p.pcl_end_dt,
        MAX(c.responder)                                             AS crv_resp,
        MIN(CASE WHEN c.responder = 1 THEN c.first_response_date END) AS crv_first_resp_dt
    FROM crv_control c
    INNER JOIN pcl_universe p
      ON p.acct_no       = c.acct_no
     AND c.crv_strt_dt  <= p.pcl_end_dt
     AND c.crv_end_dt   >= p.pcl_strt_dt
    GROUP BY c.acct_no, p.pcl_strt_dt, p.pcl_end_dt
),
-- PCL leads with Action overlap — one row per PCL lead, now carrying pcl_month1.
overlap_action AS (
    SELECT
        p.pcl_month1,
        p.responder_cli                                               AS pcl_resp,
        p.dt_cl_change                                                AS pcl_resp_dt,
        a.crv_resp,
        a.crv_first_resp_dt
    FROM pcl_universe p
    INNER JOIN crv_action_agg a
      ON a.acct_no      = p.acct_no
     AND a.pcl_strt_dt  = p.pcl_strt_dt
     AND a.pcl_end_dt   = p.pcl_end_dt
),
-- PCL leads with Control overlap — one row per PCL lead, now carrying pcl_month1.
overlap_control AS (
    SELECT
        p.pcl_month1,
        p.responder_cli                                               AS pcl_resp,
        p.dt_cl_change                                                AS pcl_resp_dt,
        c.crv_resp,
        c.crv_first_resp_dt
    FROM pcl_universe p
    INNER JOIN crv_control_agg c
      ON c.acct_no      = p.acct_no
     AND c.pcl_strt_dt  = p.pcl_strt_dt
     AND c.pcl_end_dt   = p.pcl_end_dt
),
-- Overall sentinel rows (one per arm, no month grouping)
agg_overall AS (
    SELECT
        CAST('overall' AS VARCHAR(20))                                                    AS pcl_month,
        CAST('Action'  AS VARCHAR(10))                                                    AS arm,
        COUNT(*)                                                                          AS n_overlap_leads,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 0 THEN 1 ELSE 0 END)                   AS n_crv_only,
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 1 THEN 1 ELSE 0 END)                   AS n_pcl_only,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1 THEN 1 ELSE 0 END)                   AS n_both,
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 0 THEN 1 ELSE 0 END)                   AS n_neither,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt < pcl_resp_dt  THEN 1 ELSE 0 END)             AS n_both_crv_first,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND pcl_resp_dt < crv_first_resp_dt  THEN 1 ELSE 0 END)             AS n_both_pcl_first,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt = pcl_resp_dt  THEN 1 ELSE 0 END)             AS n_both_same_day
    FROM overlap_action

    UNION ALL

    SELECT
        CAST('overall' AS VARCHAR(20)),
        CAST('Control' AS VARCHAR(10)),
        COUNT(*),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt < pcl_resp_dt  THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND pcl_resp_dt < crv_first_resp_dt  THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt = pcl_resp_dt  THEN 1 ELSE 0 END)
    FROM overlap_control
),
-- Monthly rows (one per pcl_month × arm)
agg_monthly AS (
    SELECT
        CAST(pcl_month1 AS VARCHAR(20))                                                   AS pcl_month,
        CAST('Action'   AS VARCHAR(10))                                                   AS arm,
        COUNT(*)                                                                          AS n_overlap_leads,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 0 THEN 1 ELSE 0 END)                   AS n_crv_only,
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 1 THEN 1 ELSE 0 END)                   AS n_pcl_only,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1 THEN 1 ELSE 0 END)                   AS n_both,
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 0 THEN 1 ELSE 0 END)                   AS n_neither,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt < pcl_resp_dt  THEN 1 ELSE 0 END)             AS n_both_crv_first,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND pcl_resp_dt < crv_first_resp_dt  THEN 1 ELSE 0 END)             AS n_both_pcl_first,
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt = pcl_resp_dt  THEN 1 ELSE 0 END)             AS n_both_same_day
    FROM overlap_action
    GROUP BY pcl_month1

    UNION ALL

    SELECT
        CAST(pcl_month1 AS VARCHAR(20)),
        CAST('Control'  AS VARCHAR(10)),
        COUNT(*),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 0 AND pcl_resp = 0 THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt < pcl_resp_dt  THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND pcl_resp_dt < crv_first_resp_dt  THEN 1 ELSE 0 END),
        SUM(CASE WHEN crv_resp = 1 AND pcl_resp = 1
                      AND crv_first_resp_dt = pcl_resp_dt  THEN 1 ELSE 0 END)
    FROM overlap_control
    GROUP BY pcl_month1
)
SELECT * FROM agg_overall
UNION ALL
SELECT * FROM agg_monthly
ORDER BY 1, 2
;
