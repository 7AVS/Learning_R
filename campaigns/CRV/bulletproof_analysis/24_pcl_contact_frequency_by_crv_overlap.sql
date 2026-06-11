-- ============================================================================
-- Q24 — PCL CONTACT FREQUENCY x CRV OVERLAP STATUS  (deployment-level frequency)
-- NOT CRV frequency: counts PCL deployments received per client (Feb-Apr 2026
-- mobile universe, same as Q20), sliced by CRV exposure within the same window:
-- overlap_action / overlap_control / no_overlap (never-CRV).
-- Frequency unit = distinct PCL deployment (acct_no x treatmt_strt_dt).
-- Overlap precedence: action > control > no_overlap (Q20 convention).
-- CAVEAT (open): CRV co-applicant targeting may create spurious overlap — CRV
-- contacts the co-applicant, PCL the primary, same account identifiers. Pending
-- CRV technical spec; candidate fields joint_acct / acct_relation are undecoded.
-- ============================================================================
WITH crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2026-02-01' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2026-02-01' AND action_control = 'Control'
),
pcl_universe AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt
    FROM dl_mr_prod.cards_pli_decision_resp
    WHERE treatmt_strt_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30'
      AND channel LIKE '%MB%'
),
overlap_action_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_action c
      ON c.acct_no = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
),
overlap_control_keys AS (
    SELECT DISTINCT p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt
    FROM pcl_universe p
    INNER JOIN crv_control c
      ON c.acct_no = p.acct_no
     AND c.offer_start_date <= p.treatmt_end_dt AND c.offer_end_date >= p.treatmt_strt_dt
),
pcl_flagged AS (
    SELECT
        p.clnt_no, p.acct_no, p.treatmt_strt_dt,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt
),
client_freq AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END AS overlap_status,
        COUNT(*) AS pcl_deployments
    FROM pcl_flagged
    GROUP BY clnt_no
)
SELECT
    overlap_status,
    CASE WHEN pcl_deployments >= 5 THEN '5+'
         ELSE CAST(pcl_deployments AS VARCHAR) END AS pcl_contact_freq,
    COUNT(*)             AS clients,
    SUM(pcl_deployments) AS pcl_deployments_total
FROM client_freq
GROUP BY 1, 2
ORDER BY 1, 2;
