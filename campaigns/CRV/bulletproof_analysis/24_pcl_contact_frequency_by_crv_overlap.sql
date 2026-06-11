-- ============================================================================
-- Q24 — PCL CONTACT FREQUENCY x CRV OVERLAP STATUS (Feb-Apr 2026, two statements)
-- Statement 1: deployment-level frequency + engagement overlay — per client: PCL
--   deployments received (1..5+), did they view / click the PCL banner, view-days.
-- Statement 2: engagement-level frequency — distribution of PCL banner view-days
--   per client (0..5+), same overlap slices.
-- NOT CRV frequency: counts PCL contacts/engagement, sliced by CRV exposure:
--   overlap_action / overlap_control / no_overlap (action > control precedence, Q20).
-- GA4: it_promotion_id PCL list + view_item/select_promotion (Q20 conventions —
--   impression-event question view_item vs view_promotion still open).
-- CAVEAT (open): CRV co-applicant targeting may create spurious overlap (CRV
--   contacts co-applicant, PCL the primary, same acct ids). Pending CRV tech spec.
-- ============================================================================

-- Statement 1: contact frequency x engagement overlay
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
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN event_name = 'view_item'        THEN 1 ELSE 0 END AS view_e,
        CASE WHEN event_name = 'select_promotion' THEN 1 ELSE 0 END AS click_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND month >= '02'
      AND event_date >= DATE '2026-02-01'
      AND it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
      AND event_name IN ('view_item','select_promotion')
),
client_eng AS (
    SELECT
        f.clnt_no,
        MAX(g.view_e)  AS viewed,
        MAX(g.click_e) AS clicked,
        COUNT(DISTINCT CASE WHEN g.view_e = 1 THEN g.event_date END) AS view_days
    FROM (SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_end_dt FROM pcl_universe) f
    INNER JOIN ga4 g
      ON g.clnt_no = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY 1
)
SELECT
    cf.overlap_status,
    CASE WHEN cf.pcl_deployments >= 5 THEN '5+'
         ELSE CAST(cf.pcl_deployments AS VARCHAR) END AS pcl_contact_freq,
    COUNT(*)                          AS clients,
    SUM(cf.pcl_deployments)           AS pcl_deployments_total,
    SUM(COALESCE(e.viewed, 0))        AS view_users,
    SUM(COALESCE(e.clicked, 0))       AS click_users,
    SUM(COALESCE(e.view_days, 0))     AS view_days_total
FROM client_freq cf
LEFT JOIN client_eng e ON e.clnt_no = cf.clnt_no
GROUP BY 1, 2
ORDER BY 1, 2;


-- Statement 2: engagement-level frequency — clients by PCL banner view-days (0..5+)
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
client_status AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END AS overlap_status
    FROM pcl_flagged
    GROUP BY clnt_no
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END AS view_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND month >= '02'
      AND event_date >= DATE '2026-02-01'
      AND it_promotion_id IN ('156764','156788','162326','289661','289662','289664','289665','289666')
      AND event_name = 'view_item'
),
client_eng AS (
    SELECT
        f.clnt_no,
        COUNT(DISTINCT g.event_date) AS view_days
    FROM (SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_end_dt FROM pcl_universe) f
    INNER JOIN ga4 g
      ON g.clnt_no = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY 1
)
SELECT
    s.overlap_status,
    CASE WHEN COALESCE(e.view_days, 0) >= 5 THEN '5+'
         ELSE CAST(COALESCE(e.view_days, 0) AS VARCHAR) END AS pcl_view_day_freq,
    COUNT(*) AS clients
FROM client_status s
LEFT JOIN client_eng e ON e.clnt_no = s.clnt_no
GROUP BY 1, 2
ORDER BY 1, 2;
