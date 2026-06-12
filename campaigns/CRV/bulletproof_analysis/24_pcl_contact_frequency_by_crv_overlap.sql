-- ============================================================================
-- ENGINE: Starburst/Trino (GA4 = EDL table in the query) — Trino syntax.
-- GA4 events per s2_code_selection.md (channel_bulletproofing, FINAL 2026-06-12): impression = view_promotion (view_item = co-fired twin artifact, discarded); ID allowlist updated.
-- Identity key = it_item_id ('i_'+offer id) per s7 2026-06-12: format-stable all platforms, zero disagreement, catches rows where promotion_id is absent.
-- Q24 — PCL CONTACT FREQUENCY x CRV OVERLAP STATUS (Feb-Apr 2026, two statements)
-- Statement 1: contact frequency = CUMULATIVE touch number over the FULL 20-month
--   history (Oct-2024+, Q11 crv_touch_number convention) read at the Feb-Apr 2026
--   measured deployments — "how many times contacted by the time we measured them"
--   (1..5+) — plus engagement overlay: viewed / clicked / view-days in-window.
-- Statement 2: CHANNEL-side contact frequency — clients by number of deployments where
--   the banner actually reached them (0..3+, ~= months seen), clicks + converters per bucket.
-- NOT CRV frequency: counts PCL contacts/engagement, sliced by CRV exposure:
--   overlap_action / overlap_control / no_overlap (action > control precedence, Q20).
-- CRV offer window: restricted to offer_end_date 2026-02-01..2026-04-30 (matched to PCL
--   Feb–Apr cohort window); CRV offers still running past Apr 30 are excluded → those
--   clients classify as no_overlap.
-- GA4: it_promotion_id PCL 12-id allowlist + view_promotion/select_promotion (s2_code_selection.md FINAL).
-- Co-applicant accounts EXCLUDED in both statements (Section E2 convention).
-- ============================================================================

-- Statement 1: contact frequency x engagement overlay
-- Co-applicant accounts EXCLUDED (Section E2 convention: CIDM CLNT_NO_A present and <> CLNT_NO).
WITH coapp_accts AS (
    SELECT acct_no
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
    WHERE CLNT_NO_A IS NOT NULL
      AND CLNT_NO_A <> CLNT_NO
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' AND action_control = 'Control'
),
pcl_history AS (   -- full 20-month history ranks every touch (Q11 convention, per acct)
    SELECT p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli,
           ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.treatmt_strt_dt) AS pcl_touch_number
    FROM dl_mr_prod.cards_pli_decision_resp p
    LEFT JOIN coapp_accts x
      ON x.acct_no = p.acct_no
    WHERE p.treatmt_strt_dt >= DATE '2024-10-01'
      AND p.channel LIKE '%MB%'
      AND x.acct_no IS NULL
),
pcl_universe AS (   -- measured leads = Feb-Apr 2026, carrying their cumulative touch number
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli, pcl_touch_number
    FROM pcl_history
    WHERE treatmt_strt_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30'
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.responder_cli, p.pcl_touch_number,
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
        MAX(pcl_touch_number) AS pcl_contacts_20mo,   -- cumulative contacts by end of measured window
        COUNT(*)              AS deployments_in_window,
        MAX(responder_cli)    AS responded
    FROM pcl_flagged
    GROUP BY clnt_no
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN event_name = 'view_promotion'    THEN 1 ELSE 0 END AS view_e,
        CASE WHEN event_name = 'select_promotion' THEN 1 ELSE 0 END AS click_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND month >= '02'
      AND event_date >= DATE '2026-02-01'
      AND it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
      AND event_name IN ('view_promotion','select_promotion')
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
    CASE WHEN cf.pcl_contacts_20mo >= 5 THEN '5+'
         ELSE CAST(cf.pcl_contacts_20mo AS VARCHAR) END AS pcl_contact_freq_20mo,
    COUNT(*)                          AS clients,
    SUM(cf.deployments_in_window)     AS deployments_in_window_total,
    SUM(cf.responded)                 AS converters,
    SUM(COALESCE(e.viewed, 0))        AS view_users,
    SUM(COALESCE(e.clicked, 0))       AS click_users,
    SUM(COALESCE(e.view_days, 0))     AS view_days_total,
    SUM(CASE WHEN COALESCE(e.viewed, 0) = 1 THEN cf.responded ELSE 0 END) AS converters_viewed
FROM client_freq cf
LEFT JOIN client_eng e ON e.clnt_no = cf.clnt_no
GROUP BY 1, 2
ORDER BY 1, 2;


-- Statement 2: CHANNEL-SIDE contact frequency — mirror of Statement 1 with the banner as
-- the frequency source: in how many of their Feb-Apr deployments did the banner actually
-- reach the client (>=1 view inside that deployment's own window)? Clients bucketed by
-- banner-reached deployments (0..3+ — one PCL deployment per month, so this ~= months seen),
-- with click interaction and converters per bucket.
-- Co-applicant accounts EXCLUDED (Section E2 convention).
WITH coapp_accts AS (
    SELECT acct_no
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
    WHERE CLNT_NO_A IS NOT NULL
      AND CLNT_NO_A <> CLNT_NO
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' AND channels_deployed LIKE '%IM%' AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date BETWEEN DATE '2026-02-01' AND DATE '2026-04-30' AND action_control = 'Control'
),
pcl_universe AS (
    SELECT p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli
    FROM dl_mr_prod.cards_pli_decision_resp p
    LEFT JOIN coapp_accts x
      ON x.acct_no = p.acct_no
    WHERE p.treatmt_strt_dt BETWEEN DATE '2026-02-01' AND DATE '2026-04-30'
      AND p.channel LIKE '%MB%'
      AND x.acct_no IS NULL
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        CASE WHEN event_name = 'view_promotion'    THEN 1 ELSE 0 END AS view_e,
        CASE WHEN event_name = 'select_promotion' THEN 1 ELSE 0 END AS click_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026'
      AND month >= '02'
      AND event_date >= DATE '2026-02-01'
      AND it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717','i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
      AND event_name IN ('view_promotion','select_promotion')
),
dep_eng AS (   -- deployment grain: did the banner reach / get clicked inside THIS deployment's window
    SELECT
        f.clnt_no, f.acct_no, f.treatmt_strt_dt, f.responder_cli,
        f.action_flag, f.control_flag,
        COALESCE(MAX(g.view_e),  0) AS dep_viewed,
        COALESCE(MAX(g.click_e), 0) AS dep_clicked
    FROM pcl_flagged f
    LEFT JOIN ga4 g
      ON g.clnt_no = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY 1, 2, 3, 4, 5, 6
),
client_roll AS (
    SELECT
        clnt_no,
        CASE WHEN MAX(action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(control_flag) = 1 THEN 'overlap_control'
             ELSE                            'no_overlap' END AS overlap_status,
        COUNT(*)           AS deployments,
        SUM(dep_viewed)    AS deployments_banner_seen,
        SUM(dep_clicked)   AS deployments_banner_clicked,
        MAX(responder_cli) AS responded
    FROM dep_eng
    GROUP BY clnt_no
)
SELECT
    overlap_status,
    CASE WHEN deployments_banner_seen >= 3 THEN '3+'
         ELSE CAST(deployments_banner_seen AS VARCHAR) END AS banner_contact_freq,
    COUNT(*)                                               AS clients,
    SUM(deployments)                                       AS deployments_total,
    SUM(CASE WHEN deployments_banner_clicked >= 1 THEN 1 ELSE 0 END) AS click_users,
    SUM(responded)                                         AS converters
FROM client_roll
GROUP BY 1, 2
ORDER BY 1, 2;
