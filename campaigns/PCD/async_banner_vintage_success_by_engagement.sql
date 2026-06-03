-- async_banner_vintage_success_by_engagement.sql
-- Engine: Starburst (Trino). Federated: Teradata cohort/conversion + edl0_im GA4.
--   (Sibling async_banner_vintage_success.sql is Teradata-native; this one is Trino because the
--    engagement split needs GA4, so date arithmetic uses date_diff(). O2P blocks hit the
--    federated CR_APP chain -> slow. Run ONE block at a time.)
--
-- All the vintage success measures each campaign has, OVERALL grain (no segment), + engagement.
--
-- DENOMINATOR (total_population) = DEPLOYMENT SIZE. It splits ONLY by test_control_flag and
--   cohort_arm -- NOT by engaged_class. Every engaged_class row carries the SAME deployment
--   denominator. Engagement splits the NUMERATOR (responders) only, so the per-class rates are
--   all on one base and add up: PRE/dep + POST/dep + NOT/dep = total conversion rate; ANY/dep.
--
-- engaged_class (numerator split; one exclusive label per client + ANY roll-up):
--   ENGAGED_PRE  = engaged ON/BEFORE first conversion + engaged non-converters
--   ENGAGED_POST = converted THEN engaged (mis-signal)
--   NOT_ENGAGED  = never touched the banner in window
--   ENGAGED_ANY  = roll-up = PRE + POST
--   Timing anchor = client's FIRST/broad conversion (matters only for O2P). Day spine 0..60.


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD   any/target/upgrade + NIBT. arm=ASYNC via strategy_seg_cd.   ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
class_list AS (SELECT engaged_class FROM (VALUES ('ENGAGED_PRE'),('ENGAGED_POST'),('NOT_ENGAGED'),('ENGAGED_ANY')) AS t(engaged_class)),
pcd_cohort AS (
    SELECT
        clnt_no, response_start, response_start AS wave_dt,
        responder_anyproduct, responder_targetproduct, responder_upgrade_path,
        dt_prod_change, nibt_expected_value, nibt_expec_value_upgradepath,
        CASE WHEN strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6','MFB8UJPY','MFB9BX97','MFB9HYQ7')
             THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,
        CASE WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
             WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST' END AS test_control_flag
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD' AND response_start >= DATE '2026-04-01'
),
pcd_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06') AND event_date >= DATE '2026-04-01'
      AND it_item_name IN ('PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV','PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
pcd_client AS (
    SELECT clnt_no, MIN(response_start) AS anchor_dt,
           MIN(CASE WHEN responder_anyproduct = 1 THEN dt_prod_change END) AS broad_conv_dt
    FROM pcd_cohort WHERE test_control_flag IS NOT NULL GROUP BY clnt_no
),
pcd_eng AS (
    SELECT cl.clnt_no, MIN(g.event_date) AS engage_dt
    FROM pcd_client cl JOIN pcd_ga4 g ON g.clnt_no = cl.clnt_no
        AND g.event_date BETWEEN cl.anchor_dt AND date_add('day', 60, cl.anchor_dt)
    GROUP BY cl.clnt_no
),
pcd_class AS (
    SELECT cl.clnt_no,
        CASE WHEN e.engage_dt IS NULL THEN 'NOT_ENGAGED'
             WHEN cl.broad_conv_dt IS NOT NULL AND e.engage_dt > cl.broad_conv_dt THEN 'ENGAGED_POST'
             ELSE 'ENGAGED_PRE' END AS engaged_class
    FROM pcd_client cl LEFT JOIN pcd_eng e ON e.clnt_no = cl.clnt_no
),
-- DENOMINATOR: plain deployment count, no engaged_class
pcd_population AS (
    SELECT wave_dt, test_control_flag, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
    FROM pcd_cohort WHERE test_control_flag IS NOT NULL
    GROUP BY wave_dt, test_control_flag, cohort_arm
),
-- NUMERATOR base: cohort + engaged_class, plus ANY roll-up rows
pcd_cohort_e AS (
    SELECT c.clnt_no, c.wave_dt, c.test_control_flag, c.cohort_arm, c.response_start, c.dt_prod_change,
           c.responder_anyproduct, c.responder_targetproduct, c.responder_upgrade_path,
           c.nibt_expected_value, c.nibt_expec_value_upgradepath, k.engaged_class
    FROM pcd_cohort c JOIN pcd_class k ON k.clnt_no = c.clnt_no
    WHERE c.test_control_flag IS NOT NULL
    UNION ALL
    SELECT c.clnt_no, c.wave_dt, c.test_control_flag, c.cohort_arm, c.response_start, c.dt_prod_change,
           c.responder_anyproduct, c.responder_targetproduct, c.responder_upgrade_path,
           c.nibt_expected_value, c.nibt_expec_value_upgradepath, CAST('ENGAGED_ANY' AS VARCHAR)
    FROM pcd_cohort c JOIN pcd_class k ON k.clnt_no = c.clnt_no
    WHERE c.test_control_flag IS NOT NULL AND k.engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
pcd_success_daily AS (
    SELECT wave_dt, test_control_flag, cohort_arm, engaged_class,
           date_diff('day', response_start, dt_prod_change) AS vintage_day,
           COUNT(DISTINCT CASE WHEN responder_anyproduct    = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN responder_targetproduct = 1 THEN clnt_no END) AS responders_target,
           COUNT(DISTINCT CASE WHEN responder_upgrade_path  = 1 THEN clnt_no END) AS responders_upgrade,
           SUM(CASE WHEN responder_targetproduct = 1 THEN nibt_expected_value          END) AS nibt_value_target,
           SUM(CASE WHEN responder_upgrade_path  = 1 THEN nibt_expec_value_upgradepath END) AS nibt_value_upgrade
    FROM pcd_cohort_e
    WHERE dt_prod_change IS NOT NULL AND date_diff('day', response_start, dt_prod_change) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),
pcd_base AS (
    SELECT p.wave_dt, p.test_control_flag, p.cohort_arm, cl.engaged_class, v.vd AS vintage_day,
           p.total_population,
           COALESCE(r.responders,0) AS responders, COALESCE(r.responders_target,0) AS responders_target,
           COALESCE(r.responders_upgrade,0) AS responders_upgrade,
           COALESCE(r.nibt_value_target,0) AS nibt_value_target, COALESCE(r.nibt_value_upgrade,0) AS nibt_value_upgrade
    FROM pcd_population p
    CROSS JOIN class_list cl
    CROSS JOIN vintage_days v
    LEFT JOIN pcd_success_daily r
        ON  r.wave_dt = p.wave_dt AND r.test_control_flag = p.test_control_flag
        AND r.cohort_arm = p.cohort_arm AND r.engaged_class = cl.engaged_class AND r.vintage_day = v.vd
)
SELECT
    CAST('PCD' AS VARCHAR) AS campaign,
    wave_dt AS cohort, test_control_flag, cohort_arm, engaged_class, vintage_day,
    total_population, responders, responders_target, responders_upgrade, nibt_value_target, nibt_value_upgrade,
    SUM(responders)        OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
    SUM(responders_target) OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum,
    SUM(responders_upgrade)OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_upgrade_cum,
    SUM(nibt_value_target) OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nibt_value_target_cum,
    SUM(nibt_value_upgrade)OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS nibt_value_upgrade_cum
FROM pcd_base
ORDER BY test_control_flag, cohort_arm, engaged_class, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU   responders=success, responders_target=primary_success.      ║
-- ║   arm=ASYNC if chnl_mb=1. test_control='ALL'.                               ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
class_list AS (SELECT engaged_class FROM (VALUES ('ENGAGED_PRE'),('ENGAGED_POST'),('NOT_ENGAGED'),('ENGAGED_ANY')) AS t(engaged_class)),
ctu_cohort AS (
    SELECT clnt_no, treatmt_strt_dt, treatmt_strt_dt AS wave_dt, primary_success, success, response_dt,
           CASE WHEN chnl_mb = 1 THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm,
           CAST('ALL' AS VARCHAR) AS test_control_flag
    FROM dl_mr_prod.nbo_pba_upgrade
    WHERE tactic_id = '2026098CTU' AND treatmt_strt_dt >= DATE '2026-04-01'
),
ctu_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06') AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102') AND lower(event_name) IN ('view_promotion','select_promotion')
),
ctu_client AS (
    SELECT clnt_no, MIN(treatmt_strt_dt) AS anchor_dt,
           MIN(CASE WHEN success = 1 THEN response_dt END) AS broad_conv_dt
    FROM ctu_cohort GROUP BY clnt_no
),
ctu_eng AS (
    SELECT cl.clnt_no, MIN(g.event_date) AS engage_dt
    FROM ctu_client cl JOIN ctu_ga4 g ON g.clnt_no = cl.clnt_no
        AND g.event_date BETWEEN cl.anchor_dt AND date_add('day', 60, cl.anchor_dt)
    GROUP BY cl.clnt_no
),
ctu_class AS (
    SELECT cl.clnt_no,
        CASE WHEN e.engage_dt IS NULL THEN 'NOT_ENGAGED'
             WHEN cl.broad_conv_dt IS NOT NULL AND e.engage_dt > cl.broad_conv_dt THEN 'ENGAGED_POST'
             ELSE 'ENGAGED_PRE' END AS engaged_class
    FROM ctu_client cl LEFT JOIN ctu_eng e ON e.clnt_no = cl.clnt_no
),
ctu_population AS (
    SELECT wave_dt, test_control_flag, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
    FROM ctu_cohort GROUP BY wave_dt, test_control_flag, cohort_arm
),
ctu_cohort_e AS (
    SELECT c.clnt_no, c.wave_dt, c.test_control_flag, c.cohort_arm, c.treatmt_strt_dt, c.response_dt,
           c.success, c.primary_success, k.engaged_class
    FROM ctu_cohort c JOIN ctu_class k ON k.clnt_no = c.clnt_no
    UNION ALL
    SELECT c.clnt_no, c.wave_dt, c.test_control_flag, c.cohort_arm, c.treatmt_strt_dt, c.response_dt,
           c.success, c.primary_success, CAST('ENGAGED_ANY' AS VARCHAR)
    FROM ctu_cohort c JOIN ctu_class k ON k.clnt_no = c.clnt_no
    WHERE k.engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
ctu_success_daily AS (
    SELECT wave_dt, test_control_flag, cohort_arm, engaged_class,
           date_diff('day', treatmt_strt_dt, response_dt) AS vintage_day,
           COUNT(DISTINCT CASE WHEN success         = 1 THEN clnt_no END) AS responders,
           COUNT(DISTINCT CASE WHEN primary_success = 1 THEN clnt_no END) AS responders_target
    FROM ctu_cohort_e
    WHERE response_dt IS NOT NULL AND date_diff('day', treatmt_strt_dt, response_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),
ctu_base AS (
    SELECT p.wave_dt, p.test_control_flag, p.cohort_arm, cl.engaged_class, v.vd AS vintage_day,
           p.total_population,
           COALESCE(r.responders,0) AS responders, COALESCE(r.responders_target,0) AS responders_target
    FROM ctu_population p
    CROSS JOIN class_list cl
    CROSS JOIN vintage_days v
    LEFT JOIN ctu_success_daily r
        ON  r.wave_dt = p.wave_dt AND r.test_control_flag = p.test_control_flag
        AND r.cohort_arm = p.cohort_arm AND r.engaged_class = cl.engaged_class AND r.vintage_day = v.vd
)
SELECT
    CAST('CTU' AS VARCHAR) AS campaign,
    wave_dt AS cohort, test_control_flag, cohort_arm, engaged_class, vintage_day,
    total_population, responders, responders_target,
    SUM(responders)        OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
    SUM(responders_target) OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum
FROM ctu_base
ORDER BY test_control_flag, cohort_arm, engaged_class, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P   responders=apps IN(40,41,43), responders_target=apps='43'   ║
-- ║   (own first-event dates). arm via rpt_grp allowlist. SLOW (CR_APP chain).  ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
class_list AS (SELECT engaged_class FROM (VALUES ('ENGAGED_PRE'),('ENGAGED_POST'),('NOT_ENGAGED'),('ENGAGED_ANY')) AS t(engaged_class)),
o2p_cohort AS (
    SELECT DISTINCT clnt_no, treatmt_strt_dt, treatmt_strt_dt AS wave_dt,
        CASE WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST' WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL' END AS test_control_flag,
        CASE WHEN TRIM(rpt_grp_cd) IN ('PO2PNL01','PO2PNL03','PO2PNL07','PO2POT01','PO2POT03','PO2POT07','PO2PPR01','PO2PPR03','PO2PPR07')
             THEN 'ASYNC' ELSE 'NON_ASYNC' END AS cohort_arm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P' AND treatmt_strt_dt >= DATE '2026-04-01' AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),
-- O2P conversion = ONE fresh daily snapshot (captr_dt is cumulative; latest captr_dt holds full history). Pinned to latest captr_dt; no accumulation. Base tables lag ~4 weeks.
o2p_apps AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt, d.appl_for_prod_typ
    FROM DDWV01.CR_APP_CLNT_RELTN_DLY      AS a
    JOIN DDWV01.OVRL_CR_APP_DLY            AS b
        ON  b.cr_app_id = a.cr_app_id AND b.sys_src_id = a.sys_src_id AND b.captr_dt = a.captr_dt
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN_DLY AS c
        ON  c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id = a.sys_src_id AND c.captr_dt = a.captr_dt
    JOIN DDWV01.CR_APP_PROD_DLY            AS d
        ON  d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id = c.sys_src_id AND d.captr_dt = c.captr_dt
    WHERE a.captr_dt = (SELECT MAX(captr_dt) FROM DDWV01.CR_APP_PROD_DLY WHERE captr_dt >= DATE '2026-06-01')
      AND b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN ('32','37','45','47','51','56','62')
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_dt >= DATE '2026-04-01'
),
o2p_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06') AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045') AND lower(event_name) IN ('view_promotion','select_promotion')
),
o2p_client AS (SELECT clnt_no, MIN(treatmt_strt_dt) AS anchor_dt FROM o2p_cohort GROUP BY clnt_no),
o2p_conv AS (
    SELECT cl.clnt_no, MIN(ap.app_dt) AS broad_conv_dt
    FROM o2p_client cl JOIN o2p_apps ap ON ap.clnt_no = cl.clnt_no
        AND ap.app_dt BETWEEN cl.anchor_dt AND date_add('day', 60, cl.anchor_dt)
    GROUP BY cl.clnt_no
),
o2p_eng AS (
    SELECT cl.clnt_no, MIN(g.event_date) AS engage_dt
    FROM o2p_client cl JOIN o2p_ga4 g ON g.clnt_no = cl.clnt_no
        AND g.event_date BETWEEN cl.anchor_dt AND date_add('day', 60, cl.anchor_dt)
    GROUP BY cl.clnt_no
),
o2p_class AS (
    SELECT cl.clnt_no,
        CASE WHEN e.engage_dt IS NULL THEN 'NOT_ENGAGED'
             WHEN cv.broad_conv_dt IS NOT NULL AND e.engage_dt > cv.broad_conv_dt THEN 'ENGAGED_POST'
             ELSE 'ENGAGED_PRE' END AS engaged_class
    FROM o2p_client cl LEFT JOIN o2p_conv cv ON cv.clnt_no = cl.clnt_no LEFT JOIN o2p_eng e ON e.clnt_no = cl.clnt_no
),
o2p_population AS (
    SELECT wave_dt, test_control_flag, cohort_arm, COUNT(DISTINCT clnt_no) AS total_population
    FROM o2p_cohort GROUP BY wave_dt, test_control_flag, cohort_arm
),
o2p_cohort_e AS (
    SELECT c.clnt_no, c.wave_dt, c.test_control_flag, c.cohort_arm, c.treatmt_strt_dt, k.engaged_class
    FROM o2p_cohort c JOIN o2p_class k ON k.clnt_no = c.clnt_no
    UNION ALL
    SELECT c.clnt_no, c.wave_dt, c.test_control_flag, c.cohort_arm, c.treatmt_strt_dt, CAST('ENGAGED_ANY' AS VARCHAR)
    FROM o2p_cohort c JOIN o2p_class k ON k.clnt_no = c.clnt_no
    WHERE k.engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
o2p_success_events AS (
    SELECT ce.wave_dt, ce.test_control_flag, ce.cohort_arm, ce.engaged_class, ce.clnt_no, ce.treatmt_strt_dt,
           MIN(a.app_dt) AS first_app_dt,
           MIN(CASE WHEN a.appl_for_prod_typ = '43' THEN a.app_dt END) AS first_app_dt_target
    FROM o2p_cohort_e ce JOIN o2p_apps a ON a.clnt_no = ce.clnt_no
        AND a.app_dt BETWEEN ce.treatmt_strt_dt AND date_add('day', 60, ce.treatmt_strt_dt)
    GROUP BY 1,2,3,4,5,6
),
o2p_responders_daily AS (
    SELECT wave_dt, test_control_flag, cohort_arm, engaged_class,
           date_diff('day', treatmt_strt_dt, first_app_dt) AS vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM o2p_success_events WHERE date_diff('day', treatmt_strt_dt, first_app_dt) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),
o2p_responders_target_daily AS (
    SELECT wave_dt, test_control_flag, cohort_arm, engaged_class,
           date_diff('day', treatmt_strt_dt, first_app_dt_target) AS vintage_day, COUNT(DISTINCT clnt_no) AS responders_target
    FROM o2p_success_events WHERE first_app_dt_target IS NOT NULL
      AND date_diff('day', treatmt_strt_dt, first_app_dt_target) BETWEEN 0 AND 60
    GROUP BY 1,2,3,4,5
),
o2p_base AS (
    SELECT p.wave_dt, p.test_control_flag, p.cohort_arm, cl.engaged_class, v.vd AS vintage_day, p.total_population,
           COALESCE(r1.responders,0) AS responders, COALESCE(r2.responders_target,0) AS responders_target
    FROM o2p_population p
    CROSS JOIN class_list cl
    CROSS JOIN vintage_days v
    LEFT JOIN o2p_responders_daily r1
        ON r1.wave_dt=p.wave_dt AND r1.test_control_flag=p.test_control_flag AND r1.cohort_arm=p.cohort_arm
       AND r1.engaged_class=cl.engaged_class AND r1.vintage_day=v.vd
    LEFT JOIN o2p_responders_target_daily r2
        ON r2.wave_dt=p.wave_dt AND r2.test_control_flag=p.test_control_flag AND r2.cohort_arm=p.cohort_arm
       AND r2.engaged_class=cl.engaged_class AND r2.vintage_day=v.vd
)
SELECT
    CAST('O2P' AS VARCHAR) AS campaign,
    wave_dt AS cohort, test_control_flag, cohort_arm, engaged_class, vintage_day,
    total_population, responders, responders_target,
    SUM(responders)        OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_cum,
    SUM(responders_target) OVER (PARTITION BY wave_dt,test_control_flag,cohort_arm,engaged_class ORDER BY vintage_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS responders_target_cum
FROM o2p_base
ORDER BY test_control_flag, cohort_arm, engaged_class, vintage_day
;
