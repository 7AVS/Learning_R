-- async_banner_vintage_success_by_engagement.sql
-- Engine: Starburst (Trino). Federated: Teradata cohort/conversion + edl0_im GA4.
--   (Plain async_banner_vintage_success.sql is Teradata-native; THIS one can't be — the
--    engaged split needs GA4. O2P / O2P_NONASYNC hit the federated CR_APP chain -> slow.
--    Run ONE block at a time. PCD/CTU are fast; O2P blocks are the heavy ones.)
--
-- Purpose: cumulative conversion curve (vintage_day 0..60) split by engagement class.
-- engaged_class (one EXCLUSIVE label per client, + one ROLL-UP):
--   ENGAGED_PRE  = engaged ON/BEFORE converting  (the correct/defensible signal: engage -> convert)
--                  + engaged non-converters (no conversion to bound them)
--   ENGAGED_POST = converted THEN engaged         (mis-signal; converters only -> rate is a
--                  tautological 100%, use the COUNT not a rate)
--   NOT_ENGAGED  = never touched the banner in window (baseline)
--   ENGAGED_ANY  = ROLL-UP = ENGAGED_PRE + ENGAGED_POST (the "bigger" number, pre-computed)
-- >>> PRE + POST + NOT_ENGAGED = full cohort. ENGAGED_ANY is a roll-up of PRE+POST -
--     do NOT sum all four or you double-count.
--
-- Output per block: campaign | test_control_flag | engaged_class | vintage_day | cohort_size
--                   | converters | converters_cum.  Rate = converters_cum / cohort_size.
-- Grain: one row per client, anchored to their EARLIEST wave. Day spine = sequence(0,60). Counts only.


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 1 — PCD   (conversion = dt_prod_change where responder_anyproduct=1;  ║
-- ║                  banner = it_item_name x4)                                  ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
pcd_cohort AS (
    SELECT
        clnt_no, response_start, responder_anyproduct, dt_prod_change,
        CASE
            WHEN TRIM(test_groups_period) LIKE '%C' THEN 'CONTROL'
            WHEN TRIM(test_groups_period) LIKE '%T' THEN 'TEST'
        END AS test_control_flag
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent = '2026111PCD'
      AND response_start >= DATE '2026-04-01'
),
pcd_anchor AS (
    SELECT
        clnt_no, test_control_flag,
        MIN(response_start)                                              AS anchor_dt,
        MIN(CASE WHEN responder_anyproduct = 1 THEN dt_prod_change END)  AS conv_dt
    FROM pcd_cohort
    WHERE test_control_flag IS NOT NULL
    GROUP BY clnt_no, test_control_flag
),
pcd_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND it_item_name IN (
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_AVP',
            'PB_CC_ALL_26_02_RBC_PCD_SalesModal_cardupgrade_IAV',
            'PB_CC_ALL_26_02_RBC_PCD_PPCN',
            'PB_CC_ALL_26_02_RBC_PCD_Offer_Hub_Banner'
          )
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
pcd_engage AS (
    SELECT a.clnt_no, MIN(g.event_date) AS engage_dt
    FROM pcd_anchor a
    JOIN pcd_ga4 g
        ON  g.clnt_no    = a.clnt_no
        AND g.event_date BETWEEN a.anchor_dt AND date_add('day', 60, a.anchor_dt)
    GROUP BY a.clnt_no
),
pcd_client AS (
    SELECT
        a.test_control_flag,
        CASE
            WHEN e.engage_dt IS NULL                                            THEN 'NOT_ENGAGED'
            WHEN a.conv_dt IS NOT NULL AND e.engage_dt > a.conv_dt              THEN 'ENGAGED_POST'
            ELSE 'ENGAGED_PRE'
        END AS engaged_class,
        CASE WHEN a.conv_dt IS NOT NULL THEN date_diff('day', a.anchor_dt, a.conv_dt) END AS vintage_day
    FROM pcd_anchor a
    LEFT JOIN pcd_engage e ON e.clnt_no = a.clnt_no
),
pcd_class_rows AS (
    SELECT test_control_flag, engaged_class, vintage_day FROM pcd_client
    UNION ALL
    SELECT test_control_flag, 'ENGAGED_ANY', vintage_day
    FROM pcd_client WHERE engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
pcd_population AS (
    SELECT test_control_flag, engaged_class, COUNT(*) AS cohort_size
    FROM pcd_class_rows GROUP BY test_control_flag, engaged_class
),
pcd_conv_daily AS (
    SELECT test_control_flag, engaged_class, vintage_day, COUNT(*) AS converters
    FROM pcd_class_rows
    WHERE vintage_day BETWEEN 0 AND 60
    GROUP BY test_control_flag, engaged_class, vintage_day
),
pcd_base AS (
    SELECT s.test_control_flag, s.engaged_class, v.vd AS vintage_day, s.cohort_size,
           COALESCE(d.converters, 0) AS converters
    FROM pcd_population s
    CROSS JOIN vintage_days v
    LEFT JOIN pcd_conv_daily d
        ON  d.test_control_flag = s.test_control_flag
        AND d.engaged_class     = s.engaged_class
        AND d.vintage_day       = v.vd
)
SELECT
    CAST('PCD' AS VARCHAR) AS campaign,
    test_control_flag, engaged_class, vintage_day, cohort_size, converters,
    SUM(converters) OVER (
        PARTITION BY test_control_flag, engaged_class ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS converters_cum
FROM pcd_base
ORDER BY test_control_flag, engaged_class, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 2 — CTU   (conversion = response_dt where success=1; banner i_300102) ║
-- ║                  No arm -> test_control_flag = 'ALL'.                       ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
ctu_cohort AS (
    SELECT clnt_no, treatmt_strt_dt, success, response_dt
    FROM dl_mr_prod.nbo_pba_upgrade
    WHERE tactic_id = '2026098CTU'
      AND treatmt_strt_dt >= DATE '2026-04-01'
),
ctu_anchor AS (
    SELECT
        clnt_no,
        MIN(treatmt_strt_dt)                             AS anchor_dt,
        MIN(CASE WHEN success = 1 THEN response_dt END)  AS conv_dt
    FROM ctu_cohort
    GROUP BY clnt_no
),
ctu_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_300102')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
ctu_engage AS (
    SELECT a.clnt_no, MIN(g.event_date) AS engage_dt
    FROM ctu_anchor a
    JOIN ctu_ga4 g
        ON  g.clnt_no    = a.clnt_no
        AND g.event_date BETWEEN a.anchor_dt AND date_add('day', 60, a.anchor_dt)
    GROUP BY a.clnt_no
),
ctu_client AS (
    SELECT
        CASE
            WHEN e.engage_dt IS NULL                                THEN 'NOT_ENGAGED'
            WHEN a.conv_dt IS NOT NULL AND e.engage_dt > a.conv_dt  THEN 'ENGAGED_POST'
            ELSE 'ENGAGED_PRE'
        END AS engaged_class,
        CASE WHEN a.conv_dt IS NOT NULL THEN date_diff('day', a.anchor_dt, a.conv_dt) END AS vintage_day
    FROM ctu_anchor a
    LEFT JOIN ctu_engage e ON e.clnt_no = a.clnt_no
),
ctu_class_rows AS (
    SELECT engaged_class, vintage_day FROM ctu_client
    UNION ALL
    SELECT 'ENGAGED_ANY', vintage_day
    FROM ctu_client WHERE engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
ctu_population AS (
    SELECT engaged_class, COUNT(*) AS cohort_size FROM ctu_class_rows GROUP BY engaged_class
),
ctu_conv_daily AS (
    SELECT engaged_class, vintage_day, COUNT(*) AS converters
    FROM ctu_class_rows
    WHERE vintage_day BETWEEN 0 AND 60
    GROUP BY engaged_class, vintage_day
),
ctu_base AS (
    SELECT s.engaged_class, v.vd AS vintage_day, s.cohort_size,
           COALESCE(d.converters, 0) AS converters
    FROM ctu_population s
    CROSS JOIN vintage_days v
    LEFT JOIN ctu_conv_daily d ON d.engaged_class = s.engaged_class AND d.vintage_day = v.vd
)
SELECT
    CAST('CTU' AS VARCHAR) AS campaign,
    CAST('ALL' AS VARCHAR) AS test_control_flag,
    engaged_class, vintage_day, cohort_size, converters,
    SUM(converters) OVER (
        PARTITION BY engaged_class ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS converters_cum
FROM ctu_base
ORDER BY engaged_class, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 3 — O2P   (conversion = first CR_APP in window; banner i_298045)      ║
-- ║   SLOW: federated CR_APP chain. GA4 + apps aggregated separately vs anchor. ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
o2p_cohort AS (
    SELECT DISTINCT
        clnt_no, treatmt_strt_dt,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),
o2p_anchor AS (
    SELECT clnt_no, test_control_flag, MIN(treatmt_strt_dt) AS anchor_dt
    FROM o2p_cohort GROUP BY clnt_no, test_control_flag
),
o2p_apps AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN        AS a
    JOIN DDWV01.OVRL_CR_APP              AS b
        ON  b.cr_app_id  = a.cr_app_id AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN   AS c
        ON  c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no AND c.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD              AS d
        ON  d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no AND d.sys_src_id = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN ('32','37','45','47','51','56','62')
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),
o2p_conv AS (
    SELECT a.clnt_no, MIN(ap.app_dt) AS conv_dt
    FROM o2p_anchor a
    JOIN o2p_apps ap
        ON  ap.clnt_no = a.clnt_no
        AND ap.app_dt  BETWEEN a.anchor_dt AND date_add('day', 60, a.anchor_dt)
    GROUP BY a.clnt_no
),
o2p_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_item_id) IN ('i_298045')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
o2p_engage AS (
    SELECT a.clnt_no, MIN(g.event_date) AS engage_dt
    FROM o2p_anchor a
    JOIN o2p_ga4 g
        ON  g.clnt_no    = a.clnt_no
        AND g.event_date BETWEEN a.anchor_dt AND date_add('day', 60, a.anchor_dt)
    GROUP BY a.clnt_no
),
o2p_client AS (
    SELECT
        a.test_control_flag,
        CASE
            WHEN e.engage_dt IS NULL                                THEN 'NOT_ENGAGED'
            WHEN c.conv_dt IS NOT NULL AND e.engage_dt > c.conv_dt  THEN 'ENGAGED_POST'
            ELSE 'ENGAGED_PRE'
        END AS engaged_class,
        CASE WHEN c.conv_dt IS NOT NULL THEN date_diff('day', a.anchor_dt, c.conv_dt) END AS vintage_day
    FROM o2p_anchor a
    LEFT JOIN o2p_conv   c ON c.clnt_no = a.clnt_no
    LEFT JOIN o2p_engage e ON e.clnt_no = a.clnt_no
),
o2p_class_rows AS (
    SELECT test_control_flag, engaged_class, vintage_day FROM o2p_client
    UNION ALL
    SELECT test_control_flag, 'ENGAGED_ANY', vintage_day
    FROM o2p_client WHERE engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
o2p_population AS (
    SELECT test_control_flag, engaged_class, COUNT(*) AS cohort_size
    FROM o2p_class_rows GROUP BY test_control_flag, engaged_class
),
o2p_conv_daily AS (
    SELECT test_control_flag, engaged_class, vintage_day, COUNT(*) AS converters
    FROM o2p_class_rows
    WHERE vintage_day BETWEEN 0 AND 60
    GROUP BY test_control_flag, engaged_class, vintage_day
),
o2p_base AS (
    SELECT s.test_control_flag, s.engaged_class, v.vd AS vintage_day, s.cohort_size,
           COALESCE(d.converters, 0) AS converters
    FROM o2p_population s
    CROSS JOIN vintage_days v
    LEFT JOIN o2p_conv_daily d
        ON  d.test_control_flag = s.test_control_flag
        AND d.engaged_class     = s.engaged_class
        AND d.vintage_day       = v.vd
)
SELECT
    CAST('O2P' AS VARCHAR) AS campaign,
    test_control_flag, engaged_class, vintage_day, cohort_size, converters,
    SUM(converters) OVER (
        PARTITION BY test_control_flag, engaged_class ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS converters_cum
FROM o2p_base
ORDER BY test_control_flag, engaged_class, vintage_day
;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ BLOCK 4 — O2P_NONASYNC_MB  (same O2P cohort/CR_APP; banner = OLB creatives) ║
-- ║   engagement = lower(it_creative_id) IN ('od_olb','od_xolb').               ║
-- ║   FLAGGED: confirm it_creative_id column name + casing.                     ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
WITH
vintage_days AS (SELECT vd FROM UNNEST(sequence(0, 60)) AS t(vd)),
o2pna_cohort AS (
    SELECT DISTINCT
        clnt_no, treatmt_strt_dt,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id = '2026099O2P'
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),
o2pna_anchor AS (
    SELECT clnt_no, test_control_flag, MIN(treatmt_strt_dt) AS anchor_dt
    FROM o2pna_cohort GROUP BY clnt_no, test_control_flag
),
o2pna_apps AS (
    SELECT a.clnt_no, d.prod_app_dt AS app_dt
    FROM DDWV01.CR_APP_CLNT_RELTN        AS a
    JOIN DDWV01.OVRL_CR_APP              AS b
        ON  b.cr_app_id  = a.cr_app_id AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN   AS c
        ON  c.cr_app_id = a.cr_app_id AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no AND c.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD              AS d
        ON  d.cr_app_id = c.cr_app_id AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no AND d.sys_src_id = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN ('32','37','45','47','51','56','62')
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),
o2pna_conv AS (
    SELECT a.clnt_no, MIN(ap.app_dt) AS conv_dt
    FROM o2pna_anchor a
    JOIN o2pna_apps ap
        ON  ap.clnt_no = a.clnt_no
        AND ap.app_dt  BETWEEN a.anchor_dt AND date_add('day', 60, a.anchor_dt)
    GROUP BY a.clnt_no
),
o2pna_ga4 AS (
    SELECT TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no, event_date
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE year = '2026' AND month IN ('04','05','06')
      AND event_date >= DATE '2026-04-01'
      AND lower(it_creative_id) IN ('od_olb','od_xolb')
      AND lower(event_name) IN ('view_promotion','select_promotion')
),
o2pna_engage AS (
    SELECT a.clnt_no, MIN(g.event_date) AS engage_dt
    FROM o2pna_anchor a
    JOIN o2pna_ga4 g
        ON  g.clnt_no    = a.clnt_no
        AND g.event_date BETWEEN a.anchor_dt AND date_add('day', 60, a.anchor_dt)
    GROUP BY a.clnt_no
),
o2pna_client AS (
    SELECT
        a.test_control_flag,
        CASE
            WHEN e.engage_dt IS NULL                                THEN 'NOT_ENGAGED'
            WHEN c.conv_dt IS NOT NULL AND e.engage_dt > c.conv_dt  THEN 'ENGAGED_POST'
            ELSE 'ENGAGED_PRE'
        END AS engaged_class,
        CASE WHEN c.conv_dt IS NOT NULL THEN date_diff('day', a.anchor_dt, c.conv_dt) END AS vintage_day
    FROM o2pna_anchor a
    LEFT JOIN o2pna_conv   c ON c.clnt_no = a.clnt_no
    LEFT JOIN o2pna_engage e ON e.clnt_no = a.clnt_no
),
o2pna_class_rows AS (
    SELECT test_control_flag, engaged_class, vintage_day FROM o2pna_client
    UNION ALL
    SELECT test_control_flag, 'ENGAGED_ANY', vintage_day
    FROM o2pna_client WHERE engaged_class IN ('ENGAGED_PRE','ENGAGED_POST')
),
o2pna_population AS (
    SELECT test_control_flag, engaged_class, COUNT(*) AS cohort_size
    FROM o2pna_class_rows GROUP BY test_control_flag, engaged_class
),
o2pna_conv_daily AS (
    SELECT test_control_flag, engaged_class, vintage_day, COUNT(*) AS converters
    FROM o2pna_class_rows
    WHERE vintage_day BETWEEN 0 AND 60
    GROUP BY test_control_flag, engaged_class, vintage_day
),
o2pna_base AS (
    SELECT s.test_control_flag, s.engaged_class, v.vd AS vintage_day, s.cohort_size,
           COALESCE(d.converters, 0) AS converters
    FROM o2pna_population s
    CROSS JOIN vintage_days v
    LEFT JOIN o2pna_conv_daily d
        ON  d.test_control_flag = s.test_control_flag
        AND d.engaged_class     = s.engaged_class
        AND d.vintage_day       = v.vd
)
SELECT
    CAST('O2P_NONASYNC_MB' AS VARCHAR) AS campaign,
    test_control_flag, engaged_class, vintage_day, cohort_size, converters,
    SUM(converters) OVER (
        PARTITION BY test_control_flag, engaged_class ORDER BY vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS converters_cum
FROM o2pna_base
ORDER BY test_control_flag, engaged_class, vintage_day
;
