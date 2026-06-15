-- ============================================================================
-- ENGINE: Starburst/Trino (GA4 = EDL table in the query) — Trino syntax.
-- GA4 events per s2_code_selection.md (FINAL 2026-06-12): impression = view_promotion (view_item discarded);
--   identity key = it_item_id; PCL 12-id allowlist; _reduced table.
-- Q30 — PCL SATURATION BY CRV ARM (Dec 2025-Feb 2026 cohort)
-- Purpose: measure channel saturation (PCL banner view-days within deployment windows) by
--   CRV arm. view-days used as a SECONDARY OUTCOME (Action vs Control difference in view-days
--   received), NEVER a conditioning variable. Keeps Q24's cohort, keys, and overlap logic.
-- Stmt 1: analysis spine — balance check on touch distribution + conversion + view-days
--   by arm x pcl_touch_bucket (stratum = cumulative 20-month contact number at measured wave).
-- Stmt 2: channel saturation histogram — client count + converters by arm x view-day bucket.
-- no_overlap = coverage arm only (no causal interpretation vs Action/Control).
-- Counts only — no rate/pct/mean columns (computed in Excel).
-- Co-applicant accounts EXCLUDED (Section E2 convention).
-- ============================================================================

-- Statement 1: analysis spine (arm x touch stratum)
WITH coapp_accts AS (
    SELECT acct_no
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
    WHERE CLNT_NO_A IS NOT NULL
      AND CLNT_NO_A <> CLNT_NO
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01'
      AND action_control = 'Control'
),
pcl_history AS (   -- full 20-month history ranks every touch (Q11/Q24 convention, per acct)
    SELECT p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli,
           ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.treatmt_strt_dt) AS pcl_touch_number
    FROM dl_mr_prod.cards_pli_decision_resp p
    LEFT JOIN coapp_accts x
      ON x.acct_no = p.acct_no
    WHERE p.treatmt_strt_dt >= DATE '2024-10-01'
      AND p.channel LIKE '%MB%'
      AND x.acct_no IS NULL
),
pcl_universe AS (   -- measured leads = Dec 2025-Feb 2026, carrying cumulative touch number
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli, pcl_touch_number
    FROM pcl_history
    WHERE treatmt_strt_dt BETWEEN DATE '2025-12-01' AND DATE '2026-02-28'
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli, p.pcl_touch_number,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt AND oa.treatmt_end_dt = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt AND oc.treatmt_end_dt = p.treatmt_end_dt
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        1 AS view_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE ( (year = '2025' AND month IN ('12')) OR (year = '2026' AND month IN ('01','02','03','04','05')) )
      AND event_date >= DATE '2025-12-01'
      AND it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
                         'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
      AND event_name = 'view_promotion'
),
dep_view_days AS (   -- deployment grain: count distinct view-days within THIS deployment's window
    SELECT
        f.clnt_no, f.acct_no, f.treatmt_strt_dt,
        COUNT(DISTINCT CASE WHEN g.view_e = 1 THEN g.event_date END) AS dep_view_days,
        MAX(g.view_e)                                                  AS dep_any_view
    FROM pcl_flagged f
    LEFT JOIN ga4 g
      ON g.clnt_no = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.acct_no, f.treatmt_strt_dt
),
client_roll AS (   -- client grain: aggregate across all deployments in the window
    SELECT
        f.clnt_no,
        CASE WHEN MAX(f.action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(f.control_flag) = 1 THEN 'overlap_control'
             ELSE                               'no_overlap' END AS overlap_status,
        MAX(f.pcl_touch_number)   AS pcl_touch_number,
        MAX(f.responder_cli)      AS responded,
        MAX(COALESCE(dv.dep_any_view, 0))    AS any_pcl_view,
        SUM(COALESCE(dv.dep_view_days,  0))  AS pcl_view_days
    FROM pcl_flagged f
    LEFT JOIN dep_view_days dv
      ON dv.clnt_no = f.clnt_no AND dv.acct_no = f.acct_no AND dv.treatmt_strt_dt = f.treatmt_strt_dt
    GROUP BY f.clnt_no
)
SELECT
    overlap_status,
    CASE WHEN pcl_touch_number >= 5 THEN '5+'
         ELSE CAST(pcl_touch_number AS varchar) END AS pcl_touch_bucket,
    COUNT(DISTINCT clnt_no)   AS n_clients,
    SUM(responded)            AS converters,
    SUM(any_pcl_view)         AS clients_any_pcl_view,
    SUM(pcl_view_days)        AS total_pcl_view_days
FROM client_roll
GROUP BY 1, 2
ORDER BY 1, 2;


-- Statement 2: channel saturation histogram (arm x view-day bucket)
WITH coapp_accts AS (
    SELECT acct_no
    FROM DTZTAU.CIDM_CARDS_ACCT_ATTRS
    WHERE CLNT_NO_A IS NOT NULL
      AND CLNT_NO_A <> CLNT_NO
),
crv_action AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01'
      AND channels_deployed LIKE '%IM%'
      AND action_control = 'Action'
),
crv_control AS (
    SELECT acct_no, offer_start_date, offer_end_date
    FROM dl_mr_prod.cards_crv_install_decis_resp
    WHERE offer_end_date >= DATE '2025-12-01'
      AND action_control = 'Control'
),
pcl_history AS (
    SELECT p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli,
           ROW_NUMBER() OVER (PARTITION BY p.acct_no ORDER BY p.treatmt_strt_dt) AS pcl_touch_number
    FROM dl_mr_prod.cards_pli_decision_resp p
    LEFT JOIN coapp_accts x
      ON x.acct_no = p.acct_no
    WHERE p.treatmt_strt_dt >= DATE '2024-10-01'
      AND p.channel LIKE '%MB%'
      AND x.acct_no IS NULL
),
pcl_universe AS (
    SELECT clnt_no, acct_no, treatmt_strt_dt, treatmt_end_dt, responder_cli, pcl_touch_number
    FROM pcl_history
    WHERE treatmt_strt_dt BETWEEN DATE '2025-12-01' AND DATE '2026-02-28'
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
        p.clnt_no, p.acct_no, p.treatmt_strt_dt, p.treatmt_end_dt, p.responder_cli, p.pcl_touch_number,
        CASE WHEN oa.acct_no IS NOT NULL THEN 1 ELSE 0 END AS action_flag,
        CASE WHEN oc.acct_no IS NOT NULL THEN 1 ELSE 0 END AS control_flag
    FROM pcl_universe p
    LEFT JOIN overlap_action_keys oa
      ON oa.acct_no = p.acct_no AND oa.treatmt_strt_dt = p.treatmt_strt_dt AND oa.treatmt_end_dt = p.treatmt_end_dt
    LEFT JOIN overlap_control_keys oc
      ON oc.acct_no = p.acct_no AND oc.treatmt_strt_dt = p.treatmt_strt_dt AND oc.treatmt_end_dt = p.treatmt_end_dt
),
ga4 AS (
    SELECT
        TRY_CAST(up_srf_id2_value AS BIGINT) AS clnt_no,
        event_date,
        1 AS view_e
    FROM edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
    WHERE ( (year = '2025' AND month IN ('12')) OR (year = '2026' AND month IN ('01','02','03','04','05')) )
      AND event_date >= DATE '2025-12-01'
      AND it_item_id IN ('i_156764','i_156788','i_162326','i_167715','i_167716','i_167717',
                         'i_289661','i_289662','i_289664','i_289665','i_289666','i_289698')
      AND event_name = 'view_promotion'
),
dep_view_days AS (
    SELECT
        f.clnt_no, f.acct_no, f.treatmt_strt_dt,
        COUNT(DISTINCT CASE WHEN g.view_e = 1 THEN g.event_date END) AS dep_view_days
    FROM pcl_flagged f
    LEFT JOIN ga4 g
      ON g.clnt_no = f.clnt_no
     AND g.event_date BETWEEN f.treatmt_strt_dt AND f.treatmt_end_dt
    GROUP BY f.clnt_no, f.acct_no, f.treatmt_strt_dt
),
client_roll AS (
    SELECT
        f.clnt_no,
        CASE WHEN MAX(f.action_flag)  = 1 THEN 'overlap_action'
             WHEN MAX(f.control_flag) = 1 THEN 'overlap_control'
             ELSE                               'no_overlap' END AS overlap_status,
        MAX(f.responder_cli)         AS responded,
        SUM(COALESCE(dv.dep_view_days, 0)) AS pcl_view_days
    FROM pcl_flagged f
    LEFT JOIN dep_view_days dv
      ON dv.clnt_no = f.clnt_no AND dv.acct_no = f.acct_no AND dv.treatmt_strt_dt = f.treatmt_strt_dt
    GROUP BY f.clnt_no
)
SELECT
    overlap_status,
    CASE
        WHEN pcl_view_days = 0                    THEN '0'
        WHEN pcl_view_days = 1                    THEN '1'
        WHEN pcl_view_days = 2                    THEN '2'
        WHEN pcl_view_days = 3                    THEN '3'
        WHEN pcl_view_days = 4                    THEN '4'
        WHEN pcl_view_days = 5                    THEN '5'
        WHEN pcl_view_days BETWEEN 6  AND 10      THEN '06-10'
        WHEN pcl_view_days BETWEEN 11 AND 20      THEN '11-20'
        ELSE                                           '21+'
    END AS view_day_bucket,
    CASE
        WHEN pcl_view_days = 0                    THEN 0
        WHEN pcl_view_days = 1                    THEN 1
        WHEN pcl_view_days = 2                    THEN 2
        WHEN pcl_view_days = 3                    THEN 3
        WHEN pcl_view_days = 4                    THEN 4
        WHEN pcl_view_days = 5                    THEN 5
        WHEN pcl_view_days BETWEEN 6  AND 10      THEN 6
        WHEN pcl_view_days BETWEEN 11 AND 20      THEN 11
        ELSE                                           21
    END AS sort_key,
    COUNT(DISTINCT clnt_no) AS n_clients,
    SUM(responded)          AS converters
FROM client_roll
GROUP BY 1, 2, 3
ORDER BY 1, 3;
