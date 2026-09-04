-- O2P success validation
-- Validates the campaign-conversion logic against the async-eligible O2P cohort
-- (same RPT_GRP_CD allowlist + TG4/TG7 arm split used in async_banner_vintage_tracker.sql block 3).
-- Success = approved/completed primary card application with app_dt inside the
-- treatment window. Adapted from the SAS source by stripping passthrough boilerplate
-- and joining directly to the cohort instead of substring-matching tactic_id.

WITH
cohort AS (
    SELECT
        clnt_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        TRIM(rpt_grp_cd) AS rpt_grp_cd,
        CASE
            WHEN TRIM(tst_grp_cd) = 'TG4' THEN 'TEST'
            WHEN TRIM(tst_grp_cd) = 'TG7' THEN 'CONTROL'
        END AS test_control_flag
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026099O2P','2026126O2P','2026132O2P')
      AND treatmt_strt_dt >= DATE '2026-04-01'
      AND TRIM(rpt_grp_cd) IN (
            'PO2PNL01','PO2PNL03','PO2PNL07',
            'PO2POT01','PO2POT03','PO2POT07',
            'PO2PPR01','PO2PPR03','PO2PPR07'
          )
      AND TRIM(tst_grp_cd) IN ('TG4','TG7')
),

-- Card-application chain: completed-approved primary card apps from 2025-01-01 onward
applications AS (
    SELECT
        a.clnt_no,
        d.prod_app_dt       AS app_dt,
        d.prod_app_compl_dt AS success_dt
    FROM DDWV01.CR_APP_CLNT_RELTN     AS a
    JOIN DDWV01.OVRL_CR_APP            AS b
        ON  b.cr_app_id  = a.cr_app_id
        AND b.sys_src_id = a.sys_src_id
    JOIN DDWV01.CR_APP_CLNT_PROD_RELTN AS c
        ON  c.cr_app_id          = a.cr_app_id
        AND c.cr_app_clnt_seq_no = a.cr_app_clnt_seq_no
        AND c.sys_src_id         = a.sys_src_id
    JOIN DDWV01.CR_APP_PROD            AS d
        ON  d.cr_app_id          = c.cr_app_id
        AND d.cr_app_prod_seq_no = c.cr_app_prod_seq_no
        AND d.sys_src_id         = c.sys_src_id
    WHERE b.app_typ = 'P'
      AND d.appl_for_prod_typ IN ('40','41','43')
      AND d.prod_app_sts_cd IN (32,37,45,47,51,56,62)
      AND d.prod_app_compl_dt IS NOT NULL
      AND d.prod_app_compl_dt >= DATE '2025-01-01'
),

-- Responder = cohort client with at least one qualifying app inside their treatment window
responders AS (
    SELECT DISTINCT
        c.clnt_no,
        c.rpt_grp_cd,
        c.test_control_flag
    FROM cohort c
    INNER JOIN applications a
        ON  a.clnt_no = c.clnt_no
        AND a.app_dt BETWEEN c.treatmt_strt_dt AND c.treatmt_end_dt
),

seg_counts AS (
    -- ALL grain (seg_counts across reporting groups)
    SELECT
        CAST('ALL'     AS VARCHAR(50)) AS segment,
        CAST('OVERALL' AS VARCHAR(50)) AS segment_level,
        c.test_control_flag,
        COUNT(DISTINCT c.clnt_no) AS cohort_size,
        COUNT(DISTINCT r.clnt_no) AS responders
    FROM cohort c
    LEFT JOIN responders r
        ON  r.clnt_no           = c.clnt_no
        AND r.rpt_grp_cd        = c.rpt_grp_cd
        AND r.test_control_flag = c.test_control_flag
    GROUP BY c.test_control_flag

    UNION ALL

    -- REPORT_GROUP grain (per reporting group)
    SELECT
        'REPORT_GROUP' AS segment,
        c.rpt_grp_cd   AS segment_level,
        c.test_control_flag,
        COUNT(DISTINCT c.clnt_no) AS cohort_size,
        COUNT(DISTINCT r.clnt_no) AS responders
    FROM cohort c
    LEFT JOIN responders r
        ON  r.clnt_no           = c.clnt_no
        AND r.rpt_grp_cd        = c.rpt_grp_cd
        AND r.test_control_flag = c.test_control_flag
    GROUP BY c.rpt_grp_cd, c.test_control_flag
)

SELECT
    segment,
    segment_level,
    test_control_flag,
    cohort_size,
    responders
FROM seg_counts
ORDER BY segment, segment_level, test_control_flag
;
