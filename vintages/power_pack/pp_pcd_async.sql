-- pcd_experiment_vintage.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, segment column added 2026-08-10).
--   Exactly 8 columns, this order: cohort_month | deployment | segment | grp | vintage_day
--   | base | responders | responders_cum. Counts only. segment = CAST('All' AS VARCHAR(20))
--   — constant; this async carve-out has no pre-treatment split above tst_grp_cd Test/Control.
-- SCOPE: **EXPERIMENT** — the async mobile banner carve-out inside PCD, NOT
--   the whole campaign. Companion file pcd_campaign_vintage.sql is the
--   campaign-scope curve.
--
-- *** THIS FILE FIXES A REAL BUG ***
--   vintages/pcd_vintage_monthly.sql (the file this replaces) filtered
--   tactic_id_parent = '2026111PCD' and applied NO strategy_seg_cd ASYNC
--   filter anywhere. It therefore mixed ASYNC and NON_ASYNC recipients into
--   one curve, AND it only covered 1 of the 2 confirmed async waves —
--   deployment 2026125PCD was silently missing (it appears in five other
--   validated async files: pcd_success_validation.sql, async_banner_
--   vintage_engagement.sql, click_classification_diagnostic.sql,
--   async_banner_vintage_tracker.sql, async_banner_summary.sql — the old
--   vintages file just never picked it up). Neither issue is fixed by
--   choosing a different scope label — both were plain gaps in the filter
--   logic. Fixed here: both conditions below are now enforced together.
-- ----------------------------------------------------------------------------
-- Engine: Teradata-direct. SYS_CALENDAR spine + the population/base cells both
--   live in VOLATILE TABLEs with COLLECT STATISTICS before the cross join
--   (TDWM unconstrained-product-join guard). CTEs for everything else.
-- ----------------------------------------------------------------------------
-- SOURCES
--   dl_mr_prod.cards_pcd_ongoing_decis_resp   -- curated: population + success
--   DG6V01.TACTIC_EVNT_IP_AR_HIST              -- tst_grp_cd ONLY (see below)
-- ----------------------------------------------------------------------------
-- POPULATION FILTER — BOTH conditions required:
--   (a) tactic_id_parent IN ('2026111PCD', '2026125PCD')
--   (b) strategy_seg_cd  IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
--                             'MFB8UJPY','MFB9BX97','MFB9HYQ7')            -- ASYNC carve-out
--   Both filters applied on the CURATED table, which carries strategy_seg_cd
--   as a dedicated column (char(8)) — validated in async_banner_vintage_
--   success.sql:61 (Teradata-native) and equivalent to the Trino allowlist
--   in async_banner_summary.sql:36-39. On the TACTIC table the Trino-side
--   equivalent of (b) is token 3 of tactic_decisn_vrb_info
--   (element_at(split(regexp_replace(trim(tactic_decisn_vrb_info),' +',' '),
--   ' '),3) IN (...)) — NOT used here: Teradata-direct has no split()/
--   element_at(), and since strategy_seg_cd already lives as its own column
--   on the table we're actually filtering (curated), string-token-parsing
--   the tactic table is unnecessary. The tactic table is only touched below
--   to pull tst_grp_cd for the same, already-scoped population.
-- ----------------------------------------------------------------------------
-- GRP — tst_grp_cd lives on the TACTIC EVENT table, NOT the curated table.
--   VERIFIED 2026-08-10 against schemas/pcd_curated_schemas.md (full column
--   list transcribed from HELP TABLE): dl_mr_prod.cards_pcd_ongoing_decis_resp
--   has NO tst_grp_cd column (it has test_groups_period, a different field).
--   tst_grp_cd is confirmed present on DG6V01.TACTIC_EVNT_IP_AR_HIST. Joined
--   curated -> tactic on (tactic_id = tactic_id_parent, clnt_no = clnt_no),
--   both sides restricted to the same 2 async tactic_ids. Safe join key:
--   TACTIC_ID is unique per deployment (reference_tactic_id_unique_per_
--   deployment.md) — no time-window join needed.
--   grp derivation: TRIM(tst_grp_cd) LIKE '%C' -> 'Control', LIKE '%T' -> 'Test'.
-- ----------------------------------------------------------------------------
-- SUCCESS (ONE metric, per contract rule 4):
--   responder_targetproduct = 1, event date = dt_prod_change, anchor =
--   response_start — same validated primary success flag as the campaign
--   file, restricted to the same (a)+(b) async population filter.
-- ----------------------------------------------------------------------------
-- GRAIN: client (clnt_no). COUNT(DISTINCT clnt_no) throughout.
-- DEPLOYMENT: tactic_id_parent (only 2 values possible here: 2026111PCD,
--   2026125PCD) — its own column, curves not pooled across them.
-- SPINE: vintage_day 0-60 (PCD canon window).
-- FLOOR: every scan >= DATE '2026-01-01' (contract rule 6).
-- [VERIFY]: none open. Both population conditions and the tst_grp_cd source
--   table are confirmed against the repo's validated async files and schema.
-- ----------------------------------------------------------------------------
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcd_experiment_cells;
--   DROP TABLE vt_pcd_experiment_spine;
-- ============================================================================

-- ============================================================================
-- STEP 1: denominator cells (cohort_month x deployment x grp -> base)
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_experiment_cells AS (
    WITH cohort_pop AS (
        SELECT
            clnt_no,
            tactic_id_parent                       AS deployment,
            MIN(response_start)                    AS response_start
        FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
        WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
          AND response_start >= DATE '2026-01-01'
          AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                                   'MFB8UJPY','MFB9BX97','MFB9HYQ7')
        GROUP BY clnt_no, tactic_id_parent
    ),
    arm_lookup AS (
        SELECT DISTINCT
            tactic_id,
            clnt_no,
            CASE
                WHEN TRIM(tst_grp_cd) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
                WHEN TRIM(tst_grp_cd) LIKE '%T' THEN CAST('Test'    AS VARCHAR(20))
            END AS grp
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE tactic_id IN ('2026111PCD', '2026125PCD')
          AND treatmt_strt_dt >= DATE '2026-01-01'
    ),
    cohort_arm AS (
        SELECT
            cp.clnt_no,
            cp.deployment,
            CAST(
                CAST(EXTRACT(YEAR FROM cp.response_start) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM cp.response_start) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM cp.response_start) AS VARCHAR(2))
            AS VARCHAR(7))                          AS cohort_month,
            al.grp
        FROM cohort_pop cp
        INNER JOIN arm_lookup al
            ON al.tactic_id = cp.deployment AND al.clnt_no = cp.clnt_no
        WHERE al.grp IS NOT NULL
    )
    SELECT cohort_month, deployment, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_arm
    GROUP BY cohort_month, deployment, grp
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_experiment_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-60
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_experiment_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 60
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_experiment_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
cohort_pop AS (
    SELECT
        clnt_no,
        tactic_id_parent                       AS deployment,
        MIN(response_start)                    AS response_start
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
      AND response_start >= DATE '2026-01-01'
      AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                               'MFB8UJPY','MFB9BX97','MFB9HYQ7')
    GROUP BY clnt_no, tactic_id_parent
),

arm_lookup AS (
    SELECT DISTINCT
        tactic_id,
        clnt_no,
        CASE
            WHEN TRIM(tst_grp_cd) LIKE '%C' THEN CAST('Control' AS VARCHAR(20))
            WHEN TRIM(tst_grp_cd) LIKE '%T' THEN CAST('Test'    AS VARCHAR(20))
        END AS grp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE tactic_id IN ('2026111PCD', '2026125PCD')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

cohort_arm AS (
    SELECT
        cp.clnt_no,
        cp.deployment,
        CAST(
            CAST(EXTRACT(YEAR FROM cp.response_start) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM cp.response_start) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM cp.response_start) AS VARCHAR(2))
        AS VARCHAR(7))                          AS cohort_month,
        al.grp
    FROM cohort_pop cp
    INNER JOIN arm_lookup al
        ON al.tactic_id = cp.deployment AND al.clnt_no = cp.clnt_no
    WHERE al.grp IS NOT NULL
),

-- success: primary target-product responders only, restricted to the SAME (a)+(b) async filter
success_events AS (
    SELECT
        clnt_no,
        tactic_id_parent AS deployment,
        CAST(dt_prod_change - response_start AS INTEGER) AS vintage_day
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE tactic_id_parent IN ('2026111PCD', '2026125PCD')
      AND response_start >= DATE '2026-01-01'
      AND strategy_seg_cd IN ('MSC8YUS3','MAO28CJ5','MAO2EDB1','MFB8L6X6',
                               'MFB8UJPY','MFB9BX97','MFB9HYQ7')
      AND responder_targetproduct = 1
      AND dt_prod_change IS NOT NULL
),

success_first AS (
    SELECT clnt_no, deployment, MIN(vintage_day) AS vintage_day
    FROM success_events
    GROUP BY clnt_no, deployment
),

numerator AS (
    SELECT ca.cohort_month, ca.deployment, ca.grp, sf.clnt_no, sf.vintage_day
    FROM success_first sf
    INNER JOIN cohort_arm ca
        ON ca.clnt_no = sf.clnt_no AND ca.deployment = sf.deployment
    WHERE sf.vintage_day BETWEEN 0 AND 60
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM numerator
    GROUP BY cohort_month, deployment, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_pcd_experiment_cells c
    CROSS JOIN vt_pcd_experiment_spine s
)

SELECT
    g.cohort_month,
    CAST(g.deployment AS VARCHAR(30))  AS deployment,
    CAST('All' AS VARCHAR(20))         AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.deployment, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.deployment    = g.deployment
    AND dc.grp           = g.grp
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.deployment, g.grp, g.vintage_day;

DROP TABLE vt_pcd_experiment_cells;
DROP TABLE vt_pcd_experiment_spine;
