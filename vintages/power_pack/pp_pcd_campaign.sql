-- pcd_campaign_vintage.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). Exactly 7 columns,
--   this order: cohort_month | deployment | grp | vintage_day | base | responders
--   | responders_cum. Counts only. No rates, no strategy/model/product breakouts.
-- SCOPE: **CAMPAIGN** — the whole PCD (Product Card Upgrade) campaign, every
--   deployment/wave, past and future. NO async carve-out. This is the
--   "how did the whole campaign perform" curve, distinct from
--   pcd_experiment_vintage.sql (the async-banner-only experiment curve).
-- Engine: Teradata-direct. SYS_CALENDAR spine + the population/base cells both
--   live in VOLATILE TABLEs with COLLECT STATISTICS before the cross join
--   (TDWM unconstrained-product-join guard). CTEs for everything else.
-- ----------------------------------------------------------------------------
-- SOURCES
--   dl_mr_prod.cards_pcd_ongoing_decis_resp   -- curated: population + success
--   DG6V01.TACTIC_EVNT_IP_AR_HIST              -- tst_grp_cd ONLY (see below)
-- ----------------------------------------------------------------------------
-- POPULATION FILTER (pattern, not hardcoded IDs — new waves appear automatically):
--   SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
--   (positions 8-10 of a tactic id are the campaign mnemonic — see CLAUDE.md
--   "TACTIC_ID structure". Sanity-checked 2026-08-10 against the 8 Q3 2026
--   deployments Andre gave as ground truth: 2026089PCD, 2026125PCD, 2026111PCD,
--   2026152PCD, 2026174PCD, 2026187PCD, 2026147PCD, 2026195PCD — every one of
--   these is 10 chars ending 'PCD', so the pattern picks up all 8. None of the
--   8 IDs are hardcoded anywhere in this file; they are reference-only.)
-- ----------------------------------------------------------------------------
-- GRP — tst_grp_cd lives on the TACTIC EVENT table, NOT the curated table.
--   VERIFIED 2026-08-10 against schemas/pcd_curated_schemas.md (full column
--   list transcribed from HELP TABLE): dl_mr_prod.cards_pcd_ongoing_decis_resp
--   has NO tst_grp_cd column. It has test_groups_period (varchar(25), the
--   curated team's own period-level test/control string) and act_ctl_seg —
--   neither is tst_grp_cd. tst_grp_cd is confirmed present on
--   DG6V01.TACTIC_EVNT_IP_AR_HIST (used by async_banner_summary.sql and the
--   AUH vintage files). This file therefore JOINS curated -> tactic on
--   (tactic_id = tactic_id_parent, clnt_no = clnt_no) to pull tst_grp_cd.
--   That join key is safe: TACTIC_ID is unique per deployment/wave (see
--   memory reference_tactic_id_unique_per_deployment.md) — no time-window
--   join needed, tactic_id already scopes the wave.
--   grp derivation: TRIM(tst_grp_cd) LIKE '%C' -> 'Control', LIKE '%T' -> 'Test'.
-- ----------------------------------------------------------------------------
-- SUCCESS (ONE metric, per contract rule 4):
--   responder_targetproduct = 1, event date = dt_prod_change, anchor =
--   response_start. This is the curated table's own pre-computed primary
--   success flag — the validated choice per async_banner_vintage_success.sql's
--   header, which explicitly prefers this curated flag over reconstructing
--   success via tactic-event parsing + a DLY_FULL_PORTFOLIO join.
--   (responder_anyproduct / responder_upgrade_path are secondary metrics —
--   dropped here per contract rule 4, not folded into extra columns.)
-- ----------------------------------------------------------------------------
-- GRAIN: client (clnt_no). COUNT(DISTINCT clnt_no) throughout — never COUNT(*).
-- DEPLOYMENT: tactic_id_parent, its own column. Curves never pool across
--   deployments/waves — each wave gets its own (cohort_month, deployment, grp)
--   cell family down the spine.
-- SPINE: vintage_day 0-60 (PCD canon window, unchanged from prior file).
-- FLOOR: every scan >= DATE '2026-01-01' (contract rule 6).
-- [VERIFY]: none open for this file. The tst_grp_cd table-location question
--   above was the one real unknown and is resolved by direct schema check.
-- ----------------------------------------------------------------------------
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcd_campaign_cells;
--   DROP TABLE vt_pcd_campaign_spine;
-- ============================================================================

-- ============================================================================
-- STEP 1: denominator cells (cohort_month x deployment x grp -> base)
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_campaign_cells AS (
    WITH cohort_pop AS (
        SELECT
            clnt_no,
            tactic_id_parent                       AS deployment,
            MIN(response_start)                    AS response_start
        FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
        WHERE SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
          AND response_start >= DATE '2026-01-01'
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
        WHERE SUBSTR(tactic_id, 8, 3) = 'PCD'
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

COLLECT STATISTICS ON vt_pcd_campaign_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-60
-- ============================================================================
CREATE VOLATILE TABLE vt_pcd_campaign_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 60
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcd_campaign_spine COLUMN (vintage_day);

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
    WHERE SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
      AND response_start >= DATE '2026-01-01'
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
    WHERE SUBSTR(tactic_id, 8, 3) = 'PCD'
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

-- success: primary target-product responders only, restricted to same PCD pattern + floor
success_events AS (
    SELECT
        clnt_no,
        tactic_id_parent AS deployment,
        CAST(dt_prod_change - response_start AS INTEGER) AS vintage_day
    FROM dl_mr_prod.cards_pcd_ongoing_decis_resp
    WHERE SUBSTR(tactic_id_parent, 8, 3) = 'PCD'
      AND response_start >= DATE '2026-01-01'
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
    FROM vt_pcd_campaign_cells c
    CROSS JOIN vt_pcd_campaign_spine s
)

SELECT
    g.cohort_month,
    CAST(g.deployment AS VARCHAR(30))  AS deployment,
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

DROP TABLE vt_pcd_campaign_cells;
DROP TABLE vt_pcd_campaign_spine;
