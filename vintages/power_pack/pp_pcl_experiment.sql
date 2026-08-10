-- pcl_experiment_vintage.sql
-- OUTPUT CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). Emits EXACTLY 7 columns:
--   cohort_month VARCHAR(7) 'YYYY-MM', deployment VARCHAR(30), grp VARCHAR(20) [binary],
--   vintage_day INTEGER (0..90 continuous), base INTEGER (fixed per cohort x deployment x grp),
--   responders INTEGER, responders_cum INTEGER. Counts only, no rates.
--
-- SCOPE: *** EXPERIMENT *** — PLI sales-modal challenger/champion split ONLY.
--   (The CAMPAIGN-scope sibling is pcl_campaign_vintage.sql — whole PCL campaign, no modal filter.)
--
-- Engine   : Teradata-direct. SYS_CALENDAR spine in a VOLATILE TABLE + COLLECT STATISTICS before
--            the cross join (TDWM product-join guard). CTEs for everything else (rerun-safety).
-- Source   : DL_MR_PROD.cards_pli_decision_resp
--            (Modal-experiment logic ported from campaigns/sales_modal/pcl/p9_vcl_full_measurement.sql
--            and p10_vintage_curves.sql, which run on Starburst/Trino against dw00_im.dl_mr_prod.
--            Same underlying table, same columns — re-pointed here at Teradata-direct per contract
--            rule 8. The CONVERSION metric (responder_cli/dt_cl_change) lives entirely on the
--            curated row and needs no GA4 join, so it is engine-portable; p10's second metric,
--            ENGAGEMENT (first GA4 modal view), requires a GA4/Trino federation join and is
--            deliberately NOT carried into this file — see Success note below.)
-- Grain    : client (clnt_no) — matches p9/p10, NOT acct_no (differs from the campaign-scope file,
--            which is account-grain per the original vintages/pcl_vintage_monthly.sql; the modal
--            experiment's population CTEs in p9/p10 are built on clnt_no).
-- Anchor   : treatmt_strt_dt (treatment start), consistent with p9/p10 and the campaign-scope file.
--
-- Population filter (verbatim from p9/p10, task-specified):
--   WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
--     AND treatmt_strt_dt >= DATE '2026-01-01'   -- contract floor (p9/p10 used >= 2026-05-01;
--                                                    widened here per contract rule 6 — if the modal
--                                                    experiment only ran from 2026-05 onward, earlier
--                                                    cohort_months simply will not appear in the data)
--   NOTE: strategy_id (BAU/NTC in p9/p10) is an ORTHOGONAL audience dimension, NOT the treatment
--   split. It is deliberately NOT used for grp and NOT filtered on here, per task instruction —
--   using it would collapse two different things (audience segment vs modal-served/not-served) into
--   one column and break the binary contract.
--
-- grp: WMS (report_groups_period) -> 'Challenger' (modal served); NMS -> 'Champion' (no modal).
--   This is the confirmed, behavior-verified split from p7/p8 arm contrast (see
--   campaigns/sales_modal/pcl/modal_item_id_lookup.md) — NOT the same unresolved tst_grp_cd
--   question that blocks the campaign-scope file. This mapping is treated as CONFIRMED.
--
-- Deployment: parent_tactic_id, TRIM'd, CAST VARCHAR(30). Curated table is one row per
--   (clnt_no, deployment) so COUNT(DISTINCT clnt_no) per (cohort_month, deployment, grp) is a
--   direct aggregate — no cross-deployment dedup needed (curves never pool across deployments).
--
-- Success: responder_cli = 1 (CLI response flag); event day = dt_cl_change - treatmt_strt_dt.
--   This is the CONVERSION metric only (matches p10's 'conversion' metric and the campaign-scope
--   file's metric — one shared primary success definition across both PCL files). p10's second
--   metric, ENGAGEMENT (first GA4 view_promotion on modal id i_308392/i_335273), is NOT included:
--   contract rule 4 caps this file at one success metric, engagement needs a GA4/Trino join that
--   is out of scope for a Teradata-direct file, and conversion is the metric already established
--   as primary in vintages/pcl_vintage_monthly.sql. If an engagement vintage is ever needed, it
--   should ship as its own file, on Starburst/Trino, per contract rule 4 — not bolted on here.
-- Spine    : 0..90 days, continuous, COALESCE(responders,0) on empty days.
--
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_pcl_exp_cells;
--   DROP TABLE vt_pcl_exp_spine;

-- ============================================================================
-- STEP 1: denominator cells — cohort_month x deployment x grp
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_exp_cells AS (
    SELECT
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(TRIM(parent_tactic_id) AS VARCHAR(30))    AS deployment,
        CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
             WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
        END                                            AS grp,
        COUNT(DISTINCT clnt_no)                        AS base
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
      AND treatmt_strt_dt >= DATE '2026-01-01'
    GROUP BY 1, 2, 3
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_exp_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-90
-- ============================================================================
CREATE VOLATILE TABLE vt_pcl_exp_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pcl_exp_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
population AS (
    SELECT
        clnt_no,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                                AS cohort_month,
        CAST(TRIM(parent_tactic_id) AS VARCHAR(30))    AS deployment,
        CASE WHEN report_groups_period LIKE '%R____WMS%' THEN CAST('Challenger' AS VARCHAR(20))
             WHEN report_groups_period LIKE '%R____NMS%' THEN CAST('Champion'   AS VARCHAR(20))
        END                                            AS grp,
        CASE WHEN responder_cli = 1
             THEN CAST(dt_cl_change - treatmt_strt_dt AS INTEGER)
        END                                            AS vintage_day_raw
    FROM DL_MR_PROD.cards_pli_decision_resp
    WHERE (report_groups_period LIKE '%R____WMS%' OR report_groups_period LIKE '%R____NMS%')
      AND treatmt_strt_dt >= DATE '2026-01-01'
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day_raw AS vintage_day,
           COUNT(DISTINCT clnt_no) AS responders
    FROM population
    WHERE vintage_day_raw BETWEEN 0 AND 90
    GROUP BY cohort_month, deployment, grp, vintage_day_raw
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_pcl_exp_cells c
    CROSS JOIN vt_pcl_exp_spine s
)

SELECT
    g.cohort_month,
    g.deployment,
    g.grp,
    CAST(g.vintage_day AS INTEGER)                          AS vintage_day,
    CAST(g.base AS INTEGER)                                 AS base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER)              AS responders,
    CAST(
        SUM(COALESCE(dc.responders, 0)) OVER (
            PARTITION BY g.cohort_month, g.deployment, g.grp
            ORDER BY g.vintage_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    AS INTEGER)                                              AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.deployment    = g.deployment
    AND dc.grp           = g.grp
    AND dc.vintage_day    = g.vintage_day
ORDER BY g.cohort_month, g.deployment, g.grp, g.vintage_day;

DROP TABLE vt_pcl_exp_cells;
DROP TABLE vt_pcl_exp_spine;
