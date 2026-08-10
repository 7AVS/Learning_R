-- auh_experiment_vintage.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). Exactly 7 columns,
--   this order: cohort_month | deployment | grp | vintage_day | base | responders
--   | responders_cum. Counts only.
-- SCOPE: **EXPERIMENT** — for AUH the whole deployment IS the experiment;
--   there is no coarser "campaign" superset (per EXPERIMENT_VS_CAMPAIGN_MAP.md
--   section 1/5). No separate auh_campaign_vintage.sql exists or is needed.
-- Engine: Teradata-direct. SYS_CALENDAR spine + the population/base cells both
--   live in VOLATILE TABLEs with COLLECT STATISTICS before the cross join
--   (TDWM unconstrained-product-join guard). CTEs for everything else.
-- ----------------------------------------------------------------------------
-- SOURCES
--   DG6V01.tactic_evnt_ip_ar_hist          -- population + cohort/grp
--   D3CV12A.CR_CRD_ACCT_EVNT_DLY           -- raw success event (add event)
--   D3CV12A.DLY_FULL_PORTFOLIO             -- portfolio join required by the
--                                              WORK-ENV success definition below
-- ----------------------------------------------------------------------------
-- POPULATION FILTER: tactic_id IN ('2026042AUH', '2026119AUH')  -- Phase 1 + Phase 2
-- Grain: account. acct_no = CAST(TACTIC_EVNT_ID AS BIGINT) — the tactic event's
--   own surrogate id, same convention as auh_vintage_reconstructed.sql and the
--   repo's prior auh_vintage_monthly.sql (not a portfolio account number).
-- ----------------------------------------------------------------------------
-- *** GRP COLLAPSE — DELIBERATE, READ BEFORE USING ***
--   strategy_arm (NonReward / Rewards_Offer / Rewards_No_Offer) and model_arm
--   (Web / Random / Model) are COLLAPSED ENTIRELY. Andre explicitly does NOT
--   want those slicers broken out here — binary Test vs Control only, per
--   deployment. This is a simplification, not a data limitation: both arms
--   are fully derivable from tst_grp_cd (see the work-env AUH_P2_Vintage.sql
--   structure, transcribed in EXPERIMENT_VS_CAMPAIGN_MAP.md section 5) — they
--   are just not wanted in this cube. If per-arm cuts are ever needed again,
--   build them as a SEPARATE file; do not add columns here (contract rule 5).
--   grp derivation: RIGHT(TRIM(tst_grp_cd), 2) = '_C' -> 'Control', ELSE -> 'Test'.
--
--   [VERIFY] *** the '_C' = Control convention is an UNCONFIRMED WORKING
--   ASSUMPTION for BOTH waves (2026042AUH and 2026119AUH) and is LOAD-BEARING
--   for every number this file produces. Daniel Chin's Phase 1 tracking doc
--   uses '_C' this way; Phase 2 codes seen in the wild (NRW_C, RORMC2_C)
--   appear consistent; Robin Ji's Phase 2 email (2026-05-14) confirmed the
--   TST_GRP_CD prefix-to-arm mapping WITHOUT explicitly confirming '_C' =
--   Control. Treat every Control/Test split from this file as provisional
--   until that is confirmed.
-- ----------------------------------------------------------------------------
-- *** POOLING GUARD — READ BEFORE TRUSTING A POOLED NUMBER ***
--   Collapsing strategy arms into binary Test/Control is only valid if the
--   test:control ratio is the same across the arms being pooled (otherwise
--   Simpson's paradox — a shifted mix reads as a lift/drop that isn't real).
--     - 2026119AUH (Phase 2): 50/50 between strategy arms, 70/30 test/control
--       WITHIN each arm. Same ratio both sides -> pooling is SAFE.
--     - 2026042AUH (Phase 1): NRGA/NRR/NRS ratios are UNVERIFIED.
--   base is output at (deployment x grp) grain specifically so this is
--   self-checking — if the Test:Control ratio drifts between the two
--   deployments (or looks structurally off within 2026042AUH), IT IS VISIBLE
--   ON THE FACE OF THE CUBE. Check base before trusting the Phase 1 line.
-- ----------------------------------------------------------------------------
-- SUCCESS (ONE metric, per contract rule 4) — WORK-ENV version (transcribed
--   from screenshots 2026-08-10; more correct than the repo's prior
--   auh_vintage_monthly.sql, which used the same raw event but without this
--   exact join shape documented). Chosen over an ownership-snapshot definition
--   because the snapshot's CHG_DT/captr_dt counted long-time holders, not new adds.
--     FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
--     INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
--       ON a.clnt_no = c.clnt_no AND a.evnt_dt = c.DT_RECORD_EXT AND a.acct_no = c.acct_no
--     WHERE a.dtl_evnt_typ_cd = 191 AND a.ADD_RELTN_CD = 3 AND a.evnt_dt >= DATE '2026-01-01'
--   first_owned_dt = MIN(evnt_dt) per acct_no (ANY product — see note below).
--   Attribution: INNER JOIN cohort ON acct_no, AND first_owned_dt BETWEEN
--   treatmt_strt_dt AND treatmt_end_dt.
--   METRIC CHOICE: this is the ANY-PRODUCT success (first_app_dt in the
--   work-env file), used as the single primary metric per contract rule 4.
--   The work-env file's target-product variant (first_app_dt_target, grouped
--   by acct_no + prod_cd, matched to the offered product via
--   SUBSTR(tactic_decisn_vrb_info,21,3)) is DROPPED here on purpose — one
--   metric per file, and Andre's instruction was explicit: use ANY-PRODUCT.
--   Because only the ANY-PRODUCT metric is kept, first_owned_dt is computed
--   as a single MIN(evnt_dt) per acct_no (no prod_cd grouping needed) — this
--   also means no last-touch/overlap resolution is required the way the
--   prior repo file did it: a single pre-collapsed date per account either
--   falls inside a given deployment's window or it doesn't, so it can
--   legitimately land in BOTH 2026042AUH's and 2026119AUH's curves if the
--   account and windows genuinely overlap — that is correct under this
--   contract (deployments are never pooled, each curve stands on its own),
--   not double-counting.
-- ----------------------------------------------------------------------------
-- SPINE: vintage_day 0-30 (AUH's deliberate cap, unchanged from prior file).
-- FLOOR: every scan >= DATE '2026-01-01' (contract rule 6; the old file used
--   2026-01-01 — widened here per contract).
-- ----------------------------------------------------------------------------
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_auh_experiment_cells;
--   DROP TABLE vt_auh_experiment_spine;
-- ============================================================================

-- ============================================================================
-- STEP 1: denominator cells (cohort_month x deployment x grp -> base)
-- ============================================================================
CREATE VOLATILE TABLE vt_auh_experiment_cells AS (
    WITH population AS (
        SELECT
            CAST(tactic_evnt_id AS BIGINT)         AS acct_no,
            tactic_id                              AS deployment,
            treatmt_strt_dt,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
            AS VARCHAR(7))                          AS cohort_month,
            CASE
                WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(20))
                ELSE                                        CAST('Test'    AS VARCHAR(20))
            END                                      AS grp
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE tactic_id IN ('2026042AUH', '2026119AUH')
          AND treatmt_strt_dt >= DATE '2026-01-01'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(tactic_evnt_id AS BIGINT), tactic_id
            ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort_month, deployment, grp, COUNT(DISTINCT acct_no) AS base
    FROM population
    GROUP BY cohort_month, deployment, grp
) WITH DATA PRIMARY INDEX (cohort_month, deployment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_auh_experiment_cells COLUMN (cohort_month, deployment, grp);

-- ============================================================================
-- STEP 2: day spine 0-30
-- ============================================================================
CREATE VOLATILE TABLE vt_auh_experiment_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 30
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_auh_experiment_spine COLUMN (vintage_day);

-- ============================================================================
-- STEP 3: final curve
-- ============================================================================
WITH
population AS (
    SELECT
        CAST(tactic_evnt_id AS BIGINT)         AS acct_no,
        tactic_id                              AS deployment,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                          AS cohort_month,
        CASE
            WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(20))
            ELSE                                        CAST('Test'    AS VARCHAR(20))
        END                                      AS grp
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH', '2026119AUH')
      AND treatmt_strt_dt >= DATE '2026-01-01'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CAST(tactic_evnt_id AS BIGINT), tactic_id
        ORDER BY treatmt_strt_dt ASC
    ) = 1
),

-- raw success events: authorized-user add, any product, no deployment key on the event table itself
events AS (
    SELECT
        CAST(a.acct_no AS BIGINT) AS acct_no,
        a.evnt_dt
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY a
    INNER JOIN D3CV12A.DLY_FULL_PORTFOLIO c
        ON  a.clnt_no = c.clnt_no
        AND a.evnt_dt = c.DT_RECORD_EXT
        AND a.acct_no = c.acct_no
    WHERE a.dtl_evnt_typ_cd = 191
      AND a.ADD_RELTN_CD    = 3
      AND a.evnt_dt         >= DATE '2026-01-01'
),

-- ANY-PRODUCT success: single earliest add date per account (no prod_cd split —
-- the target-product variant is dropped per the one-metric rule, see header)
first_owned AS (
    SELECT acct_no, MIN(evnt_dt) AS first_owned_dt
    FROM events
    GROUP BY acct_no
),

-- attribute to every deployment window the account's first-owned date actually falls in
-- (can legitimately match both waves if windows overlap for that account — see header)
success_raw AS (
    SELECT
        p.cohort_month, p.deployment, p.grp, p.acct_no,
        CAST(fo.first_owned_dt - p.treatmt_strt_dt AS INTEGER) AS vintage_day
    FROM population p
    INNER JOIN first_owned fo
        ON  fo.acct_no        = p.acct_no
        AND fo.first_owned_dt BETWEEN p.treatmt_strt_dt AND p.treatmt_end_dt
),

daily_counts AS (
    SELECT cohort_month, deployment, grp, vintage_day, COUNT(DISTINCT acct_no) AS responders
    FROM success_raw
    WHERE vintage_day BETWEEN 0 AND 30
    GROUP BY cohort_month, deployment, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.deployment, c.grp, c.base, s.vintage_day
    FROM vt_auh_experiment_cells c
    CROSS JOIN vt_auh_experiment_spine s
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

DROP TABLE vt_auh_experiment_cells;
DROP TABLE vt_auh_experiment_spine;
