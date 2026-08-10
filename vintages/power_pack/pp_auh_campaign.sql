-- auh_experiment_vintage.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10, `deployment` DROPPED 2026-08-10;
--   `mne` ADDED 2026-08-10 as first column).
--   Exactly 8 columns, this order: mne | cohort_month | segment | grp | vintage_day
--   | base | responders | responders_cum. Counts only. mne = CAST('AUH' AS VARCHAR(20)) —
--   campaign mnemonic, or the experiment name where the file measures an experiment.
-- SCOPE: **EXPERIMENT** — for AUH the whole deployment IS the experiment;
--   there is no coarser "campaign" superset (per EXPERIMENT_VS_CAMPAIGN_MAP.md
--   section 1/5). No separate auh_campaign_vintage.sql exists or is needed.
-- Engine: Teradata-direct. SYS_CALENDAR spine + the population/base cells both
--   live in VOLATILE TABLEs with COLLECT STATISTICS before the cross join
--   (TDWM unconstrained-product-join guard). CTEs for everything else.
-- ----------------------------------------------------------------------------
-- SOURCES
--   DG6V01.tactic_evnt_ip_ar_hist          -- population + cohort/grp/segment
--   D3CV12A.CR_CRD_ACCT_EVNT_DLY           -- raw success event (add event)
--   D3CV12A.DLY_FULL_PORTFOLIO             -- portfolio join required by the
--                                              WORK-ENV success definition below
-- ----------------------------------------------------------------------------
-- POPULATION FILTER: tactic_id IN ('2026042AUH', '2026119AUH')  -- Phase 1 + Phase 2
-- Grain: account. acct_no = CAST(TACTIC_EVNT_ID AS BIGINT) — the tactic event's
--   own surrogate id, same convention as auh_vintage_reconstructed.sql and the
--   repo's prior auh_vintage_monthly.sql (not a portfolio account number).
--   *** DEDUP IDENTIFIER: acct_no *** (this file's success join keys on acct_no
--   throughout — see SUCCESS below — so acct_no is what the first-touch dedup
--   below is keyed on, per task instruction to use whichever id the success
--   join already uses).
-- ----------------------------------------------------------------------------
-- *** [WARNING] DROPPING `deployment` MERGES PHASE 1 AND PHASE 2, 2026-08-10 ***
--   Both AUH waves start in the SAME cohort_month: 2026042AUH and 2026119AUH
--   both begin 2026-04-30 -> cohort_month = '2026-04' for BOTH. With
--   `deployment` gone, there is only ONE '2026-04' cohort row per segment x
--   grp — Phase 1 and Phase 2 are now the SAME row, not two rows Andre could
--   tell apart.
--   Phase 1 (2026042AUH) is 100% NonReward. Phase 2 (2026119AUH) has all three
--   segments (NonReward, Rewards_NoOffer, Rewards_Offer). Because both phases
--   land in cohort_month '2026-04', the '2026-04' x NonReward x {Action,Control}
--   line now MIXES Phase 1 clients and Phase 2-NonReward-arm clients into one
--   base/responders count. There is no way to separate them back out of this
--   file's output — the Rewards_* segment lines are pure Phase 2 (since Phase 1
--   has no rewards segments), but the NonReward line is NOT pure either phase.
--   This is a real interpretive hazard, not a cosmetic one: any Phase-1-only or
--   Phase-2-only read off the NonReward line will be wrong. If a clean
--   phase-separated cut is ever needed again, it has to come from a
--   deployment-aware file (the analytical AUH_P2_Vintage.sql), not this one.
-- ----------------------------------------------------------------------------
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***
--   If a client appeared twice in one cohort_month with opposing arms, grp comes from their
--   FIRST treatment. Per Andre (2026-08-10) this should never fire: a client already live in a
--   deployment is not re-decisioned until it ends (trigger-style decisioning). Kept as a cheap
--   guard for reminder-style sends inside a deployment. The diagnostic at the bottom of this file
--   confirms it — expect zeros.
--   For AUH specifically this is doubly moot (the two waves are also disjoint
--   populations per Andre — see DEDUP note below) but the rule is still applied
--   uniformly for consistency with every other Power Pack file. segment is
--   resolved by the SAME first-touch row as grp (both live on one tactic-event
--   row), so a first-touch segment/grp pair always travels together.
-- ----------------------------------------------------------------------------
-- *** DEDUP — REQUIRED, EVEN THOUGH IT SHOULD BE A NO-OP HERE ***
--   Per Andre: the two AUH waves are DISJOINT populations (no client is
--   targeted by both 2026042AUH and 2026119AUH). So first-touch dedup at
--   (acct_no, cohort_month) should collapse zero rows in practice. It is
--   applied anyway for consistency with every other Power Pack file and as a
--   safety net if that disjointness assumption is ever wrong.
--   One row per (acct_no, cohort_month), anchored on that account's earliest
--   treatmt_strt_dt within the month:
--     QUALIFY ROW_NUMBER() OVER (PARTITION BY acct_no, cohort_month
--                                ORDER BY treatmt_strt_dt ASC) = 1
--   The success-window join below uses [MIN(treatmt_strt_dt), MAX(treatmt_end_dt)]
--   across all of that account's rows in the cohort_month (not just the
--   first-touch row's own window) so a later wave's window is never silently
--   truncated by picking only the first-touch wave's dates.
-- ----------------------------------------------------------------------------
-- *** SEGMENT — REWARDS vs NON-REWARDS, ADDED 2026-08-10 ***
--   Andre: "don't separate by model arm, but do separate rewards and
--   non-rewards, then test and control for non-rewards and test and control
--   for rewards." segment is a PRE-TREATMENT split, derived from tst_grp_cd,
--   that sits ABOVE grp (Action/Control) — base is now computed at
--   cohort_month x segment x grp grain (deployment dropped 2026-08-10).
--   MAPPING REWRITTEN 2026-08-10 after a run produced an Unknown bucket of
--   ~63k accounts in cohort 2026-04. Root cause: the previous version used
--   three-character prefix IN-lists copied from the work-env file, and one
--   member was missing — 'RNR' was absent from the Rewards_NoOffer list, so
--   every RNR* code fell through to Unknown.
--
--   The code structure makes the 3-char lists unnecessary and fragile:
--     chars 1-2 = STRATEGY   ('NR' non-reward, 'RN' rewards-no-offer,
--                             'RO' rewards-offer)
--     char  3   = MODEL ARM  (R = Random, M = Model, W = Web)
--   The model arm is collapsed here anyway (see GRP COLLAPSE below), so
--   matching on chars 1-2 covers every model variant, both waves, and any
--   future code, with no IN-list to fall out of date:
--     SUBSTR(TRIM(tst_grp_cd),1,2) = 'NR' -> 'NonReward'
--     SUBSTR(TRIM(tst_grp_cd),1,2) = 'RN' -> 'Rewards_NoOffer'
--     SUBSTR(TRIM(tst_grp_cd),1,2) = 'RO' -> 'Rewards_Offer'
--     anything else -> 'Unknown' (NOT silently folded into a real segment).
--
--   This also removes the tactic_id dependency the old CASE carried. Phase 1
--   codes (NRGA, NRR, NRS and their _C variants) all begin 'NR', which lands
--   them in NonReward — consistent with Phase 1 being 100% non-rewards, so the
--   per-wave branching bought nothing.
--
--   Unknown should now be zero or near-zero. If it is not, the residual codes
--   are genuinely outside the NR/RN/RO scheme and need to be looked at before
--   the cube is trusted — check with the tst_grp_cd roll-up at the bottom of
--   this file.
--   THREE segment values are emitted on purpose, not two — see prior header
--   revision for the full DOE-contrast rationale (unchanged by the deployment
--   drop). [WARNING] block above explains why the NonReward line now mixes
--   phases even though the segment derivation logic itself is untouched.
-- ----------------------------------------------------------------------------
-- *** GRP COLLAPSE — DELIBERATE, READ BEFORE USING ***
--   model_arm (Web / Random / Model) is COLLAPSED ENTIRELY — Andre explicitly
--   does NOT want that slicer broken out here. grp itself stays binary Action vs
--   Control, per segment. grp derivation: RIGHT(TRIM(tst_grp_cd), 2) = '_C' ->
--   'Control', ELSE -> 'Action'.
--
--   [VERIFY] *** the '_C' = Control convention is an UNCONFIRMED WORKING
--   ASSUMPTION for BOTH waves (2026042AUH and 2026119AUH) and is LOAD-BEARING
--   for every number this file produces. Daniel Chin's Phase 1 tracking doc
--   uses '_C' this way; Phase 2 codes seen in the wild (NRW_C, RORMC2_C)
--   appear consistent; Robin Ji's Phase 2 email (2026-05-14) confirmed the
--   TST_GRP_CD prefix-to-arm mapping WITHOUT explicitly confirming '_C' =
--   Control. Treat every Control/Action split from this file as provisional
--   until that is confirmed.
-- ----------------------------------------------------------------------------
-- *** POOLING GUARD — READ BEFORE TRUSTING A POOLED NUMBER ***
--   Collapsing model_arm into a single segment/grp cell is only valid if the
--   test:control ratio is the same across the model arms being pooled
--   (otherwise Simpson's paradox — a shifted mix reads as a lift/drop that
--   isn't real).
--     - 2026119AUH (Phase 2): 50/50 between strategy arms, 70/30 test/control
--       WITHIN each arm. Same ratio both sides -> pooling is SAFE.
--     - 2026042AUH (Phase 1): NRGA/NRR/NRS ratios are UNVERIFIED.
--   base is output at (cohort_month x segment x grp) grain — with deployment
--   gone, Phase 1 vs Phase 2 ratio drift is NO LONGER separately visible on
--   the face of the cube for the NonReward line (see [WARNING] above). Check
--   the Rewards_* lines (pure Phase 2) against NonReward with this in mind.
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
--   Attribution: INNER JOIN cohort ON acct_no, AND first_owned_dt BETWEEN the
--   cohort's pooled window [MIN(treatmt_strt_dt), MAX(treatmt_end_dt)] for that
--   (acct_no, cohort_month) — see DEDUP note above.
--   METRIC CHOICE: this is the ANY-PRODUCT success (first_app_dt in the
--   work-env file), used as the single primary metric per contract rule 4.
--   The work-env file's target-product variant is DROPPED here on purpose —
--   one metric per file, and Andre's instruction was explicit: use ANY-PRODUCT.
-- ----------------------------------------------------------------------------
-- SPINE: vintage_day 0-30 (AUH's deliberate cap, unchanged from prior file).
-- FLOOR: population floor guard >= DATE '2026-01-01' (contract rule 6) stays in place;
--   population is now ALSO filtered to treatmt_end_dt in the Q3 window (see QUARTER WINDOW
--   block below) — end-date is the SELECTOR, treatmt_strt_dt remains the cohort/day-0 anchor.
--   Success-side scan (CR_CRD_ACCT_EVNT_DLY + DLY_FULL_PORTFOLIO) tightened to
--   [2026-02-01, 2026-08-01) — see <<WINDOW>> tags below.
-- ----------------------------------------------------------------------------
-- Drop residual volatile tables if rerunning in the same session:
--   DROP TABLE vt_auh_experiment_cells;
--   DROP TABLE vt_auh_experiment_spine;
-- ----------------------------------------------------------------------------
-- SCOPE: this file is scoped to deployments ENDING in the quarter window below
--   (population filtered on treatmt_end_dt). cohort_month and day-0 still
--   anchor on treatmt_strt_dt (the START column) — unchanged. Retargeting a
--   quarter = editing the two <<WINDOW>> literals below only.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW — EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (treatmt_end_dt) falls in the window.
--   Cohort month and day 0 still anchor on treatmt_strt_dt (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

-- ============================================================================
-- STEP 1: denominator cells (cohort_month x segment x grp -> base)
-- ============================================================================
CREATE VOLATILE TABLE vt_auh_experiment_cells AS (
    WITH population_raw AS (
        SELECT
            CAST(tactic_evnt_id AS BIGINT)         AS acct_no,
            treatmt_strt_dt,
            treatmt_end_dt,
            CAST(
                CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
                CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
                CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
            AS VARCHAR(7))                          AS cohort_month,
            -- segment: rewards vs non-rewards, derived per-wave from tst_grp_cd — see header
            CASE
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 2) = 'NR'
                    THEN CAST('NonReward'       AS VARCHAR(20))
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 2) = 'RN'
                    THEN CAST('Rewards_NoOffer' AS VARCHAR(20))
                WHEN SUBSTR(TRIM(tst_grp_cd), 1, 2) = 'RO'
                    THEN CAST('Rewards_Offer'   AS VARCHAR(20))
                ELSE CAST('Unknown' AS VARCHAR(20))
            END                                      AS segment,
            CASE
                WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(20))
                ELSE                                        CAST('Action'  AS VARCHAR(20))
            END                                      AS grp
        FROM DG6V01.tactic_evnt_ip_ar_hist
        WHERE tactic_id IN ('2026042AUH', '2026119AUH')
          AND treatmt_strt_dt >= DATE '2026-01-01'                          -- floor guard
          AND treatmt_end_dt  >= DATE '2026-05-01'                          -- <<WINDOW>>
          AND treatmt_end_dt  <  DATE '2026-08-01'                          -- <<WINDOW>>
    ),
    -- [NOTE] first-touch: one row per (acct_no, cohort_month), earliest treatmt_strt_dt wins (never expected to fire — see header)
    cohort_first AS (
        SELECT acct_no, cohort_month, segment, grp
        FROM population_raw
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY acct_no, cohort_month ORDER BY treatmt_strt_dt ASC
        ) = 1
    )
    SELECT cohort_month, segment, grp, COUNT(DISTINCT acct_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, segment, grp
) WITH DATA PRIMARY INDEX (cohort_month, segment, grp) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_auh_experiment_cells COLUMN (cohort_month, segment, grp);

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
population_raw AS (
    SELECT
        CAST(tactic_evnt_id AS BIGINT)         AS acct_no,
        treatmt_strt_dt,
        treatmt_end_dt,
        CAST(
            CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
            CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
            CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
        AS VARCHAR(7))                          AS cohort_month,
        CASE
            WHEN tactic_id = '2026042AUH'
                 AND TRIM(tst_grp_cd) IN ('NRGA','NRGA_C','NRR','NRR_C','NRS','NRS_C')
                THEN CAST('NonReward' AS VARCHAR(20))
            WHEN tactic_id = '2026119AUH'
                 AND SUBSTR(TRIM(tst_grp_cd), 1, 3) IN ('NRR','NRM','NRW')
                THEN CAST('NonReward' AS VARCHAR(20))
            WHEN tactic_id = '2026119AUH'
                 AND SUBSTR(TRIM(tst_grp_cd), 1, 3) IN ('RNM','RNW')
                THEN CAST('Rewards_NoOffer' AS VARCHAR(20))
            WHEN tactic_id = '2026119AUH'
                 AND SUBSTR(TRIM(tst_grp_cd), 1, 3) IN ('ROR','ROM','ROW')
                THEN CAST('Rewards_Offer' AS VARCHAR(20))
            ELSE CAST('Unknown' AS VARCHAR(20))
        END                                      AS segment,
        CASE
            WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(20))
            ELSE                                        CAST('Action'  AS VARCHAR(20))
        END                                      AS grp
    FROM DG6V01.tactic_evnt_ip_ar_hist
    WHERE tactic_id IN ('2026042AUH', '2026119AUH')
      AND treatmt_strt_dt >= DATE '2026-01-01'                              -- floor guard
      AND treatmt_end_dt  >= DATE '2026-05-01'                              -- <<WINDOW>>
      AND treatmt_end_dt  <  DATE '2026-08-01'                              -- <<WINDOW>>
),

-- [NOTE] first-touch: grp/segment come from the account's earliest row this month (never expected to fire — see header)
cohort_first AS (
    SELECT acct_no, cohort_month, segment, grp, treatmt_strt_dt AS anchor_dt
    FROM population_raw
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY acct_no, cohort_month ORDER BY treatmt_strt_dt ASC
    ) = 1
),

-- pooled search window: earliest start to latest end across ALL of the account's
-- rows in the cohort_month, so a later wave's window is never truncated
cohort_window AS (
    SELECT acct_no, cohort_month, MAX(treatmt_end_dt) AS window_end
    FROM population_raw
    GROUP BY acct_no, cohort_month
),

population AS (
    SELECT cf.acct_no, cf.cohort_month, cf.segment, cf.grp, cf.anchor_dt, cw.window_end
    FROM cohort_first cf
    INNER JOIN cohort_window cw
        ON cw.acct_no = cf.acct_no AND cw.cohort_month = cf.cohort_month
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
      AND a.evnt_dt         >= DATE '2026-02-01'                            -- <<WINDOW>> lower: earliest start of a Q3-ending deployment
      AND a.evnt_dt         <  DATE '2026-08-01'                            -- <<WINDOW>> upper: no selected deployment runs past this
      AND c.DT_RECORD_EXT   >= DATE '2026-02-01'                            -- <<WINDOW>> direct bound on DLY_FULL_PORTFOLIO (the expensive table)
      AND c.DT_RECORD_EXT   <  DATE '2026-08-01'                            -- <<WINDOW>>
),

-- ANY-PRODUCT success: single earliest add date per account (no prod_cd split —
-- the target-product variant is dropped per the one-metric rule, see header)
first_owned AS (
    SELECT acct_no, MIN(evnt_dt) AS first_owned_dt
    FROM events
    GROUP BY acct_no
),

-- attribute within the pooled cohort window; vintage_day is relative to the
-- first-touch anchor date, not necessarily the window the event fell in
success_raw AS (
    SELECT
        p.cohort_month, p.segment, p.grp, p.acct_no,
        CAST(fo.first_owned_dt - p.anchor_dt AS INTEGER) AS vintage_day
    FROM population p
    INNER JOIN first_owned fo
        ON  fo.acct_no        = p.acct_no
        AND fo.first_owned_dt BETWEEN p.anchor_dt AND p.window_end
),

daily_counts AS (
    SELECT cohort_month, segment, grp, vintage_day, COUNT(DISTINCT acct_no) AS responders
    FROM success_raw
    WHERE vintage_day BETWEEN 0 AND 30
    GROUP BY cohort_month, segment, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.segment, c.grp, c.base, s.vintage_day
    FROM vt_auh_experiment_cells c
    CROSS JOIN vt_auh_experiment_spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('AUH' AS VARCHAR(20)) AS mne,
    g.cohort_month,
    g.segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.segment, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.segment       = g.segment
    AND dc.grp           = g.grp
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.segment, g.grp, g.vintage_day;

DROP TABLE vt_auh_experiment_cells;
DROP TABLE vt_auh_experiment_spine;

-- ============================================================================
-- DIAGNOSTIC A (commented out): every tst_grp_cd and where it lands.
-- Run this FIRST if any Unknown rows appear. ~20 rows. It shows the actual
-- code strings, which segment each maps to, and the Action/Control split, so a
-- residual Unknown bucket can be read off directly instead of guessed at.
-- ============================================================================
-- SELECT
--       tactic_id
--     , TRIM(tst_grp_cd)                                  AS tst_grp_cd
--     , SUBSTR(TRIM(tst_grp_cd), 1, 2)                    AS strategy_2char
--     , CASE
--           WHEN SUBSTR(TRIM(tst_grp_cd),1,2)='NR' THEN 'NonReward'
--           WHEN SUBSTR(TRIM(tst_grp_cd),1,2)='RN' THEN 'Rewards_NoOffer'
--           WHEN SUBSTR(TRIM(tst_grp_cd),1,2)='RO' THEN 'Rewards_Offer'
--           ELSE 'Unknown' END                            AS segment
--     , CASE WHEN RIGHT(TRIM(tst_grp_cd),2)='_C' THEN 'Control' ELSE 'Action' END AS grp
--     , COUNT(DISTINCT CAST(tactic_evnt_id AS BIGINT))    AS accts
-- FROM DG6V01.tactic_evnt_ip_ar_hist
-- WHERE tactic_id IN ('2026042AUH', '2026119AUH')
--   AND treatmt_strt_dt >= DATE '2026-01-01'
-- GROUP BY 1,2,3,4,5
-- ORDER BY 1,2;

-- ============================================================================
-- DIAGNOSTIC B (commented out): how many accounts hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_accounts FROM (
--     SELECT acct_no, cohort_month FROM (
--         SELECT
--             CAST(tactic_evnt_id AS BIGINT) AS acct_no,
--             CAST(
--                 CAST(EXTRACT(YEAR FROM treatmt_strt_dt) AS VARCHAR(4)) || '-' ||
--                 CASE WHEN EXTRACT(MONTH FROM treatmt_strt_dt) < 10 THEN '0' ELSE '' END ||
--                 CAST(EXTRACT(MONTH FROM treatmt_strt_dt) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             CASE
--                 WHEN RIGHT(TRIM(tst_grp_cd), 2) = '_C' THEN CAST('Control' AS VARCHAR(20))
--                 ELSE                                        CAST('Action'  AS VARCHAR(20))
--             END AS grp
--         FROM DG6V01.tactic_evnt_ip_ar_hist
--         WHERE tactic_id IN ('2026042AUH', '2026119AUH')
--           AND treatmt_strt_dt >= DATE '2026-01-01'
--     ) raw
--     GROUP BY acct_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
