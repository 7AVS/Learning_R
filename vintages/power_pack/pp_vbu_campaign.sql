-- pp_vbu_campaign.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 8 columns, exact order:
--   mne | cohort_month | segment | grp | vintage_day | base | responders | responders_cum.
--   mne = CAST('VBU' AS VARCHAR(20)). segment = CAST('All' AS VARCHAR(20))  constant. VBU has
--   no pre-treatment population split above Action/Control; segment carries real values only
--   for AUH.
-- Campaign: VBU (Visa Benefit Upgrade)  CAMPAIGN scope.
--
-- *** METHODOLOGY FIX 2026-08-10 (Andre confirmed)  SUCCESS REDEFINED, REVERSES THE
--     2026-07-22 "REINSTATE SCOT FOR ALL 4 VBA/VBU FILES" CHANGE ***
--   VBU = Visa Benefit UPGRADE: an existing cardholder swaps to a richer product ON THE SAME
--   ACCOUNT. That is a PRODUCT CHANGE, not a new credit application. The prior build of this
--   file (and vintages/vbu_vintage_monthly.sql) used Casper+SCOT credit-application approval as
--   the success signal  copied wholesale from VBA's methodology. That is WRONG for VBU: a
--   client who upgrades in place without submitting a new credit application is invisible to
--   Casper/SCOT and was silently excluded from every VBU number this repo has produced since
--   2026-07-22.
--   schemas/cards_bizups_vbu_descresp_clnt.md:3  "NBA curated outcomes table for VBU (Visa
--   Benefit Upgrade)  encodes **product change** as the success signal, not a new credit card
--   application. Different methodology from the VBA table."
--   This REVERSES the 2026-07-22 swap that vintages/vbu_vintage_monthly.sql:13-24 itself flagged
--   as "a bigger swap than 'add a second source' and should be confirmed, not assumed permanent
--    FLAG THIS TO ANDRE" and that WORKSTREAMS.md:21-24 has carried as OPEN item #3 ("VBU
--   success: Casper/SCOT application signal (current build) vs product-change (Daniel's
--   original)") ever since, unanswered. Andre confirmed 2026-08-10: VBU must NOT use SCOT.
--   Success reverts to the product-change definition  Daniel Chin's original framework
--   (campaigns/VBA_VBU/vbu_vintage_original.sql), now sourced off the curated table's pre-built
--   flag instead of a hand-rolled dly_full_portfolio join.
--
--   VBA IS UNCHANGED AND STAYS ON CASPER+SCOT. VBA = Visa Benefit ADD, a real credit-card
--   acquisition  Casper+SCOT application approval IS the correct signal there. This fix does
--   not touch pp_vba_campaign.sql.
--
-- ENGINE: Teradata-direct. Touches only Teradata tables (dl_mr_prod.cards_bizups_vbu_descresp_clnt),
--   so no catalog prefix and no Trino functions (DATE_TRUNC/DATE_DIFF/UNNEST/SEQUENCE). Uses
--   QUALIFY for dedup and a SYS_CALENDAR-backed VOLATILE TABLE for the day spine  both native
--   Teradata, both unavailable on Trino/Starburst. (CRV and VBA are the exceptions in this
--   folder  they reach EDL and stay on Trino/Starburst.)
--
-- SOURCE: dl_mr_prod.cards_bizups_vbu_descresp_clnt  curated VBU outcomes table.
--   VBU-specific by construction (table name + schema doc framing), so no
--   SUBSTR(tactic_id,8,3)='VBU' filter is applied here  unlike the shared
--   DG6V01.tactic_evnt_ip_ar_hist table the OLD VBU population query ran against.
--   [VERIFY] this table is exhaustive for VBU population (no non-VBU rows leak in)  not
--   independently confirmed, going on the table name + schema doc.
--
-- COLUMNS USED (all confirmed against schemas/cards_bizups_vbu_descresp_clnt.md, cited by line):
--   clnt_no                   client id                                          (doc line 15)
--   tactic_id                 deployment/wave key                                (doc line 16)
--   response_start             treatment START date / Day-0 anchor                (doc line 24)
--   response_end               treatment END date / <<WINDOW>> selector           (doc line 25)
--   responder_targetproduct    success flag, ONE metric per contract rule 4       (doc line 69)
--   dt_prod_change_client      product-change date = success event date          (doc line 72)
--   [grp is NOT sourced from this table's test_group/control  see GRP note below. It comes from
--    DTZV01.TACTIC_EVNT_IP_AR_H60M.TST_GRP_CD, joined in on (tactic_id, clnt_no).]
--
-- SUCCESS METRIC: responder_targetproduct = 1, event date = dt_prod_change_client. Per the
--   schema doc's own framing (lines 63-69), the table pre-builds BOTH "any product change"
--   (responder_anyproduct) and "primary/target upgrade" (responder_targetproduct)  mirroring
--   Daniel Chin's original two-definition framework in vbu_vintage_original.sql ("any" vs
--   "primary"/AIB-only). responder_targetproduct is the narrower, campaign-intent-aligned metric
--   (change TO the offered target product, not any incidental product move)  the same choice
--   pp_pcd_async.sql made for its analogous any/target pair. Going with it as the ONE
--   contract-mandated metric; responder_anyproduct is available on the source table for a future
--   secondary cut but is not a column in this 8-column output.
--
-- GRP: *** FIXED 2026-08-10 *** grp no longer comes from the curated table. The curated table's
--   own `test_group` / `control` columns do NOT yield a usable binary arm (every row evaluated to
--   grp = NULL, emptying the whole output). grp is now derived from DTZV01.TACTIC_EVNT_IP_AR_H60M
--   TST_GRP_CD  first char 'C' -> Control, else -> Action (see vbu_arm CTE below), the SAME
--   convention every other pp_*.sql file uses off tst_grp_cd. VERIFIED against measured counts
--   2026-08-10: tactic_id 2026070VBU  Action 34,435 / Control 11,326 (also 2026103VBU
--   28,821/11,133 and 2026133VBU 23,947/10,600). Joined onto the curated population on
--   (tactic_id, clnt_no)  both column names confirmed against schemas/cards_bizups_vbu_descresp_clnt.md
--   lines 15-16.
--
-- *** JOIN-KEY FIX 2026-08-10 *** the vbu_arm <-> curated-table join is normalised on BOTH sides:
--   TRIM() on tactic_id, CAST(...AS DECIMAL(38,0)) on clnt_no. Same documented trap as CRV's
--   (references/vintage_datalab_method.md, "JOIN-KEY FORMAT TRAP"): one side numeric, one side a
--   padded/typed string, raw equality silently matches nothing. Unnormalised, this join returned
--   ZERO rows on 2026-08-10 (empty wave_pop -> empty vt_vbu_cells -> empty output).
--
-- DEDUP IDENTIFIER: clnt_no. Same two-stage pattern as every other pp_*.sql file: wave_pop (one
--   row per clnt_no, tactic_id, earliest response_start wins) -> cohort_first (one row per
--   clnt_no, cohort_month, first-touch wins grp + anchor date). Applied defensively  the
--   curated table's grain has never been reconciled: schemas/cards_bizups_vbu_descresp_clnt.md:139
--   leaves it an open question ("Grain confirmation  is this 1 row per (clnt_no, tactic_id)?
--   Per (clnt_no, treatment_period)?"). If the table is already 1 row per (clnt_no, tactic_id),
--   this dedup is a no-op; if not, it protects against double-counting exactly like it does in
--   every sibling file. Dedup uses QUALIFY ROW_NUMBER() ... = 1 (Teradata-native), not a ranked
--   subquery with an outer WHERE rn = 1.
--
-- *** SEED EXCLUSION 2026-08-10 (Andre confirmed) ***
--   Deployments with an empty Control arm are excluded  a deployment with no control clients
--   is not an experiment and cannot produce a valid curve. This is a DATA-DRIVEN rule, not a
--   hard-coded tactic_id list: any tactic_id where COUNT(DISTINCT clnt_no) in grp='Control' = 0
--   is dropped before wave_arm/cohort_first are built (see valid_deployments CTE below, applied
--   in both the vt_vbu_cells volatile table and the main query's numerator chain).
--   Measured 2026-08-10 against real waves ending 2026-03-01 onward: seed/test deployments
--   (e.g. 2026101VBU, 2026129VBU, 2026164VBU, 2026192VBU) carried 8-70 clients total and ZERO in
--   Control; real waves (e.g. 2026070VBU, 2026103VBU, 2026133VBU) carry ~24-40K total, ~10-12K
--   in Control. Applies at the tactic_id (deployment) grain, before the cohort_month rollup.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH ***  same convention as every pp_*.sql file: if a
--   client appears twice in one cohort_month with opposing arms, grp comes from their FIRST
--   treatment. Per Andre (2026-08-10) this should never fire (trigger-style decisioning). Bottom
--   diagnostic (commented out) confirms it  expect zeros.
--
-- SUCCESS POOLING: dt_prod_change_client is already an absolute date embedded on the curated
--   row (same shape as PCD's dt_prod_change on cards_pcd_ongoing_decis_resp) so, exactly like
--   pp_pcd_async.sql, success is pooled across every wave the client touched that cohort_month
--   via a plain MIN(dt_prod_change_client) joined back through wave_arm, then rebased to the
--   first-touch anchor date  no separate raw-event-table search-window join needed (unlike
--   VBA's Casper/SCOT union, which has no deployment key and must join on a date range instead).
--
-- Spine : fixed 0-90  same cap as VBA and the prior VBU builds. Built as a Teradata VOLATILE
--   TABLE off SYS_CALENDAR.CALENDAR (see below), not UNNEST(SEQUENCE(...))  that is a Trino-only
--   function. TDWM blocks an unconstrained product join against SYS_CALENDAR, so both the spine
--   AND the denominator cells (vt_vbu_cells) are materialized as VOLATILE TABLEs with COLLECT
--   STATISTICS before the CROSS JOIN that builds the dense grid.
-- Floor : DATE '2026-01-01' on every scan (contract rule 6).
--
-- [VERIFY  RAW ALTERNATIVE, NOT BUILT HERE]: this file trusts the CURATED table's pre-built
--   responder_targetproduct / dt_prod_change_client flags. The alternative is rebuilding product
--   changes from scratch off D3CV12A.dly_full_portfolio + d3cv12a.cr_crd_rpts_acct, exactly as
--   campaigns/VBA_VBU/vbu_vintage_original.sql does (its acct_changes CTE, excluding no-ops,
--   flip-backs, and prior-AIB-holders). The curated table's grain vs that raw rebuild has NEVER
--   been reconciled (schemas/cards_bizups_vbu_descresp_clnt.md:139). If Andre wants the raw
--   build instead of trusting the curated flag, vbu_vintage_original.sql is the reference logic.
-- ----------------------------------------------------------------------------
-- SCOPE: deployments ENDING in the quarter window below (population filtered on response_end,
--   the schema-doc-confirmed treatment-end column). cohort_month and day-0 anchor on
--   response_start (START). Retargeting a quarter = editing the two <<WINDOW>> literals below.
-- ============================================================================

-- ============================================================================
-- QUARTER WINDOW  EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (response_end) falls in the window.
--   Cohort month and day 0 still anchor on response_start (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

-- ============================================================================
-- RERUN GUARD  if re-running this file in the SAME Teradata session, the volatile tables
-- below will already exist. Uncomment and run these two drops first:
--   DROP TABLE vt_vbu_spine;
--   DROP TABLE vt_vbu_cells;
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Day spine 0-90, off SYS_CALENDAR. VOLATILE so it can CROSS JOIN vt_vbu_cells below without
-- tripping the TDWM unconstrained-product-join block.
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu_spine AS (
    SELECT (calendar_date - DATE '2000-01-01') AS vintage_day
    FROM SYS_CALENDAR.CALENDAR
    WHERE (calendar_date - DATE '2000-01-01') BETWEEN 0 AND 90
) WITH DATA PRIMARY INDEX (vintage_day) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu_spine COLUMN (vintage_day);

-- ----------------------------------------------------------------------------
-- Denominator cells (cohort_month x grp x base). VOLATILE for the same TDWM reason  it is the
-- other side of the spine CROSS JOIN. Rebuilds wave_pop -> wave_arm -> cohort_first internally;
-- the same CTE chain is repeated in the main query below (Teradata volatile-table creation is a
-- standalone statement, it cannot see CTEs defined outside it).
-- ----------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_vbu_cells AS (
    WITH
    -- grp source: TACTIC table, NOT the curated table's test_group/control (see GRP header note)
    vbu_arm AS (
        SELECT
              TRIM(TACTIC_ID)                                    AS tactic_id
            , CLNT_NO                                            AS clnt_no
            , CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD),1,1) = 'C' THEN 'Control'
                        ELSE 'Action' END AS VARCHAR(20))        AS grp
        FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
        WHERE SUBSTR(TACTIC_ID, 8, 3) = 'VBU'
          AND TREATMT_STRT_DT >= DATE '2026-01-01'
        GROUP BY TRIM(TACTIC_ID), CLNT_NO,
                 CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD),1,1) = 'C' THEN 'Control'
                           ELSE 'Action' END AS VARCHAR(20))
    ),
    wave_pop AS (
        SELECT
            vc.clnt_no,
            vc.tactic_id,
            vc.response_start,
            va.grp
        FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt vc
        INNER JOIN vbu_arm va
            ON  TRIM(va.tactic_id) = TRIM(vc.tactic_id)
            AND CAST(va.clnt_no AS DECIMAL(38,0)) = CAST(vc.clnt_no AS DECIMAL(38,0))
        WHERE vc.response_start >= DATE '2026-01-01'                          -- floor guard
          AND vc.response_end   >= DATE '2026-05-01'                          -- <<WINDOW>>
          AND vc.response_end   <  DATE '2026-08-01'                          -- <<WINDOW>>
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY vc.clnt_no, vc.tactic_id ORDER BY vc.response_start ASC
        ) = 1
    ),
    -- seed exclusion: keep only tactic_ids with a non-empty Control arm (see header note above)
    valid_deployments AS (
        SELECT tactic_id
        FROM wave_pop
        GROUP BY tactic_id
        HAVING COUNT(DISTINCT CASE WHEN grp = 'Control' THEN clnt_no END) > 0
    ),
    wave_arm AS (
        SELECT
            wp.clnt_no,
            wp.tactic_id,
            wp.response_start,
            CAST(
              CAST(EXTRACT(YEAR FROM wp.response_start) AS VARCHAR(4)) || '-' ||
              CASE WHEN EXTRACT(MONTH FROM wp.response_start) < 10 THEN '0' ELSE '' END ||
              CAST(EXTRACT(MONTH FROM wp.response_start) AS VARCHAR(2))
            AS VARCHAR(7)) AS cohort_month,
            wp.grp
        FROM wave_pop wp
        INNER JOIN valid_deployments vd ON vd.tactic_id = wp.tactic_id
        WHERE wp.grp IS NOT NULL
    ),
    cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire  see header)
        SELECT clnt_no, cohort_month, grp, response_start AS anchor_dt
        FROM wave_arm
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY clnt_no, cohort_month ORDER BY response_start ASC
        ) = 1
    )
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
) WITH DATA PRIMARY INDEX (cohort_month, grp) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_vbu_cells COLUMN (cohort_month, grp);

-- ----------------------------------------------------------------------------
-- Main query  numerator side rebuilds the same wave_pop/wave_arm/cohort_first chain (regular
-- CTEs are fine here; only the spine CROSS JOIN needed the volatile-table workaround), pools
-- success, then joins the dense grid off the two volatile tables built above.
-- ----------------------------------------------------------------------------
WITH
-- grp source: TACTIC table, NOT the curated table's test_group/control (see GRP header note)
vbu_arm AS (
    SELECT
          TRIM(TACTIC_ID)                                    AS tactic_id
        , CLNT_NO                                            AS clnt_no
        , CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD),1,1) = 'C' THEN 'Control'
                    ELSE 'Action' END AS VARCHAR(20))        AS grp
    FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'VBU'
      AND TREATMT_STRT_DT >= DATE '2026-01-01'
    GROUP BY TRIM(TACTIC_ID), CLNT_NO,
             CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD),1,1) = 'C' THEN 'Control'
                       ELSE 'Action' END AS VARCHAR(20))
),

wave_pop AS (
    SELECT
        vc.clnt_no,
        vc.tactic_id,
        vc.response_start,
        va.grp
    FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt vc
    INNER JOIN vbu_arm va
        ON  TRIM(va.tactic_id) = TRIM(vc.tactic_id)
        AND CAST(va.clnt_no AS DECIMAL(38,0)) = CAST(vc.clnt_no AS DECIMAL(38,0))
    WHERE vc.response_start >= DATE '2026-01-01'                          -- floor guard
      AND vc.response_end   >= DATE '2026-05-01'                          -- <<WINDOW>>
      AND vc.response_end   <  DATE '2026-08-01'                          -- <<WINDOW>>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY vc.clnt_no, vc.tactic_id ORDER BY vc.response_start ASC
    ) = 1
),

-- seed exclusion: keep only tactic_ids with a non-empty Control arm (see header note above)
valid_deployments AS (
    SELECT tactic_id
    FROM wave_pop
    GROUP BY tactic_id
    HAVING COUNT(DISTINCT CASE WHEN grp = 'Control' THEN clnt_no END) > 0
),

wave_arm AS (
    SELECT
        wp.clnt_no,
        wp.tactic_id,
        wp.response_start,
        CAST(
          CAST(EXTRACT(YEAR FROM wp.response_start) AS VARCHAR(4)) || '-' ||
          CASE WHEN EXTRACT(MONTH FROM wp.response_start) < 10 THEN '0' ELSE '' END ||
          CAST(EXTRACT(MONTH FROM wp.response_start) AS VARCHAR(2))
        AS VARCHAR(7)) AS cohort_month,
        wp.grp
    FROM wave_pop wp
    INNER JOIN valid_deployments vd ON vd.tactic_id = wp.tactic_id
    WHERE wp.grp IS NOT NULL
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire  see header)
    SELECT clnt_no, cohort_month, grp, response_start AS anchor_dt
    FROM wave_arm
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY clnt_no, cohort_month ORDER BY response_start ASC
    ) = 1
),

-- success: primary target-product responders only, same population window as wave_pop
success_events AS (
    SELECT
        clnt_no,
        tactic_id,
        dt_prod_change_client AS success_dt_abs
    FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt
    WHERE response_start >= DATE '2026-01-01'                              -- floor guard
      AND response_end   >= DATE '2026-05-01'                              -- <<WINDOW>> keeps success scan aligned to selected deployments
      AND response_end   <  DATE '2026-08-01'                              -- <<WINDOW>>
      AND responder_targetproduct = 1
      AND dt_prod_change_client IS NOT NULL
),

-- pool success across every wave the client touched this cohort_month  join via wave_arm
-- to attach cohort_month, then take the earliest absolute success date for the client-month
success_pooled AS (
    SELECT wa.clnt_no, wa.cohort_month, MIN(se.success_dt_abs) AS success_dt_abs
    FROM success_events se
    INNER JOIN wave_arm wa
        ON wa.clnt_no = se.clnt_no AND wa.tactic_id = se.tactic_id
    GROUP BY wa.clnt_no, wa.cohort_month
),

numerator AS (
    SELECT
        cf.cohort_month, cf.grp, cf.clnt_no,
        CAST(sp.success_dt_abs - cf.anchor_dt AS INTEGER) AS vintage_day
    FROM cohort_first cf
    INNER JOIN success_pooled sp
        ON sp.clnt_no = cf.clnt_no AND sp.cohort_month = cf.cohort_month
),

daily_counts AS (
    SELECT cohort_month, grp, vintage_day, COUNT(DISTINCT clnt_no) AS responders
    FROM numerator
    WHERE vintage_day BETWEEN 0 AND 90
    GROUP BY cohort_month, grp, vintage_day
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vt_vbu_cells c
    CROSS JOIN vt_vbu_spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('VBU' AS VARCHAR(20)) AS mne,
    g.cohort_month,
    CAST('All' AS VARCHAR(20)) AS segment,
    g.grp,
    g.vintage_day,
    g.base,
    CAST(COALESCE(dc.responders, 0) AS INTEGER) AS responders,
    CAST(SUM(COALESCE(dc.responders, 0)) OVER (
        PARTITION BY g.cohort_month, g.grp
        ORDER BY g.vintage_day
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS INTEGER) AS responders_cum
FROM dense_grid g
LEFT JOIN daily_counts dc
    ON  dc.cohort_month = g.cohort_month
    AND dc.grp           = g.grp
    AND dc.vintage_day   = g.vintage_day
ORDER BY g.cohort_month, g.grp, g.vintage_day;

-- ============================================================================
-- DIAGNOSTIC (commented out): how many clients hit both arms in one month?
-- ============================================================================
-- SELECT cohort_month, COUNT(*) AS conflicted_clients FROM (
--     SELECT clnt_no, cohort_month FROM (
--         SELECT
--             wp.clnt_no,
--             CAST(
--               CAST(EXTRACT(YEAR FROM wp.response_start) AS VARCHAR(4)) || '-' ||
--               CASE WHEN EXTRACT(MONTH FROM wp.response_start) < 10 THEN '0' ELSE '' END ||
--               CAST(EXTRACT(MONTH FROM wp.response_start) AS VARCHAR(2))
--             AS VARCHAR(7)) AS cohort_month,
--             wp.grp
--         FROM (
--             SELECT vc.clnt_no, vc.tactic_id, vc.response_start, va.grp
--             FROM dl_mr_prod.cards_bizups_vbu_descresp_clnt vc
--             INNER JOIN (
--                 SELECT TRIM(TACTIC_ID) AS tactic_id, CLNT_NO AS clnt_no,
--                     CAST(CASE WHEN SUBSTR(TRIM(TST_GRP_CD),1,1) = 'C' THEN 'Control' ELSE 'Action' END AS VARCHAR(20)) AS grp
--                 FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
--                 WHERE SUBSTR(TACTIC_ID, 8, 3) = 'VBU' AND TREATMT_STRT_DT >= DATE '2026-01-01'
--                 GROUP BY 1, 2, 3
--             ) va ON TRIM(va.tactic_id) = TRIM(vc.tactic_id) AND CAST(va.clnt_no AS DECIMAL(38,0)) = CAST(vc.clnt_no AS DECIMAL(38,0))
--             WHERE vc.response_start >= DATE '2026-01-01'
--             QUALIFY ROW_NUMBER() OVER (PARTITION BY vc.clnt_no, vc.tactic_id ORDER BY vc.response_start ASC) = 1
--         ) wp
--         WHERE wp.grp IS NOT NULL
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
