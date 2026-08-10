-- pp_vbu_campaign.sql
-- ============================================================================
-- CONTRACT: vintages/OUTPUT_CONTRACT.md (locked 2026-08-10). 8 columns, exact order:
--   mne | cohort_month | segment | grp | vintage_day | base | responders | responders_cum.
--   mne = CAST('VBU' AS VARCHAR(20)). segment = CAST('All' AS VARCHAR(20)) — constant. VBU has
--   no pre-treatment population split above Action/Control; segment carries real values only
--   for AUH.
-- Campaign: VBU (Visa Benefit Upgrade) — CAMPAIGN scope.
--
-- *** METHODOLOGY FIX 2026-08-10 (Andre confirmed) — SUCCESS REDEFINED, REVERSES THE
--     2026-07-22 "REINSTATE SCOT FOR ALL 4 VBA/VBU FILES" CHANGE ***
--   VBU = Visa Benefit UPGRADE: an existing cardholder swaps to a richer product ON THE SAME
--   ACCOUNT. That is a PRODUCT CHANGE, not a new credit application. The prior build of this
--   file (and vintages/vbu_vintage_monthly.sql) used Casper+SCOT credit-application approval as
--   the success signal — copied wholesale from VBA's methodology. That is WRONG for VBU: a
--   client who upgrades in place without submitting a new credit application is invisible to
--   Casper/SCOT and was silently excluded from every VBU number this repo has produced since
--   2026-07-22.
--   schemas/cards_bizups_vbu_descresp_clnt.md:3 — "NBA curated outcomes table for VBU (Visa
--   Benefit Upgrade) — encodes **product change** as the success signal, not a new credit card
--   application. Different methodology from the VBA table."
--   This REVERSES the 2026-07-22 swap that vintages/vbu_vintage_monthly.sql:13-24 itself flagged
--   as "a bigger swap than 'add a second source' and should be confirmed, not assumed permanent
--   — FLAG THIS TO ANDRE" and that WORKSTREAMS.md:21-24 has carried as OPEN item #3 ("VBU
--   success: Casper/SCOT application signal (current build) vs product-change (Daniel's
--   original)") ever since, unanswered. Andre confirmed 2026-08-10: VBU must NOT use SCOT.
--   Success reverts to the product-change definition — Daniel Chin's original framework
--   (campaigns/VBA_VBU/vbu_vintage_original.sql), now sourced off the curated table's pre-built
--   flag instead of a hand-rolled dly_full_portfolio join.
--
--   VBA IS UNCHANGED AND STAYS ON CASPER+SCOT. VBA = Visa Benefit ADD, a real credit-card
--   acquisition — Casper+SCOT application approval IS the correct signal there. This fix does
--   not touch pp_vba_campaign.sql.
--
-- ENGINE: Trino / Starburst. No volatile tables, no QUALIFY, no SYS_CALENDAR.
--
-- SOURCE: dw00_im.dl_mr_prod.cards_bizups_vbu_descresp_clnt — curated VBU outcomes table.
--   VBU-specific by construction (table name + schema doc framing), so no
--   SUBSTR(tactic_id,8,3)='VBU' filter is applied here — unlike the shared
--   DG6V01.tactic_evnt_ip_ar_hist table the OLD VBU population query ran against.
--   [VERIFY] this table is exhaustive for VBU population (no non-VBU rows leak in) — not
--   independently confirmed, going on the table name + schema doc.
--
-- COLUMNS USED (all confirmed against schemas/cards_bizups_vbu_descresp_clnt.md, cited by line):
--   clnt_no                  — client id                                          (doc line 15)
--   tactic_id                — deployment/wave key                                (doc line 16)
--   response_start            — treatment START date / Day-0 anchor                (doc line 24)
--   response_end              — treatment END date / <<WINDOW>> selector           (doc line 25)
--   test_group / control      — arm columns, see GRP note below                    (doc lines 27-28)
--   responder_targetproduct   — success flag, ONE metric per contract rule 4       (doc line 69)
--   dt_prod_change_client     — product-change date = success event date          (doc line 72)
--
-- SUCCESS METRIC: responder_targetproduct = 1, event date = dt_prod_change_client. Per the
--   schema doc's own framing (lines 63-69), the table pre-builds BOTH "any product change"
--   (responder_anyproduct) and "primary/target upgrade" (responder_targetproduct) — mirroring
--   Daniel Chin's original two-definition framework in vbu_vintage_original.sql ("any" vs
--   "primary"/AIB-only). responder_targetproduct is the narrower, campaign-intent-aligned metric
--   (change TO the offered target product, not any incidental product move) — the same choice
--   pp_pcd_async.sql made for its analogous any/target pair. Going with it as the ONE
--   contract-mandated metric; responder_anyproduct is available on the source table for a future
--   secondary cut but is not a column in this 8-column output.
--
-- GRP: *** [VERIFY] UNCONFIRMED DERIVATION *** — the schema doc lists `test_group` ("Test group
--   label") and `control` ("Control group label") as two SEPARATE columns (doc lines 27-28), not
--   a single tst_grp_cd / test_groups_period code column with a C/T suffix like every other
--   Power Pack source. The doc does not state how the two columns combine into a binary arm
--   (mutually exclusive per row? mutually exclusive population?), and no confirmed convention
--   for this specific table exists anywhere else in the repo. Implemented below as the most
--   defensible reading: test_group populated AND control null -> Action; control populated AND
--   test_group null -> Control; anything else (both populated, both null) -> EXCLUDED, same as
--   every other pp_*.sql file's binary-grp guard (contract rule 5). This is a GUESS, flagged the
--   same way AUH's `_C`=Control convention is flagged [UNCONFIRMED] elsewhere in this repo
--   (OUTPUT_CONTRACT.md file inventory) — CONFIRM WITH ANDRE before trusting grp splits on this
--   file's output.
--
-- DEDUP IDENTIFIER: clnt_no. Same two-stage pattern as every other pp_*.sql file: wave_pop (one
--   row per clnt_no, tactic_id, earliest response_start wins) -> cohort_first (one row per
--   clnt_no, cohort_month, first-touch wins grp + anchor date). Applied defensively — the
--   curated table's grain has never been reconciled: schemas/cards_bizups_vbu_descresp_clnt.md:139
--   leaves it an open question ("Grain confirmation — is this 1 row per (clnt_no, tactic_id)?
--   Per (clnt_no, treatment_period)?"). If the table is already 1 row per (clnt_no, tactic_id),
--   this dedup is a no-op; if not, it protects against double-counting exactly like it does in
--   every sibling file.
--
-- *** [NOTE] grp tie-break = FIRST-TOUCH *** — same convention as every pp_*.sql file: if a
--   client appears twice in one cohort_month with opposing arms, grp comes from their FIRST
--   treatment. Per Andre (2026-08-10) this should never fire (trigger-style decisioning). Bottom
--   diagnostic (commented out) confirms it — expect zeros.
--
-- SUCCESS POOLING: dt_prod_change_client is already an absolute date embedded on the curated
--   row (same shape as PCD's dt_prod_change on cards_pcd_ongoing_decis_resp) so, exactly like
--   pp_pcd_async.sql, success is pooled across every wave the client touched that cohort_month
--   via a plain MIN(dt_prod_change_client) joined back through wave_arm, then rebased to the
--   first-touch anchor date — no separate raw-event-table search-window join needed (unlike
--   VBA's Casper/SCOT union, which has no deployment key and must join on a date range instead).
--
-- Spine : fixed 0-90 — same cap as VBA and the prior VBU builds. UNNEST(SEQUENCE(0,90)).
-- Floor : DATE '2026-01-01' on every scan (contract rule 6).
--
-- [VERIFY — RAW ALTERNATIVE, NOT BUILT HERE]: this file trusts the CURATED table's pre-built
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
-- QUARTER WINDOW — EDIT THESE TWO DATES TO RETARGET THE PACK
--   Selects deployments whose END date (response_end) falls in the window.
--   Cohort month and day 0 still anchor on response_start (START), not these.
--     Q3 FY2026 = 2026-05-01 .. 2026-07-31
-- ============================================================================
-- WINDOW START : DATE '2026-05-01'
-- WINDOW END   : DATE '2026-07-31'   (inclusive; coded as < DATE '2026-08-01')

WITH
-- stage 1 dedup: one row per (clnt_no, tactic_id) — earliest response_start wins; grp read off
-- that same row
wave_pop AS (
    SELECT clnt_no, tactic_id, response_start, grp
    FROM (
        SELECT
            clnt_no,
            tactic_id,
            response_start,
            CASE
                WHEN test_group IS NOT NULL AND control IS NULL THEN CAST('Action'  AS VARCHAR(20))
                WHEN control IS NOT NULL AND test_group IS NULL THEN CAST('Control' AS VARCHAR(20))
            END AS grp,
            ROW_NUMBER() OVER (
                PARTITION BY clnt_no, tactic_id ORDER BY response_start ASC
            ) AS rn
        FROM dw00_im.dl_mr_prod.cards_bizups_vbu_descresp_clnt
        WHERE response_start >= DATE '2026-01-01'                          -- floor guard
          AND response_end   >= DATE '2026-05-01'                          -- <<WINDOW>>
          AND response_end   <  DATE '2026-08-01'                          -- <<WINDOW>>
    ) ranked
    WHERE rn = 1
),

wave_arm AS (
    SELECT
        wp.clnt_no,
        wp.tactic_id,
        wp.response_start,
        DATE_TRUNC('month', wp.response_start) AS cohort_month,
        wp.grp
    FROM wave_pop wp
    WHERE wp.grp IS NOT NULL
),

cohort_first AS (   -- [NOTE] first-touch: earliest wave wins grp + anchor date (never expected to fire — see header)
    SELECT clnt_no, cohort_month, grp, response_start AS anchor_dt
    FROM (
        SELECT clnt_no, cohort_month, grp, response_start,
               ROW_NUMBER() OVER (
                   PARTITION BY clnt_no, cohort_month ORDER BY response_start ASC
               ) AS rn
        FROM wave_arm
    ) ranked
    WHERE rn = 1
),

vbu_cells AS (
    SELECT cohort_month, grp, COUNT(DISTINCT clnt_no) AS base
    FROM cohort_first
    GROUP BY cohort_month, grp
),

-- success: primary target-product responders only, same population window as wave_pop
success_events AS (
    SELECT
        clnt_no,
        tactic_id,
        dt_prod_change_client AS success_dt_abs
    FROM dw00_im.dl_mr_prod.cards_bizups_vbu_descresp_clnt
    WHERE response_start >= DATE '2026-01-01'                              -- floor guard
      AND response_end   >= DATE '2026-05-01'                              -- <<WINDOW>> keeps success scan aligned to selected deployments
      AND response_end   <  DATE '2026-08-01'                              -- <<WINDOW>>
      AND responder_targetproduct = 1
      AND dt_prod_change_client IS NOT NULL
),

-- pool success across every wave the client touched this cohort_month — join via wave_arm
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
        DATE_DIFF('day', cf.anchor_dt, sp.success_dt_abs) AS vintage_day
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

-- day spine 0-90 (deliberate fixed cap, preserved from source)
spine AS (
    SELECT s.vintage_day
    FROM UNNEST(SEQUENCE(0, 90)) AS s(vintage_day)
),

dense_grid AS (
    SELECT c.cohort_month, c.grp, c.base, s.vintage_day
    FROM vbu_cells c
    CROSS JOIN spine s
)

SELECT
    -- VARCHAR(20) in EVERY file on purpose: in a Teradata UNION ALL the character
    -- length is fixed by the FIRST SELECT block, so stacking a 3-char 'PCD' block
    -- ahead of 'PCD Sales Modal' would silently truncate the longer labels.
    CAST('VBU' AS VARCHAR(20)) AS mne,
    CAST(SUBSTR(CAST(g.cohort_month AS VARCHAR), 1, 7) AS VARCHAR(7)) AS cohort_month,
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
--             DATE_TRUNC('month', wp.response_start) AS cohort_month,
--             wp.grp
--         FROM (
--             SELECT clnt_no, tactic_id, response_start,
--                 CASE WHEN test_group IS NOT NULL AND control IS NULL THEN CAST('Action'  AS VARCHAR(20))
--                      WHEN control IS NOT NULL AND test_group IS NULL THEN CAST('Control' AS VARCHAR(20))
--                 END AS grp,
--                 ROW_NUMBER() OVER (PARTITION BY clnt_no, tactic_id ORDER BY response_start ASC) AS rn
--             FROM dw00_im.dl_mr_prod.cards_bizups_vbu_descresp_clnt
--             WHERE response_start >= DATE '2026-01-01'
--         ) wp
--         WHERE wp.rn = 1 AND wp.grp IS NOT NULL
--     ) raw
--     GROUP BY clnt_no, cohort_month
--     HAVING COUNT(DISTINCT grp) > 1
-- ) x GROUP BY 1 ORDER BY 1;
