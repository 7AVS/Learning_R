-- ENGINE: Teradata-direct
-- Arm probe — PCD and PCQ. One SELECT per candidate arm column (no UNION — types differ),
-- then the off-table join probe for the campaigns whose arm doesn't live on the curated
-- table, then an SRM-style control-share-by-segment check for one recent month.
--
-- Every column used here is evidenced in:
--   references/campaign_query_cards.md (PCD + PCQ cards)
--   schemas/pcd_curated_schemas.md (PCD only — no schema.md exists for the PCQ curated
--     table; PCQ columns below come solely from the PCQ card's SQL-usage evidence)
--   references/query_engine_guidelines.md (engine/syntax rules)
--   memory: reference_pcd_action_control_derivation.md, reference_cards_no_cell_code_lookup.md,
--     reference_tactic_decisn_vrb_info_structure.md
-- Anything not directly stated in those files is marked [VERIFY].
--
-- EDIT before running: the month window in Step 2/3 of each campaign (currently June 2026,
-- floored no earlier than 2026-01-01 per this task's instruction).

-- =====================================================================================
-- PCD — DL_MR_PROD.cards_pcd_ongoing_decis_resp
-- =====================================================================================
--
-- What PCD's card tells us: `act_ctl_seg` is CONFIRMED WRONG for row-level Action/Control
-- (Andre, 2026-05-26) — it's a segment-level field, not on-table arm. The real arm,
-- `tst_grp_cd`, lives only on DG6V01.TACTIC_EVNT_IP_AR_HIST, joined on clnt_no, filtered to
-- this campaign's tactic_ids via SUBSTR(tactic_id,8,3)='PCD' (TACTIC_ID positions 8-10 = MNE,
-- per CLAUDE.md). Derivation: tst_grp_cd LIKE '%T' -> TEST(Action), LIKE '%C' -> CONTROL,
-- else 'OTHER' (non-suffix codes exist; the mapping for them lives outside the repo — do NOT
-- drop OTHER rows, keep and label them).
--
-- STEP 1 proves: whether any on-table column could stand in for arm, and documents exactly
-- how act_ctl_seg fails (uneven bucket sizes, values that don't read as Action/Control, or a
-- cardinality that doesn't match a 2-arm test). Also profiles the other segment columns the
-- card lists, in case one of them turns out to nest cleanly with the off-table arm in Step 2.
-- EXPECTED: act_ctl_seg does NOT split ~evenly into two buckets that look like Action/Control
-- — if it does, that contradicts Andre's 2026-05-26 finding and must be re-raised with him
-- before using it anywhere. test_groups_period / report_groups_period / strategy_seg_cd /
-- cmpgn_seg are expected to show wave/segment codes, not a clean 2-way arm split.
-- IF IT FAILS (act_ctl_seg looks like a clean 2-way split): stop, do not wire it into any
-- query — flag to Andre, this would overturn a confirmed finding.

SELECT CAST(TRIM(act_ctl_seg) AS VARCHAR(10)) AS act_ctl_seg, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(TRIM(strategy_seg_cd) AS VARCHAR(8)) AS strategy_seg_cd, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(TRIM(cmpgn_seg) AS VARCHAR(10)) AS cmpgn_seg, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(TRIM(test_groups_period) AS VARCHAR(25)) AS test_groups_period, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(TRIM(report_groups_period) AS VARCHAR(60)) AS report_groups_period, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp
GROUP BY 1
ORDER BY 2 DESC;

-- STEP 2 proves: the off-table join (curated x tactic event, on clnt_no, mnemonic-filtered)
-- actually resolves an arm for the population, and gives a first read of Action vs Control
-- volume + response for one recent month.
-- [VERIFY] join key: this replicates the PCD card's own template EXACTLY — join on clnt_no
-- only, no tactic_id match between the curated row's tactic_id_parent and the tactic-event
-- row's tactic_id. If a client has more than one PCD tactic deployment in TACTIC_EVNT_IP_AR_HIST
-- under this mnemonic, this fans out. CHECK: compare COUNT(*) in this result's total against
-- a plain COUNT(*) on the curated table for the same month filter — if the joined total is
-- higher, add a tactic_id_parent = t.tactic_id condition and re-run.
-- Response flag: responder_targetproduct (smallint, =1 = converted) — present + in active use
-- per the card, but not independently ground-truth-checked.
-- EXPECTED: three buckets (TEST, CONTROL, OTHER), TEST >> CONTROL (targeted test, not a 50/50
-- design), OTHER small relative to TEST+CONTROL. Zero rows with NULL arm (LEFT JOIN found no
-- tactic-event match) should be rare — a large NULL bucket means the mnemonic filter or the
-- clnt_no join itself is missing matches.
-- IF IT FAILS (large NULL/unmatched bucket, or OTHER dominates): the tactic table's tactic_id
-- mnemonic positions may not be 8-10 for this table, or PCD deployments aren't captured in
-- TACTIC_EVNT_IP_AR_HIST the way the card assumes — stop and re-verify against a raw sample of
-- tactic_id values before trusting any arm split downstream.

SELECT
    CASE WHEN TRIM(t.tst_grp_cd) LIKE '%T' THEN 'TEST'
         WHEN TRIM(t.tst_grp_cd) LIKE '%C' THEN 'CONTROL'
         WHEN t.clnt_no IS NULL THEN 'NO_TACTIC_MATCH'
         ELSE 'OTHER' END AS arm,
    COUNT(*) AS row_count,
    SUM(CAST(CASE WHEN c.responder_targetproduct = 1 THEN 1 ELSE 0 END AS BIGINT)) AS resp_target
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp c
LEFT JOIN (
    SELECT clnt_no, tst_grp_cd
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'PCD'
) t
    ON t.clnt_no = c.clnt_no
WHERE c.response_start >= DATE '2026-06-01' AND c.response_start < DATE '2026-07-01'
GROUP BY 1
ORDER BY 2 DESC;

-- STEP 3 proves: control share is stable across segments in that same month (SRM-style
-- check) — if control % swings wildly by strategy_seg_cd, either the design isn't a clean
-- randomized holdout within every segment, or the join in Step 2 is picking up the wrong
-- population for some segments.
-- EXPECTED: TEST vs CONTROL row_count ratio roughly constant across strategy_seg_cd values
-- (allow for small-N segment noise). Counts only, per repo convention — compute the ratio by
-- eye or downstream, don't bake a rate into the query output.
-- IF IT FAILS (ratio swings by segment, e.g. one strategy_seg_cd has near-zero CONTROL): flag
-- before using this arm split for any lift calc — a segment with no control cannot support a
-- lift claim for that segment (same guard as VBA's ITA-dilution issue).

SELECT
    CASE WHEN TRIM(t.tst_grp_cd) LIKE '%T' THEN 'TEST'
         WHEN TRIM(t.tst_grp_cd) LIKE '%C' THEN 'CONTROL'
         WHEN t.clnt_no IS NULL THEN 'NO_TACTIC_MATCH'
         ELSE 'OTHER' END AS arm,
    CAST(TRIM(c.strategy_seg_cd) AS VARCHAR(8)) AS strategy_seg_cd,
    COUNT(DISTINCT c.clnt_no) AS clients
FROM DL_MR_PROD.cards_pcd_ongoing_decis_resp c
LEFT JOIN (
    SELECT clnt_no, tst_grp_cd
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'PCD'
) t
    ON t.clnt_no = c.clnt_no
WHERE c.response_start >= DATE '2026-06-01' AND c.response_start < DATE '2026-07-01'
GROUP BY 1, 2
ORDER BY 2, 1;


-- =====================================================================================
-- PCQ — DL_MR_PROD.cards_tpa_pcq_decision_resp
-- =====================================================================================
--
-- What PCQ's card tells us: no schema.md exists for this table in the repo — every column
-- below comes from the PCQ card's SQL-usage evidence only. No action/control column has ever
-- been found on the curated table (grepped, confirmed absent). Champion/challenger codes
-- (NG3_CHMP / NG3_CHLN / NG3_CHLD — exact values, use IN-list not LIKE) live only on
-- DG6V01.TACTIC_EVNT_IP_AR_HIST's tst_grp_cd. The card describes this as a "two-hop join
-- (tactic event -> client list -> curated)" — no intermediate "client list" table is named in
-- any evidenced file, so this probe tests the direct one-hop join (tactic event -> curated on
-- clnt_no, same pattern as PCD) and treats a bad match rate as the signal that the missing
-- middle table is actually required.
-- Mandatory filter: tpa_ita = 'TPA' (non-TPA rows are noise, confirmed 2026-05-10). Never
-- filter on `mnemonic` (redundant, has caused failures per the card).
-- Response: app_approved, gated by TRIM(asc_on_app_source) = 'Period-ASC' INSIDE the numerator
-- CASE only — never in WHERE (collapses denominator to responders only, per the card).
--
-- STEP 1 proves: none of the curated table's own segment-like columns is secretly the arm —
-- documents what tpa_ita and mnemonic actually contain (expected: TPA-only noise column and a
-- constant/near-constant mnemonic, neither a 2-arm split), and profiles the two other
-- segment columns the card names.
-- EXPECTED: tpa_ita shows TPA dominant with a small non-TPA noise share (consistent with the
-- card's "non-TPA rows are data noise" note); mnemonic near-constant (single value, confirming
-- "never filter on it" — nothing to filter); response_channel_grp and model_score_decile show
-- ordinary channel/decile buckets, not a 2-way arm split.
-- IF IT FAILS (tpa_ita or mnemonic reads as a clean 2-way split): that would contradict the
-- card's "no action/control column" finding — stop and re-verify before treating either as arm.

SELECT CAST(TRIM(tpa_ita) AS VARCHAR(10)) AS tpa_ita, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(TRIM(mnemonic) AS VARCHAR(20)) AS mnemonic, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(TRIM(response_channel_grp) AS VARCHAR(30)) AS response_channel_grp, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
GROUP BY 1
ORDER BY 2 DESC;

SELECT CAST(model_score_decile AS VARCHAR(10)) AS model_score_decile, COUNT(*) AS row_count
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp
GROUP BY 1
ORDER BY 2 DESC;

-- STEP 2 proves: whether the direct clnt_no join to TACTIC_EVNT_IP_AR_HIST (mnemonic-filtered
-- to PCQ) actually surfaces the NG3_CHMP/NG3_CHLN/NG3_CHLD codes against the curated
-- population, and gives a first read of champion/challenger volume + approval.
-- [VERIFY] join key + fan-out: same caveat as PCD Step 2 — clnt_no only, no tactic_id match.
-- [VERIFY] two-hop: if NO_TACTIC_MATCH dominates, or if row_count fans out past the curated
-- table's own row count for the month, that's the signal the card's "two-hop... client list"
-- join really is required — stop, do not trust this arm split, and go find the intermediate
-- table (not evidenced in any file read for this task) before proceeding.
-- EXPECTED: three named buckets (NG3_CHMP, NG3_CHLN, NG3_CHLD) plus NO_TACTIC_MATCH /
-- OTHER_CODE catch-alls. Raw codes only — do not relabel CHLN/CHLD as "challenger A/B" or
-- similar; their exact semantics beyond CHMP=champion aren't evidenced anywhere read.
-- IF IT FAILS: see [VERIFY] two-hop note above.

SELECT
    CASE WHEN t.clnt_no IS NULL THEN 'NO_TACTIC_MATCH'
         WHEN TRIM(t.tst_grp_cd) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLD') THEN TRIM(t.tst_grp_cd)
         ELSE 'OTHER_CODE' END AS arm_code,
    COUNT(*) AS row_count,
    SUM(CAST(CASE WHEN c.app_approved = 1 AND TRIM(c.asc_on_app_source) = 'Period-ASC'
                  THEN 1 ELSE 0 END AS BIGINT)) AS approved_period_asc
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp c
LEFT JOIN (
    SELECT clnt_no, tst_grp_cd
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'PCQ'
) t
    ON t.clnt_no = c.clnt_no
WHERE c.tpa_ita = 'TPA'
  AND c.treatmt_start_dt >= DATE '2026-06-01' AND c.treatmt_start_dt < DATE '2026-07-01'
GROUP BY 1
ORDER BY 2 DESC;

-- STEP 3 proves: champion/challenger share is stable across model_score_decile for that same
-- month (SRM-style check) — a decile with a near-empty challenger bucket means the join or the
-- design isn't giving a clean comparison for that decile.
-- EXPECTED: NG3_CHMP vs NG3_CHLN/NG3_CHLD row_count ratio roughly constant across deciles.
-- Counts only. IF IT FAILS: same guard as PCD Step 3 — don't claim a champion/challenger lift
-- for a decile with a near-empty challenger arm.

SELECT
    CASE WHEN t.clnt_no IS NULL THEN 'NO_TACTIC_MATCH'
         WHEN TRIM(t.tst_grp_cd) IN ('NG3_CHMP', 'NG3_CHLN', 'NG3_CHLD') THEN TRIM(t.tst_grp_cd)
         ELSE 'OTHER_CODE' END AS arm_code,
    CAST(c.model_score_decile AS VARCHAR(10)) AS model_score_decile,
    COUNT(DISTINCT c.clnt_no) AS clients
FROM DL_MR_PROD.cards_tpa_pcq_decision_resp c
LEFT JOIN (
    SELECT clnt_no, tst_grp_cd
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(tactic_id, 8, 3) = 'PCQ'
) t
    ON t.clnt_no = c.clnt_no
WHERE c.tpa_ita = 'TPA'
  AND c.treatmt_start_dt >= DATE '2026-06-01' AND c.treatmt_start_dt < DATE '2026-07-01'
GROUP BY 1, 2
ORDER BY 2, 1;
