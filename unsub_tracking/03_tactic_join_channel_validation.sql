-- =============================================================================
-- Tactic Join + Channel Validation — MASTER x TACTIC_EVNT_IP_AR_HIST
-- =============================================================================
--
-- Purpose:
--   (A) Validate the join between DTZV01.VENDOR_FEEDBACK_MASTER (email send
--       master) and the decisioning table DG6V01.TACTIC_EVNT_IP_AR_HIST —
--       coverage at treatment level (J2), row level (J3), and grain/fan-out
--       on the matched set (J4). J1 is the baseline.
--   (B) Discover where "campaign targets email channel (EM)" is recorded on
--       the decisioning side, by profiling candidate marker sources —
--       MASTER.channel_type_cd (C1), MASTER.cntct_mthd_typ (C2),
--       TACTIC_DECISN_VRB_INFO position 121 (C3), TREATMT_MN (C4) — and
--       cross-checking the strongest candidate (VRB pos 121) against the
--       de-facto email flag: a tactic simply APPEARING in MASTER at all,
--       since MASTER only ever receives email sends (C5).
--
-- Tables:
--   DTZV01.VENDOR_FEEDBACK_MASTER      -- email master (one row per send)
--   DG6V01.TACTIC_EVNT_IP_AR_HIST      -- decisioning / tactic history
--
-- ENGINE: Teradata-direct
--   Two-part addressing, no catalog prefix. TOP allowed. CAST(COUNT(*) AS BIGINT)
--   on any count that could run large. WITH (CTEs) only — no volatile tables
--   needed (no TDWM cross-join / sys_calendar hazard in this pack).
--
-- Confirmed columns in scope (per schemas/vendor_feedback_tables_schema.md +
-- repo hard facts) — use ONLY these:
--   MASTER: TREATMENT_ID (=TACTIC_ID), CLNT_NO, channel_type_cd char(3),
--           cntct_mthd_typ char(3).
--   TACTIC_EVNT_IP_AR_HIST: CLNT_NO, TACTIC_ID, TST_GRP_CD, RPT_GRP_CD,
--           TACTIC_CELL_CD, TREATMT_STRT_DT, TREATMT_END_DT, TREATMT_MN,
--           TACTIC_DECISN_VRB_INFO, ADDNL_DECISN_DATA1.
--   Join keys: m.TREATMENT_ID = t.TACTIC_ID AND m.CLNT_NO = t.CLNT_NO.
--   MNE = SUBSTR(TACTIC_ID, 8, 3) (not used in this pack — no MNE breakdown
--   requested here).
--
-- Windowing convention:
--   History floor DATE '2024-01-01' on tactic-side PROFILING scans (J4, C3-C5).
--   J2/J3 coverage checks are deliberately UNWINDOWED: MASTER has no date column
--   to exclude pre-2024 sends, so windowing the tactic side would make old sends
--   read as join failures — coverage must measure key validity, not the window.
--   (HIST is retention-bounded on its own.)
--
-- TACTIC_DECISN_VRB_INFO is a packed string — NEVER GROUP BY the raw column,
-- only a SUBSTR of it. Position 121, length 30 is the repo-confirmed slot for
-- a channel-ish marker: campaigns/PCQ/modal_sales/pcq_ms_summary.sql and
-- pcq_ms_vs_benchmark.sql both use SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE
-- '%MS%' to find modal-sales-flagged tactic rows — same slot, different code.
-- C3/C5's '%EM%' probe here is DISCOVERY ONLY. Per governance/channel_codes.md,
-- EM = Email (top-level bucket AND channel code, P=Proactive). Once the exact
-- position/code for email is confirmed, production logic must switch to an
-- exact-code match (no substrings) — standing repo rule.
--
-- Counts only, no divisions/rates computed in SQL. Small outputs print-only,
-- nothing saved to disk.
--
-- Block index: J1-J4 = join validation. C1-C5 = channel-marker discovery.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- J1: MASTER — baseline volume, cardinality (no join)
-- ---------------------------------------------------------------------------
-- Proves: total MASTER rows + distinct TREATMENT_ID/CLNT_NO — the denominator
-- for the coverage %s in J2/J3. MASTER has no date column; unwindowed by
-- necessity.
-- ---------------------------------------------------------------------------

SELECT
    CAST(COUNT(*) AS BIGINT)        AS master_rows,
    COUNT(DISTINCT TREATMENT_ID)    AS distinct_treatments,
    COUNT(DISTINCT CLNT_NO)         AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_MASTER;


-- ---------------------------------------------------------------------------
-- J2: Treatment-level join coverage — distinct TREATMENT_ID -> TACTIC_ID
-- ---------------------------------------------------------------------------
-- Proves: of MASTER's distinct TREATMENT_IDs, how many exist as a TACTIC_ID in
-- the tactic history at all. LEFT JOIN of two DISTINCT projections — no
-- fan-out, and no EXISTS in the select list (Teradata only allows EXISTS as a
-- WHERE/ON predicate). Unwindowed: measures key validity, not the window.
-- WARN if distinct_treatments_matched / distinct_treatments_total < 95%.
-- ---------------------------------------------------------------------------

WITH distinct_treatments AS (
    SELECT DISTINCT TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER
),
tactic_ids AS (
    SELECT DISTINCT TACTIC_ID
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
)
SELECT
    CAST(COUNT(*) AS BIGINT)        AS distinct_treatments_total,
    CAST(SUM(CASE WHEN ti.TACTIC_ID IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
                                    AS distinct_treatments_matched
FROM distinct_treatments dt
LEFT JOIN tactic_ids ti
    ON ti.TACTIC_ID = dt.TREATMENT_ID;


-- ---------------------------------------------------------------------------
-- J3: Row-level join coverage — MASTER rows with a tactic match
-- ---------------------------------------------------------------------------
-- Proves: of ALL MASTER rows (not just distinct treatments), how many resolve
-- to a tactic row on both keys (TREATMENT_ID=TACTIC_ID AND CLNT_NO=CLNT_NO).
-- Compare against J1.master_rows for the row-level coverage %.
-- WARN if row coverage < 95% — the send-timing join is unreliable below that.
-- ---------------------------------------------------------------------------

SELECT
    CAST(COUNT(*) AS BIGINT)        AS master_rows_matched
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
WHERE EXISTS (
    SELECT 1
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TACTIC_ID = m.TREATMENT_ID
      AND t.CLNT_NO   = m.CLNT_NO
);


-- ---------------------------------------------------------------------------
-- J4: Grain / fan-out — rows per (CLNT_NO, TACTIC_ID) on matched tactic rows
-- ---------------------------------------------------------------------------
-- Proves: whether a (CLNT_NO, TACTIC_ID) pair decisions more than once (e.g.
-- multi-wave) among 2024+ tactic rows whose TACTIC_ID appears in MASTER.
-- Determines whether the send-timing join needs a dedup rule (MIN/MAX
-- TREATMT_STRT_DT) before use downstream.
-- WARN if the "2" or "3+" bucket holds > 1% of pairs — dedup rule required.
-- ---------------------------------------------------------------------------

WITH matched_tactic_rows AS (
    SELECT t.CLNT_NO, t.TACTIC_ID
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND EXISTS (
          SELECT 1
          FROM DTZV01.VENDOR_FEEDBACK_MASTER m
          WHERE m.TREATMENT_ID = t.TACTIC_ID
      )
),
pair_counts AS (
    SELECT CLNT_NO, TACTIC_ID, COUNT(*) AS rows_per_pair
    FROM matched_tactic_rows
    GROUP BY CLNT_NO, TACTIC_ID
)
SELECT
    CASE WHEN rows_per_pair = 1 THEN '1'
         WHEN rows_per_pair = 2 THEN '2'
         ELSE '3+' END              AS rows_per_pair_bucket,
    CAST(COUNT(*) AS BIGINT)        AS pair_count
FROM pair_counts
GROUP BY 1
ORDER BY 1;


-- ---------------------------------------------------------------------------
-- C1: channel_type_cd value distribution — MASTER (candidate source #1)
-- ---------------------------------------------------------------------------
-- Proves: the value set + frequency of channel_type_cd on MASTER. MASTER-side
-- only, no date column, unwindowed. Cross-check the value(s) seen against
-- governance/channel_codes.md — expect something resolving to EM (Email).
-- ---------------------------------------------------------------------------

SELECT
    channel_type_cd,
    CAST(COUNT(*) AS BIGINT)        AS master_rows,
    COUNT(DISTINCT TREATMENT_ID)    AS distinct_treatments
FROM DTZV01.VENDOR_FEEDBACK_MASTER
GROUP BY channel_type_cd
ORDER BY master_rows DESC;


-- ---------------------------------------------------------------------------
-- C2: cntct_mthd_typ value distribution — MASTER (candidate source #2)
-- ---------------------------------------------------------------------------
-- Proves: the value set + frequency of cntct_mthd_typ, same shape as C1.
-- MASTER-side only, unwindowed. If channel_type_cd and cntct_mthd_typ disagree
-- on which rows read as email, that disagreement is itself a finding.
-- ---------------------------------------------------------------------------

SELECT
    cntct_mthd_typ,
    CAST(COUNT(*) AS BIGINT)        AS master_rows,
    COUNT(DISTINCT TREATMENT_ID)    AS distinct_treatments
FROM DTZV01.VENDOR_FEEDBACK_MASTER
GROUP BY cntct_mthd_typ
ORDER BY master_rows DESC;


-- ---------------------------------------------------------------------------
-- C3: TACTIC_DECISN_VRB_INFO position-121 marker — candidate source #3
-- ---------------------------------------------------------------------------
-- Proves: TOP 50 values of SUBSTR(TACTIC_DECISN_VRB_INFO,121,30), 2024+ tactic
-- rows, split into (a) tactics whose TACTIC_ID IS in MASTER (de-facto email)
-- vs (b) tactics that are NOT. A value that concentrates almost entirely in
-- (a) and is rare/absent in (b) is the channel-marker candidate.
-- DISCOVERY split only — no LIKE probe yet, raw value distribution.
-- ---------------------------------------------------------------------------

-- C3a: tactic rows whose TACTIC_ID appears in MASTER
SELECT TOP 50
    SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) AS vrb_pos121_30,
    CAST(COUNT(*) AS BIGINT)                AS tactic_rows
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
  AND EXISTS (
      SELECT 1 FROM DTZV01.VENDOR_FEEDBACK_MASTER m
      WHERE m.TREATMENT_ID = t.TACTIC_ID
  )
GROUP BY SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30)
ORDER BY tactic_rows DESC;

-- C3b: tactic rows whose TACTIC_ID does NOT appear in MASTER
SELECT TOP 50
    SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30) AS vrb_pos121_30,
    CAST(COUNT(*) AS BIGINT)                AS tactic_rows
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
  AND NOT EXISTS (
      SELECT 1 FROM DTZV01.VENDOR_FEEDBACK_MASTER m
      WHERE m.TREATMENT_ID = t.TACTIC_ID
  )
GROUP BY SUBSTR(TACTIC_DECISN_VRB_INFO, 121, 30)
ORDER BY tactic_rows DESC;


-- ---------------------------------------------------------------------------
-- C4: TREATMT_MN value distribution — candidate source #4 (in vs not-in MASTER)
-- ---------------------------------------------------------------------------
-- Proves: TOP 50 TREATMT_MN values by frequency, 2024+ tactic rows, same
-- in-MASTER vs not-in-MASTER split as C3. A clean separation (email-sounding
-- values concentrated on the "in MASTER" side only) signals TREATMT_MN also
-- carries a usable channel marker — compare side by side against C3.
-- ---------------------------------------------------------------------------

-- C4a: tactic rows whose TACTIC_ID appears in MASTER
SELECT TOP 50
    TREATMT_MN,
    CAST(COUNT(*) AS BIGINT)        AS tactic_rows
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
  AND EXISTS (
      SELECT 1 FROM DTZV01.VENDOR_FEEDBACK_MASTER m
      WHERE m.TREATMENT_ID = t.TACTIC_ID
  )
GROUP BY TREATMT_MN
ORDER BY tactic_rows DESC;

-- C4b: tactic rows whose TACTIC_ID does NOT appear in MASTER
SELECT TOP 50
    TREATMT_MN,
    CAST(COUNT(*) AS BIGINT)        AS tactic_rows
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
  AND NOT EXISTS (
      SELECT 1 FROM DTZV01.VENDOR_FEEDBACK_MASTER m
      WHERE m.TREATMENT_ID = t.TACTIC_ID
  )
GROUP BY TREATMT_MN
ORDER BY tactic_rows DESC;


-- ---------------------------------------------------------------------------
-- C5: Agreement matrix — flag_in_master x flag_vrb_em, per distinct TACTIC_ID
-- ---------------------------------------------------------------------------
-- Proves: 2x2 count of distinct TACTIC_IDs (2024+) by flag_in_master (EXISTS
-- in MASTER by TREATMENT_ID — the de-facto email flag) x flag_vrb_em (MAX over
-- rows of CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%EM%' THEN 1
-- ELSE 0 END — DISCOVERY probe only, not a production filter).
-- Read: (1,1)+(0,0) large and (1,0)/(0,1) near-zero CONFIRMS pos-121 '%EM%' as
-- the channel marker. WARN if either off-diagonal cell is large relative to
-- the diagonal — the marker disagrees with the de-facto email flag and pos
-- 121 is not (solely) the channel slot.
-- ---------------------------------------------------------------------------

WITH tactic_vrb AS (
    SELECT
        t.TACTIC_ID,
        MAX(CASE WHEN SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
                 THEN 1 ELSE 0 END)  AS flag_vrb_em
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
    GROUP BY t.TACTIC_ID
),
master_treatments AS (
    SELECT DISTINCT TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_MASTER
)
SELECT
    CASE WHEN mt.TREATMENT_ID IS NOT NULL THEN 1 ELSE 0 END AS flag_in_master,
    v.flag_vrb_em,
    CAST(COUNT(*) AS BIGINT)        AS distinct_tactic_ids
FROM tactic_vrb v
LEFT JOIN master_treatments mt
    ON mt.TREATMENT_ID = v.TACTIC_ID
GROUP BY 1, 2
ORDER BY 1 DESC, 2 DESC;
