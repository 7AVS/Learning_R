-- 59: CPC state of same-program prior-unsub decisions, split by sent vs not sent (Cards MNEs, Teradata-direct)
-- Built on Pack 58's finding: PRIOR_UNSUB_SAME_MNE decisions still get sent ~20% of the time
-- (37,690 of 188,909), concentrated after day 90 (Block 3). This file asks WHY: does CPC show
-- a live No/blank on 1002/1012/1014 for the re-sent clients (meaning the send system is
-- ignoring a real CPC signal) or does CPC look the SAME for sent vs not-sent (meaning the
-- decay is elsewhere — an SFMC list reset / re-consent, not a CPC-read failure)?
-- SOURCES CONSULTED BEFORE WRITING (do not guess columns): schemas/cpc_rb_pref_log_schema.md
-- (table+column list, consent code decode), UNSUB_TRACKING_KNOWLEDGE.md §17 (consent codes,
-- 1002=email gate, 1014=sharing not email gate, blank=No only on 1014/1015),
-- reference_sfmc_unsub_blueprint.md, reference_cpc_1014_decisioning_parameter.md,
-- feedback_cpc_universe_filters.md (personal/active filters — not applied here: this
-- population is already a fixed set of clients who WERE decisioned, so CLNT_TYP/CLNT_STS
-- would only re-filter a population that's already real; noted, not applied, flagged here).
-- REUSED PATTERN: archaeology/21a_cpc_landscape.sql's `vt_cpc_latest` CTE (CLNT_NO, PREF_ID,
-- CLNT_CONSENT_TYP, CHG_TMSTMP columns off DDWV01.CPC_RB_PREF; ROW_NUMBER-in-a-CTE latest-
-- state pattern). NOT reused verbatim because that pack uses ONE global as-of date for every
-- client; this file needs a PER-DECISION as-of date (each decision has its own TREATMT_STRT_DT),
-- so the state reconstruction here uses GROUP BY/MAX instead of ROW_NUMBER (§20.8: ordered
-- analytics inside a subquery throw 3706 — MAX avoids the whole class of problem and scales
-- the same way to a per-row as-of date, which ROW_NUMBER-per-partition would also need a
-- CROSS JOIN per decision to do — messier for no benefit).
-- AS-OF METHOD CHOSEN: DDWV01.CPC_RB_PREF (the write table), latest row strictly before
-- each decision's TREATMT_STRT_DT, NOT a monthly snapshot table (CPC_RB_PREF_MTHLY) — a
-- decision-level as-of date needs day-level precision (Pack 58 Block 3 found the effect
-- decays inside a 90-day window; a month-end snapshot would blur exactly the window that
-- matters).
-- RUN ORDER: Step 0 (reset, incl. 54/57/58 leftovers) -> Step A (EMAIL_ACTION decisions) ->
-- Step A2 (tactic-id driver) -> Step B1-B3 (sent) -> Step C (first unsub per MNE) -> Step D
-- (zero-send months) -> Step E (PRIOR_UNSUB_SAME_MNE decisions only, ~189K rows) -> Step F1-F3
-- (CPC state as of each decision's date) -> PROFILING (sanity check on the raw consent codes
-- seen in this population) -> Block 1 (1012 x 1002) -> Block 2 (1014) -> Block 3 (mne x 1012,
-- cube) -> Block 4 (any CPC write between unsub and decision).
-- Grain = (CLNT_NO, TACTIC_ID). Counts only, no rates.
-- =============================================================================

-- STEP 0 removed 2026-09-04: no DROP statements in packs. If you rerun a pack in the same
-- session and hit 'table already exists', run 00_reset_volatiles.sql first.


-- ===== PARAMETER BLOCK: MNE SCOPE + TWO WINDOWS (same as Pack 57/58) =====
-- MNE scope — Cards personal MNEs, exactly as Pack 17 scoped them:
-- archaeology/17_em_decision_vendor_coverage.sql -> SUBSTR(t.TACTIC_ID,8,3) IN ('CRV','PCL','PCQ','PCD','AUH')
-- DECISION window start — DATE '2025-01-01', used in Step A and Step B2.
-- UNSUB lookback floor — DATE '2024-01-01', used in Step C ONLY, ON PURPOSE.
-- Both values are written directly into the relevant step below.


-- ===== STEP A: VOLATILE — EMAIL_ACTION decisions, Cards MNEs, 2025-01-01+ (ALL, not pre-filtered to same-MNE — needed for the zero-send-month check and the flag) =====
-- DROP TABLE vt_em_decis59;  -- also see STEP 0

CREATE VOLATILE TABLE vt_em_decis59 AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                        AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)         AS cohort_yyyymm,
        t.TREATMT_STRT_DT
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2025-01-01'                          -- PARAMETER BLOCK: decision window start
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')     -- PARAMETER BLOCK: MNE scope
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
) WITH DATA
PRIMARY INDEX (TACTIC_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_em_decis59 COLUMN (TACTIC_ID, CLNT_NO);


-- ===== STEP A2: VOLATILE — distinct tactic ids from Step A (join driver) =====
-- DROP TABLE vt_tactic_ids59;  -- also see STEP 0

CREATE VOLATILE TABLE vt_tactic_ids59 AS (
    SELECT DISTINCT TACTIC_ID FROM vt_em_decis59
) WITH DATA
PRIMARY INDEX (TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tactic_ids59 COLUMN (TACTIC_ID);


-- ===== STEP B1: VOLATILE — MASTER match, restricted via the tactic-id driver =====
-- DROP TABLE vt_master59;  -- also see STEP 0

CREATE VOLATILE TABLE vt_master59 AS (
    SELECT DISTINCT
        m.TREATMENT_ID,
        m.CLNT_NO,
        m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    INNER JOIN vt_tactic_ids59 x
        ON x.TACTIC_ID = m.TREATMENT_ID
    WHERE m.CLNT_NO IS NOT NULL
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_master59 COLUMN (TREATMENT_ID, CLNT_NO);


-- ===== STEP B2: VOLATILE — SENT (disposition_cd=1) events, restricted via the tactic-id driver =====
-- DROP TABLE vt_sent_evt59;  -- also see STEP 0

CREATE VOLATILE TABLE vt_sent_evt59 AS (
    SELECT DISTINCT
        e.TREATMENT_ID,
        e.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_tactic_ids59 x
        ON x.TACTIC_ID = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-01-01'  -- PARAMETER BLOCK: decision window start
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, consumer_id_hashed)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent_evt59 COLUMN (TREATMENT_ID, consumer_id_hashed);


-- ===== STEP B3: VOLATILE — sent, two-way join of B1+B2 (spool-safe pattern from Pack 54/57/58) =====
-- DROP TABLE vt_sent59;  -- also see STEP 0

CREATE VOLATILE TABLE vt_sent59 AS (
    SELECT DISTINCT
        m.CLNT_NO,
        m.TREATMENT_ID
    FROM vt_master59 m
    INNER JOIN vt_sent_evt59 s
        ON  s.TREATMENT_ID       = m.TREATMENT_ID
        AND s.consumer_id_hashed = m.consumer_id_hashed
) WITH DATA
PRIMARY INDEX (TREATMENT_ID, CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sent59 COLUMN (TREATMENT_ID, CLNT_NO);

DROP TABLE vt_master59;
DROP TABLE vt_sent_evt59;


-- ===== STEP C: VOLATILE — first-ever unsub PER CLIENT PER MNE, bank-wide, 2024-01-01+ =====
-- DROP TABLE vt_first_unsub_by_mne59;  -- also see STEP 0
-- Same construction as Pack 58's Step C58. Junk TREATMENT_IDs excluded (§20.12).

CREATE VOLATILE TABLE vt_first_unsub_by_mne59 AS (
    SELECT
        m.CLNT_NO,
        SUBSTR(e.TREATMENT_ID, 8, 3)  AS unsub_mne,
        MIN(e.disposition_dt_tm)      AS first_unsub_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.TREATMENT_ID       = e.TREATMENT_ID
        AND m.consumer_id_hashed = e.consumer_id_hashed
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'   -- PARAMETER BLOCK: unsub lookback floor
      AND e.TREATMENT_ID NOT IN ('DEFAULT', 'CABVRSN1')
      AND m.CLNT_NO IS NOT NULL
    GROUP BY m.CLNT_NO, SUBSTR(e.TREATMENT_ID, 8, 3)
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_first_unsub_by_mne59 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_first_unsub_by_mne59 COLUMN (CLNT_NO, unsub_mne);


-- ===== STEP D: VOLATILE — mne x cohort_yyyymm pairs with ZERO sends (operational gaps) =====
-- DROP TABLE vt_zero_send_months59;  -- also see STEP 0
-- Same scope as Pack 57/58 — expect CRV 202502-202505 and PCL 202607. Excluded downstream so
-- an operational no-send month isn't misread as CPC-driven suppression.

CREATE VOLATILE TABLE vt_zero_send_months59 AS (
    SELECT
        d.mne,
        d.cohort_yyyymm
    FROM vt_em_decis59 d
    LEFT JOIN vt_sent59 s
        ON  s.TREATMENT_ID = d.TACTIC_ID
        AND s.CLNT_NO       = d.CLNT_NO
    GROUP BY d.mne, d.cohort_yyyymm
    HAVING SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) = 0
) WITH DATA
PRIMARY INDEX (mne, cohort_yyyymm)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_zero_send_months59 COLUMN (mne, cohort_yyyymm);


-- ===== STEP E: VOLATILE — PRIOR_UNSUB_SAME_MNE decisions only =====
-- DROP TABLE vt_same_mne59;  -- also see STEP 0
-- Same population as Pack 58 Block 3 (decisions with a same-MNE unsub strictly before the
-- decision date). Expect ~188,909 rows / ~32,840 distinct clients (Pack 58 Block 2 SAME total).

CREATE VOLATILE TABLE vt_same_mne59 AS (
    SELECT
        d.CLNT_NO,
        d.TACTIC_ID,
        d.mne,
        d.cohort_yyyymm,
        d.TREATMT_STRT_DT,
        sm.first_unsub_dt_tm AS first_same_unsub_dt_tm,
        MAX(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_sent
    FROM vt_em_decis59 d
    INNER JOIN vt_first_unsub_by_mne59 sm
        ON  sm.CLNT_NO   = d.CLNT_NO
        AND sm.unsub_mne = d.mne
    LEFT JOIN vt_sent59 s
        ON  s.TREATMENT_ID = d.TACTIC_ID
        AND s.CLNT_NO       = d.CLNT_NO
    WHERE sm.first_unsub_dt_tm < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
    GROUP BY d.CLNT_NO, d.TACTIC_ID, d.mne, d.cohort_yyyymm, d.TREATMT_STRT_DT, sm.first_unsub_dt_tm
) WITH DATA
PRIMARY INDEX (CLNT_NO, TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_same_mne59 COLUMN (CLNT_NO, TACTIC_ID);


-- ===== STEP F1: VOLATILE — CPC log, narrowed to this population's clients + tracked prefs =====
-- DROP TABLE vt_cpc_clients59;  -- also see STEP 0
-- DROP TABLE vt_cpc_log59;      -- also see STEP 0
-- Narrow BEFORE touching the 91M-row log (same driver-table discipline as Pack 54/57/58's
-- MASTER/EVENT restriction) — never IN-subquery a 32,840-client filter against a 91M-row table.

CREATE VOLATILE TABLE vt_cpc_clients59 AS (
    SELECT DISTINCT CLNT_NO FROM vt_same_mne59
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_clients59 COLUMN (CLNT_NO);

CREATE VOLATILE TABLE vt_cpc_log59 AS (
    SELECT
        c.CLNT_NO,
        c.PREF_ID,
        c.CLNT_CONSENT_TYP,
        c.CHG_TMSTMP
    FROM DDWV01.CPC_RB_PREF c
    INNER JOIN vt_cpc_clients59 x
        ON x.CLNT_NO = c.CLNT_NO
    WHERE c.PREF_ID IN (1002, 1012, 1014)
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_log59 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_cpc_log59 COLUMN (CLNT_NO, PREF_ID);


-- ===== STEP F2: VOLATILE — latest CPC change timestamp before EACH decision's own date, per pref =====
-- DROP TABLE vt_cpc_asof_latest59;  -- also see STEP 0
-- Per-decision as-of date (not one global snapshot) — GROUP BY/MAX, not ROW_NUMBER, per §20.8
-- (ordered analytics inside a subquery throw 3706; MAX has no such restriction and gives the
-- same "latest row before X" answer).

CREATE VOLATILE TABLE vt_cpc_asof_latest59 AS (
    SELECT
        d.CLNT_NO,
        d.TACTIC_ID,
        c.PREF_ID,
        MAX(c.CHG_TMSTMP) AS latest_chg_tmstmp
    FROM vt_same_mne59 d
    INNER JOIN vt_cpc_log59 c
        ON  c.CLNT_NO = d.CLNT_NO
        AND c.CHG_TMSTMP < CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
    GROUP BY d.CLNT_NO, d.TACTIC_ID, c.PREF_ID
) WITH DATA
PRIMARY INDEX (CLNT_NO, TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_asof_latest59 COLUMN (CLNT_NO, TACTIC_ID);


-- ===== STEP F3: VOLATILE — CPC state (raw consent code) as of each decision, wide by pref =====
-- DROP TABLE vt_cpc_state_asof59;  -- also see STEP 0
-- consent_1002/1012/1014 = the CLNT_CONSENT_TYP in force immediately before the decision;
-- NULL = no CPC row for that pref existed before this decision at all ("NotInCPC" downstream —
-- distinct from an explicit 5003 blank row, per the NO-ROW-vs-BLANK distinction in this file's
-- canon). Tie-break note: if two log rows share the exact same CHG_TMSTMP for one
-- (CLNT_NO, PREF_ID) — not expected, not seen in any prior pack — MAX(CLNT_CONSENT_TYP) below
-- picks one arbitrarily; flagging this as an untested edge case, not a designed behavior.

CREATE VOLATILE TABLE vt_cpc_state_asof59 AS (
    SELECT
        d.CLNT_NO,
        d.TACTIC_ID,
        d.mne,
        d.cohort_yyyymm,
        d.f_sent,
        MAX(CASE WHEN al.PREF_ID = 1002 THEN c.CLNT_CONSENT_TYP END) AS consent_1002,
        MAX(CASE WHEN al.PREF_ID = 1012 THEN c.CLNT_CONSENT_TYP END) AS consent_1012,
        MAX(CASE WHEN al.PREF_ID = 1014 THEN c.CLNT_CONSENT_TYP END) AS consent_1014
    FROM vt_same_mne59 d
    LEFT JOIN vt_cpc_asof_latest59 al
        ON  al.CLNT_NO   = d.CLNT_NO
        AND al.TACTIC_ID = d.TACTIC_ID
    LEFT JOIN vt_cpc_log59 c
        ON  c.CLNT_NO       = al.CLNT_NO
        AND c.PREF_ID        = al.PREF_ID
        AND c.CHG_TMSTMP      = al.latest_chg_tmstmp
    GROUP BY d.CLNT_NO, d.TACTIC_ID, d.mne, d.cohort_yyyymm, d.f_sent
) WITH DATA
PRIMARY INDEX (CLNT_NO, TACTIC_ID)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_state_asof59 COLUMN (CLNT_NO, TACTIC_ID);


-- ===== PROFILING (sanity check, NOT a column-name guess — schema + join pattern both confirmed against schemas/cpc_rb_pref_log_schema.md and archaeology/21a_cpc_landscape.sql before writing any of the above) =====
-- QUESTION: what raw CLNT_CONSENT_TYP values actually appear for 1002/1012/1014 in this
--   specific population, and how many decisions have no prior CPC row at all per pref?
-- ROWS: <=12 (3 prefs x up to 4 codes seen)
-- GOOD LOOKS LIKE: only 5001/5002/5003 appear (5004 is a 1016-only code per the schema and
--   shouldn't show up here); a NULL/no-row share worth noting before trusting Blocks 1-4
-- WHAT TO DO WITH IT: record the result

SELECT
    CAST('1002' AS VARCHAR(4)) AS pref_id, consent_1002 AS raw_consent_code,
    CAST(COUNT(*) AS BIGINT) AS decisions
FROM vt_cpc_state_asof59
GROUP BY consent_1002
UNION ALL
SELECT
    CAST('1012' AS VARCHAR(4)) AS pref_id, consent_1012 AS raw_consent_code,
    CAST(COUNT(*) AS BIGINT) AS decisions
FROM vt_cpc_state_asof59
GROUP BY consent_1012
UNION ALL
SELECT
    CAST('1014' AS VARCHAR(4)) AS pref_id, consent_1014 AS raw_consent_code,
    CAST(COUNT(*) AS BIGINT) AS decisions
FROM vt_cpc_state_asof59
GROUP BY consent_1014
ORDER BY 1, 2;


-- ===== BLOCK 1: sent flag x cpc_1012_state x cpc_1002_state, zero-send months EXCLUDED =====
-- QUESTION: for same-program prior-unsub decisions, does the CPC 1012 (email consent) or
--   1002 (do-not-solicit / email-gate, §17-T3) state look different between sent and not-sent?
-- ROWS: <=32 (2 sent flags x 4 states x 4 states)
-- GOOD LOOKS LIKE: SENT rows concentrate in Yes/Blank; NOT_SENT rows concentrate in No
-- WHAT TO DO WITH IT: record the result

SELECT
    CASE WHEN v.f_sent = 1 THEN 'SENT' ELSE 'NOT_SENT' END AS sent_flag,
    CASE
        WHEN v.consent_1012 IS NULL      THEN 'NotInCPC'
        WHEN v.consent_1012 IN (5001,5004) THEN 'Yes'
        WHEN v.consent_1012 = 5002        THEN 'No'
        WHEN v.consent_1012 = 5003        THEN 'Blank'
        ELSE 'Other'
    END AS cpc_1012_state,
    CASE
        WHEN v.consent_1002 IS NULL      THEN 'NotInCPC'
        WHEN v.consent_1002 IN (5001,5004) THEN 'Yes'
        WHEN v.consent_1002 = 5002        THEN 'No'
        WHEN v.consent_1002 = 5003        THEN 'Blank'
        ELSE 'Other'
    END AS cpc_1002_state,
    CAST(COUNT(*) AS BIGINT)                  AS decisions,
    CAST(COUNT(DISTINCT v.CLNT_NO) AS BIGINT) AS distinct_clients
FROM vt_cpc_state_asof59 v
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months59 z
    WHERE z.mne = v.mne AND z.cohort_yyyymm = v.cohort_yyyymm
)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- ===== BLOCK 2: sent flag x cpc_1014_state, zero-send months EXCLUDED =====
-- QUESTION: does 1014 (cross-entity sharing, NOT the email gate per §17-T3) look any
--   different between sent and not-sent — checking it's genuinely uninvolved, as canon says
-- ROWS: <=8 (2 sent flags x 4 states)
-- GOOD LOOKS LIKE: no strong split (1014 isn't expected to explain send behavior at all)
-- WHAT TO DO WITH IT: record the result

SELECT
    CASE WHEN v.f_sent = 1 THEN 'SENT' ELSE 'NOT_SENT' END AS sent_flag,
    CASE
        WHEN v.consent_1014 IS NULL        THEN 'NotInCPC'
        WHEN v.consent_1014 IN (5001,5004) THEN 'Yes'
        WHEN v.consent_1014 = 5002         THEN 'No'
        WHEN v.consent_1014 = 5003         THEN 'Blank'
        ELSE 'Other'
    END AS cpc_1014_state,
    CAST(COUNT(*) AS BIGINT)                  AS decisions,
    CAST(COUNT(DISTINCT v.CLNT_NO) AS BIGINT) AS distinct_clients
FROM vt_cpc_state_asof59 v
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months59 z
    WHERE z.mne = v.mne AND z.cohort_yyyymm = v.cohort_yyyymm
)
GROUP BY 1, 2
ORDER BY 1, 2;


-- ===== BLOCK 3: mne x sent flag x cpc_1012_state cube, zero-send months EXCLUDED =====
-- QUESTION: does the CPC 1012 read differ by MNE — e.g. is PCL's high resend rate (Pack 58)
--   also a CPC-blindness pattern, or specific to PCL's own suppression logic?
-- ROWS: <=40 (5 MNEs x 2 sent flags x 4 states)
-- GOOD LOOKS LIKE: if PCL's NOT_SENT/No share looks like the other MNEs, PCL's problem is not
--   a CPC-read gap; if PCL shows more SENT-while-No than others, PCL specifically isn't
--   checking 1012 the way the other MNEs do
-- WHAT TO DO WITH IT: save to Excel, do not paste

SELECT
    v.mne,
    CASE WHEN v.f_sent = 1 THEN 'SENT' ELSE 'NOT_SENT' END AS sent_flag,
    CASE
        WHEN v.consent_1012 IS NULL        THEN 'NotInCPC'
        WHEN v.consent_1012 IN (5001,5004) THEN 'Yes'
        WHEN v.consent_1012 = 5002         THEN 'No'
        WHEN v.consent_1012 = 5003         THEN 'Blank'
        ELSE 'Other'
    END AS cpc_1012_state,
    CAST(COUNT(*) AS BIGINT)                  AS decisions,
    CAST(COUNT(DISTINCT v.CLNT_NO) AS BIGINT) AS distinct_clients
FROM vt_cpc_state_asof59 v
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months59 z
    WHERE z.mne = v.mne AND z.cohort_yyyymm = v.cohort_yyyymm
)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- ===== BLOCK 4: sent flag x "any CPC write between the same-MNE unsub and the decision", zero-send months EXCLUDED =====
-- QUESTION: did CPC ever hear anything about this client at all in the gap between their
--   unsub and this decision — testing whether CPC ever had a chance to reflect the unsub,
--   regardless of what it says
-- SCOPE NOTE (assumption, flagged): "any CPC write" here means any row on the SAME three
--   tracked prefs (1002/1012/1014) — the only prefs vt_cpc_log59 was narrowed to. It does NOT
--   mean literally any of the ~30 PREF_IDs on the full log; broadening that would need a
--   second, wider pull. Ask if the broader version is wanted.
-- ROWS: 4 (2 sent flags x 2 yes/no)
-- GOOD LOOKS LIKE: if SENT rows show "no write" far more than NOT_SENT rows, the send system
--   never got a chance to see anything new — consistent with the send system reading CPC at
--   send time and simply finding nothing since the unsub (list-rebuild-from-CPC hypothesis,
--   §22). If both groups show similar write rates, CPC saw something either way and the
--   split is elsewhere.
-- WHAT TO DO WITH IT: record the result

WITH any_write AS (
    SELECT
        d.CLNT_NO,
        d.TACTIC_ID,
        MAX(CASE WHEN c.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_any_cpc_write
    FROM vt_same_mne59 d
    LEFT JOIN vt_cpc_log59 c
        ON  c.CLNT_NO = d.CLNT_NO
        AND c.CHG_TMSTMP >  d.first_same_unsub_dt_tm
        AND c.CHG_TMSTMP <  CAST(d.TREATMT_STRT_DT AS TIMESTAMP(0))
    GROUP BY d.CLNT_NO, d.TACTIC_ID
)
SELECT
    CASE WHEN d.f_sent = 1 THEN 'SENT' ELSE 'NOT_SENT' END AS sent_flag,
    CASE WHEN aw.f_any_cpc_write = 1 THEN 'CPC_WRITE_IN_GAP' ELSE 'NO_CPC_WRITE_IN_GAP' END AS cpc_write_in_gap,
    CAST(COUNT(*) AS BIGINT)                  AS decisions,
    CAST(COUNT(DISTINCT d.CLNT_NO) AS BIGINT) AS distinct_clients
FROM vt_same_mne59 d
INNER JOIN any_write aw
    ON  aw.CLNT_NO   = d.CLNT_NO
    AND aw.TACTIC_ID = d.TACTIC_ID
WHERE NOT EXISTS (
    SELECT 1 FROM vt_zero_send_months59 z
    WHERE z.mne = d.mne AND z.cohort_yyyymm = d.cohort_yyyymm
)
GROUP BY 1, 2
ORDER BY 1, 2;
