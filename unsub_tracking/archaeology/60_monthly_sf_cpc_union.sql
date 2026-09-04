-- 60: Salesforce-primary unsub count vs CPC-1012-revocation count, monthly (Teradata-direct)
-- DIRECTOR'S ASK (verbatim): "if we use Salesforce as the primary unsub source, what % of
-- unsubs would be added if we included CPC changes where e-mail channel consent has been
-- revoked?"
-- Universe, active-check style, EVENT<->MASTER merge and MNE derivation copied verbatim from
-- 45_audit_queries.sql: Q1 (SF unsub, lines 59-148), Q2 (CPC 1012 writes, 151-239), Q3b
-- (waterfall v3 universe, 589-782), Q5 (the [-1,+14]-day match rule, 483-586).
-- RUN ORDER: Step 0 (reset) -> Step A (SF unsub, first per CLNT_NO/cohort_yyyymm) -> Step B (CPC
-- 1012->5002 write, first per CLNT_NO/cohort_yyyymm) -> Step C1-C3 (pairs + flags) -> Block 1 (cohort_yyyymm x
-- segment, paste) -> Block 2A/2B (segment totals + APP_SYS_CD for CPC_ONLY, paste) -> Block 3
-- (cohort_yyyymm x mne, Excel) -> Block 4 (mne totals, paste).
-- RECONCILES TO: 48_q3b_q3c_results.md and 49_q1_q2_monthly_consolidated.md - these two
-- disagree on the SF unsub total (473,863 vs 506,646); this file follows Q1's number - see
-- the report for why. Grain = CLNT_NO x cohort_yyyymm. Counts only, plus one ratio column that IS the
-- director's literal ask.
-- =============================================================================

-- STEP 0 removed 2026-09-04: no DROP statements in packs. If you rerun a pack in the same
-- session and hit 'table already exists', run 00_reset_volatiles.sql first.


-- ===== PARAMETER BLOCK: WINDOW (matches Q3b, Aug-24 -> Jul-26) =====
-- WIN_START       = DATE '2024-08-01'  - written into Step A and Step B below.
-- WIN_END_EXCL    = DATE '2026-08-01'  - written into Step A and Step B below.
-- MASTER SUBSTR lo = '2024122' (WIN_START minus 3 months, as julian YYYYDDD - 2024-05-01 is
--   day 122 of a leap year), MASTER SUBSTR hi = '2026212' (2026-07-31, same bound Q1/Q3b use).
--   If WIN_START changes, recompute the lo bound the same way (3 months earlier, julian).
-- MATCH WINDOW    = [-1, +14] days, CPC write vs SF unsub date - Q5's rule, not a parameter.
-- No shared variables across statements in a plain Teradata script - edit all four literals
-- in place (Step A, Step B) if the window changes.


-- ===== STEP A: VOLATILE — SF unsubs, first disposition_cd=4 per (CLNT_NO, cohort_yyyymm) =====
-- DROP TABLE vt_sf_unsub60;  -- also see STEP 0
-- Universe + locked merge copied verbatim from Q1 (45_audit_queries.sql lines 67-131): act
-- spine = RB_CLNT_DLY CLNT_STS='A' CLNT_TYP=1, date-matched at the event's OWN cohort_yyyymm-end;
-- MASTER reduced to DISTINCT (consumer_id_hashed, TREATMENT_ID, CLNT_NO), CLNT_NO NOT NULL;
-- shape guard (10-char, numeric 7-prefix) is the actual mechanism Q1/Q3b use to exclude
-- DEFAULT/CABVRSN1 and other vendor junk IDs (kept as-is rather than an explicit NOT IN list).

CREATE VOLATILE TABLE vt_sf_unsub60 AS (
    WITH act AS (
        SELECT SNAP_DT, CLNT_NO
        FROM DDWV01.RB_CLNT_DLY
        WHERE CLNT_STS = 'A'
          AND SNAP_DT >= DATE '2024-08-01'   -- PARAMETER BLOCK: WIN_START
          AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
          AND CLNT_TYP = 1
    ),
    base AS (
        SELECT m.CLNT_NO,
               e.disposition_dt_tm          AS dt,
               SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
               e.TREATMENT_ID,
               TRIM(EXTRACT(YEAR FROM e.disposition_dt_tm)) || '-' ||
                 TRIM(CASE WHEN EXTRACT(MONTH FROM e.disposition_dt_tm) < 10
                           THEN '0' ELSE '' END) ||
                 TRIM(EXTRACT(MONTH FROM e.disposition_dt_tm))        AS evt_month
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024122' AND '2026212'  -- PARAMETER BLOCK: MASTER SUBSTR bounds
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        INNER JOIN act ON act.CLNT_NO = m.CLNT_NO
                      AND act.SNAP_DT = ADD_MONTHS(CAST(e.disposition_dt_tm AS DATE)
                                                   - EXTRACT(DAY FROM e.disposition_dt_tm) + 1, 1) - 1
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-08-01'   -- PARAMETER BLOCK: WIN_START
          AND e.disposition_dt_tm <  DATE '2026-08-01'   -- PARAMETER BLOCK: WIN_END_EXCL
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ),
    ranked AS (
        SELECT CLNT_NO, evt_month, mne, dt, TREATMENT_ID,
               ROW_NUMBER() OVER (PARTITION BY CLNT_NO, evt_month
                                  ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn
        FROM base
    )
    SELECT CLNT_NO, evt_month, mne, CAST(dt AS DATE) AS unsub_dt
    FROM ranked
    WHERE rn = 1
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sf_unsub60 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_sf_unsub60 COLUMN (CLNT_NO, evt_month);


-- ===== STEP B: VOLATILE — CPC 1012 writes to No (5002), first per (CLNT_NO, cohort_yyyymm) =====
-- DROP TABLE vt_cpc_write60;  -- also see STEP 0
-- "Write to No" copied verbatim from Q2's writes CTE (45_audit_queries.sql lines 197-222):
-- DDWV01.CPC_RB_PREF, PREF_ID=1012, CLNT_CONSENT_TYP=5002, act join at the WRITE's own
-- cohort_yyyymm-end (RB_CLNT_DLY CLNT_STS='A' CLNT_TYP=1 - same spine as Step A, re-declared locally).
-- ANY APP_SYS_CD kept (director's ask covers any origin); APP_SYS_CD carried through for
-- Block 2B. Client-cohort_yyyymm grain (first write per client per cohort_yyyymm) mirrors Step A's dedup so
-- Step C matches one CLNT_NO x date row per side.

CREATE VOLATILE TABLE vt_cpc_write60 AS (
    WITH act AS (
        SELECT SNAP_DT, CLNT_NO
        FROM DDWV01.RB_CLNT_DLY
        WHERE CLNT_STS = 'A'
          AND SNAP_DT >= DATE '2024-08-01'   -- PARAMETER BLOCK: WIN_START
          AND EXTRACT(DAY FROM SNAP_DT + 1) = 1
          AND CLNT_TYP = 1
    ),
    base AS (
        SELECT w.CLNT_NO,
               w.APP_SYS_CD,
               w.CHG_TMSTMP AS ts,
               TRIM(EXTRACT(YEAR FROM CAST(w.CHG_TMSTMP AS DATE))) || '-' ||
                 TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(w.CHG_TMSTMP AS DATE)) < 10
                           THEN '0' ELSE '' END) ||
                 TRIM(EXTRACT(MONTH FROM CAST(w.CHG_TMSTMP AS DATE)))   AS wr_month
        FROM DDWV01.CPC_RB_PREF w
        INNER JOIN act ON act.CLNT_NO = w.CLNT_NO
                      AND act.SNAP_DT = ADD_MONTHS(CAST(w.CHG_TMSTMP AS DATE)
                                                   - EXTRACT(DAY FROM w.CHG_TMSTMP) + 1, 1) - 1
        WHERE w.PREF_ID = 1012
          AND w.CLNT_CONSENT_TYP = 5002
          AND w.CHG_TMSTMP >= DATE '2024-08-01'   -- PARAMETER BLOCK: WIN_START
          AND w.CHG_TMSTMP <  DATE '2026-08-01'   -- PARAMETER BLOCK: WIN_END_EXCL
    ),
    ranked AS (
        SELECT CLNT_NO, wr_month, APP_SYS_CD, ts,
               ROW_NUMBER() OVER (PARTITION BY CLNT_NO, wr_month
                                  ORDER BY ts ASC, APP_SYS_CD ASC) AS rn
        FROM base
    )
    SELECT CLNT_NO, wr_month, APP_SYS_CD, CAST(ts AS DATE) AS write_dt
    FROM ranked
    WHERE rn = 1
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_write60 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_cpc_write60 COLUMN (CLNT_NO, wr_month);


-- ===== STEP C1: VOLATILE — matched pairs, Q5's [-1,+14]-day CLNT_NO rule =====
-- DROP TABLE vt_pairs60;  -- also see STEP 0
-- Match rule copied verbatim from Q5 (45_audit_queries.sql lines 555-561): a CPC write dated
-- within [unsub_dt - 1, unsub_dt + 14] counts as matched. Both sides are already deduped to
-- CLNT_NO x cohort_yyyymm (Steps A/B) so this is a small driver join, not a big-table join - single
-- INNER JOIN, per pack 58's convention (never a 3-way join in one CREATE).

CREATE VOLATILE TABLE vt_pairs60 AS (
    SELECT DISTINCT s.CLNT_NO, s.evt_month AS sf_month, s.unsub_dt,
           w.wr_month AS cpc_month, w.write_dt
    FROM vt_sf_unsub60 s
    INNER JOIN vt_cpc_write60 w
        ON  w.CLNT_NO   = s.CLNT_NO
        AND w.write_dt BETWEEN s.unsub_dt - 1 AND s.unsub_dt + 14
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pairs60 COLUMN (CLNT_NO);


-- ===== STEP C2: VOLATILE — SF unsub rows flagged matched/unmatched to a CPC write =====
-- DROP TABLE vt_sf_flagged60;  -- also see STEP 0

CREATE VOLATILE TABLE vt_sf_flagged60 AS (
    SELECT s.CLNT_NO, s.evt_month, s.mne, s.unsub_dt,
           CASE WHEN p.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS matched_cpc
    FROM vt_sf_unsub60 s
    LEFT JOIN (SELECT DISTINCT CLNT_NO, sf_month, unsub_dt FROM vt_pairs60) p
        ON  p.CLNT_NO  = s.CLNT_NO
        AND p.sf_month = s.evt_month
        AND p.unsub_dt = s.unsub_dt
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sf_flagged60 COLUMN (CLNT_NO);


-- ===== STEP C3: VOLATILE — CPC write rows flagged matched/unmatched to an SF unsub =====
-- DROP TABLE vt_cpc_flagged60;  -- also see STEP 0
-- CPC_ONLY = matched_sf = 0 here - a 1012 revocation with no SF unsub anywhere in its
-- [-14,+1]-day mirror window. This is the population the director's question is about.

CREATE VOLATILE TABLE vt_cpc_flagged60 AS (
    SELECT w.CLNT_NO, w.wr_month, w.APP_SYS_CD, w.write_dt,
           CASE WHEN p.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS matched_sf
    FROM vt_cpc_write60 w
    LEFT JOIN (SELECT DISTINCT CLNT_NO, cpc_month, write_dt FROM vt_pairs60) p
        ON  p.CLNT_NO   = w.CLNT_NO
        AND p.cpc_month = w.wr_month
        AND p.write_dt  = w.write_dt
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_flagged60 COLUMN (CLNT_NO);

-- Drop the pairs driver now that both sides are flagged — frees spool for the blocks below.
DROP TABLE vt_pairs60;


-- ===== BLOCK 1: cohort_yyyymm x {SF_ONLY, SF_AND_CPC, CPC_ONLY}, one row per cohort_yyyymm + TOTAL =====
-- QUESTION: if Salesforce were the primary unsub source, how many CPC-1012-revoke clients per
--   cohort_yyyymm would be ADDED that Salesforce alone misses?
-- ROWS: 25 (24 months Aug-24..Jul-26 + 1 TOTAL row)
-- GOOD LOOKS LIKE: sf_only + sf_and_cpc TOTAL ~506,646 (Q1's first_unsub_clients grand total,
--   49_q1_q2_monthly_consolidated.md) - NOT Q3b's 473,863 (see report: different universe).
--   cpc_only is the raw count Salesforce would be missing; the % added = cpc_only / (sf_only + sf_and_cpc), computed in Excel (counts only in output). It was the
--   director's number directly.
-- WHAT TO DO WITH IT: paste to Claude

WITH sf_side AS (
    SELECT evt_month AS cohort_yyyymm,
           CAST(SUM(CASE WHEN matched_cpc = 0 THEN 1 ELSE 0 END) AS BIGINT) AS sf_only,
           CAST(SUM(CASE WHEN matched_cpc = 1 THEN 1 ELSE 0 END) AS BIGINT) AS sf_and_cpc
    FROM vt_sf_flagged60
    GROUP BY 1
),
cpc_side AS (
    SELECT wr_month AS cohort_yyyymm,
           CAST(SUM(CASE WHEN matched_sf = 0 THEN 1 ELSE 0 END) AS BIGINT) AS cpc_only
    FROM vt_cpc_flagged60
    GROUP BY 1
),
final AS (
    SELECT CAST(COALESCE(sf.cohort_yyyymm, cpc.cohort_yyyymm) AS VARCHAR(10)) AS cohort_yyyymm,
           COALESCE(sf.sf_only, 0)    AS sf_only,
           COALESCE(sf.sf_and_cpc, 0) AS sf_and_cpc,
           COALESCE(cpc.cpc_only, 0)  AS cpc_only
    FROM sf_side sf
    FULL OUTER JOIN cpc_side cpc ON cpc.cohort_yyyymm = sf.cohort_yyyymm
)
SELECT cohort_yyyymm, sf_only, sf_and_cpc, cpc_only
FROM final
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(10)),
       CAST(SUM(sf_only) AS BIGINT),
       CAST(SUM(sf_and_cpc) AS BIGINT),
       CAST(SUM(cpc_only) AS BIGINT)
FROM final
ORDER BY 1;


-- ===== BLOCK 2A: overall totals by segment (whole window) =====
-- QUESTION: over the whole Aug-24..Jul-26 window, how big is each segment?
-- ROWS: 3
-- GOOD LOOKS LIKE: SF_ONLY + SF_AND_CPC = Block 1's TOTAL row sf_only+sf_and_cpc
-- WHAT TO DO WITH IT: paste to Claude

SELECT CAST('SF_ONLY' AS VARCHAR(20)) AS segment,
       CAST(SUM(CASE WHEN matched_cpc = 0 THEN 1 ELSE 0 END) AS BIGINT) AS clients
FROM vt_sf_flagged60
UNION ALL
SELECT CAST('SF_AND_CPC' AS VARCHAR(20)),
       CAST(SUM(CASE WHEN matched_cpc = 1 THEN 1 ELSE 0 END) AS BIGINT)
FROM vt_sf_flagged60
UNION ALL
SELECT CAST('CPC_ONLY' AS VARCHAR(20)),
       CAST(SUM(CASE WHEN matched_sf = 0 THEN 1 ELSE 0 END) AS BIGINT)
FROM vt_cpc_flagged60
ORDER BY 1;


-- ===== BLOCK 2B: CPC_ONLY revocations by APP_SYS_CD (who writes them) =====
-- QUESTION: which system/channel writes the CPC-only 1012 revocations Salesforce never sees?
-- ROWS: <=10 (distinct APP_SYS_CD values seen on 1012 writes)
-- GOOD LOOKS LIKE: 7020 (email backfeed) likely dominates, per Q3b's seg_email_cpc (72,346)
-- WHAT TO DO WITH IT: paste to Claude

SELECT APP_SYS_CD, CAST(COUNT(*) AS BIGINT) AS cpc_only_clients
FROM vt_cpc_flagged60
WHERE matched_sf = 0
GROUP BY APP_SYS_CD
ORDER BY cpc_only_clients DESC;


-- ===== BLOCK 3: cohort_yyyymm x mne, SF_ONLY / SF_AND_CPC only (Excel, not paste) =====
-- QUESTION: by MNE, how does the SF-only vs SF-and-CPC split move over time - a lower/upper
--   bound view per program?
-- CPC_ONLY has NO mne (preference-center writes carry no campaign attribution) and CANNOT be
--   added to this cut. If an MNE-level upper bound is needed, apply Block 1's bank-wide
--   the CPC-only ratio from Block 1 to this table's totals - it is not a per-MNE number.
-- ROWS: many (up to 24 months x MNE count) - Excel only
-- GOOD LOOKS LIKE: summed across mne, reproduces Block 1's sf_only/sf_and_cpc per cohort_yyyymm
-- WHAT TO DO WITH IT: save to Excel, do not paste

SELECT evt_month AS cohort_yyyymm, mne,
       CAST(SUM(CASE WHEN matched_cpc = 0 THEN 1 ELSE 0 END) AS BIGINT) AS sf_only,
       CAST(SUM(CASE WHEN matched_cpc = 1 THEN 1 ELSE 0 END) AS BIGINT) AS sf_and_cpc
FROM vt_sf_flagged60
GROUP BY 1, 2
ORDER BY 1, 2;


-- ===== BLOCK 4: mne totals, top 8 by SF volume + OTHER (whole window) =====
-- QUESTION: which MNEs carry the SF-only vs SF-and-CPC volume?
-- ROWS: <=9 (top 8 MNEs by total SF volume + 1 OTHER row)
-- GOOD LOOKS LIKE: LOYALTY-family MNEs (VRE/VME/VRG) dominate total_sf, per Q1/Q4 precedent
-- WHAT TO DO WITH IT: paste to Claude

WITH mne_totals AS (
    SELECT mne,
           CAST(SUM(CASE WHEN matched_cpc = 0 THEN 1 ELSE 0 END) AS BIGINT) AS sf_only,
           CAST(SUM(CASE WHEN matched_cpc = 1 THEN 1 ELSE 0 END) AS BIGINT) AS sf_and_cpc,
           CAST(COUNT(*) AS BIGINT) AS total_sf
    FROM vt_sf_flagged60
    GROUP BY 1
),
ranked AS (
    SELECT mne, sf_only, sf_and_cpc, total_sf,
           ROW_NUMBER() OVER (ORDER BY total_sf DESC) AS rn
    FROM mne_totals
)
SELECT CAST(mne AS VARCHAR(10)) AS mne, sf_only, sf_and_cpc, total_sf
FROM ranked
WHERE rn <= 8
UNION ALL
SELECT CAST('OTHER' AS VARCHAR(10)),
       CAST(SUM(sf_only) AS BIGINT),
       CAST(SUM(sf_and_cpc) AS BIGINT),
       CAST(SUM(total_sf) AS BIGINT)
FROM ranked
WHERE rn > 8
ORDER BY 4 DESC;
