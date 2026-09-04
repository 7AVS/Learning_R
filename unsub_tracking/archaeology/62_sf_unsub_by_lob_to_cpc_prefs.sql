-- UNPARKED 2026-09-04 (Andre): run as a MECHANICS check only. The matched pairs are the ~3% of SF
--   unsubs that reached CPC: a selected sample, so NO rate from Block 1 may be quoted as a population
--   share. The one question it answers: among LOYALTY-email unsubs that wrote 1012, did 1046 get
--   written too? If 1012_ONLY dominates within LOYALTY, the Avion page is no exception and the
--   director's 19% is a floor (design doc: RBC-wide writes 1012 alone).
-- 62: SF unsubs by line of business -> CPC preference writes (Teradata-direct)
-- QUESTION: for SF unsubs by LOB (Loyalty/Avion vs Cards vs Others), which CPC preference(s)
--   get written within a day?
-- READING A: RBC-wide choice on the Avion page does not write 1046 -> LOYALTY 1012_ONLY large.
-- READING B: Avion page writes both -> LOYALTY splits ~80/20 toward 1046_ONLY (director's split).
-- ENGINE: Teradata-direct, volatile tables, pattern matches packs 60/61.
-- RUN ORDER: Step A (SF unsubs + LOB) -> Step B (CPC 7020 writes) -> Step C (matched+classified)
--   -> Block 1 (lob x write_class, paste) -> Block 2 (LOYALTY monthly, paste) -> Block 3
--   (other-pref ids, paste).
-- COMPANION TO pack 61 v2 (1012<->1046 cascade). Grain = CLNT_NO x unsub_dt. Counts only.
-- No DROP statements (2026-09-04 rule) - see 00_reset_volatiles.sql for reruns.
-- =============================================================================

-- No DROP / reset statements in this file. If rerunning in the same session and you hit
-- "table already exists", run 00_reset_volatiles.sql first (this pack's names are in it).


-- ===== PARAMETER BLOCK: WINDOWS =====
-- SF_WIN_START      = DATE '2025-05-01'  - director's window, Step A.
-- SF_WIN_END_EXCL   = DATE '2026-06-26'  - Step A, exclusive (covers through 2026-06-25).
-- CPC_WIN_START     = DATE '2025-04-30'  - Step B, one day earlier than SF_WIN_START to allow
--   the unsub_dt-1 side of the +/-1-day match on the very first SF unsub date.
-- CPC_WIN_END_EXCL  = DATE '2026-06-27'  - Step B, exclusive (covers through 2026-06-26, one
--   day after the last SF unsub date, for the unsub_dt+1 side of the match).
-- No shared variables across statements in a plain Teradata script - edit all four literals
-- in place (Step A, Step B) if the window changes.


-- ===== PARAMETER BLOCK: MNE -> LOB MAPPING =====
-- SOURCE: unsub_tracking/archaeology/47_mne_lob_mapping_cards_loyalty.csv, column LOB_MANUAL
--   (the manually-curated column - same one 45_audit_queries.sql Q4/Q5 point at as the
--   authoritative split, "Andre maps MNE -> LOB in Excel (LOB MANUAL)"). That csv only
--   catalogues 45 mnemonics; it is the ONLY LOB source on file, so any mne NOT in it maps to
--   OTHERS below - this is a coverage gap, not a business decision, and OTHERS will include
--   both true "other LOB" sends and un-catalogued mnemonics. If OTHERS turns out large, that's
--   a catalogue gap to flag, not a finding.
-- LOYALTY (16, from the csv's LOYALTY rows): WEF VJB VMB VRG VOA BXY VME VN7 VN4 VMF VMS VO3
--   VO4 VRE VO1 VRD
-- CARDS (29, from the csv's CARDS rows): AUH VBA CRV VIF COB VLI PCQ WJR FWC CEC OTC RPF MVP
--   PCL VBU WJA WJF CLL HCD VCL BAF BCO CRO MWA AML MEF PON MET POT WNH PCD
-- Everything else -> OTHERS. The CASE expression below is the embedded lookup - edit here if
-- the csv is extended.


-- ===== STEP A: VOLATILE — SF unsubs, first disposition_cd=4 per (CLNT_NO, day), LOB-mapped =====
-- DROP TABLE vt_sf_unsub62;  -- also see 00_reset_volatiles.sql
-- Join pattern = pack 57 Step C's MASTER/EVENT merge (TREATMENT_ID NOT IN ('DEFAULT',
-- 'CABVRSN1'), CLNT_NO NOT NULL, no active-personal spine - not needed for a channel-mix
-- question) + pack 60 Step A's ranked/rn=1 dedup shape (base -> ranked -> rn=1). Grain is
-- CLNT_NO x calendar day, NOT CLNT_NO alone - a client can carry more than one SF unsub event
-- in the window and each gets its own row / own CPC match window in Step C.

CREATE VOLATILE TABLE vt_sf_unsub62 AS (
    WITH base AS (
        SELECT
            m.CLNT_NO,
            e.disposition_dt_tm               AS dt,
            CAST(e.disposition_dt_tm AS DATE) AS unsub_dt,
            SUBSTR(e.TREATMENT_ID, 8, 3)      AS mne,
            e.TREATMENT_ID
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2025-05-01'   -- PARAMETER BLOCK: SF_WIN_START
          AND e.disposition_dt_tm <  DATE '2026-06-26'   -- PARAMETER BLOCK: SF_WIN_END_EXCL
          AND e.TREATMENT_ID NOT IN ('DEFAULT', 'CABVRSN1')
    ),
    ranked AS (
        SELECT CLNT_NO, unsub_dt, mne, TREATMENT_ID,
               ROW_NUMBER() OVER (PARTITION BY CLNT_NO, unsub_dt
                                  ORDER BY dt ASC, mne ASC, TREATMENT_ID ASC) AS rn
        FROM base
    )
    SELECT
        CLNT_NO,
        unsub_dt,
        mne,
        CAST(
          CASE
            WHEN mne IN ('WEF','VJB','VMB','VRG','VOA','BXY','VME','VN7','VN4','VMF','VMS',
                         'VO3','VO4','VRE','VO1','VRD') THEN 'LOYALTY'
            WHEN mne IN ('AUH','VBA','CRV','VIF','COB','VLI','PCQ','WJR','FWC','CEC','OTC',
                         'RPF','MVP','PCL','VBU','WJA','WJF','CLL','HCD','VCL','BAF','BCO',
                         'CRO','MWA','AML','MEF','PON','MET','POT','WNH','PCD') THEN 'CARDS'
            ELSE 'OTHERS'
          END AS VARCHAR(10)
        ) AS lob
    FROM ranked
    WHERE rn = 1
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sf_unsub62 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_sf_unsub62 COLUMN (lob);


-- ===== STEP A2: VOLATILE — small driver, distinct CLNT_NO from Step A =====
-- DROP TABLE vt_sf_clients62;  -- also see 00_reset_volatiles.sql
-- Small driver table for the INNER JOIN in Step B, per convention (never IN-subquery a large
-- table against the full CLNT_NO population).

CREATE VOLATILE TABLE vt_sf_clients62 AS (
    SELECT DISTINCT CLNT_NO
    FROM vt_sf_unsub62
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_sf_clients62 COLUMN (CLNT_NO);


-- ===== STEP B: VOLATILE — CPC 7020 (email page) writes to 5002, restricted to Step A clients =====
-- DROP TABLE vt_cpc_write62;  -- also see 00_reset_volatiles.sql
-- Table/columns proven in pack 61 (DDWV01.CPC_RB_PREF, not the LOG - CLNT_NO, PREF_ID,
-- CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD) and pack 60 Step B. APP_SYS_CD = 7020 only (the
-- email page) - the population this pack is about. 1012/1046 broken out; every other PREF_ID
-- grouped as OTHER_PREF so Block 3 can still see which other programs' pages write here.

CREATE VOLATILE TABLE vt_cpc_write62 AS (
    SELECT
        w.CLNT_NO,
        CAST(w.CHG_TMSTMP AS DATE) AS write_dt,
        w.PREF_ID,
        CAST(
          CASE
            WHEN w.PREF_ID = 1012 THEN 'PREF_1012'
            WHEN w.PREF_ID = 1046 THEN 'PREF_1046'
            ELSE 'OTHER_PREF'
          END AS VARCHAR(10)
        ) AS pref_class
    FROM DDWV01.CPC_RB_PREF w
    INNER JOIN vt_sf_clients62 d
        ON d.CLNT_NO = w.CLNT_NO
    WHERE w.CLNT_CONSENT_TYP = 5002
      AND w.APP_SYS_CD = 7020
      AND w.CHG_TMSTMP >= DATE '2025-04-30'   -- PARAMETER BLOCK: CPC_WIN_START
      AND w.CHG_TMSTMP <  DATE '2026-06-27'   -- PARAMETER BLOCK: CPC_WIN_END_EXCL
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_write62 COLUMN (CLNT_NO);


-- ===== STEP C: VOLATILE — matched (+/-1 day) and classified per SF unsub row =====
-- DROP TABLE vt_classified62;  -- also see 00_reset_volatiles.sql
-- LEFT JOIN so an SF unsub with no matching CPC write still gets a row (NO_CPC_WRITE), not
-- dropped. Grain unchanged from Step A: CLNT_NO x unsub_dt.

CREATE VOLATILE TABLE vt_classified62 AS (
    WITH matched AS (
        SELECT
            s.CLNT_NO,
            s.unsub_dt,
            s.mne,
            s.lob,
            MAX(CASE WHEN c.pref_class = 'PREF_1012'  THEN 1 ELSE 0 END) AS has_1012,
            MAX(CASE WHEN c.pref_class = 'PREF_1046'  THEN 1 ELSE 0 END) AS has_1046,
            MAX(CASE WHEN c.pref_class = 'OTHER_PREF' THEN 1 ELSE 0 END) AS has_other_pref
        FROM vt_sf_unsub62 s
        LEFT JOIN vt_cpc_write62 c
            ON  c.CLNT_NO  = s.CLNT_NO
            AND c.write_dt BETWEEN s.unsub_dt - 1 AND s.unsub_dt + 1
        GROUP BY s.CLNT_NO, s.unsub_dt, s.mne, s.lob
    )
    SELECT
        CLNT_NO,
        unsub_dt,
        mne,
        lob,
        TRIM(EXTRACT(YEAR FROM unsub_dt)) || '-' ||
          TRIM(CASE WHEN EXTRACT(MONTH FROM unsub_dt) < 10 THEN '0' ELSE '' END) ||
          TRIM(EXTRACT(MONTH FROM unsub_dt))                                     AS cohort_yyyymm,
        CAST(
          CASE
            WHEN has_1012 = 1 AND has_1046 = 1 THEN '1012_AND_1046'
            WHEN has_1012 = 1 AND has_1046 = 0 THEN '1012_ONLY'
            WHEN has_1012 = 0 AND has_1046 = 1 THEN '1046_ONLY'
            WHEN has_other_pref = 1            THEN 'OTHER_PREF_ONLY'
            ELSE 'NO_CPC_WRITE'
          END AS VARCHAR(20)
        ) AS write_class
    FROM matched
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_classified62 COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_classified62 COLUMN (lob);


-- ===== BLOCK 1: lob x write_class, one row per combo + TOTAL per lob =====
-- QUESTION: across the three LOBs, what fraction of SF unsubs get a matching CPC write, and
--   which preference(s)?
-- ROWS: <=15 combos (3 lob x 5 write_class) + 3 TOTAL rows (one per lob)
-- GOOD LOOKS LIKE: NO_CPC_WRITE dominates every lob (the known gap). Among LOYALTY rows with a
--   write: if 1012_ONLY is large relative to 1046_ONLY, RBC-wide choices do not write 1046
--   (reading A). If 1046_ONLY is large and 1012_ONLY small, reading B holds.
-- WHAT TO DO WITH IT: paste to Claude

WITH combo AS (
    SELECT
        CAST(lob AS VARCHAR(10))         AS lob,
        CAST(write_class AS VARCHAR(20)) AS write_class,
        CAST(COUNT(*) AS BIGINT)         AS clients
    FROM vt_classified62
    GROUP BY 1, 2
),
lob_total AS (
    SELECT
        CAST(lob AS VARCHAR(10))      AS lob,
        CAST('TOTAL' AS VARCHAR(20))  AS write_class,
        CAST(COUNT(*) AS BIGINT)      AS clients
    FROM vt_classified62
    GROUP BY 1
)
SELECT lob, write_class, clients FROM combo
UNION ALL
SELECT lob, write_class, clients FROM lob_total
ORDER BY 1, 2;


-- ===== BLOCK 2: LOYALTY only, monthly write_class mix =====
-- QUESTION: for Loyalty/Avion unsubs specifically, does the write_class mix move over time or
--   is it stable across the whole window?
-- ROWS: <=14 (months in the parameter window)
-- GOOD LOOKS LIKE: n_no_write is the largest column every month, consistent with Block 1;
--   n_1012_only vs n_1046_only tells reading A vs B, and should not swing wildly month to month.
-- WHAT TO DO WITH IT: paste to Claude

SELECT
    cohort_yyyymm,
    CAST(SUM(CASE WHEN write_class = '1012_AND_1046'  THEN 1 ELSE 0 END) AS BIGINT) AS n_1012_and_1046,
    CAST(SUM(CASE WHEN write_class = '1012_ONLY'       THEN 1 ELSE 0 END) AS BIGINT) AS n_1012_only,
    CAST(SUM(CASE WHEN write_class = '1046_ONLY'       THEN 1 ELSE 0 END) AS BIGINT) AS n_1046_only,
    CAST(SUM(CASE WHEN write_class = 'OTHER_PREF_ONLY' THEN 1 ELSE 0 END) AS BIGINT) AS n_other_only,
    CAST(SUM(CASE WHEN write_class = 'NO_CPC_WRITE'    THEN 1 ELSE 0 END) AS BIGINT) AS n_no_write
FROM vt_classified62
WHERE lob = 'LOYALTY'
GROUP BY 1
ORDER BY 1;


-- ===== BLOCK 3: other-program preference ids seen among matched writes (top 10) =====
-- QUESTION: for SF unsubs with ANY 7020 write within a day, which non-1012/1046 preference ids
--   appear? Tells us which other pages/programs the email ESP writes to on this same page visit.
-- ROWS: <=10
-- GOOD LOOKS LIKE: a small, interpretable set of PREF_IDs (e.g. other e-newsletter or product
--   preferences) - not a long tail, which would suggest 7020 writes broadly rather than
--   page-specific gates.
-- WHAT TO DO WITH IT: paste to Claude

WITH other_matches AS (
    SELECT DISTINCT s.CLNT_NO, c.PREF_ID
    FROM vt_sf_unsub62 s
    INNER JOIN vt_cpc_write62 c
        ON  c.CLNT_NO   = s.CLNT_NO
        AND c.write_dt  BETWEEN s.unsub_dt - 1 AND s.unsub_dt + 1
        AND c.pref_class = 'OTHER_PREF'
)
SELECT TOP 10
    PREF_ID,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS clients
FROM other_matches
GROUP BY PREF_ID
ORDER BY clients DESC;
