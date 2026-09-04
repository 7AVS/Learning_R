-- GRAIN NOTE (2026-09-04): built on pack 60 v1 logic (one row per client-MONTH). Pack 60 v2 moved to one row per
--   client in the window. This probe is about the MECHANISM behind the 7020 CPC-only rows, so the grain does not
--   change its answer; its counts will not match pack 60 v2's totals exactly.
-- 64: Why does 7020 (SFMC unsub page) write 100,847 CPC-only revocations? (Teradata-direct)
-- PUZZLE: a client submitting the SFMC unsubscribe page should also land a Salesforce
-- disposition_cd=4 event same-day - pack 60 found 100,847 CPC-only 1012 writes from
-- APP_SYS_CD=7020 with no such SF match in [-1,+14] days; either the client key does not
-- line up to VENDOR_FEEDBACK_MASTER/EVENT, or SF genuinely has no record.
-- Steps A-D rebuild pack 60's Steps A/B/C1/C3 verbatim under 64-suffixed names (see
-- 60_monthly_sf_cpc_union.sql for full provenance/citations), then isolate 7020 CPC-only
-- rows and probe them against EVENT/MASTER with NO act-join or shape-guard restriction
-- (deliberately looser than Step A) to see if those restrictions, not a real SF gap, are
-- the cause. No DROP statements (add these volatile names to 00_reset_volatiles.sql).
-- RUN ORDER: Steps A->D (build) -> Block 1 -> Block 2 -> Block 3 -> Block 4.
-- =============================================================================


-- ===== STEP A: VOLATILE — SF unsubs, first disposition_cd=4 per (CLNT_NO, cohort_yyyymm) =====
-- Verbatim copy of pack 60 Step A (60_monthly_sf_cpc_union.sql lines 41-89).

CREATE VOLATILE TABLE vt_sf_unsub64 AS (
    WITH act AS (
        SELECT SNAP_DT, CLNT_NO
        FROM DDWV01.RB_CLNT_DLY
        WHERE CLNT_STS = 'A'
          AND SNAP_DT >= DATE '2024-08-01'
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
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024122' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        INNER JOIN act ON act.CLNT_NO = m.CLNT_NO
                      AND act.SNAP_DT = ADD_MONTHS(CAST(e.disposition_dt_tm AS DATE)
                                                   - EXTRACT(DAY FROM e.disposition_dt_tm) + 1, 1) - 1
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-08-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
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

COLLECT STATISTICS ON vt_sf_unsub64 COLUMN (CLNT_NO);


-- ===== STEP B: VOLATILE — CPC 1012 writes to No (5002), first per (CLNT_NO, cohort_yyyymm) =====
-- Verbatim copy of pack 60 Step B (60_monthly_sf_cpc_union.sql lines 101-138).

CREATE VOLATILE TABLE vt_cpc_write64 AS (
    WITH act AS (
        SELECT SNAP_DT, CLNT_NO
        FROM DDWV01.RB_CLNT_DLY
        WHERE CLNT_STS = 'A'
          AND SNAP_DT >= DATE '2024-08-01'
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
          AND w.CHG_TMSTMP >= DATE '2024-08-01'
          AND w.CHG_TMSTMP <  DATE '2026-08-01'
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

COLLECT STATISTICS ON vt_cpc_write64 COLUMN (CLNT_NO);


-- ===== STEP C1: VOLATILE — matched pairs, Q5's [-1,+14]-day CLNT_NO rule =====
-- Verbatim copy of pack 60 Step C1 (60_monthly_sf_cpc_union.sql lines 151-160).

CREATE VOLATILE TABLE vt_pairs64 AS (
    SELECT DISTINCT s.CLNT_NO, s.evt_month AS sf_month, s.unsub_dt,
           w.wr_month AS cpc_month, w.write_dt
    FROM vt_sf_unsub64 s
    INNER JOIN vt_cpc_write64 w
        ON  w.CLNT_NO   = s.CLNT_NO
        AND w.write_dt BETWEEN s.unsub_dt - 1 AND s.unsub_dt + 14
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_pairs64 COLUMN (CLNT_NO);


-- ===== STEP C2: VOLATILE — CPC write rows flagged matched/unmatched to an SF unsub =====
-- Verbatim copy of pack 60 Step C3 (60_monthly_sf_cpc_union.sql lines 188-198).

CREATE VOLATILE TABLE vt_cpc_flagged64 AS (
    SELECT w.CLNT_NO, w.wr_month, w.APP_SYS_CD, w.write_dt,
           CASE WHEN p.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS matched_sf
    FROM vt_cpc_write64 w
    LEFT JOIN (SELECT DISTINCT CLNT_NO, cpc_month, write_dt FROM vt_pairs64) p
        ON  p.CLNT_NO   = w.CLNT_NO
        AND p.cpc_month = w.wr_month
        AND p.write_dt  = w.write_dt
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_flagged64 COLUMN (CLNT_NO);


-- ===== STEP D1: VOLATILE — the 7020 CPC-only rows (CLNT_NO, write_dt) =====
-- This is the exact puzzle population: a 1012-to-No write from the SFMC email
-- unsubscribe page (APP_SYS_CD=7020) with no SF disposition_cd=4 match in [-1,+14] days.

CREATE VOLATILE TABLE vt_cpc_only_7020_64 AS (
    SELECT CLNT_NO, write_dt
    FROM vt_cpc_flagged64
    WHERE matched_sf = 0
      AND APP_SYS_CD = 7020
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_only_7020_64 COLUMN (CLNT_NO);


-- ===== STEP D2: VOLATILE — distinct clients in the 7020 CPC-only population =====

CREATE VOLATILE TABLE vt_cpc_only_clients64 AS (
    SELECT DISTINCT CLNT_NO
    FROM vt_cpc_only_7020_64
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_only_clients64 COLUMN (CLNT_NO);


-- ===== STEP D3: VOLATILE — MASTER keys for those clients, ANY treatment_id, full history =====
-- No SUBSTR(TREATMENT_ID,...) window bound here (unlike Step A) - the question is whether the
-- client was EVER in the email vendor system, not just inside the Aug-24..Jul-26 window.

CREATE VOLATILE TABLE vt_master_keys64 AS (
    SELECT DISTINCT m.consumer_id_hashed, m.TREATMENT_ID, m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    INNER JOIN vt_cpc_only_clients64 c ON c.CLNT_NO = m.CLNT_NO
    WHERE m.CLNT_NO IS NOT NULL
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_master_keys64 COLUMN (CLNT_NO);


-- ===== STEP D4: VOLATILE — sent (disposition_cd=1) and unsub (disposition_cd=4) events =====
-- ASSUMPTION: deliberately NO act-join and NO TREATMENT_ID shape guard here (unlike Step A) -
-- the point is to test whether Step A's restrictions, not a genuine SF gap, drop real matches.

CREATE VOLATILE TABLE vt_event_sent64 AS (
    SELECT DISTINCT mk.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS sent_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_master_keys64 mk
        ON  mk.consumer_id_hashed = e.consumer_id_hashed
        AND mk.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_event_sent64 COLUMN (CLNT_NO);

CREATE VOLATILE TABLE vt_event_unsub64 AS (
    SELECT DISTINCT mk.CLNT_NO, CAST(e.disposition_dt_tm AS DATE) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_master_keys64 mk
        ON  mk.consumer_id_hashed = e.consumer_id_hashed
        AND mk.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_event_unsub64 COLUMN (CLNT_NO);


-- ===== STEP D5: VOLATILE — the IN_MASTER subset of the 7020 CPC-only rows =====

CREATE VOLATILE TABLE vt_cpc_only_master64 AS (
    SELECT o.CLNT_NO, o.write_dt
    FROM vt_cpc_only_7020_64 o
    INNER JOIN (SELECT DISTINCT CLNT_NO FROM vt_master_keys64) mk ON mk.CLNT_NO = o.CLNT_NO
) WITH DATA
PRIMARY INDEX (CLNT_NO)
ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_only_master64 COLUMN (CLNT_NO);


-- ===== BLOCK 1: were these clients ever in the email vendor system at all? =====
-- QUESTION: of the 7020 CPC-only clients, how many appear in VENDOR_FEEDBACK_MASTER at all
--   (any TREATMENT_ID, no date bound)?
-- ROWS: 3 (IN_MASTER, NOT_IN_MASTER, TOTAL)
-- GOOD LOOKS LIKE: if NOT_IN_MASTER is large, the CLNT_NO key does not line up to MASTER (or
--   these clients were never emailed through SFMC at all) - a data/key problem, not a timing one.
-- WHAT TO DO WITH IT: paste to Claude

WITH c AS (
    SELECT DISTINCT CLNT_NO FROM vt_cpc_only_clients64
),
m AS (
    SELECT DISTINCT CLNT_NO FROM vt_master_keys64
),
flagged AS (
    SELECT c.CLNT_NO,
           CASE WHEN m.CLNT_NO IS NOT NULL THEN 'IN_MASTER' ELSE 'NOT_IN_MASTER' END AS bucket
    FROM c
    LEFT JOIN m ON m.CLNT_NO = c.CLNT_NO
)
SELECT CAST(bucket AS VARCHAR(20)) AS bucket, CAST(COUNT(*) AS BIGINT) AS clients
FROM flagged
GROUP BY 1
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(20)), CAST(COUNT(*) AS BIGINT)
FROM flagged
ORDER BY 1;


-- ===== BLOCK 2: of IN_MASTER clients, were they sent an email in the 90 days before write_dt? =====
-- QUESTION: of the IN_MASTER rows, how many have a disposition_cd=1 (sent) event in the 90 days
--   before write_dt - i.e. a page visit is even plausible - vs none?
-- ROWS: 3 (SENT_WITHIN_90D_BEFORE, NO_SEND_90D_BEFORE, TOTAL)
-- GOOD LOOKS LIKE: if NO_SEND_90D_BEFORE is large, these clients were not being actively
--   emailed at the time - a page-submission origin story becomes less plausible.
-- WHAT TO DO WITH IT: paste to Claude

WITH flagged AS (
    SELECT o.CLNT_NO, o.write_dt,
           CASE WHEN EXISTS (
                     SELECT 1 FROM vt_event_sent64 s
                     WHERE s.CLNT_NO = o.CLNT_NO
                       AND s.sent_dt >= o.write_dt - 90
                       AND s.sent_dt <  o.write_dt
                ) THEN 'SENT_WITHIN_90D_BEFORE' ELSE 'NO_SEND_90D_BEFORE' END AS bucket
    FROM vt_cpc_only_master64 o
)
SELECT CAST(bucket AS VARCHAR(30)) AS bucket, CAST(COUNT(*) AS BIGINT) AS rows_
FROM flagged
GROUP BY 1
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(30)), CAST(COUNT(*) AS BIGINT)
FROM flagged
ORDER BY 1;


-- ===== BLOCK 3: of IN_MASTER clients, how far is the NEAREST disposition_cd=4 from write_dt? =====
-- QUESTION: with no act-join / shape-guard restriction (unlike Step A), does an SF unsub event
--   exist near write_dt at all, and how far away?
-- ROWS: <=6 (SAME_DAY, 1-14_DAYS_EITHER_SIDE, 15-90_DAYS, 91+_DAYS, NEVER, TOTAL)
-- GOOD LOOKS LIKE: if most are NEVER, SF genuinely has no unsub record for a page submission
--   (real gap). If many are 1-14_DAYS or 15-90_DAYS, Step A's act-join/shape-guard/dedup
--   restrictions - not a real SF gap - are dropping matches that do exist in the raw data.
-- WHAT TO DO WITH IT: paste to Claude
-- ASSUMPTION: "1-14_DAYS_EITHER_SIDE" folds in the +/-1-day edge of pack 60's asymmetric
--   [-1,+14] match window (unsub up to 14d before write, or 1d after) - flag if a strict
--   2-14 split (excluding the +/-1 edge) is wanted instead.

WITH candidates AS (
    SELECT o.CLNT_NO, o.write_dt, u.unsub_dt,
           (u.unsub_dt - o.write_dt) AS day_diff,
           ROW_NUMBER() OVER (PARTITION BY o.CLNT_NO, o.write_dt
                              ORDER BY ABS(u.unsub_dt - o.write_dt) ASC) AS rn
    FROM vt_cpc_only_master64 o
    LEFT JOIN vt_event_unsub64 u ON u.CLNT_NO = o.CLNT_NO
),
nearest AS (
    SELECT CLNT_NO, write_dt, day_diff
    FROM candidates
    WHERE rn = 1
),
bucketed AS (
    SELECT CLNT_NO, write_dt,
           CASE
               WHEN day_diff IS NULL THEN 'NEVER'
               WHEN day_diff = 0 THEN 'SAME_DAY'
               WHEN ABS(day_diff) BETWEEN 1 AND 14 THEN '1-14_DAYS_EITHER_SIDE'
               WHEN ABS(day_diff) BETWEEN 15 AND 90 THEN '15-90_DAYS'
               ELSE '91+_DAYS'
           END AS bucket
    FROM nearest
)
SELECT CAST(bucket AS VARCHAR(30)) AS bucket, CAST(COUNT(*) AS BIGINT) AS rows_
FROM bucketed
GROUP BY 1
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(30)), CAST(COUNT(*) AS BIGINT)
FROM bucketed
ORDER BY 1;


-- ===== BLOCK 4: 7020 CPC-only volume vs 7020 SF_AND_CPC volume, by quarter of write_dt =====
-- QUESTION: is the 7020 mismatch uniform over time, or concentrated in specific quarters (e.g.
--   before the March-2026 7020 collapse mentioned in prior packs)?
-- ROWS: <=10 (9 quarters, Aug-24..Jul-26, + TOTAL)
-- GOOD LOOKS LIKE: a quarter (or quarters) with a much higher cpc_only-to-sf_and_cpc ratio than
--   the rest points at a specific system change, not a steady-state key mismatch.
-- WHAT TO DO WITH IT: paste to Claude

WITH q AS (
    SELECT wr_month,
           SUBSTR(wr_month, 1, 4) || '-Q' ||
             TRIM((CAST(SUBSTR(wr_month, 6, 2) AS INTEGER) - 1) / 3 + 1) AS quarter_label,
           matched_sf
    FROM vt_cpc_flagged64
    WHERE APP_SYS_CD = 7020
),
by_q AS (
    SELECT quarter_label,
           CAST(SUM(CASE WHEN matched_sf = 0 THEN 1 ELSE 0 END) AS BIGINT) AS cpc_only_7020,
           CAST(SUM(CASE WHEN matched_sf = 1 THEN 1 ELSE 0 END) AS BIGINT) AS sf_and_cpc_7020
    FROM q
    GROUP BY 1
)
SELECT CAST(quarter_label AS VARCHAR(10)) AS quarter_label, cpc_only_7020, sf_and_cpc_7020
FROM by_q
UNION ALL
SELECT CAST('TOTAL' AS VARCHAR(10)),
       CAST(SUM(cpc_only_7020) AS BIGINT),
       CAST(SUM(sf_and_cpc_7020) AS BIGINT)
FROM by_q
ORDER BY 1;
