-- 30: Unsub -> CPC client TRAIL. Raw per-client evidence of the missing bridge (and the rare real one).
-- ENGINE: Teradata-direct. Two-part addressing, no catalog prefix.
-- Generalises unsub_tracking/spotlight/diag_unsub_to_cpc_trace.sql (which is hardcoded to 7 clients).
-- Rerun: DROP TABLE block at EOF (4 tables), then rerun from top.
--
-- WHAT THIS ANSWERS
--   Follow one client at a time: got the email (disposition 1) -> unsubscribed (disposition 4)
--   -> and then what did CPC_RB_PREF_LOG record? For ~99.7% of clients: nothing. That IS the finding.
--   For the rare few with a CPC write, show the whole trail so the crossing can be inspected.
--
-- THE DISTINCTION THAT MATTERS (Q1 splits on it):
--   NO_CPC_ROW_AT_ALL  = client never appears in the CPC log in the window. Absence of data.
--   CPC_ROWS_BUT_NONE_AFTER = client demonstrably writes to CPC, just not after the unsub. Absence of effect.
--   Only the second is evidence of no bridge. The first is a coverage question.
--
-- OUTPUT
--   Q1  ~4 rows   scale + the three-way split above. Read this before anything else.
--   Q2  ~200 rows THE PICTURE. One interleaved timeline per client (email events and CPC writes in one
--                 column, time-ordered), 15 POSITIVE + 15 NEGATIVE clients side by side.
--   Q3  ~60 rows  flat table: each unsub event -> the next CPC write after it, or NULL.
--   Q4  ~80 rows  raw CPC_RB_PREF_LOG rows for the POSITIVE clients only, unmodified, full history in window.

-- editable window / parameters -------------------------------------------------
--   unsub window     : 2026-01-01 to 2026-07-01
--   CPC look-forward : 90 days after first unsub
--   CPC look-back    : 180 days before first unsub (context in the timeline)
--   sample size      : 15 positives + 15 negatives
-- Change the literals in each block below; they are marked "editable".
---------------------------------------------------------------------------------


------------------------------------------------------------------------------
-- VT 1: unsub events in window, CLNT_NO resolved, with the send that preceded them
------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_tr_unsub AS (
    WITH ev AS (
        SELECT
            e.consumer_id_hashed,
            e.TREATMENT_ID,
            e.disposition_dt_tm AS unsub_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2026-01-01'   -- editable: unsub window start
          AND e.disposition_dt_tm <  DATE '2026-07-01'   -- editable: unsub window end
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    )
    SELECT
        m.CLNT_NO,
        v.consumer_id_hashed,
        v.TREATMENT_ID,
        SUBSTR(v.TREATMENT_ID, 8, 3) AS mne,
        v.unsub_tm,
        s.send_tm
    FROM ev v
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = v.consumer_id_hashed
        AND m.TREATMENT_ID       = v.TREATMENT_ID
        AND m.load_tm >= ADD_MONTHS(DATE '2026-01-01', -1)
        AND m.load_tm <  ADD_MONTHS(DATE '2026-07-01',  1)
    LEFT JOIN (
        SELECT consumer_id_hashed, TREATMENT_ID, MIN(disposition_dt_tm) AS send_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 1
          AND disposition_dt_tm >= ADD_MONTHS(DATE '2026-01-01', -2)
          AND disposition_dt_tm <  DATE '2026-07-01'
        GROUP BY 1, 2
    ) s
        ON  s.consumer_id_hashed = v.consumer_id_hashed
        AND s.TREATMENT_ID       = v.TREATMENT_ID
    GROUP BY 1, 2, 3, 4, 5, 6
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tr_unsub COLUMN (CLNT_NO);


------------------------------------------------------------------------------
-- VT 2: one row per client — first unsub anchors every lag calculation below
------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_tr_client AS (
    SELECT
        CLNT_NO,
        MIN(unsub_tm)              AS first_unsub_tm,
        COUNT(*)                   AS n_unsub_events,
        COUNT(DISTINCT mne)        AS n_mne
    FROM vt_tr_unsub
    GROUP BY CLNT_NO
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tr_client COLUMN (CLNT_NO);


------------------------------------------------------------------------------
-- VT 3: CPC log rows for those clients only (bounded scan — join drives the filter)
------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_tr_cpc AS (
    SELECT
        c.CLNT_NO,
        c.PREF_ID,
        c.CLNT_CONSENT_TYP,
        c.APP_SYS_CD,
        c.CHG_TMSTMP,
        t.first_unsub_tm,
        CAST(c.CHG_TMSTMP AS DATE) - CAST(t.first_unsub_tm AS DATE) AS lag_days
    FROM DDWV01.CPC_RB_PREF_LOG c
    INNER JOIN vt_tr_client t
        ON t.CLNT_NO = c.CLNT_NO
    WHERE c.CHG_TMSTMP >= DATE '2024-01-01'   -- hard floor
      AND CAST(c.CHG_TMSTMP AS DATE) >= CAST(t.first_unsub_tm AS DATE) - 180   -- editable: look-back
      AND CAST(c.CHG_TMSTMP AS DATE) <= CAST(t.first_unsub_tm AS DATE) + 180   -- editable: look-forward ctx
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tr_cpc COLUMN (CLNT_NO);


------------------------------------------------------------------------------
-- Q1: SCALE — the three-way split. Absence of data vs absence of effect.
------------------------------------------------------------------------------
WITH flags AS (
    SELECT
        t.CLNT_NO,
        MAX(CASE WHEN c.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)                       AS has_any_cpc_row,
        MAX(CASE WHEN c.lag_days >= 0 AND c.lag_days <= 90 THEN 1 ELSE 0 END)        AS has_post_write_90d
    FROM vt_tr_client t
    LEFT JOIN vt_tr_cpc c ON c.CLNT_NO = t.CLNT_NO
    GROUP BY t.CLNT_NO
)
SELECT
    CASE WHEN has_post_write_90d = 1        THEN CAST('1. POSITIVE - CPC write within 90d of unsub' AS VARCHAR(50))
         WHEN has_any_cpc_row    = 1        THEN CAST('2. CPC rows exist, none after the unsub'      AS VARCHAR(50))
         ELSE                                    CAST('3. No CPC row at all in window'               AS VARCHAR(50))
    END AS cohort,
    COUNT(*) AS clients
FROM flags
GROUP BY 1
ORDER BY 1;


------------------------------------------------------------------------------
-- VT 4: sample — 15 positives + 15 negatives, labelled
------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_tr_sample AS (
    WITH flags AS (
        SELECT
            t.CLNT_NO,
            t.first_unsub_tm,
            MAX(CASE WHEN c.lag_days >= 0 AND c.lag_days <= 90 THEN 1 ELSE 0 END) AS has_post_write_90d,
            MAX(CASE WHEN c.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)                AS has_any_cpc_row
        FROM vt_tr_client t
        LEFT JOIN vt_tr_cpc c ON c.CLNT_NO = t.CLNT_NO
        GROUP BY t.CLNT_NO, t.first_unsub_tm
    ),
    ranked AS (
        SELECT
            CLNT_NO,
            first_unsub_tm,
            CASE WHEN has_post_write_90d = 1 THEN CAST('POSITIVE' AS VARCHAR(10))
                 ELSE CAST('NEGATIVE' AS VARCHAR(10)) END AS cohort,
            has_any_cpc_row,
            ROW_NUMBER() OVER (PARTITION BY has_post_write_90d ORDER BY CLNT_NO) AS rn
        FROM flags
        -- NEGATIVE side restricted to clients that DO write to CPC otherwise,
        -- so a null trail proves "no effect", not "no coverage". Drop this line to sample all negatives.
        WHERE has_post_write_90d = 1 OR has_any_cpc_row = 1
    )
    SELECT CLNT_NO, first_unsub_tm, cohort, has_any_cpc_row
    FROM ranked
    WHERE rn <= 15   -- editable: sample size per cohort
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_tr_sample COLUMN (CLNT_NO);


------------------------------------------------------------------------------
-- Q2: THE PICTURE — one interleaved timeline per client.
--     Email events and CPC writes in a single time-ordered column.
--     Read one CLNT_NO block top to bottom: SENT -> UNSUB -> (CPC rows, or nothing).
------------------------------------------------------------------------------
SELECT * FROM (
    -- vendor: the send
    SELECT
        s.CLNT_NO,
        s.cohort,
        CAST('VENDOR' AS VARCHAR(8))                                      AS src,
        u.send_tm                                                         AS event_tm,
        CAST(CAST(u.send_tm AS DATE) - CAST(s.first_unsub_tm AS DATE) AS INTEGER) AS days_from_unsub,
        CAST('SENT  mne=' || u.mne AS VARCHAR(60))                        AS what,
        CAST(u.TREATMENT_ID AS VARCHAR(50))                               AS ref
    FROM vt_tr_sample s
    INNER JOIN vt_tr_unsub u ON u.CLNT_NO = s.CLNT_NO
    WHERE u.send_tm IS NOT NULL

    UNION ALL

    -- vendor: the unsub
    SELECT
        s.CLNT_NO,
        s.cohort,
        CAST('VENDOR' AS VARCHAR(8)),
        u.unsub_tm,
        CAST(CAST(u.unsub_tm AS DATE) - CAST(s.first_unsub_tm AS DATE) AS INTEGER),
        CAST('UNSUB mne=' || u.mne AS VARCHAR(60)),
        CAST(u.TREATMENT_ID AS VARCHAR(50))
    FROM vt_tr_sample s
    INNER JOIN vt_tr_unsub u ON u.CLNT_NO = s.CLNT_NO

    UNION ALL

    -- cpc: every preference write in the -180d/+180d context window
    SELECT
        s.CLNT_NO,
        s.cohort,
        CAST('CPC' AS VARCHAR(8)),
        c.CHG_TMSTMP,
        CAST(c.lag_days AS INTEGER),
        CAST('PREF ' || TRIM(CAST(c.PREF_ID AS VARCHAR(10)))
             || ' -> ' || TRIM(CAST(c.CLNT_CONSENT_TYP AS VARCHAR(10)))
             || ' by ' || TRIM(CAST(c.APP_SYS_CD AS VARCHAR(10))) AS VARCHAR(60)),
        CAST('' AS VARCHAR(50))
    FROM vt_tr_sample s
    INNER JOIN vt_tr_cpc c ON c.CLNT_NO = s.CLNT_NO
) tl
ORDER BY cohort, CLNT_NO, event_tm;
-- DECODE: CLNT_CONSENT_TYP 5001=Yes 5002=No 5003=blank/never-answered.
--         APP_SYS_CD 7020=Exact Target/SFMC (the ESP - the only automated crossing found so far)
--                    7001=branch  7004=online banking  7016=RBC.COM  7006=internal batch  99999=SRF batch
--         PREF_ID 1002=RBC entity consent (the email gate)  1012=Banking E-Mail  1014=Share for Marketing


------------------------------------------------------------------------------
-- Q3: FLAT — each unsub event -> the NEXT CPC write after it, or NULL.
--     NULL in the PREF_ID column is the no-bridge evidence, one row per unsub.
------------------------------------------------------------------------------
SELECT
    s.cohort,
    u.CLNT_NO,
    u.mne,
    u.TREATMENT_ID,
    u.unsub_tm,
    c.PREF_ID,
    c.CLNT_CONSENT_TYP,
    c.APP_SYS_CD,
    c.CHG_TMSTMP,
    CAST(c.CHG_TMSTMP AS DATE) - CAST(u.unsub_tm AS DATE) AS lag_days
FROM vt_tr_sample s
INNER JOIN vt_tr_unsub u ON u.CLNT_NO = s.CLNT_NO
LEFT JOIN vt_tr_cpc c
    ON  c.CLNT_NO    = u.CLNT_NO
    AND c.CHG_TMSTMP > u.unsub_tm
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.CLNT_NO, u.TREATMENT_ID, u.unsub_tm
                           ORDER BY c.CHG_TMSTMP ASC) = 1
ORDER BY s.cohort, u.CLNT_NO, u.unsub_tm;


------------------------------------------------------------------------------
-- Q4: RAW CPC ROWS for the POSITIVE clients — straight from CPC_RB_PREF_LOG, unmodified.
------------------------------------------------------------------------------
SELECT c.*
FROM DDWV01.CPC_RB_PREF_LOG c
INNER JOIN vt_tr_sample s ON s.CLNT_NO = c.CLNT_NO
WHERE s.cohort = 'POSITIVE'
  AND c.CHG_TMSTMP >= DATE '2024-01-01'
ORDER BY c.CLNT_NO, c.CHG_TMSTMP;


------------------------------------------------------------------------------
DROP TABLE vt_tr_sample;
DROP TABLE vt_tr_cpc;
DROP TABLE vt_tr_client;
DROP TABLE vt_tr_unsub;
