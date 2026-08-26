/* ===========================================================================
   50_hatched_clients.sql - the 473,863 "unsubscribed in Salesforce, CPC 1012
   still open" clients: client list with hash id, and their other CPC gates.
   Split out of 45_audit_queries.sql on 2026-08-26. Numbering: 46-49 are local notes/CSV (gitignored), hence 50.
   Anchors: active + 1012=5001 at Jul-26; SF disposition 4 Sep-24..Jul-26.
   Teradata-direct. No row caps.
   =========================================================================== */
/* ---------------------------------------------------------------------------
[Q1] CLIENT LIST - the 473,863 hatched-bar clients (2026-08-26)
      Active + CPC 1012 = 5001 (open) at Jul-26, with a Salesforce unsub
      (disposition 4) in Sep-24..Jul-26. Same CTEs as 45_audit_queries.sql Q3b; no row cap.
      One row per client; hash id = the one on the client's LATEST unsub
      event (a client can carry several hashes across sends).
      Expected row count = seg_email_sf_open (v3) = 473,863.
--------------------------------------------------------------------------- */
WITH u_b AS (
    SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31' AND CLNT_STS = 'A'
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD AS writer_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31' AND CLNT_TYP_CD = 1
      AND CLNT_CONSENT_TYP = 5001
),
v AS (
    -- latest Salesforce unsub event per client, carrying its hash id / treatment id
    SELECT CLNT_NO, consumer_id_hashed, TREATMENT_ID, disposition_dt_tm
    FROM (
        SELECT m.CLNT_NO, m.consumer_id_hashed, m.TREATMENT_ID, e.disposition_dt_tm,
               ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY e.disposition_dt_tm DESC) AS rn
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) x
    WHERE rn = 1
)
SELECT b.CLNT_NO,
       v.consumer_id_hashed,
       v.TREATMENT_ID                      AS latest_unsub_treatment_id,
       SUBSTR(v.TREATMENT_ID, 8, 3)        AS latest_unsub_mne,
       v.disposition_dt_tm                 AS latest_unsub_dt_tm,
       b.cons_b                            AS cpc_1012_jul26,
       b.writer_b                          AS cpc_1012_writer_jul26
FROM b
INNER JOIN u_b ON u_b.CLNT_NO = b.CLNT_NO
INNER JOIN v   ON v.CLNT_NO   = b.CLNT_NO
ORDER BY v.disposition_dt_tm;

/* ---------------------------------------------------------------------------
[Q2] PROBE - which gates did writer 7020 (email backfeed) write at Jul-26 for
      the 473,863 gray-bar clients? PREF_ID x consent value, counts. <= 20 rows.
      Run BEFORE Q3 so its pivot columns are grounded, not guessed.
--------------------------------------------------------------------------- */
WITH u_b AS (
    SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31' AND CLNT_STS = 'A'
),
b AS (
    SELECT CLNT_NO FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31' AND CLNT_TYP_CD = 1
      AND CLNT_CONSENT_TYP = 5001
),
v AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-09-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
),
hatched AS (
    SELECT b.CLNT_NO FROM b
    INNER JOIN u_b ON u_b.CLNT_NO = b.CLNT_NO
    INNER JOIN v   ON v.CLNT_NO   = b.CLNT_NO
)
SELECT p.PREF_ID,
       p.CLNT_CONSENT_TYP                         AS consent_jul26,
       CAST(COUNT(DISTINCT p.CLNT_NO) AS BIGINT)  AS clients
FROM DDWV01.CPC_RB_PREF_MTHLY p
INNER JOIN hatched h ON h.CLNT_NO = p.CLNT_NO
WHERE p.MTH_END_DT = DATE '2026-07-31' AND p.CLNT_TYP_CD = 1
  AND p.APP_SYS_CD = 7020                        -- email backfeed writer ONLY (Andre 2026-08-26)
GROUP BY 1,2
ORDER BY 1,2;

/* ---------------------------------------------------------------------------
[Q3] Q1 + OTHER GATES at Jul-26, pivoted so rows stay 1 per client (473,863)
      1046 gets its own columns (7020 writes 1012 + 1046 - unsub_tracking/
      sfmc_unsub_blueprint_notes.md). Every other gate is summarised as counts
      so nothing is missed without hard-coding the gate list. Check Q2 first.
--------------------------------------------------------------------------- */
WITH u_b AS (
    SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31' AND CLNT_STS = 'A'
),
b AS (
    SELECT CLNT_NO, CLNT_CONSENT_TYP AS cons_b, APP_SYS_CD AS writer_b
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31' AND CLNT_TYP_CD = 1
      AND CLNT_CONSENT_TYP = 5001
),
v AS (
    SELECT CLNT_NO, consumer_id_hashed, TREATMENT_ID, disposition_dt_tm
    FROM (
        SELECT m.CLNT_NO, m.consumer_id_hashed, m.TREATMENT_ID, e.disposition_dt_tm,
               ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY e.disposition_dt_tm DESC) AS rn
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                    FROM DTZV01.VENDOR_FEEDBACK_MASTER
                    WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2024153' AND '2026212'
                      AND CLNT_NO IS NOT NULL) m
          ON  m.consumer_id_hashed = e.consumer_id_hashed
          AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2024-09-01'
          AND e.disposition_dt_tm <  DATE '2026-08-01'
          AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
          AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    ) x
    WHERE rn = 1
),
hatched AS (
    SELECT b.CLNT_NO, b.cons_b, b.writer_b,
           v.consumer_id_hashed, v.TREATMENT_ID, v.disposition_dt_tm
    FROM b
    INNER JOIN u_b ON u_b.CLNT_NO = b.CLNT_NO
    INNER JOIN v   ON v.CLNT_NO   = b.CLNT_NO
),
-- all gates at Jul-26 for these clients, pivoted to one row per client
gates AS (
    SELECT p.CLNT_NO,
           MAX(CASE WHEN p.PREF_ID = 1046 THEN p.CLNT_CONSENT_TYP END)                      AS cons_1046_jul26,
           MAX(CASE WHEN p.PREF_ID = 1046 THEN p.APP_SYS_CD END)                            AS writer_1046_jul26,
           SUM(CASE WHEN p.PREF_ID NOT IN (1012, 1046) AND p.CLNT_CONSENT_TYP = 5002
                     AND p.APP_SYS_CD = 7020 THEN 1 ELSE 0 END)                              AS n_other_gates_closed_by_7020,
           SUM(CASE WHEN p.PREF_ID NOT IN (1012, 1046) AND p.CLNT_CONSENT_TYP = 5002
                    THEN 1 ELSE 0 END)                                                       AS n_other_gates_closed_any_writer,
           SUM(CASE WHEN p.APP_SYS_CD = 7020 THEN 1 ELSE 0 END)                              AS n_gates_written_by_7020,
           COUNT(*)                                                                          AS n_gates_total
    FROM DDWV01.CPC_RB_PREF_MTHLY p
    INNER JOIN hatched h ON h.CLNT_NO = p.CLNT_NO
    WHERE p.MTH_END_DT = DATE '2026-07-31' AND p.CLNT_TYP_CD = 1
    GROUP BY p.CLNT_NO
)
SELECT h.CLNT_NO,
       h.consumer_id_hashed,
       h.TREATMENT_ID                      AS latest_unsub_treatment_id,
       SUBSTR(h.TREATMENT_ID, 8, 3)        AS latest_unsub_mne,
       h.disposition_dt_tm                 AS latest_unsub_dt_tm,
       h.cons_b                            AS cpc_1012_jul26,
       h.writer_b                          AS cpc_1012_writer_jul26,
       g.cons_1046_jul26,
       g.writer_1046_jul26,
       g.n_other_gates_closed_by_7020,
       g.n_other_gates_closed_any_writer,
       g.n_gates_written_by_7020,
       g.n_gates_total
FROM hatched h
LEFT JOIN gates g ON g.CLNT_NO = h.CLNT_NO
ORDER BY h.disposition_dt_tm;
