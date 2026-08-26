/* ===========================================================================
   50_hatched_clients.sql - "unsubscribed in Salesforce, CPC 1012 still open"
   Split out of 45_audit_queries.sql on 2026-08-26. Numbering: 46-49 are local
   notes/CSV (gitignored), hence 50.

   WHY THE OTHER GATES MATTER (Andre 2026-08-26): the unsubscribe page offers
   two choices - enterprise-wide (1012, always shown) or program-specific (one
   of 1004 1006 1010 1023 1024 1025 1026 1044 1045 1046). If writer 7020 closed
   ANY of those gates, CPC DID capture the unsubscribe - just not on 1012.
   Such clients must be REMOVED from the "not in CPC" gray bar.
   => n_gates_closed_by_7020 must be 0 on every client that stays in the bar.
   [Q1] full list, 473,863 rows.   [Q2] sizing: KEEP vs REMOVE, 2 rows.
   Anchors: active + 1012 = 5001 at Jul-26; SF disposition 4 Sep-24..Jul-26.
   Consent values from CPC_RB_PREF_MTHLY at Jul-26; change dates from
   CPC_RB_PREF (current-state table, CHG_TMSTMP) - a date later than Jul-26
   means the write happened after the anchor. Teradata-direct. No row caps.
   =========================================================================== */

/* ---------------------------------------------------------------------------
[Q1] CLIENT LIST + THE 11 GATES THE UNSUB PAGE CAN WRITE (7020 only), 1 row per
     client. Per gate: cons_<gate>_7020 = Jul-26 value if 7020 wrote it, else
     NULL; dt_<gate>_7020 = CHG_TMSTMP of the 7020 write on that gate, else NULL.
     gray_bar_status: KEEP (not in CPC) / REMOVE (captured on a program gate).
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
),
hatched AS (
    SELECT b.CLNT_NO, b.cons_b, b.writer_b,
           v.consumer_id_hashed, v.TREATMENT_ID, v.disposition_dt_tm
    FROM b
    INNER JOIN u_b ON u_b.CLNT_NO = b.CLNT_NO
    INNER JOIN v   ON v.CLNT_NO   = b.CLNT_NO
),
-- Jul-26 consent value per gate, only where 7020 is the writer
gates AS (
    SELECT p.CLNT_NO,
           MAX(CASE WHEN p.PREF_ID = 1004 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1004_7020,
           MAX(CASE WHEN p.PREF_ID = 1006 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1006_7020,
           MAX(CASE WHEN p.PREF_ID = 1010 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1010_7020,
           MAX(CASE WHEN p.PREF_ID = 1012 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1012_7020,
           MAX(CASE WHEN p.PREF_ID = 1023 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1023_7020,
           MAX(CASE WHEN p.PREF_ID = 1024 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1024_7020,
           MAX(CASE WHEN p.PREF_ID = 1025 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1025_7020,
           MAX(CASE WHEN p.PREF_ID = 1026 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1026_7020,
           MAX(CASE WHEN p.PREF_ID = 1044 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1044_7020,
           MAX(CASE WHEN p.PREF_ID = 1045 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1045_7020,
           MAX(CASE WHEN p.PREF_ID = 1046 AND p.APP_SYS_CD = 7020 THEN p.CLNT_CONSENT_TYP END) AS cons_1046_7020,
           SUM(CASE WHEN p.APP_SYS_CD = 7020 AND p.CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS n_gates_closed_by_7020
    FROM DDWV01.CPC_RB_PREF_MTHLY p
    INNER JOIN hatched h ON h.CLNT_NO = p.CLNT_NO
    WHERE p.MTH_END_DT = DATE '2026-07-31' AND p.CLNT_TYP_CD = 1
      AND p.PREF_ID IN (1004,1006,1010,1012,1023,1024,1025,1026,1044,1045,1046)
    GROUP BY p.CLNT_NO
),
-- when 7020 wrote each gate (current-state table carries the change timestamp)
dts AS (
    SELECT w.CLNT_NO,
           MAX(CASE WHEN w.PREF_ID = 1004 THEN w.CHG_TMSTMP END) AS dt_1004_7020,
           MAX(CASE WHEN w.PREF_ID = 1006 THEN w.CHG_TMSTMP END) AS dt_1006_7020,
           MAX(CASE WHEN w.PREF_ID = 1010 THEN w.CHG_TMSTMP END) AS dt_1010_7020,
           MAX(CASE WHEN w.PREF_ID = 1012 THEN w.CHG_TMSTMP END) AS dt_1012_7020,
           MAX(CASE WHEN w.PREF_ID = 1023 THEN w.CHG_TMSTMP END) AS dt_1023_7020,
           MAX(CASE WHEN w.PREF_ID = 1024 THEN w.CHG_TMSTMP END) AS dt_1024_7020,
           MAX(CASE WHEN w.PREF_ID = 1025 THEN w.CHG_TMSTMP END) AS dt_1025_7020,
           MAX(CASE WHEN w.PREF_ID = 1026 THEN w.CHG_TMSTMP END) AS dt_1026_7020,
           MAX(CASE WHEN w.PREF_ID = 1044 THEN w.CHG_TMSTMP END) AS dt_1044_7020,
           MAX(CASE WHEN w.PREF_ID = 1045 THEN w.CHG_TMSTMP END) AS dt_1045_7020,
           MAX(CASE WHEN w.PREF_ID = 1046 THEN w.CHG_TMSTMP END) AS dt_1046_7020,
           COUNT(*) AS n_gates_written_by_7020_ever
    FROM DDWV01.CPC_RB_PREF w
    INNER JOIN hatched h ON h.CLNT_NO = w.CLNT_NO
    WHERE w.APP_SYS_CD = 7020 AND w.PREF_ID IN (1004,1006,1010,1012,1023,1024,1025,1026,1044,1045,1046)
    GROUP BY w.CLNT_NO
)
SELECT h.CLNT_NO,
       h.consumer_id_hashed,
       h.TREATMENT_ID                      AS latest_unsub_treatment_id,
       SUBSTR(h.TREATMENT_ID, 8, 3)        AS latest_unsub_mne,
       h.disposition_dt_tm                 AS latest_unsub_dt_tm,
       h.cons_b                            AS cpc_1012_jul26,
       h.writer_b                          AS cpc_1012_writer_jul26,
       g.cons_1004_7020, d.dt_1004_7020,
       g.cons_1006_7020, d.dt_1006_7020,
       g.cons_1010_7020, d.dt_1010_7020,
       g.cons_1012_7020, d.dt_1012_7020,
       g.cons_1023_7020, d.dt_1023_7020,
       g.cons_1024_7020, d.dt_1024_7020,
       g.cons_1025_7020, d.dt_1025_7020,
       g.cons_1026_7020, d.dt_1026_7020,
       g.cons_1044_7020, d.dt_1044_7020,
       g.cons_1045_7020, d.dt_1045_7020,
       g.cons_1046_7020, d.dt_1046_7020,
       COALESCE(g.n_gates_closed_by_7020, 0) AS n_gates_closed_by_7020,
       CASE WHEN COALESCE(g.n_gates_closed_by_7020, 0) > 0
            THEN CAST('REMOVE - captured on program gate' AS VARCHAR(40))
            ELSE 'KEEP - not in CPC' END     AS gray_bar_status
FROM hatched h
LEFT JOIN gates g ON g.CLNT_NO = h.CLNT_NO
LEFT JOIN dts   d ON d.CLNT_NO = h.CLNT_NO
ORDER BY h.disposition_dt_tm;

/* ---------------------------------------------------------------------------
[Q2] SIZING - corrected gray bar. 2 rows. KEEP = the real "not in CPC" number.
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
),
hatched AS (
    SELECT b.CLNT_NO FROM b
    INNER JOIN u_b ON u_b.CLNT_NO = b.CLNT_NO
    INNER JOIN v   ON v.CLNT_NO   = b.CLNT_NO
),
closed AS (
    SELECT DISTINCT p.CLNT_NO
    FROM DDWV01.CPC_RB_PREF_MTHLY p
    INNER JOIN hatched h ON h.CLNT_NO = p.CLNT_NO
    WHERE p.MTH_END_DT = DATE '2026-07-31' AND p.CLNT_TYP_CD = 1
      AND p.PREF_ID IN (1004,1006,1010,1012,1023,1024,1025,1026,1044,1045,1046) AND p.APP_SYS_CD = 7020 AND p.CLNT_CONSENT_TYP = 5002
)
SELECT CASE WHEN c.CLNT_NO IS NULL THEN CAST('KEEP - not in CPC (gray bar)' AS VARCHAR(40))
            ELSE 'REMOVE - captured on program gate' END  AS gray_bar_status,
       CAST(COUNT(*) AS BIGINT)                            AS clients
FROM hatched h
LEFT JOIN closed c ON c.CLNT_NO = h.CLNT_NO
GROUP BY 1
ORDER BY 1;
