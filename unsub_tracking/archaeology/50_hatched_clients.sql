/* ===========================================================================
   50_hatched_clients.sql - EVIDENCE FILE: clients who unsubscribed in
   Salesforce and for whom NOTHING reached CPC afterwards.
   (Split out of 45_audit_queries.sql 2026-08-26; 46-49 are local notes.)

   Population: active + CPC 1012 = 5001 (open) at Jul-26, with a Salesforce
   unsubscribe (disposition 4) between Sep-24 and Jul-26.  That is the
   473,863 gray-bar set.
   EXCLUDED (not flagged - removed): any client with a CPC write, by ANY
   writer, on ANY of the 11 gates the unsubscribe page can set
   (1004 1006 1010 1012 1023 1024 1025 1026 1044 1045 1046) dated AFTER the
   client's first Salesforce unsubscribe in the window.  A write after the
   click means the pipeline worked for that client - they are not evidence.
   What is left = unsubscribed, and CPC never moved. Row count = the number.
   Gate columns show the current consent value on each gate (CPC_RB_PREF,
   current-state) for reference only; by construction none of them changed
   after the unsubscribe. Teradata-direct. No row cap.
   =========================================================================== */
WITH u_b AS (
    SELECT DISTINCT CLNT_NO FROM DDWV01.RB_CLNT_DLY
    WHERE SNAP_DT = DATE '2026-07-31' AND CLNT_STS = 'A'
),
b AS (
    SELECT CLNT_NO
    FROM DDWV01.CPC_RB_PREF_MTHLY
    WHERE PREF_ID = 1012 AND MTH_END_DT = DATE '2026-07-31' AND CLNT_TYP_CD = 1
      AND CLNT_CONSENT_TYP = 5001
),
-- every Salesforce unsub click in the window, with the hash / treatment it rode on
sf AS (
    SELECT m.CLNT_NO, m.consumer_id_hashed, m.TREATMENT_ID, e.disposition_dt_tm
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
-- one row per client: FIRST unsub in the window (the earliest point CPC should have reacted),
-- plus the hash / treatment of that first click
first_unsub AS (
    SELECT CLNT_NO, consumer_id_hashed, TREATMENT_ID, disposition_dt_tm AS first_unsub_dt_tm
    FROM (
        SELECT sf.*, ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY disposition_dt_tm) AS rn
        FROM sf
    ) x
    WHERE rn = 1
),
gray_bar AS (
    SELECT f.CLNT_NO, f.consumer_id_hashed, f.TREATMENT_ID, f.first_unsub_dt_tm
    FROM first_unsub f
    INNER JOIN b   ON b.CLNT_NO   = f.CLNT_NO
    INNER JOIN u_b ON u_b.CLNT_NO = f.CLNT_NO
),
-- current position on the 11 page-writable gates (reference columns)
gates AS (
    SELECT p.CLNT_NO,
           MAX(CASE WHEN p.PREF_ID = 1004 THEN p.CLNT_CONSENT_TYP END) AS cons_1004,
           MAX(CASE WHEN p.PREF_ID = 1006 THEN p.CLNT_CONSENT_TYP END) AS cons_1006,
           MAX(CASE WHEN p.PREF_ID = 1010 THEN p.CLNT_CONSENT_TYP END) AS cons_1010,
           MAX(CASE WHEN p.PREF_ID = 1012 THEN p.CLNT_CONSENT_TYP END) AS cons_1012,
           MAX(CASE WHEN p.PREF_ID = 1023 THEN p.CLNT_CONSENT_TYP END) AS cons_1023,
           MAX(CASE WHEN p.PREF_ID = 1024 THEN p.CLNT_CONSENT_TYP END) AS cons_1024,
           MAX(CASE WHEN p.PREF_ID = 1025 THEN p.CLNT_CONSENT_TYP END) AS cons_1025,
           MAX(CASE WHEN p.PREF_ID = 1026 THEN p.CLNT_CONSENT_TYP END) AS cons_1026,
           MAX(CASE WHEN p.PREF_ID = 1044 THEN p.CLNT_CONSENT_TYP END) AS cons_1044,
           MAX(CASE WHEN p.PREF_ID = 1045 THEN p.CLNT_CONSENT_TYP END) AS cons_1045,
           MAX(CASE WHEN p.PREF_ID = 1046 THEN p.CLNT_CONSENT_TYP END) AS cons_1046,
           MAX(p.CHG_TMSTMP)                                           AS last_cpc_write_any_gate
    FROM DDWV01.CPC_RB_PREF p
    INNER JOIN gray_bar g ON g.CLNT_NO = p.CLNT_NO
    WHERE p.PREF_ID IN (1004,1006,1010,1012,1023,1024,1025,1026,1044,1045,1046)
    GROUP BY p.CLNT_NO
)
SELECT g.CLNT_NO,
       g.consumer_id_hashed,
       g.TREATMENT_ID                  AS first_unsub_treatment_id,
       SUBSTR(g.TREATMENT_ID, 8, 3)    AS first_unsub_mne,
       g.first_unsub_dt_tm,
       t.cons_1004, t.cons_1006, t.cons_1010, t.cons_1012, t.cons_1023, t.cons_1024,
       t.cons_1025, t.cons_1026, t.cons_1044, t.cons_1045, t.cons_1046,
       t.last_cpc_write_any_gate        -- always < first_unsub_dt_tm (or NULL) by construction
FROM gray_bar g
LEFT JOIN gates t ON t.CLNT_NO = g.CLNT_NO
-- REMOVE anyone CPC touched on any of the 11 gates after their first unsub click
WHERE NOT EXISTS (
    SELECT 1 FROM DDWV01.CPC_RB_PREF w
    WHERE w.CLNT_NO = g.CLNT_NO
      AND w.PREF_ID IN (1004,1006,1010,1012,1023,1024,1025,1026,1044,1045,1046)
      AND w.CHG_TMSTMP > g.first_unsub_dt_tm
)
ORDER BY g.first_unsub_dt_tm;
