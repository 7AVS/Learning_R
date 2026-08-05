-- profile_master_columns.sql — learn SRVC_PROVDR_NM, LEGAL_ENTITY_CD,
-- SOURCE_EVNT_ID empirically. Teradata-direct. Recent slice only.

-- [1] service provider: who actually sends
SELECT TOP 20 SRVC_PROVDR_NM, COUNT(*) AS n
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE load_tm >= DATE '2026-06-01'
GROUP BY 1 ORDER BY n DESC;

-- [2] legal entity: which RBC entity the send belongs to
SELECT TOP 20 LEGAL_ENTITY_CD, COUNT(*) AS n
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE load_tm >= DATE '2026-06-01'
GROUP BY 1 ORDER BY n DESC;

-- [3] SOURCE_EVNT_ID grain: unique per send, per row, or per treatment?
SELECT COUNT(*) AS rows_,
       COUNT(DISTINCT SOURCE_EVNT_ID) AS distinct_source_evnt,
       COUNT(DISTINCT consumer_id_hashed || '~' || TREATMENT_ID) AS distinct_hash_treatment
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE load_tm >= DATE '2026-07-01';
