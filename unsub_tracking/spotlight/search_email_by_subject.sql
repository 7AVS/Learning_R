-- search_email_by_subject.sql — find treatments by email subject line.
-- Replace <SEARCH_TERM> (case-insensitive substring). Teradata-direct.
-- MASTER is card-grained (duplicated rows) -> recipients counted as
-- DISTINCT consumer_id_hashed. load_tm floor = load stamp, keep margin.

SELECT SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
       m.TREATMENT_ID,
       m.email_subj_line,
       COUNT(DISTINCT m.consumer_id_hashed) AS recipients
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
WHERE m.load_tm >= DATE '2025-10-01'
  AND LOWER(m.email_subj_line) LIKE LOWER('%<SEARCH_TERM>%')
GROUP BY 1, 2, 3
ORDER BY recipients DESC;
