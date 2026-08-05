-- diag_unsub_to_cpc_trace.sql — for the 7 receipt clients (same-day
-- multi-campaign unsubbers from evidence_repeat_unsub), what did CPC
-- record after each vendor unsub event: next write, which gate
-- (PREF_ID), which state (CLNT_CONSENT_TYP), which system (APP_SYS_CD),
-- how long after. Teradata-direct.
-- APP_SYS_CD decode: 7020=Exact Target(SFMC/ESP) 7001=branch
-- 7004=online banking 7016=RBC.COM 7006=internal batch 99999=SRF batch.
-- Prior baseline: only 0.33% of vendor unsubs get ANY CPC write in 90d,
-- so expect mostly NULLs — any 7020 row is the finding.

-- [1] context: full CPC change log for the 7 clients since Oct 2025
SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP, APP_SYS_CD, CHG_TMSTMP
FROM DDWV01.CPC_RB_PREF_LOG
WHERE CLNT_NO IN (142780923,144004330,230520538,270607922,
                  281925669,355852781,383716735)
  AND CHG_TMSTMP >= TIMESTAMP '2025-10-01 00:00:00'
ORDER BY CLNT_NO, CHG_TMSTMP;

-- [2] each unsub event -> the NEXT CPC write after it (any gate, any state)
WITH ids AS (
    SELECT DISTINCT consumer_id_hashed, CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_MASTER
    WHERE CLNT_NO IN (142780923,144004330,230520538,270607922,
                      281925669,355852781,383716735)
      AND load_tm >= DATE '2025-10-01'
),
u AS (
    SELECT i.CLNT_NO,
           SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
           e.disposition_dt_tm AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN ids i ON i.consumer_id_hashed = e.consumer_id_hashed
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-01-01'
      AND e.disposition_dt_tm <  DATE '2026-05-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    GROUP BY 1, 2, 3
)
SELECT u.CLNT_NO, u.mne, u.unsub_tm,
       c.PREF_ID, c.CLNT_CONSENT_TYP, c.APP_SYS_CD, c.CHG_TMSTMP,
       CAST(c.CHG_TMSTMP AS DATE) - CAST(u.unsub_tm AS DATE) AS lag_days
FROM u
LEFT JOIN DDWV01.CPC_RB_PREF_LOG c
  ON  c.CLNT_NO = u.CLNT_NO
  AND c.CHG_TMSTMP > u.unsub_tm
  AND c.CHG_TMSTMP >= TIMESTAMP '2026-01-01 00:00:00'
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.CLNT_NO, u.mne, u.unsub_tm
                           ORDER BY c.CHG_TMSTMP ASC) = 1
ORDER BY u.CLNT_NO, u.unsub_tm;
