-- =============================================================================
-- Population lost trend — month x MNE, ALL campaigns, with emailed base
-- =============================================================================
-- One row per (MNE, month) since 2024-01-01, long format — pivot in Excel.
--   em_clients_sent     = distinct consumers with a SENT event (disposition_cd=1)
--                         for that MNE that month (SENT, not decisioned).
--   clients_first_unsub = clients whose FIRST-EVER unsub (disposition_cd=4,
--                         deduped 1/client) was on a send of that MNE that month.
--   tracked_mne         = Y for the 17 in-scope Cards/Payments/Loans MNEs.
-- Counts only; grain caveat: sends by consumer_id_hashed, unsubs by CLNT_NO.
-- ENGINE: Teradata-direct.
-- =============================================================================

WITH sends AS (
    SELECT
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
        EXTRACT(YEAR FROM e.disposition_dt_tm) * 100
          + EXTRACT(MONTH FROM e.disposition_dt_tm) AS yyyymm,
        COUNT(DISTINCT e.consumer_id_hashed) AS em_clients_sent
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1, 2
),
first_unsub AS (
    SELECT
        m.CLNT_NO,
        e.TREATMENT_ID,
        e.disposition_dt_tm,
        ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                           ORDER BY e.disposition_dt_tm ASC) AS rn
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
unsubs AS (
    SELECT
        SUBSTR(TREATMENT_ID, 8, 3) AS mne,
        EXTRACT(YEAR FROM disposition_dt_tm) * 100
          + EXTRACT(MONTH FROM disposition_dt_tm) AS yyyymm,
        CAST(COUNT(*) AS BIGINT) AS clients_first_unsub
    FROM first_unsub
    WHERE rn = 1
    GROUP BY 1, 2
)
SELECT
    COALESCE(s.mne, u.mne)          AS mne_out,
    COALESCE(s.yyyymm, u.yyyymm)    AS yyyymm_out,
    CASE WHEN COALESCE(s.mne, u.mne) IN
         ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
          'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
         THEN 'Y' ELSE 'N' END      AS tracked_mne,
    CAST(COALESCE(s.em_clients_sent, 0) AS BIGINT)     AS em_clients_sent,
    CAST(COALESCE(u.clients_first_unsub, 0) AS BIGINT) AS clients_first_unsub
FROM sends s
FULL OUTER JOIN unsubs u
    ON  s.mne    = u.mne
    AND s.yyyymm = u.yyyymm
ORDER BY 1, 2;
