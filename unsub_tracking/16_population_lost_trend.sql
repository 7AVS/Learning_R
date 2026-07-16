-- =============================================================================
-- Population lost trend — month x MNE, ALL campaigns, with emailed base
-- =============================================================================
-- S0 prints what the output means (keep it with the data).
-- S1 = one row per (MNE, month), long format — pivot in Excel as needed.
--      Row count = #MNEs x ~31 months; this is an extract, not a screenshot.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- S0: readme — what S1 is and its constraints
SELECT 1 AS ord, CAST('QUESTION' AS VARCHAR(20)) AS item,
       CAST('Per campaign per month: how many clients did we email, and how many clients did we lose (first-ever unsub) to that campaign?' AS VARCHAR(220)) AS detail
UNION ALL SELECT 2, 'GRAIN',       'One row per (MNE, month), ALL MNEs in the vendor feedback data since 2024-01-01. MNE = SUBSTR(TREATMENT_ID,8,3).'
UNION ALL SELECT 3, 'EM_CLIENTS',  'em_clients_sent = distinct consumers with a SENT event (disposition_cd=1) for that MNE that month. SENT, not decisioned - tactic-side EM channel codes not locked yet.'
UNION ALL SELECT 4, 'UNSUBS',      'clients_first_unsub = clients whose FIRST-EVER unsub (disposition_cd=4, deduped 1/client) was on a send of that MNE that month. Recorded attribution, not inferred.'
UNION ALL SELECT 5, 'TRACKED',     'tracked_mne = Y for: Cards PCQ PCL PCD AUH CLI MVP CRV CTU O2P | Payments VDT VUI VUT VDA VAW VCN | Pers.Loans RCU RCL.'
UNION ALL SELECT 6, 'CAVEAT',      'Grain differs by column: sends counted by consumer_id_hashed (no join); unsubs counted by CLNT_NO (via MASTER join). Counts only - compute rates downstream.'
ORDER BY ord;


-- S1: month x MNE — emailed base + first-unsubs
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
    COALESCE(s.mne, u.mne)          AS mne,
    COALESCE(s.yyyymm, u.yyyymm)    AS yyyymm,
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
ORDER BY mne, yyyymm;
