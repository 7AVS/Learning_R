-- =============================================================================
-- Population lost trend — THE plain read for Power Pack outcome #2
-- =============================================================================
-- ONE question: how many clients do we lose email access to each month,
-- and how many of those losses are booked to our tracked campaigns?
-- One row per month (~31 rows). Read it straight off the screen:
--   clients_lost           = distinct clients whose FIRST-ever unsub was that month
--   lost_to_tracked_mnes   = of those, triggered by a Cards/Payments/Loans send
-- Expect clients_lost ~35K/mo; total since 2024-01 should sum to ~649,885.
-- ENGINE: Teradata-direct.
-- =============================================================================

WITH first_unsub AS (
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
)
SELECT
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm)   AS unsub_month_yyyymm,
    CAST(COUNT(*) AS BIGINT)                    AS clients_lost,
    CAST(SUM(CASE WHEN SUBSTR(TREATMENT_ID, 8, 3) IN
             ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
              'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
             THEN 1 ELSE 0 END) AS BIGINT)      AS lost_to_tracked_mnes
FROM first_unsub
WHERE rn = 1
GROUP BY 1
ORDER BY 1;
