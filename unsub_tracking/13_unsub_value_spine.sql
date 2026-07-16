-- =============================================================================
-- Unsub value spine — one row per client x FIRST unsub, with recorded attribution
-- =============================================================================
-- S1 [IN-ENV EXTRACT, ~650K rows]: the base for outcome #1 (value of an unsub).
-- Channel dies at the FIRST unsub; the triggering deployment is RECORDED
-- (treatment_id on the code-4 row), not inferred.
-- Enrichment (Spark/UCP side): merge on (CLNT_NO, MONTH_END_DATE of the month
-- BEFORE first_unsub_tm) -> TIBC product-category counts + age; then segment
-- matrix -> per-segment LTV lookup -> LTV given up by trigger_mne.
-- S2 [screenshot, <=20 rows]: first-unsubs per tracked MNE — the Cards read.
-- ENGINE: Teradata-direct.
-- =============================================================================

-- S1: the spine (extract — keep in-env for Spark enrichment)
WITH first_unsub AS (
    SELECT
        m.CLNT_NO,
        e.consumer_id_hashed,
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
    CLNT_NO,
    consumer_id_hashed,
    TREATMENT_ID               AS trigger_treatment_id,
    SUBSTR(TREATMENT_ID, 8, 3) AS trigger_mne,
    disposition_dt_tm          AS first_unsub_tm,
    EXTRACT(YEAR FROM disposition_dt_tm) * 100
      + EXTRACT(MONTH FROM disposition_dt_tm) AS unsub_month_yyyymm
FROM first_unsub
WHERE rn = 1;


-- S2: first-unsubs by tracked MNE (screenshot-sized — the Cards headline)
-- Exact IN-list per scope doc; extraction stays all-MNE in S1, this is the cut.
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
    SUBSTR(TREATMENT_ID, 8, 3)  AS trigger_mne,
    CAST(COUNT(*) AS BIGINT)    AS clients_first_unsub
FROM first_unsub
WHERE rn = 1
  AND SUBSTR(TREATMENT_ID, 8, 3) IN
      ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
       'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
GROUP BY 1
ORDER BY clients_first_unsub DESC;
