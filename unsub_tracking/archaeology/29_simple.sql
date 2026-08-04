-- 29_simple.sql — SIMPLE version: July-2025 unsubs (PCQ/PCL/PCD/CRV only) — recontacted in the next 12mo?
-- Date: 2026-08-03. ENGINE: Teradata-direct. Answers: one 4-row table, one row per MNE, counts only.
-- CAVEAT 1: consumer_id_hashed tracked directly from EVENT, NO join to VENDOR_FEEDBACK_MASTER anywhere — it's ~1:1 with client per T4, so 'clients' below means distinct hashed consumers.
-- CAVEAT 2: only shape-valid (programmed) TREATMENT_IDs count as sends — DEFAULT/junk excluded, since the campaign mnemonic in positions 8-10 is only meaningful on a real tactic id.
-- Day 0 (the unsub's own day) is excluded from the follow-up window: a same-day send is usually the email that triggered the unsub. Pre-clean DROPs: 'does not exist' is harmless on a fresh session.
DROP TABLE vt_after;
DROP TABLE vt_anchor;

-- STEP 1: one row per (consumer, unsub_mne), July 2025, shape-valid TREATMENT_ID, MNE in scope — plain GROUP BY, no ROW_NUMBER
CREATE VOLATILE TABLE vt_anchor AS (
    SELECT
        consumer_id_hashed,
        SUBSTR(TRIM(TREATMENT_ID), 8, 3)      AS unsub_mne,
        CAST(MIN(disposition_dt_tm) AS DATE)  AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2025-07-01'
      AND disposition_dt_tm <  DATE '2025-08-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
      AND SUBSTR(TRIM(TREATMENT_ID), 8, 3) IN ('PCQ','PCL','PCD','CRV')
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_anchor COLUMN (consumer_id_hashed);

-- STEP 2: shape-valid sends to those consumers, day-0 excluded, within 12mo of each consumer's own anchor date
CREATE VOLATILE TABLE vt_after AS (
    SELECT DISTINCT
        a.consumer_id_hashed,
        a.unsub_mne,
        a.unsub_dt,
        e.TREATMENT_ID,
        CAST(e.disposition_dt_tm AS DATE)   AS send_dt,
        SUBSTR(TRIM(e.TREATMENT_ID), 8, 3)  AS send_mne
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN vt_anchor a ON a.consumer_id_hashed = e.consumer_id_hashed
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2025-07-02'   -- optimizer hint: earliest possible anchor+1day
      AND e.disposition_dt_tm <  DATE '2026-08-01'   -- optimizer hint: latest possible anchor+12mo
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(TRIM(e.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
      AND CAST(e.disposition_dt_tm AS DATE) >= a.unsub_dt + 1              -- day-0 exclusion
      AND CAST(e.disposition_dt_tm AS DATE) <  ADD_MONTHS(a.unsub_dt, 12)
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_after COLUMN (consumer_id_hashed);

-- STEP 3: final 4-row table — counts only, no percentages
WITH jul_sends AS (
    -- distinct (consumer,TREATMENT_ID,day) sends of each in-scope MNE during July 2025 itself
    SELECT mne, COUNT(*) AS emails_sent_jul25
    FROM (
        SELECT DISTINCT
            consumer_id_hashed, TREATMENT_ID, CAST(disposition_dt_tm AS DATE) AS dt,
            SUBSTR(TRIM(TREATMENT_ID), 8, 3) AS mne
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 1
          AND disposition_dt_tm >= DATE '2025-07-01'
          AND disposition_dt_tm <  DATE '2025-08-01'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TRIM(TREATMENT_ID), 8, 3) IN ('PCQ','PCL','PCD','CRV')
    ) x
    GROUP BY 1
),
cohort AS (
    SELECT unsub_mne, COUNT(DISTINCT consumer_id_hashed) AS clients_unsub_jul25
    FROM vt_anchor
    GROUP BY 1
),
recontact AS (
    SELECT
        unsub_mne,
        COUNT(DISTINCT CASE WHEN send_dt < ADD_MONTHS(unsub_dt, 3) THEN consumer_id_hashed END) AS recontacted_3m,
        COUNT(DISTINCT CASE WHEN send_dt < ADD_MONTHS(unsub_dt, 6) THEN consumer_id_hashed END) AS recontacted_6m,
        COUNT(DISTINCT consumer_id_hashed)                                                       AS recontacted_12m,
        COUNT(DISTINCT CASE WHEN send_mne = unsub_mne  THEN consumer_id_hashed END)              AS same_campaign_12m,
        COUNT(DISTINCT CASE WHEN send_mne <> unsub_mne THEN consumer_id_hashed END)              AS other_campaign_12m
    FROM vt_after
    GROUP BY 1
)
SELECT
    c.unsub_mne,
    COALESCE(j.emails_sent_jul25, 0)  AS emails_sent_jul25,
    c.clients_unsub_jul25,
    COALESCE(r.recontacted_3m, 0)     AS recontacted_3m,
    COALESCE(r.recontacted_6m, 0)     AS recontacted_6m,
    COALESCE(r.recontacted_12m, 0)    AS recontacted_12m,
    COALESCE(r.same_campaign_12m, 0)  AS same_campaign_12m,
    COALESCE(r.other_campaign_12m, 0) AS other_campaign_12m
FROM cohort c
LEFT JOIN jul_sends j ON j.mne = c.unsub_mne
LEFT JOIN recontact r ON r.unsub_mne = c.unsub_mne
ORDER BY c.unsub_mne;

DROP TABLE vt_after;
DROP TABLE vt_anchor;
