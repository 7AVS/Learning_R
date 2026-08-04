-- 29_simple_jan26.sql — SIMPLE version: January-2026 unsubs (PCQ/PCL/PCD/CRV only) — recontacted through the data edge (2026-07-31)?
-- Date: 2026-08-03. ENGINE: Teradata-direct. Answers: one 4-row table, one row per MNE, counts only.
-- PURPOSE: tests the CURRENT process vintage (vs the Jul-2025 cohort in 29_simple.sql) — the comparison column
-- across the two runs is clients_recontact_any_6m.
-- Follow-up window is capped at the 2026-07-31 data edge, NOT a full 12 months: 12mo isn't closed for this
-- cohort yet — a Jan-1 unsub gets at most ~7mo of follow-up, a Jan-31 unsub as little as ~6mo.
-- CAVEAT 1: consumer_id_hashed tracked directly from EVENT, NO join to VENDOR_FEEDBACK_MASTER anywhere — it's ~1:1 with client per T4, so 'clients' below means distinct hashed consumers.
-- CAVEAT 2: only shape-valid (programmed) TREATMENT_IDs count as sends — DEFAULT/junk excluded, since the campaign mnemonic in positions 8-10 is only meaningful on a real tactic id.
-- Day 0 (the unsub's own day) is excluded from the follow-up window: a same-day send is usually the email that triggered the unsub. Pre-clean DROPs: 'does not exist' is harmless on a fresh session.
DROP TABLE vt_after;
DROP TABLE vt_anchor;

-- STEP 1: one row per (consumer, unsub_mne), January 2026, shape-valid TREATMENT_ID, MNE in scope — plain GROUP BY, no ROW_NUMBER
CREATE VOLATILE TABLE vt_anchor AS (
    SELECT
        consumer_id_hashed,
        SUBSTR(TRIM(TREATMENT_ID), 8, 3)      AS unsub_mne,
        CAST(MIN(disposition_dt_tm) AS DATE)  AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT
    WHERE disposition_cd = 4
      AND disposition_dt_tm >= DATE '2026-01-01'
      AND disposition_dt_tm <  DATE '2026-02-01'
      AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
      AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
      AND SUBSTR(TRIM(TREATMENT_ID), 8, 3) IN ('PCQ','PCL','PCD','CRV')
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_anchor COLUMN (consumer_id_hashed);

-- STEP 2: shape-valid sends to those consumers, day-0 excluded, capped at the 2026-07-31 data edge — NOT 12mo (12mo isn't closed for this cohort)
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
      AND e.disposition_dt_tm >= DATE '2026-01-02'   -- optimizer hint: earliest possible anchor+1day
      AND e.disposition_dt_tm <  DATE '2026-08-01'   -- optimizer hint: data edge
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(TRIM(e.TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
      AND CAST(e.disposition_dt_tm AS DATE) >= a.unsub_dt + 1              -- day-0 exclusion
      AND CAST(e.disposition_dt_tm AS DATE) <  DATE '2026-08-01'           -- capped at data edge, NOT 12mo
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;
COLLECT STATISTICS ON vt_after COLUMN (consumer_id_hashed);

-- STEP 3: final 4-row table — counts only, no percentages
-- clients_* = distinct clients; emails_* = email count at (client, tactic, day) grain; any = same + other
-- campaigns, deduped; _3m/_6m are cumulative from each client's own unsub date; _todate = through the
-- 2026-07-31 data edge (NOT a closed 12mo window for this cohort — see header).
WITH jan_sends AS (
    -- distinct (consumer,TREATMENT_ID,day) sends of each in-scope MNE in January 2026 itself, to ALL clients
    -- (cohort context, not restricted to the unsub cohort)
    SELECT mne, COUNT(*) AS jan26_sends_all_clients
    FROM (
        SELECT DISTINCT
            consumer_id_hashed, TREATMENT_ID, CAST(disposition_dt_tm AS DATE) AS dt,
            SUBSTR(TRIM(TREATMENT_ID), 8, 3) AS mne
        FROM DTZV01.VENDOR_FEEDBACK_EVENT
        WHERE disposition_cd = 1
          AND disposition_dt_tm >= DATE '2026-01-01'
          AND disposition_dt_tm <  DATE '2026-02-01'
          AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
          AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
          AND SUBSTR(TRIM(TREATMENT_ID), 8, 3) IN ('PCQ','PCL','PCD','CRV')
    ) x
    GROUP BY 1
),
cohort AS (
    SELECT unsub_mne, COUNT(DISTINCT consumer_id_hashed) AS clients_unsubscribed_jan26
    FROM vt_anchor
    GROUP BY 1
),
recontact AS (
    SELECT
        unsub_mne,
        COUNT(DISTINCT CASE WHEN send_dt < ADD_MONTHS(unsub_dt, 3) THEN consumer_id_hashed END) AS clients_recontact_any_3m,
        -- 6m closed for the whole cohort only barely: a Jan-31 unsub's 6m window ends Jul-31, right at the
        -- data edge — assumes the feed is complete through 2026-07-31 (see header)
        COUNT(DISTINCT CASE WHEN send_dt < ADD_MONTHS(unsub_dt, 6) THEN consumer_id_hashed END) AS clients_recontact_any_6m,
        COUNT(DISTINCT consumer_id_hashed)                                                       AS clients_recontact_any_todate,
        COUNT(DISTINCT CASE WHEN send_mne = unsub_mne  THEN consumer_id_hashed END)              AS clients_recontact_same_todate,
        COUNT(DISTINCT CASE WHEN send_mne <> unsub_mne THEN consumer_id_hashed END)              AS clients_recontact_other_todate,
        COUNT(CASE WHEN send_mne = unsub_mne  THEN 1 END)                                        AS emails_recd_same_camp_todate,
        COUNT(CASE WHEN send_mne <> unsub_mne THEN 1 END)                                        AS emails_recd_other_camp_todate
    FROM vt_after
    GROUP BY 1
)
SELECT
    c.unsub_mne                                  AS unsub_campaign,
    COALESCE(j.jan26_sends_all_clients, 0)        AS jan26_sends_all_clients,
    c.clients_unsubscribed_jan26,
    COALESCE(r.clients_recontact_any_3m, 0)       AS clients_recontact_any_3m,
    COALESCE(r.clients_recontact_any_6m, 0)       AS clients_recontact_any_6m,
    COALESCE(r.clients_recontact_any_todate, 0)   AS clients_recontact_any_todate,
    COALESCE(r.clients_recontact_same_todate, 0)  AS clients_recontact_same_todate,
    COALESCE(r.clients_recontact_other_todate, 0) AS clients_recontact_other_todate,
    COALESCE(r.emails_recd_same_camp_todate, 0)   AS emails_recd_same_camp_todate,
    COALESCE(r.emails_recd_other_camp_todate, 0)  AS emails_recd_other_camp_todate
FROM cohort c
LEFT JOIN jan_sends j ON j.mne = c.unsub_mne
LEFT JOIN recontact r ON r.unsub_mne = c.unsub_mne
ORDER BY c.unsub_mne;

DROP TABLE vt_after;
DROP TABLE vt_anchor;
