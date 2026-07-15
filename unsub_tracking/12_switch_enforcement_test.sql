-- =============================================================================
-- Switch enforcement test — which switch ACTUALLY stops email? (16 rows)
-- =============================================================================
-- The cube shows writes; this shows EFFECTS. For clients whose switch was
-- already set BEFORE the window, did email still reach them inside it?
--   State: latest CPC row per (client, switch) with CHG_TMSTMP < window start
--          (pre-treatment: mid-window flips can't contaminate the read).
--   Outcome: received any email (disposition_cd = 1) inside the window.
-- Read: a switch enforced against email -> its "=1" rows show received_email
-- collapsing to ~0 vs the all-zeros baseline row. 1007 (direct mail) is the
-- NEGATIVE CONTROL — its No must NOT reduce email receipt; if it does, the
-- test is misreading something structural.
-- Settles: 1014 dictionary ("sharing only") vs team lore ("out of all
-- marketing") — and certifies 1012/1002 as email blockers.
-- Window: Q2 2026 (edit both date pairs together).
-- ENGINE: Teradata-direct.
-- =============================================================================

WITH state_asof AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                           ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014, 1007)
      AND CHG_TMSTMP < DATE '2026-04-01'
),
flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1002,
        MAX(CASE WHEN PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1012,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1014,
        MAX(CASE WHEN PREF_ID = 1007 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1007_dm_control
    FROM state_asof
    WHERE rn = 1
    GROUP BY 1
),
emailed AS (
    SELECT DISTINCT m.CLNT_NO
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
)
SELECT
    f.out_1002,
    f.out_1012,
    f.out_1014,
    f.out_1007_dm_control,
    CAST(COUNT(*) AS BIGINT)  AS clients,
    CAST(SUM(CASE WHEN em.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS received_email_in_window
FROM flags f
LEFT JOIN emailed em
    ON em.CLNT_NO = f.CLNT_NO
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;

-- Note: population = clients with at least one CPC row on 1002/1012/1014/1007
-- before the window (states are only observable for them). The all-zeros row
-- is the baseline receive rate; compare each =1 group against it, not against
-- the full bank.
