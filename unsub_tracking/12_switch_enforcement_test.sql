-- =============================================================================
-- Switch enforcement test — which switches ACTUALLY gate the EMAIL channel?
-- =============================================================================
-- The object of the analysis is the CHANNEL: is email alive or dead for a
-- client, and what kills it. The cube shows writes; this shows EFFECTS.
-- E1 makes NO assumption about which switches matter: it scans ALL of them.
--   State: latest CPC row per (client, switch) with CHG_TMSTMP < window start
--          (pre-treatment: mid-window flips can't contaminate the read).
--   Outcome: received any email (disposition_cd = 1) inside the window.
-- Read E1: divide received/clients per PREF_ID downstream and compare to the
-- BASELINE row (PREF_ID = -1, all clients with any pre-window CPC state).
-- A switch that gates email shows a collapsed rate; sharing/topic switches
-- ride at baseline. Whatever collapses IS the email-gating set — discovered,
-- not assumed. Settles 1014 dictionary-vs-lore and certifies 1012/1002.
-- E2 (cross-tab) runs AFTER E1, on the shortlist, for interactions.
-- Window: Q2 2026 (edit all date pairs together).
-- ENGINE: Teradata-direct.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- E1: all-switch scan — email receipt among pre-window No-holders, per switch
-- (~50 rows + 1 baseline row)
-- ---------------------------------------------------------------------------

WITH state_asof AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID
                           ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE CHG_TMSTMP < DATE '2026-04-01'
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
),
no_holders AS (
    SELECT PREF_ID, CLNT_NO
    FROM state_asof
    WHERE rn = 1
      AND CLNT_CONSENT_TYP = 5002
),
per_switch AS (
    SELECT
        n.PREF_ID,
        CAST(COUNT(*) AS BIGINT) AS clients_no_before_window,
        CAST(SUM(CASE WHEN em.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
                                 AS received_email_in_window
    FROM no_holders n
    LEFT JOIN emailed em ON em.CLNT_NO = n.CLNT_NO
    GROUP BY 1
),
baseline AS (
    SELECT
        -1 AS PREF_ID,
        CAST(COUNT(*) AS BIGINT) AS clients_no_before_window,   -- here: ALL state-holders
        CAST(SUM(CASE WHEN em.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
                                 AS received_email_in_window
    FROM (SELECT DISTINCT CLNT_NO FROM state_asof) s
    LEFT JOIN emailed em ON em.CLNT_NO = s.CLNT_NO
)
SELECT * FROM per_switch
UNION ALL
SELECT * FROM baseline
ORDER BY PREF_ID;


-- ---------------------------------------------------------------------------
-- E2: interaction cross-tab on the shortlist (16 rows) — run AFTER E1.
-- Adjust the IN-list to whatever switches E1 shows as email-gating; 1007
-- stays as the direct-mail negative control.
-- ---------------------------------------------------------------------------

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
--
-- E2 RESULT (2026-07-15 run): NOTHING collapses to zero — baseline 57.6%
-- received; 1012-only 48.7%; 1014-only 37.5%; 1002-only 25.8%; DM control
-- 34.7% (control FAILED -> selection confounds marginals). Either enforcement
-- leaks or disposition_cd=1 includes service/transactional sends. E3 decides.


-- ---------------------------------------------------------------------------
-- E3: what KIND of email reaches 1002-opted-out clients? (small cross-tab)
-- ---------------------------------------------------------------------------
-- Profiles Q2 sends by contact_purps_typ x cntct_evnt_initiator (MASTER cols,
-- never profiled), split by whether the recipient was 1002=No BEFORE the
-- window. If the opted-out group's sends are overwhelmingly service-purpose,
-- enforcement is intact and E1/E2 must be re-run on MARKETING sends only.
-- If marketing-purpose sends reach entity-opted-out clients at volume, that
-- is a serious finding — verify before communicating.
-- ---------------------------------------------------------------------------

WITH state_asof AS (
    SELECT
        CLNT_NO,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO
                           ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID = 1002
      AND CHG_TMSTMP < DATE '2026-04-01'
),
optout_1002 AS (
    SELECT CLNT_NO
    FROM state_asof
    WHERE rn = 1
      AND CLNT_CONSENT_TYP = 5002
),
sends AS (
    SELECT
        m.CLNT_NO,
        m.contact_purps_typ,
        m.cntct_evnt_initiator
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
)
SELECT
    CASE WHEN o.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS is_1002_optout,
    s.contact_purps_typ,
    s.cntct_evnt_initiator,
    CAST(COUNT(*) AS BIGINT)   AS send_rows,
    COUNT(DISTINCT s.CLNT_NO)  AS distinct_clients
FROM sends s
LEFT JOIN optout_1002 o
    ON o.CLNT_NO = s.CLNT_NO
GROUP BY 1, 2, 3
ORDER BY is_1002_optout DESC, send_rows DESC;
