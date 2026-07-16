-- =============================================================================
-- Email Journey by MNE x Cohort — decisioned denominator + disposition funnel
-- =============================================================================
-- Denominator: clients DECISIONED to email (tactic side, confirmed two-field
-- rule, env-validated 2026-07-15 — 184-MNE production scope, no MNE list needed):
--   SUBSTR(tactic_decisn_vrb_info,121,30) LIKE '%EM%'        (Priority 1, 55 MNEs)
--   OR UPPER(COALESCE(addnl_decisn_data1,'')) LIKE '%EM%'    (Priority 2, 129 MNEs)
-- Cohort: MNE x month of TREATMT_STRT_DT. Multiple deployments within the month
-- collapse to one cohort; a client counts ONCE per MNE x cohort month.
-- Funnel: of decisioned clients, distinct clients with disposition 1..6
-- (1=sent 2=opened 3=clicked 4=unsub 5=hardbounce 6=complaint).
--
-- ATTACHMENT RULE (final, 2026-07-16): NO time window. TACTIC_ID is unique per
-- deployment (MNE + julian date) and a client never duplicates on one TACTIC_ID
-- (Andre) — so the exact key (TACTIC_ID, CLNT_NO) -> (TREATMENT_ID, hash) IS the
-- deployment; every disposition attaches to its wave by key alone.
--
-- Denominator counts DECISIONED (targeted), not sent — suppressed clients stay
-- in it by design; clients_sent / clients_decisioned is the deliverability read.
-- NOT covered here: the ~68 MNEs present in vendor feedback with zero
-- TACTIC_EVNT_IP_AR_HIST rows (different decisioning system) — for those use
-- 02_campaign_unsub_tracker (EVENT-only view).
--
-- ENGINE: Teradata-direct. Counts only; divide downstream.
-- Window: TREATMT_STRT_DT >= DATE '2024-01-01' (edit here, one place per CTE).
-- =============================================================================

WITH email_decis AS (
    SELECT
        t.CLNT_NO,
        t.TACTIC_ID,
        t.TREATMT_STRT_DT,
        SUBSTR(t.TACTIC_ID, 8, 3)  AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT) AS cohort_yyyymm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
),
denom AS (
    SELECT
        mne,
        cohort_yyyymm,
        CAST(COUNT(*) AS BIGINT)     AS decision_rows,
        COUNT(DISTINCT CLNT_NO)      AS clients_decisioned_email
    FROM email_decis
    GROUP BY 1, 2
),
client_journeys AS (
    -- one row per MNE x cohort x client, stage flags over dispositions of that
    -- client's deployments in the cohort month. Exact-key joins only —
    -- TACTIC_ID pins the wave (MAX collapses multi-deployment months).
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        MAX(CASE WHEN e.disposition_cd = 1 THEN 1 ELSE 0 END) AS f_sent,
        MAX(CASE WHEN e.disposition_cd = 2 THEN 1 ELSE 0 END) AS f_opened,
        MAX(CASE WHEN e.disposition_cd = 3 THEN 1 ELSE 0 END) AS f_clicked,
        MAX(CASE WHEN e.disposition_cd = 4 THEN 1 ELSE 0 END) AS f_unsub,
        MAX(CASE WHEN e.disposition_cd = 5 THEN 1 ELSE 0 END) AS f_hardbounce,
        MAX(CASE WHEN e.disposition_cd = 6 THEN 1 ELSE 0 END) AS f_complaint
    FROM email_decis d
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.TREATMENT_ID = d.TACTIC_ID
        AND m.CLNT_NO      = d.CLNT_NO
    INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
        ON  e.consumer_id_hashed = m.consumer_id_hashed
        AND e.TREATMENT_ID       = m.TREATMENT_ID
    GROUP BY 1, 2, 3
),
funnel AS (
    SELECT
        mne,
        cohort_yyyymm,
        COUNT(*)          AS clients_with_feedback,
        SUM(f_sent)       AS clients_sent,
        SUM(f_opened)     AS clients_opened,
        SUM(f_clicked)    AS clients_clicked,
        SUM(f_unsub)      AS clients_unsub,
        SUM(f_hardbounce) AS clients_hardbounce,
        SUM(f_complaint)  AS clients_complaint
    FROM client_journeys
    GROUP BY 1, 2
)
SELECT
    d.mne,
    d.cohort_yyyymm,
    d.decision_rows,
    d.clients_decisioned_email,
    COALESCE(f.clients_with_feedback, 0) AS clients_with_feedback,
    COALESCE(f.clients_sent, 0)          AS clients_sent,
    COALESCE(f.clients_opened, 0)        AS clients_opened,
    COALESCE(f.clients_clicked, 0)       AS clients_clicked,
    COALESCE(f.clients_unsub, 0)         AS clients_unsub,
    COALESCE(f.clients_hardbounce, 0)    AS clients_hardbounce,
    COALESCE(f.clients_complaint, 0)     AS clients_complaint
FROM denom d
LEFT JOIN funnel f
    ON  f.mne           = d.mne
    AND f.cohort_yyyymm = d.cohort_yyyymm
ORDER BY d.mne, d.cohort_yyyymm;
