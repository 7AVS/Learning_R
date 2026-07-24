-- =============================================================================
-- EM-decisioned -> vendor feedback coverage reconciliation
-- =============================================================================
-- Question: of clients DECISIONED to email, how many actually appear in the
-- vendor feedback chain — and where do the losses happen?
-- SCOPE: Cards personal MNEs ONLY — CRV, PCL, PCQ, PCD, AUH (Andre 2026-07-16).
-- One row per (MNE, cohort month of TREATMT_STRT_DT) since 2025-01-01, ~95 rows.
--
--   clients_decisioned_em  = distinct clients decisioned to email that month
--                            (two-field rule, tactic table)
--   clients_in_master      = of those, with a MASTER row for that exact
--                            (tactic_id, clnt_no) — handed to the vendor at some
--                            point. MASTER has NO date column, so this is
--                            client x tactic EVER, not per-wave.
--   clients_sent..clients_complaint = of those, distinct clients with that
--                            disposition on this TACTIC_ID (1=sent 2=opened
--                            3=clicked 4=unsub 5=hardbounce 6=complaint).
--                            No time window: TACTIC_ID is unique per deployment
--                            (MNE + julian date), the key alone pins the wave.
--
-- How to read the gaps:
--   decisioned - in_master = never reached the vendor: suppression, channel
--                            reassignment, or a different ESP.
--   in_master - sent       = mastered but never sent: suppressed at send time
--                            or sent-event logging gap.
--   clients_unsub here     = ANY unsub on that deployment (per-deployment funnel),
--                            NOT the first-ever-per-client dedup used in 16.
-- A normally-covered MNE with one month at 0 sent = wave-level gap -> investigate.
--
-- Counts only — divide in Excel. ENGINE: Teradata-direct.
-- =============================================================================

WITH em_decis AS (
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        t.TREATMT_STRT_DT,
        t.TREATMT_END_DT,
        SUBSTR(t.TACTIC_ID, 8, 3) AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT) AS cohort_yyyymm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2025-01-01'
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
),
in_master AS (
    SELECT DISTINCT m.TREATMENT_ID, m.CLNT_NO, m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
),
vf_events AS (
    -- ALL disposition codes; date floor is scan pruning only
    SELECT DISTINCT e.TREATMENT_ID, e.consumer_id_hashed, e.disposition_cd
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_dt_tm >= DATE '2025-01-01'
      AND SUBSTR(e.TREATMENT_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
),
flags AS (
    -- one row per decisioned deployment row, with master + per-disposition flags.
    -- Exact keys only: TACTIC_ID is unique per deployment, so no time
    -- conditions are needed to pin the wave.
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        MAX(CASE WHEN im.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END)  AS f_in_master,
        MAX(CASE WHEN ev.disposition_cd = 1 THEN 1 ELSE 0 END)   AS f_sent,
        MAX(CASE WHEN ev.disposition_cd = 2 THEN 1 ELSE 0 END)   AS f_opened,
        MAX(CASE WHEN ev.disposition_cd = 3 THEN 1 ELSE 0 END)   AS f_clicked,
        MAX(CASE WHEN ev.disposition_cd = 4 THEN 1 ELSE 0 END)   AS f_unsub,
        MAX(CASE WHEN ev.disposition_cd = 5 THEN 1 ELSE 0 END)   AS f_hardbounce,
        MAX(CASE WHEN ev.disposition_cd = 6 THEN 1 ELSE 0 END)   AS f_complaint
    FROM em_decis d
    LEFT JOIN in_master im
        ON  im.TREATMENT_ID = d.TACTIC_ID
        AND im.CLNT_NO      = d.CLNT_NO
    LEFT JOIN vf_events ev
        ON  ev.TREATMENT_ID       = d.TACTIC_ID
        AND ev.consumer_id_hashed = im.consumer_id_hashed
    GROUP BY 1, 2, 3
)
SELECT
    mne,
    cohort_yyyymm,
    CAST(COUNT(*) AS BIGINT)          AS clients_decisioned_em,
    CAST(SUM(f_in_master) AS BIGINT)  AS clients_in_master,
    CAST(SUM(f_sent) AS BIGINT)       AS clients_sent,
    CAST(SUM(f_opened) AS BIGINT)     AS clients_opened,
    CAST(SUM(f_clicked) AS BIGINT)    AS clients_clicked,
    CAST(SUM(f_unsub) AS BIGINT)      AS clients_unsub,
    CAST(SUM(f_hardbounce) AS BIGINT) AS clients_hardbounce,
    CAST(SUM(f_complaint) AS BIGINT)  AS clients_complaint
FROM flags
GROUP BY 1, 2
ORDER BY 1, 2;
