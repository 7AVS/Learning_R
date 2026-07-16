-- =============================================================================
-- EM-decisioned -> vendor feedback coverage reconciliation
-- =============================================================================
-- Question: of clients DECISIONED to email, how many actually appear in the
-- vendor feedback chain — and where do the losses happen?
-- SCOPE: Cards personal MNEs ONLY — CRV, PCL, PCQ, PCD, AUH (Andre 2026-07-16).
-- One row per (MNE, cohort month of TREATMT_STRT_DT) since 2024-01-01, ~155 rows.
--
--   clients_decisioned_em  = distinct clients decisioned to email that month
--                            (two-field rule, tactic table)
--   clients_in_master      = of those, with a MASTER row for that exact
--                            (tactic_id, clnt_no) — handed to the vendor at some
--                            point. MASTER has NO date column, so this is
--                            client x tactic EVER, not per-wave.
--   clients_sent_in_window = of those, with a SENT event (disposition_cd=1)
--                            timestamped inside THIS deployment's treatment
--                            window [TREATMT_STRT_DT, TREATMT_END_DT] — the
--                            per-deployment verified send.
--
-- How to read the gaps:
--   decisioned - in_master       = never reached the vendor: suppression,
--                                  channel reassignment, or a different ESP.
--                                  An MNE at 0 in_master across ALL months =
--                                  not in this vendor universe at all.
--   in_master - sent_in_window   = mastered but no sent event inside this
--                                  window: suppressed at send time, sent in a
--                                  different wave, or sent-event logging gap.
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
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
),
in_master AS (
    SELECT DISTINCT m.TREATMENT_ID, m.CLNT_NO, m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
),
sent_events AS (
    SELECT DISTINCT e.TREATMENT_ID, e.consumer_id_hashed, e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND SUBSTR(e.TREATMENT_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
),
flags AS (
    -- one row per decisioned deployment row, with master/sent flags
    SELECT
        d.mne,
        d.cohort_yyyymm,
        d.CLNT_NO,
        MAX(CASE WHEN im.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS f_in_master,
        MAX(CASE WHEN se.consumer_id_hashed IS NOT NULL THEN 1 ELSE 0 END) AS f_sent_in_window
    FROM em_decis d
    LEFT JOIN in_master im
        ON  im.TREATMENT_ID = d.TACTIC_ID
        AND im.CLNT_NO      = d.CLNT_NO
    LEFT JOIN sent_events se
        ON  se.TREATMENT_ID       = d.TACTIC_ID
        AND se.consumer_id_hashed = im.consumer_id_hashed
        AND se.disposition_dt_tm >= CAST(d.TREATMT_STRT_DT AS TIMESTAMP(6))
        AND se.disposition_dt_tm <  CAST(d.TREATMT_END_DT + INTERVAL '1' DAY AS TIMESTAMP(6))
    GROUP BY 1, 2, 3
)
SELECT
    mne,
    cohort_yyyymm,
    CAST(COUNT(*) AS BIGINT)              AS clients_decisioned_em,
    CAST(SUM(f_in_master) AS BIGINT)      AS clients_in_master,
    CAST(SUM(f_sent_in_window) AS BIGINT) AS clients_sent_in_window
FROM flags
GROUP BY 1, 2
ORDER BY 1, 2;
