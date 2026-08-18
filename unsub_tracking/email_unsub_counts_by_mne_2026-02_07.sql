-- =============================================================================
-- Email decisioned / sent / unsubscribed — distinct clients per MNE x cohort month
-- =============================================================================
-- Scope   : PCL, PCQ, PCD, VBA, VBU, CRV, CRO
-- Cohort  : month of TREATMT_STRT_DT, Feb-2026 .. Jul-2026. Every deployment
--           (TACTIC_ID) that starts in the month rolls up into that month.
-- Grain   : one row per (mne, cohort_month); every count is DISTINCT CLNT_NO.
-- ENGINE  : Teradata-direct (pure EDW, no EDL table). Bare schema names.
--
-- Chain (exact-key joins only, no time windows — TACTIC_ID pins the deployment):
--   DG6V01.TACTIC_EVNT_IP_AR_HIST   (CLNT_NO, TACTIC_ID)            decisioned
--     -> DTZV01.VENDOR_FEEDBACK_MASTER (TREATMENT_ID=TACTIC_ID, CLNT_NO) -> consumer_id_hashed
--       -> DTZV01.VENDOR_FEEDBACK_EVENT (TREATMENT_ID, consumer_id_hashed) disposition_cd
--
-- Columns:
--   clients_decisioned     distinct clients with a tactic row for the MNE that month
--                          (ALL channels — no channel filter, as asked)
--   clients_decisioned_em  subset flagged email in the decision record
--                          (TACTIC_DECISN_VRB_INFO pos 121-150 or ADDNL_DECISN_DATA1 LIKE '%EM%')
--   clients_sent           subset with a disposition_cd = 1 (sent) on that TACTIC_ID
--   clients_unsub          subset with a disposition_cd = 4 (unsubscribed) on that TACTIC_ID
--   Funnel is nested: sent and unsub are subsets of decisioned; a client is
--   attributed to the deployment (and therefore cohort month) they were decisioned into.
--
-- Guards (canon: unsub_tracking/UNSUB_TRACKING_KNOWLEDGE.md §20):
--   * MASTER is card-level duplicated (1.123 rows/key, ~11% inflation on raw joins)
--     -> SELECT DISTINCT (TREATMENT_ID, CLNT_NO, consumer_id_hashed), CLNT_NO IS NOT NULL.
--   * MASTER is NOT date-floored on load_tm: some tactic ids are reused across
--     years (§20.10), so the client x tactic MASTER row may predate the cohort.
--   * EVENT floored at disposition_dt_tm >= 2026-01-01 (scan pruning + drops
--     stale events on reused ids). NO upper cap: late unsubs on a Jul send still
--     land in the Jul cohort. Rerun later = numbers grow.
--
-- Read-me for the numbers:
--   * disposition_cd 4 = opt-out submitted on the vendor page for THAT list.
--     It is not a global consent flip; SFMC footer/CloudPages and EMO suppression
--     paths leave no disposition-4 row. Undercount, not overcount.
--   * Jul-2026 (and to a lesser degree Jun) is still accruing: ~71% of unsubs
--     land within 30 days of send (§20.6). Stamp the run date on any share.
--   * Counts only. Divide downstream (Excel).
-- =============================================================================

WITH decis AS (
    -- one row per client x deployment; MNE from TACTIC_ID positions 8-10
    SELECT DISTINCT
        t.CLNT_NO,
        t.TACTIC_ID,
        SUBSTR(t.TACTIC_ID, 8, 3)                              AS mne,
        EXTRACT(YEAR  FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT)              AS cohort_month,
        CASE WHEN SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
               OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%'
             THEN 1 ELSE 0 END                                 AS f_em
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2026-02-01'
      AND t.TREATMT_STRT_DT <  DATE '2026-08-01'
      AND SUBSTR(t.TACTIC_ID, 8, 3) IN ('PCL','PCQ','PCD','VBA','VBU','CRV','CRO')
),
master_dedup AS (
    -- vendor handoff record; collapse card-level duplication to the join key
    SELECT DISTINCT
        m.TREATMENT_ID,
        m.CLNT_NO,
        m.consumer_id_hashed
    FROM DTZV01.VENDOR_FEEDBACK_MASTER m
    WHERE m.CLNT_NO IS NOT NULL
      AND SUBSTR(m.TREATMENT_ID, 8, 3) IN ('PCL','PCQ','PCD','VBA','VBU','CRV','CRO')
),
vf AS (
    -- sent (1) and unsubscribed (4) events, one flag row per consumer x tactic x code
    SELECT DISTINCT
        e.TREATMENT_ID,
        e.consumer_id_hashed,
        e.disposition_cd
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_dt_tm >= TIMESTAMP '2026-01-01 00:00:00'
      AND e.disposition_cd IN (1, 4)
      AND SUBSTR(e.TREATMENT_ID, 8, 3) IN ('PCL','PCQ','PCD','VBA','VBU','CRV','CRO')
),
client_flags AS (
    -- collapse to one row per client per (mne, cohort_month); MAX = any deployment that month
    SELECT
        d.mne,
        d.cohort_month,
        d.CLNT_NO,
        MAX(d.f_em)                                                 AS f_em,
        MAX(CASE WHEN v.disposition_cd = 1 THEN 1 ELSE 0 END)       AS f_sent,
        MAX(CASE WHEN v.disposition_cd = 4 THEN 1 ELSE 0 END)       AS f_unsub
    FROM decis d
    LEFT JOIN master_dedup m
        ON  m.TREATMENT_ID = d.TACTIC_ID
        AND m.CLNT_NO      = d.CLNT_NO
    LEFT JOIN vf v
        ON  v.TREATMENT_ID       = d.TACTIC_ID
        AND v.consumer_id_hashed = m.consumer_id_hashed
    GROUP BY 1, 2, 3
)
SELECT
    mne,
    cohort_month,
    CAST(COUNT(*)      AS BIGINT) AS clients_decisioned,
    CAST(SUM(f_em)     AS BIGINT) AS clients_decisioned_em,
    CAST(SUM(f_sent)   AS BIGINT) AS clients_sent,
    CAST(SUM(f_unsub)  AS BIGINT) AS clients_unsub
FROM client_flags
GROUP BY 1, 2
ORDER BY 1, 2;
