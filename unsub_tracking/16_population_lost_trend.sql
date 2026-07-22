-- =============================================================================
-- Population lost trend v3 — month x MNE: EM-DECISIONED base + first unsubs
-- =============================================================================
-- v3 2026-07-22: + clients_sent (vendor-only, rate denominator downstream),
--                + decision-sized rollup blocks (end of file).
--
-- One row per (MNE, deployment-cohort month) since 2024-01-01, ALL MNEs,
-- long format — pivot in Excel.
--
--   clients_decisioned_em = distinct clients DECISIONED to email that month
--     (tactic table, env-confirmed two-field rule: VRB_INFO pos 121 LIKE '%EM%'
--      OR ADDNL_DECISN_DATA1 LIKE '%EM%'). Month = TREATMT_STRT_DT month.
--   clients_first_unsub   = clients whose FIRST-EVER unsub (disposition_cd=4,
--     deduped 1/client) was triggered by that MNE, booked to the month of the
--     TRIGGERING DEPLOYMENT (latest TREATMT_STRT_DT <= unsub time for that
--     client x tactic) — SAME CLOCK as the denominator, so columns divide.
--     Fallback: unsub calendar month when no tactic row matches (the ~68
--     vendor-only MNEs with no tactic-side decisioning; their denominator is 0).
--   clients_sent          = distinct clients with a SENT disposition
--     (disposition_cd=1, same vendor idiom as 17_em_decision_vendor_coverage.sql)
--     for that MNE x month. VENDOR-ONLY: mne from SUBSTR(TREATMENT_ID,8,3) same
--     as everywhere else in this file; month from the vendor's own
--     disposition_dt_tm — no tactic join, so this also covers the ~68
--     vendor-only MNEs the decisioned denominator can't see.
--     This is the RATE denominator (unsub rate = unsubs/sent) — divide
--     downstream in Excel, not here.
--   tracked_mne = Y for the 17 in-scope Cards/Payments/Loans MNEs.
--
-- Counts only — divide in Excel. ENGINE: Teradata-direct.
-- =============================================================================

WITH em_decis AS (
    SELECT
        t.CLNT_NO,
        SUBSTR(t.TACTIC_ID, 8, 3) AS mne,
        EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
          + EXTRACT(MONTH FROM t.TREATMT_STRT_DT) AS yyyymm
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST t
    WHERE t.TREATMT_STRT_DT >= DATE '2024-01-01'
      AND (   SUBSTR(t.TACTIC_DECISN_VRB_INFO, 121, 30) LIKE '%EM%'
           OR UPPER(COALESCE(t.ADDNL_DECISN_DATA1, '')) LIKE '%EM%' )
),
denom AS (
    SELECT mne, yyyymm,
           COUNT(DISTINCT CLNT_NO) AS clients_decisioned_em
    FROM em_decis
    GROUP BY 1, 2
),
first_unsub AS (
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
),
unsub_booked AS (
    -- TACTIC_ID is unique per deployment (MNE + julian date; Andre 2026-07-16)
    -- and a client never duplicates on one TACTIC_ID -> the exact-key join IS
    -- the deployment. No time window needed anywhere. Date floor kept only for
    -- scan pruning; an unsub whose deployment predates the floor gets
    -- trig_strt_dt NULL -> calendar-month fallback downstream.
    SELECT
        u.CLNT_NO,
        u.TREATMENT_ID,
        u.disposition_dt_tm,
        t.TREATMT_STRT_DT AS trig_strt_dt
    FROM first_unsub u
    LEFT JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON  t.TACTIC_ID = u.TREATMENT_ID
        AND t.CLNT_NO   = u.CLNT_NO
        AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
    WHERE u.rn = 1
),
unsubs AS (
    SELECT
        SUBSTR(TREATMENT_ID, 8, 3) AS mne,
        COALESCE(
            EXTRACT(YEAR FROM trig_strt_dt) * 100
              + EXTRACT(MONTH FROM trig_strt_dt),
            EXTRACT(YEAR FROM disposition_dt_tm) * 100
              + EXTRACT(MONTH FROM disposition_dt_tm)
        ) AS yyyymm,
        CAST(COUNT(*) AS BIGINT) AS clients_first_unsub
    FROM unsub_booked
    GROUP BY 1, 2
),
sent_raw AS (
    -- vendor-only: no tactic join, no dependency on the decisioned denominator.
    -- mne = same SUBSTR used everywhere in this file; month = the vendor's own
    -- disposition_dt_tm (no TREATMT_STRT_DT to borrow without a tactic join).
    SELECT
        m.CLNT_NO,
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
        EXTRACT(YEAR FROM e.disposition_dt_tm) * 100
          + EXTRACT(MONTH FROM e.disposition_dt_tm) AS yyyymm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2024-01-01'
),
sent AS (
    SELECT mne, yyyymm, CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS clients_sent
    FROM sent_raw
    GROUP BY 1, 2
)
SELECT
    COALESCE(d.mne, u.mne, s.mne)           AS mne_out,
    COALESCE(d.yyyymm, u.yyyymm, s.yyyymm)  AS yyyymm_out,
    CASE WHEN COALESCE(d.mne, u.mne, s.mne) IN
         ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
          'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
         THEN 'Y' ELSE 'N' END   AS tracked_mne,
    CAST(COALESCE(d.clients_decisioned_em, 0) AS BIGINT) AS clients_decisioned_em,
    CAST(COALESCE(u.clients_first_unsub, 0)   AS BIGINT) AS clients_first_unsub,
    CAST(COALESCE(s.clients_sent, 0)          AS BIGINT) AS clients_sent
FROM denom d
FULL OUTER JOIN unsubs u
    ON  u.mne    = d.mne
    AND u.yyyymm = d.yyyymm
FULL OUTER JOIN sent s
    ON  s.mne    = COALESCE(d.mne, u.mne)
    AND s.yyyymm = COALESCE(d.yyyymm, u.yyyymm)
ORDER BY 1, 2;


-- =============================================================================
-- ROLLUP (a): Cards five trend for spotlight — photograph this
-- =============================================================================
-- ONE decision this answers: is the Cards-five first-unsub trend (against
-- sent volume) moving the right way, month over month? Trailing ~8 months
-- (self-maintaining off CURRENT_DATE), Cards five only -> stays screenshot
-- sized (5 MNE x ~8 months, <= ~40 rows).
-- Recomputed per house style (see 13_unsub_value_spine.sql S1/S2 precedent —
-- CTEs don't carry across statements). Same first-EVER-unsub dedup as the
-- main query: the mne filter is applied AFTER rn=1, so a client's true first
-- unsub still counts even though it was found across all MNEs, not just
-- these five. Counts only.
-- =============================================================================
WITH first_unsub_a AS (
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
),
unsub_booked_a AS (
    SELECT
        u.CLNT_NO,
        u.TREATMENT_ID,
        u.disposition_dt_tm,
        t.TREATMT_STRT_DT AS trig_strt_dt
    FROM first_unsub_a u
    LEFT JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON  t.TACTIC_ID = u.TREATMENT_ID
        AND t.CLNT_NO   = u.CLNT_NO
        AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
    WHERE u.rn = 1
),
unsubs_a AS (
    SELECT
        SUBSTR(TREATMENT_ID, 8, 3) AS mne,
        COALESCE(
            EXTRACT(YEAR FROM trig_strt_dt) * 100
              + EXTRACT(MONTH FROM trig_strt_dt),
            EXTRACT(YEAR FROM disposition_dt_tm) * 100
              + EXTRACT(MONTH FROM disposition_dt_tm)
        ) AS yyyymm,
        CAST(COUNT(*) AS BIGINT) AS clients_first_unsub
    FROM unsub_booked_a
    WHERE SUBSTR(TREATMENT_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
    GROUP BY 1, 2
),
sent_a AS (
    -- vendor-only sent idiom, same as the main query's sent_raw/sent CTEs,
    -- mne filter pushed down here since sent has no cross-mne dedup to protect.
    SELECT
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
        EXTRACT(YEAR FROM e.disposition_dt_tm) * 100
          + EXTRACT(MONTH FROM e.disposition_dt_tm) AS yyyymm,
        CAST(COUNT(DISTINCT m.CLNT_NO) AS BIGINT) AS clients_sent
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND SUBSTR(e.TREATMENT_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
      AND e.disposition_dt_tm >= ADD_MONTHS(CURRENT_DATE, -9)
    GROUP BY 1, 2
)
SELECT
    COALESCE(u.mne, s.mne)       AS mne,
    COALESCE(u.yyyymm, s.yyyymm) AS cohort_month,
    CAST(COALESCE(u.clients_first_unsub, 0) AS BIGINT) AS clients_first_unsub,
    CAST(COALESCE(s.clients_sent, 0)        AS BIGINT) AS clients_sent
FROM unsubs_a u
FULL OUTER JOIN sent_a s
    ON  s.mne = u.mne AND s.yyyymm = u.yyyymm
WHERE COALESCE(u.yyyymm, s.yyyymm) >=
      EXTRACT(YEAR FROM ADD_MONTHS(CURRENT_DATE, -7)) * 100
        + EXTRACT(MONTH FROM ADD_MONTHS(CURRENT_DATE, -7))
ORDER BY 1, 2;


-- =============================================================================
-- ROLLUP (b): top 15 MNEs by total first-unsub volume — full window
-- =============================================================================
-- ONE decision this answers: which MNEs drive program unsubs — is Cards
-- share small? All-MNE, full window (since 2024-01-01), one row per MNE,
-- ranked by total first-ever unsubs, cut to TOP 15. Counts only.
-- =============================================================================
WITH first_unsub_b AS (
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
),
unsubs_b AS (
    SELECT
        SUBSTR(TREATMENT_ID, 8, 3) AS mne,
        CAST(COUNT(*) AS BIGINT)   AS clients_first_unsub_total
    FROM first_unsub_b
    WHERE rn = 1
    GROUP BY 1
),
sent_b AS (
    SELECT
        SUBSTR(e.TREATMENT_ID, 8, 3)              AS mne,
        CAST(COUNT(DISTINCT m.CLNT_NO) AS BIGINT) AS clients_sent_total
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= DATE '2024-01-01'
    GROUP BY 1
)
SELECT TOP 15
    COALESCE(u.mne, s.mne)                                    AS mne,
    CAST(COALESCE(u.clients_first_unsub_total, 0) AS BIGINT)  AS clients_first_unsub_total,
    CAST(COALESCE(s.clients_sent_total, 0)        AS BIGINT)  AS clients_sent_total
FROM unsubs_b u
FULL OUTER JOIN sent_b s
    ON  s.mne = u.mne
ORDER BY clients_first_unsub_total DESC;
