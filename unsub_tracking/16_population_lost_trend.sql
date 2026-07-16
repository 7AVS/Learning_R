-- =============================================================================
-- Population lost trend v2 — month x MNE: EM-DECISIONED base + first unsubs
-- =============================================================================
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
    -- book each first unsub to the deployment whose TREATMENT WINDOW contains
    -- it: TREATMT_STRT_DT <= unsub <= TREATMT_END_DT for that client x tactic
    -- (exact-key join; the unsub row already names the treatment, this join
    -- only picks the deployment instance/month). MAX() disambiguates if two
    -- windows of the same tactic overlap. Unsubs outside every window get
    -- trig_strt_dt NULL -> calendar-month fallback downstream.
    SELECT
        u.CLNT_NO,
        u.TREATMENT_ID,
        u.disposition_dt_tm,
        MAX(t.TREATMT_STRT_DT) AS trig_strt_dt
    FROM first_unsub u
    LEFT JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON  t.TACTIC_ID = u.TREATMENT_ID
        AND t.CLNT_NO   = u.CLNT_NO
        AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
        AND t.TREATMT_STRT_DT <= CAST(u.disposition_dt_tm AS DATE)
        AND t.TREATMT_END_DT  >= CAST(u.disposition_dt_tm AS DATE)
    WHERE u.rn = 1
    GROUP BY 1, 2, 3
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
)
SELECT
    COALESCE(d.mne, u.mne)       AS mne_out,
    COALESCE(d.yyyymm, u.yyyymm) AS yyyymm_out,
    CASE WHEN COALESCE(d.mne, u.mne) IN
         ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P',
          'VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')
         THEN 'Y' ELSE 'N' END   AS tracked_mne,
    CAST(COALESCE(d.clients_decisioned_em, 0) AS BIGINT) AS clients_decisioned_em,
    CAST(COALESCE(u.clients_first_unsub, 0)   AS BIGINT) AS clients_first_unsub
FROM denom d
FULL OUTER JOIN unsubs u
    ON  u.mne    = d.mne
    AND u.yyyymm = d.yyyymm
ORDER BY 1, 2;
