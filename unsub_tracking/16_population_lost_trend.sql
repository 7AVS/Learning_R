-- 16: Population lost trend v3 — month x MNE: EM-decisioned base + first unsubs
-- ENGINE: Teradata-direct.
-- 3 statements → 3 result tabs: grid / rollup a / rollup b

WITH em_decis AS (
    -- decisioned base: tactic EM rule (VRB_INFO/ADDNL_DECISN_DATA1 LIKE '%EM%')
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
    -- first-ever unsub per client (disposition_cd=4), rn=1 = first
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
    -- book first unsub to triggering deployment month (TACTIC_ID exact-key join)
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
    -- book sends to deployment month (v4 fix), same mechanism as unsub_booked
    SELECT
        m.CLNT_NO,
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
        COALESCE(
            EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
              + EXTRACT(MONTH FROM t.TREATMT_STRT_DT),
            EXTRACT(YEAR FROM e.disposition_dt_tm) * 100
              + EXTRACT(MONTH FROM e.disposition_dt_tm)
        ) AS yyyymm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    LEFT JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON  t.TACTIC_ID = e.TREATMENT_ID
        AND t.CLNT_NO   = m.CLNT_NO
        AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
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
    -- editable: tracked MNE list
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


-- ROLLUP (a): Cards five trend — photograph this
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
    -- editable: Cards five MNE list
    WHERE SUBSTR(TREATMENT_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
    GROUP BY 1, 2
),
sent_a AS (
    -- same deployment-month booking as sent_raw; mne filter pushed down
    SELECT
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne,
        COALESCE(
            EXTRACT(YEAR FROM t.TREATMT_STRT_DT) * 100
              + EXTRACT(MONTH FROM t.TREATMT_STRT_DT),
            EXTRACT(YEAR FROM e.disposition_dt_tm) * 100
              + EXTRACT(MONTH FROM e.disposition_dt_tm)
        ) AS yyyymm,
        CAST(COUNT(DISTINCT m.CLNT_NO) AS BIGINT) AS clients_sent
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    LEFT JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST t
        ON  t.TACTIC_ID = e.TREATMENT_ID
        AND t.CLNT_NO   = m.CLNT_NO
        AND t.TREATMT_STRT_DT >= DATE '2024-01-01'
    WHERE e.disposition_cd = 1
      -- editable: Cards five MNE list
      AND SUBSTR(e.TREATMENT_ID, 8, 3) IN ('CRV','PCL','PCQ','PCD','AUH')
      -- editable: trailing window (9mo scan)
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
-- editable: trailing window (7mo display)
WHERE COALESCE(u.yyyymm, s.yyyymm) >=
      EXTRACT(YEAR FROM ADD_MONTHS(CURRENT_DATE, -7)) * 100
        + EXTRACT(MONTH FROM ADD_MONTHS(CURRENT_DATE, -7))
ORDER BY 1, 2;


-- ROLLUP (b): top 15 MNEs by total first-unsub volume — full window
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
-- editable: MNE cut (top 15)
SELECT TOP 15
    COALESCE(u.mne, s.mne)                                    AS mne,
    CAST(COALESCE(u.clients_first_unsub_total, 0) AS BIGINT)  AS clients_first_unsub_total,
    CAST(COALESCE(s.clients_sent_total, 0)        AS BIGINT)  AS clients_sent_total
FROM unsubs_b u
FULL OUTER JOIN sent_b s
    ON  s.mne = u.mne
ORDER BY clients_first_unsub_total DESC;
