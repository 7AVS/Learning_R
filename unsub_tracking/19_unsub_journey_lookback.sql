-- 19: Unsub journey lookback — contact history before first-unsub vs stayed baseline
-- ENGINE: Teradata-direct.
-- Rerun: if a run fails midway, run the 4 DROP TABLE lines at EOF, then rerun from top. 5 statements.

-- VT1: unsub events, all history
CREATE VOLATILE TABLE vt_unsub_events AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 4
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_events COLUMN (consumer_id_hashed, TREATMENT_ID);


-- VT2: resolve to CLNT_NO via MASTER, rn=1 = first-ever unsub
CREATE VOLATILE TABLE vt_unsub_resolved AS (
    SELECT
        m.CLNT_NO,
        u.disposition_dt_tm AS first_unsub_tm,
        ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO
                           ORDER BY u.disposition_dt_tm ASC) AS rn
    FROM vt_unsub_events u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_resolved COLUMN (CLNT_NO);


-- VT3: baseline candidates (sliced, deduped, never-unsubbed)
-- baseline is 1-in-10 sliced: compare within-group shares, never raw counts vs unsub
CREATE VOLATILE TABLE vt_baseline_spine AS (
    WITH baseline_sends AS (
        SELECT
            m.CLNT_NO,
            e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = e.consumer_id_hashed
            AND m.TREATMENT_ID       = e.TREATMENT_ID
        WHERE e.disposition_cd = 1
          AND e.disposition_dt_tm >= DATE '2026-04-01'                     -- SPOTLIGHT start (inclusive)
          AND e.disposition_dt_tm <  DATE '2026-07-01'                     -- SPOTLIGHT end (exclusive)
          AND m.load_tm           >= ADD_MONTHS(DATE '2026-04-01', -1)     -- SPOTLIGHT start - 1mo margin
          AND m.load_tm           <  ADD_MONTHS(DATE '2026-07-01',  1)     -- SPOTLIGHT end + 1mo margin
          -- editable: slice modulus (1-in-10)
          AND MOD(m.CLNT_NO, 10) = 0
    ),
    window_sends AS (
        SELECT CLNT_NO, MAX(disposition_dt_tm) AS last_send_dt
        FROM baseline_sends
        GROUP BY CLNT_NO
    )
    SELECT w.CLNT_NO, w.last_send_dt AS index_dt
    FROM window_sends w
    WHERE NOT EXISTS (
        SELECT 1 FROM vt_unsub_resolved au WHERE au.CLNT_NO = w.CLNT_NO
    )
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_baseline_spine COLUMN (CLNT_NO);


-- VT4: population spine — unsub cohort UNION ALL baseline
CREATE VOLATILE TABLE vt_unsub_journey_pop AS (
    SELECT CLNT_NO, first_unsub_tm AS index_dt, CAST('unsub' AS VARCHAR(10)) AS cohort_group
    FROM vt_unsub_resolved
    WHERE rn = 1
      AND first_unsub_tm >= DATE '2026-04-01'   -- SPOTLIGHT start (inclusive)
      AND first_unsub_tm <  DATE '2026-07-01'   -- SPOTLIGHT end (exclusive)
    UNION ALL
    SELECT CLNT_NO, index_dt, CAST('stayed' AS VARCHAR(10)) AS cohort_group
    FROM vt_baseline_spine
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (CLNT_NO);


-- STEP 5: 12-month lookback + final rollup
WITH sends AS (
    SELECT
        m.CLNT_NO,
        e.TREATMENT_ID,
        e.disposition_dt_tm,
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= ADD_MONTHS(DATE '2026-04-01', -12)  -- 12mo before SPOTLIGHT start = 2025-04-01
      AND e.disposition_dt_tm <  DATE '2026-07-01'                   -- SPOTLIGHT end (exclusive)
),
sends_pop AS (                     -- narrow to population clients BEFORE the per-client date range/aggregation
    SELECT s.CLNT_NO, s.TREATMENT_ID, s.disposition_dt_tm, s.mne
    FROM sends s
    INNER JOIN vt_unsub_journey_pop p ON p.CLNT_NO = s.CLNT_NO
),
lookback AS (                      -- 12 months strictly before each client's OWN index date
    SELECT
        p.CLNT_NO,
        p.cohort_group,
        p.index_dt,
        COUNT(DISTINCT s.TREATMENT_ID) AS lookback_contacts,
        COUNT(DISTINCT s.mne)          AS lookback_mnes
    FROM vt_unsub_journey_pop p
    LEFT JOIN sends_pop s
        ON  s.CLNT_NO = p.CLNT_NO
        AND s.disposition_dt_tm >= ADD_MONTHS(CAST(p.index_dt AS DATE), -12)
        AND s.disposition_dt_tm <  p.index_dt
    GROUP BY 1, 2, 3
)
SELECT
    cohort_group,
    EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
    COUNT(DISTINCT CLNT_NO)                       AS n_clients,
    CAST(AVG(lookback_contacts) AS DECIMAL(10,1)) AS avg_contacts,
    CAST(AVG(lookback_mnes)     AS DECIMAL(10,1)) AS avg_mnes,
    -- editable: contact bands
    SUM(CASE WHEN lookback_contacts = 0             THEN 1 ELSE 0 END) AS contacts_0,
    SUM(CASE WHEN lookback_contacts = 1             THEN 1 ELSE 0 END) AS contacts_1,
    SUM(CASE WHEN lookback_contacts = 2             THEN 1 ELSE 0 END) AS contacts_2,
    SUM(CASE WHEN lookback_contacts BETWEEN 3 AND 4 THEN 1 ELSE 0 END) AS contacts_3_4,
    SUM(CASE WHEN lookback_contacts BETWEEN 5 AND 6 THEN 1 ELSE 0 END) AS contacts_5_6,
    SUM(CASE WHEN lookback_contacts BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS contacts_7_9,
    SUM(CASE WHEN lookback_contacts BETWEEN 10 AND 14 THEN 1 ELSE 0 END) AS contacts_10_14,
    SUM(CASE WHEN lookback_contacts >= 15           THEN 1 ELSE 0 END) AS contacts_15p,
    -- editable: mne bands
    SUM(CASE WHEN lookback_mnes = 0                 THEN 1 ELSE 0 END) AS mnes_0,
    SUM(CASE WHEN lookback_mnes = 1                 THEN 1 ELSE 0 END) AS mnes_1,
    SUM(CASE WHEN lookback_mnes = 2                 THEN 1 ELSE 0 END) AS mnes_2,
    SUM(CASE WHEN lookback_mnes = 3                 THEN 1 ELSE 0 END) AS mnes_3,
    SUM(CASE WHEN lookback_mnes = 4                 THEN 1 ELSE 0 END) AS mnes_4,
    SUM(CASE WHEN lookback_mnes >= 5                THEN 1 ELSE 0 END) AS mnes_5p
FROM lookback
GROUP BY 1, 2
ORDER BY 1, 2;

DROP TABLE vt_unsub_journey_pop;
DROP TABLE vt_baseline_spine;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_events;
