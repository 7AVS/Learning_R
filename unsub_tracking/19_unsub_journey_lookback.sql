-- 19 v5: Unsub journey lookback — window-bounded cohort vs baseline, 12mo contact history
-- ENGINE: Teradata-direct. Unsub = address-level; no first-ever/history semantics.
-- Rerun: DROP TABLE block at EOF (4 tables), then rerun from top.

-- VT1: unsub events, window-bounded (spotlight only)
CREATE VOLATILE TABLE vt_unsub_events AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2026-04-01'   -- SPOTLIGHT start (inclusive)
      AND e.disposition_dt_tm <  DATE '2026-07-01'   -- SPOTLIGHT end (exclusive)
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_events COLUMN (consumer_id_hashed, TREATMENT_ID);


-- VT2: cohort — resolve CLNT_NO, index_dt = earliest unsub in window, all addresses kept
CREATE VOLATILE TABLE vt_unsub_cohort AS (
    WITH resolved AS (
        SELECT
            m.CLNT_NO,
            u.consumer_id_hashed,
            u.disposition_dt_tm,
            ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY u.disposition_dt_tm ASC) AS rn
        FROM vt_unsub_events u
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = u.consumer_id_hashed
            AND m.TREATMENT_ID       = u.TREATMENT_ID
        WHERE m.load_tm >= ADD_MONTHS(DATE '2026-04-01', -1)   -- SPOTLIGHT start - 1mo margin
          AND m.load_tm <  ADD_MONTHS(DATE '2026-07-01',  1)   -- SPOTLIGHT end + 1mo margin
    ),
    client_index AS (
        SELECT CLNT_NO, disposition_dt_tm AS index_dt
        FROM resolved
        WHERE rn = 1
    )
    SELECT DISTINCT r.CLNT_NO, r.consumer_id_hashed, ci.index_dt
    FROM resolved r
    INNER JOIN client_index ci ON ci.CLNT_NO = r.CLNT_NO
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_cohort COLUMN (CLNT_NO);
COLLECT STATISTICS ON vt_unsub_cohort COLUMN (consumer_id_hashed);


-- VT3: baseline spine — disp=1 in window, 1-in-10 sliced, excl. VT2 clients (window-scoped)
CREATE VOLATILE TABLE vt_baseline_spine AS (
    WITH baseline_sends AS (
        SELECT
            m.CLNT_NO,
            e.consumer_id_hashed,
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
          AND MOD(m.CLNT_NO, 10) = 0                                       -- editable: slice modulus (1-in-10)
    ),
    client_index AS (
        SELECT CLNT_NO, MAX(disposition_dt_tm) AS index_dt
        FROM baseline_sends
        GROUP BY CLNT_NO
    )
    SELECT DISTINCT b.CLNT_NO, b.consumer_id_hashed, ci.index_dt
    FROM baseline_sends b
    INNER JOIN client_index ci ON ci.CLNT_NO = b.CLNT_NO
    WHERE NOT EXISTS (
        SELECT 1 FROM vt_unsub_cohort u WHERE u.CLNT_NO = b.CLNT_NO
    )
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_baseline_spine COLUMN (CLNT_NO);


-- VT4: population spine — VT2 cohort UNION ALL VT3 baseline
CREATE VOLATILE TABLE vt_unsub_journey_pop AS (
    SELECT
        consumer_id_hashed, CLNT_NO,
        CAST('unsub' AS VARCHAR(10)) AS cohort_group,
        EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
        index_dt
    FROM vt_unsub_cohort
    UNION ALL
    SELECT
        consumer_id_hashed, CLNT_NO,
        CAST('stayed' AS VARCHAR(10)) AS cohort_group,
        EXTRACT(YEAR FROM index_dt) * 100 + EXTRACT(MONTH FROM index_dt) AS cohort_month,
        index_dt
    FROM vt_baseline_spine
) WITH DATA PRIMARY INDEX (consumer_id_hashed) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (consumer_id_hashed);
COLLECT STATISTICS ON vt_unsub_journey_pop COLUMN (CLNT_NO);


-- STEP 5: 12mo lookback — EVENT joined directly on consumer_id_hashed (no MASTER), banded rollup
WITH sends AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm,
        SUBSTR(e.TREATMENT_ID, 8, 3) AS mne
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= ADD_MONTHS(DATE '2026-04-01', -12)  -- window start - 12mo
      AND e.disposition_dt_tm <  DATE '2026-07-01'                   -- window end (exclusive)
),
lookback AS (                      -- 12mo strictly before each client's OWN index_dt
    SELECT
        p.CLNT_NO,
        p.cohort_group,
        p.cohort_month,
        COUNT(DISTINCT s.TREATMENT_ID) AS lookback_contacts,
        COUNT(DISTINCT s.mne)          AS lookback_mnes
    FROM vt_unsub_journey_pop p
    LEFT JOIN sends s
        ON  s.consumer_id_hashed = p.consumer_id_hashed
        AND s.disposition_dt_tm >= ADD_MONTHS(CAST(p.index_dt AS DATE), -12)
        AND s.disposition_dt_tm <  p.index_dt
    GROUP BY 1, 2, 3
)
SELECT
    cohort_group,
    cohort_month,
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


-- DIAGNOSTIC: multi-unsub scale — is the multi-address blind spot real?
WITH client_unsub_counts AS (
    SELECT
        v2.CLNT_NO,
        COUNT(*)                             AS n_unsub_events,
        COUNT(DISTINCT v1.consumer_id_hashed) AS n_unsub_addresses
    FROM vt_unsub_events v1
    INNER JOIN vt_unsub_cohort v2 ON v2.consumer_id_hashed = v1.consumer_id_hashed
    GROUP BY v2.CLNT_NO
)
SELECT
    CASE WHEN n_unsub_events = 1 THEN '1' WHEN n_unsub_events = 2 THEN '2' ELSE '3+' END AS unsub_event_band,
    CASE WHEN n_unsub_addresses = 1 THEN '1' ELSE '2+' END AS unsub_address_band,
    COUNT(*) AS n_clients
FROM client_unsub_counts
GROUP BY 1, 2
ORDER BY 1, 2;


DROP TABLE vt_unsub_journey_pop;
DROP TABLE vt_baseline_spine;
DROP TABLE vt_unsub_cohort;
DROP TABLE vt_unsub_events;

-- OPTIONAL: chunked fallback if CPU abort persists
-- CREATE VOLATILE TABLE vt_lookback_chunks AS (
--     SELECT p.CLNT_NO, p.cohort_group, p.cohort_month,
--            COUNT(DISTINCT s.TREATMENT_ID)              AS lookback_contacts,
--            COUNT(DISTINCT SUBSTR(s.TREATMENT_ID,8,3))  AS lookback_mnes
--     FROM vt_unsub_journey_pop p
--     LEFT JOIN DTZV01.VENDOR_FEEDBACK_EVENT s
--            ON s.consumer_id_hashed = p.consumer_id_hashed AND s.disposition_cd = 1
--           AND s.disposition_dt_tm >= ADD_MONTHS(CAST(p.index_dt AS DATE), -12)
--           AND s.disposition_dt_tm <  p.index_dt
--     WHERE MOD(p.CLNT_NO, 4) = 0
--     GROUP BY 1, 2, 3
--     -- UNION ALL  SELECT ... same joins ...  WHERE MOD(p.CLNT_NO, 4) = 1  GROUP BY 1, 2, 3
--     -- UNION ALL  SELECT ... same joins ...  WHERE MOD(p.CLNT_NO, 4) = 2  GROUP BY 1, 2, 3
--     -- UNION ALL  SELECT ... same joins ...  WHERE MOD(p.CLNT_NO, 4) = 3  GROUP BY 1, 2, 3
-- ) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;
-- then run STEP 5's final banded SELECT against vt_lookback_chunks instead of the lookback CTE
-- DROP TABLE vt_lookback_chunks;
