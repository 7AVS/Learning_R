-- 20: Multi-unsub probe — repeat-unsub span vs MNE breadth (H-lag vs H-percampaign), standalone re-derive of 19's window
-- ENGINE: Teradata-direct. Decides suppression lag (short span, 1 MNE) vs per-campaign unsub (long span, 2+ MNE)
-- Rerun: DROP TABLE block at EOF (1 table), then rerun from top

-- VT: unsub events (disp=4, window-bounded), CLNT_NO resolved via MASTER +/-1mo margin, deduped
CREATE VOLATILE TABLE vt_multi_unsub_events AS (
    WITH unsub_events AS (
        SELECT
            e.consumer_id_hashed,
            e.TREATMENT_ID,
            e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2026-04-01'   -- editable: SPOTLIGHT start (inclusive)
          AND e.disposition_dt_tm <  DATE '2026-07-01'   -- editable: SPOTLIGHT end (exclusive)
    )
    SELECT DISTINCT
        m.CLNT_NO,
        u.TREATMENT_ID,
        u.disposition_dt_tm,
        SUBSTR(u.TREATMENT_ID, 8, 3) AS mne
    FROM unsub_events u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
    WHERE m.load_tm >= ADD_MONTHS(DATE '2026-04-01', -1)   -- editable: SPOTLIGHT start - 1mo margin
      AND m.load_tm <  ADD_MONTHS(DATE '2026-07-01',  1)   -- editable: SPOTLIGHT end + 1mo margin
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_multi_unsub_events COLUMN (CLNT_NO);


-- per-client span_days / n_distinct_mne / n_events, keep clients with >=2 unsub events
WITH client_stats AS (
    SELECT
        CLNT_NO,
        COUNT(*)                                                                    AS n_events,
        COUNT(DISTINCT mne)                                                         AS n_distinct_mne,
        CAST(MAX(disposition_dt_tm) AS DATE) - CAST(MIN(disposition_dt_tm) AS DATE)  AS span_days
    FROM vt_multi_unsub_events
    GROUP BY CLNT_NO
    HAVING COUNT(*) >= 2
)
-- DECISION: repeat unsubs = suppression lag (0-3d, 1 MNE) or per-campaign unsubs (8d+, 2+ MNEs)?
SELECT
    CASE WHEN span_days = 0              THEN '0 days'
         WHEN span_days BETWEEN 1 AND 3  THEN '1-3'
         WHEN span_days BETWEEN 4 AND 7  THEN '4-7'
         WHEN span_days BETWEEN 8 AND 30 THEN '8-30'
         ELSE '31+' END AS span_band,             -- editable: span bands
    CASE WHEN n_distinct_mne = 1 THEN '1'
         WHEN n_distinct_mne = 2 THEN '2'
         ELSE '3+' END AS mne_band,                -- editable: mne bands
    COUNT(*) AS n_clients
FROM client_stats
GROUP BY 1, 2
ORDER BY 1, 2;


-- OPTIONAL: top MNE-pair patterns, 2+ MNE clients only
WITH client_mne_list AS (
    SELECT DISTINCT CLNT_NO, mne FROM vt_multi_unsub_events
),
multi_mne_clients AS (
    SELECT CLNT_NO FROM client_mne_list GROUP BY CLNT_NO HAVING COUNT(DISTINCT mne) >= 2
),
mne_pairs AS (
    SELECT
        a.CLNT_NO,
        CASE WHEN a.mne < b.mne THEN a.mne ELSE b.mne END AS mne_lo,
        CASE WHEN a.mne < b.mne THEN b.mne ELSE a.mne END AS mne_hi
    FROM client_mne_list a
    INNER JOIN client_mne_list b ON a.CLNT_NO = b.CLNT_NO AND a.mne < b.mne
    INNER JOIN multi_mne_clients m ON m.CLNT_NO = a.CLNT_NO
)
SELECT mne_lo, mne_hi, COUNT(DISTINCT CLNT_NO) AS n_clients
FROM mne_pairs
GROUP BY 1, 2
ORDER BY 3 DESC;


DROP TABLE vt_multi_unsub_events;
