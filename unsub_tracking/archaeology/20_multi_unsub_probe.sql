-- 20: Same-day multi-MNE unsub — RAW EVIDENCE DUMP (replaces the old banded-counts version, 2026-08-12)
-- ENGINE: Teradata-direct. Two-part addressing DTZV01.<table>, no catalog prefix.
-- QUESTION: do real clients fire 2+ unsub events on the SAME CALENDAR DAY against DIFFERENT campaigns (MNEs)?
-- OUTPUT: not bands, not counts — the actual rows as they sit in VENDOR_FEEDBACK_EVENT / _MASTER.
-- Rerun: DROP TABLE block at EOF (2 tables), then rerun from top.
--
-- READING THE OUTPUT
--   Q1  1 row       scale: how many clients / client-days show the pattern, out of how many unsubbers
--   Q2  ~20-40 rows THE EVIDENCE. Every EVENT column, verbatim, for 10 sampled client-days.
--                   Read down a single clnt_no block: same date, different mne, different treatment_id.
--   Q3  ~50-200 rows the surrounding journey (sent/open/click/unsub) for those same treatment_ids
--   Q4  ~20-40 rows the raw MASTER rows behind them (SELECT * — all 29 columns, nothing hidden)


------------------------------------------------------------------------------
-- VT 1: unsub events in window, CLNT_NO resolved via MASTER (+/-1mo load_tm margin)
------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_mu_events AS (
    WITH unsub_events AS (
        SELECT
            e.consumer_id_hashed,
            e.TREATMENT_ID,
            e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= DATE '2026-04-01'   -- editable: window start (inclusive)
          AND e.disposition_dt_tm <  DATE '2026-07-01'   -- editable: window end (exclusive)
    )
    SELECT DISTINCT
        m.CLNT_NO,
        u.consumer_id_hashed,
        u.TREATMENT_ID,
        u.disposition_dt_tm,
        CAST(u.disposition_dt_tm AS DATE) AS unsub_dt,
        SUBSTR(u.TREATMENT_ID, 8, 3)      AS mne
    FROM unsub_events u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
    WHERE m.load_tm >= ADD_MONTHS(DATE '2026-04-01', -1)   -- editable: window start - 1mo margin
      AND m.load_tm <  ADD_MONTHS(DATE '2026-07-01',  1)   -- editable: window end + 1mo margin
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_mu_events COLUMN (CLNT_NO);


------------------------------------------------------------------------------
-- Q1: SCALE — is the pattern real and how common? 1 row.
------------------------------------------------------------------------------
SELECT
    (SELECT COUNT(DISTINCT CLNT_NO) FROM vt_mu_events)                     AS clients_any_unsub,
    COUNT(DISTINCT CLNT_NO)                                                AS clients_sameday_multi_mne,
    COUNT(*)                                                               AS client_days_sameday_multi_mne,
    MAX(n_mne)                                                             AS max_mnes_in_one_day
FROM (
    SELECT CLNT_NO, unsub_dt, COUNT(DISTINCT mne) AS n_mne
    FROM vt_mu_events
    GROUP BY CLNT_NO, unsub_dt
    HAVING COUNT(DISTINCT mne) >= 2          -- editable: >=2 MNEs on one calendar day
) d;


------------------------------------------------------------------------------
-- VT 2: 10 sampled qualifying client-days (widest MNE spread first, then stable by CLNT_NO)
------------------------------------------------------------------------------
CREATE VOLATILE TABLE vt_mu_sample AS (
    SELECT CLNT_NO, unsub_dt, n_mne, n_events
    FROM (
        SELECT
            CLNT_NO,
            unsub_dt,
            COUNT(DISTINCT mne) AS n_mne,
            COUNT(*)            AS n_events
        FROM vt_mu_events
        GROUP BY CLNT_NO, unsub_dt
        HAVING COUNT(DISTINCT mne) >= 2
    ) d
    QUALIFY ROW_NUMBER() OVER (ORDER BY n_mne DESC, n_events DESC, CLNT_NO) <= 10   -- editable: sample size
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_mu_sample COLUMN (CLNT_NO);


------------------------------------------------------------------------------
-- Q2: THE RAW UNSUB ROWS. Every VENDOR_FEEDBACK_EVENT column, unmodified.
--     mne is the only derived column (SUBSTR of treatment_id, pos 8-10) so the pattern is readable.
------------------------------------------------------------------------------
SELECT
    v.CLNT_NO,
    v.unsub_dt,
    SUBSTR(e.treatment_id, 8, 3)  AS mne,
    e.consumer_id_hashed,
    e.srvc_provdr_nm,
    e.legal_entity_cd,
    e.source_evnt_id,
    e.disposition_dt_tm,
    e.disposition_tm_zone,
    e.disposition_cd,
    e.treatment_id,
    e.load_tm
FROM vt_mu_sample s
INNER JOIN vt_mu_events v
    ON v.CLNT_NO = s.CLNT_NO AND v.unsub_dt = s.unsub_dt
INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
    ON  e.consumer_id_hashed        = v.consumer_id_hashed
    AND e.treatment_id              = v.treatment_id
    AND e.disposition_cd            = 4
    AND CAST(e.disposition_dt_tm AS DATE) = v.unsub_dt
ORDER BY v.CLNT_NO, e.disposition_dt_tm, e.treatment_id;


------------------------------------------------------------------------------
-- Q3: SURROUNDING JOURNEY — every disposition (1 sent / 2 open / 3 click / 4 unsub / 5 bounce / 6 complaint)
--     on those same treatment_ids, so the send that triggered each unsub is visible.
------------------------------------------------------------------------------
SELECT
    v.CLNT_NO,
    SUBSTR(e.treatment_id, 8, 3) AS mne,
    e.treatment_id,
    e.disposition_dt_tm,
    e.disposition_cd,
    CASE e.disposition_cd
        WHEN 1 THEN 'sent' WHEN 2 THEN 'opened' WHEN 3 THEN 'clicked'
        WHEN 4 THEN 'UNSUB' WHEN 5 THEN 'hardbounce' WHEN 6 THEN 'complaint'
        ELSE 'other' END         AS disposition_label,
    e.source_evnt_id,
    e.consumer_id_hashed,
    e.load_tm
FROM vt_mu_sample s
INNER JOIN vt_mu_events v
    ON v.CLNT_NO = s.CLNT_NO AND v.unsub_dt = s.unsub_dt
INNER JOIN DTZV01.VENDOR_FEEDBACK_EVENT e
    ON  e.consumer_id_hashed = v.consumer_id_hashed
    AND e.treatment_id       = v.treatment_id
ORDER BY v.CLNT_NO, e.treatment_id, e.disposition_dt_tm;


------------------------------------------------------------------------------
-- Q4: RAW MASTER ROWS behind the sampled treatment_ids — SELECT *, all columns, nothing filtered out.
------------------------------------------------------------------------------
SELECT m.*
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
INNER JOIN (
    SELECT DISTINCT v.consumer_id_hashed, v.treatment_id
    FROM vt_mu_sample s
    INNER JOIN vt_mu_events v
        ON v.CLNT_NO = s.CLNT_NO AND v.unsub_dt = s.unsub_dt
) k
    ON  m.consumer_id_hashed = k.consumer_id_hashed
    AND m.treatment_id       = k.treatment_id
WHERE m.load_tm >= ADD_MONTHS(DATE '2026-04-01', -1)   -- editable: keep aligned with VT 1 margins
  AND m.load_tm <  ADD_MONTHS(DATE '2026-07-01',  1)
ORDER BY m.clnt_no, m.treatment_id;


------------------------------------------------------------------------------
DROP TABLE vt_mu_sample;
DROP TABLE vt_mu_events;
