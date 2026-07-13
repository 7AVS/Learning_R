-- =============================================================================
-- Vendor Feedback (Email) — EDA / Validation Pack
-- =============================================================================
--
-- Tables:
--   DTZV01.VENDOR_FEEDBACK_MASTER   -- email master (one row per send)
--   DTZV01.VENDOR_FEEDBACK_EVENT    -- email disposition events (open/click/unsub/...)
--
-- These two tables have never been fully documented for this pod. This pack
-- gets: column catalog, retention window, disposition distribution, MASTER/EVENT
-- join-key reconciliation, and unsubscribe (disposition_cd=4) attribution coverage.
--
-- ENGINE: Starburst/Trino
--   Every confirmed use of these two tables in this repo runs through Trino:
--     - campaigns/AUH/auh_explore.sql Query 4 header: "(Teradata via Trino — email
--       master)" / "(Teradata via Trino — email events)", queries say "Run this in Trino."
--     - campaigns/AUH/auh_tracking.sql Query 5 uses the same two-part DTZV01.* addressing,
--       no catalog prefix, joined into a Trino-syntax CTE.
--     - schemas/imt_pipeline_edw.py: EMAIL_MASTER/EMAIL_EVENT comment "Teradata via Trino".
--   So this pack is Trino syntax throughout: no QUALIFY, no TOP, no NULLIFZERO, explicit
--   casts only where needed. Addressing matches existing files exactly: DTZV01.<table>,
--   no dw00_im/catalog prefix (per query_engine_guidelines.md, DTZV01.* is used directly).
--
-- Confirmed columns (from repo usage, do NOT invent others):
--   MASTER: TREATMENT_ID (= TACTIC_ID), CLNT_NO, consumer_id_hashed, FEEDBACK_ID, SEND_DT
--   EVENT:  EVENT_TYPE, disposition_cd, disposition_dt_tm, FEEDBACK_ID,
--           consumer_id_hashed, TREATMENT_ID
--   disposition_cd (confirmed AUH Phase 1): 1=sent, 2=opened, 3=clicked, 4=unsubscribed,
--                                            5=hardbounce, 6=complaint
--
-- Two confirmed join paths (both must appear here, per the two source files):
--   (a) FEEDBACK_ID            -- auh_explore.sql Query 4: m.FEEDBACK_ID = e.FEEDBACK_ID
--   (b) consumer_id_hashed + TREATMENT_ID -- auh_tracking.sql Query 5 and
--       schemas/imt_pipeline.py Cell 4b: e.consumer_id_hashed = m.consumer_id_hashed
--       AND e.TREATMENT_ID = m.TREATMENT_ID
--
-- Q0 exists to discover the FULL column list on both tables. Everything from Q1 on
-- uses only the repo-confirmed columns above — treat those as provisional until Q0
-- output is reviewed; column names/types may need adjustment afterward.
--
-- Counts only, no rates computed in SQL (numerators/denominators as separate columns,
-- divide client-side) -- avoids the Teradata pushdown ROUND-9881 hazard on a fully
-- pushed-down single-source (all-Teradata) Trino statement.
--
-- ---------------------------------------------------------------------------
-- EDIT WINDOW BOUNDARIES HERE (repeated as literals below, same convention as
-- auh_explore.sql / auh_tracking.sql -- Trino has no session variables here):
--   Q1b monthly trend, ~24 months : SEND_DT           >= DATE '2024-07-01'
--   Q3/Q4 recent window, last 3 full months (run 2026-07-13): disposition_dt_tm
--                                  >= DATE '2026-04-01' AND < DATE '2026-07-01'
--   Q5 trailing 12 months          : disposition_dt_tm >= DATE '2025-07-01' AND < DATE '2026-07-01'
-- ---------------------------------------------------------------------------


-- ---------------------------------------------------------------------------
-- Q0a: Full column catalog — MASTER
-- ---------------------------------------------------------------------------
-- Proves: complete column list/types for VENDOR_FEEDBACK_MASTER as seen through
-- Trino. Expect to see TREATMENT_ID, CLNT_NO, consumer_id_hashed, FEEDBACK_ID,
-- SEND_DT plus whatever else the table carries that we've never catalogued.
-- ---------------------------------------------------------------------------

SHOW COLUMNS FROM DTZV01.VENDOR_FEEDBACK_MASTER;


-- ---------------------------------------------------------------------------
-- Q0b: Full column catalog — EVENT
-- ---------------------------------------------------------------------------
-- Proves: complete column list/types for VENDOR_FEEDBACK_EVENT as seen through
-- Trino. Expect EVENT_TYPE, disposition_cd, disposition_dt_tm, FEEDBACK_ID,
-- consumer_id_hashed, TREATMENT_ID plus anything uncatalogued.
-- ---------------------------------------------------------------------------

SHOW COLUMNS FROM DTZV01.VENDOR_FEEDBACK_EVENT;


-- ---------------------------------------------------------------------------
-- Q1a: MASTER — volume, cardinality, date range
-- ---------------------------------------------------------------------------
-- Proves: overall MASTER row count, distinct clients/treatments, and the
-- SEND_DT min/max (retention window — how far back this table actually goes).
-- ---------------------------------------------------------------------------

SELECT
    COUNT(*)                        AS master_rows,
    COUNT(DISTINCT CLNT_NO)         AS distinct_clients,
    COUNT(DISTINCT TREATMENT_ID)    AS distinct_treatments,
    MIN(SEND_DT)                    AS min_send_dt,
    MAX(SEND_DT)                    AS max_send_dt
FROM DTZV01.VENDOR_FEEDBACK_MASTER;


-- ---------------------------------------------------------------------------
-- Q1b: MASTER — monthly send volume, last ~24 months
-- ---------------------------------------------------------------------------
-- Proves: volume trend by month of SEND_DT -- confirms retention isn't a hard
-- cutoff mid-window and shows any send-volume anomalies month to month.
-- ---------------------------------------------------------------------------

SELECT
    date_trunc('month', SEND_DT)    AS send_month,
    COUNT(*)                        AS master_rows,
    COUNT(DISTINCT CLNT_NO)         AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_MASTER
WHERE SEND_DT >= DATE '2024-07-01'
GROUP BY date_trunc('month', SEND_DT)
ORDER BY send_month;


-- ---------------------------------------------------------------------------
-- Q2a: EVENT — disposition_cd distribution, whole table
-- ---------------------------------------------------------------------------
-- Proves: how many rows fall into each disposition_cd across all history.
-- Expect codes 1/2/3/4/5/6 (sent/opened/clicked/unsub/hardbounce/complaint);
-- flag anything outside that set as an unknown code to chase down.
-- ---------------------------------------------------------------------------

SELECT
    disposition_cd,
    COUNT(*)                        AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
GROUP BY disposition_cd
ORDER BY event_rows DESC;


-- ---------------------------------------------------------------------------
-- Q2b: EVENT — disposition_cd by year of disposition_dt_tm
-- ---------------------------------------------------------------------------
-- Proves: whether disposition mix is stable year over year, and confirms the
-- EVENT table's own retention window (earliest/latest years present per code).
-- ---------------------------------------------------------------------------

SELECT
    EXTRACT(YEAR FROM disposition_dt_tm)   AS disposition_year,
    disposition_cd,
    COUNT(*)                               AS event_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT
GROUP BY EXTRACT(YEAR FROM disposition_dt_tm), disposition_cd
ORDER BY disposition_year, disposition_cd;


-- ---------------------------------------------------------------------------
-- Q3: MASTER <-> EVENT join-key reconciliation (same window, both join paths)
-- ---------------------------------------------------------------------------
-- Proves: of all EVENT rows in a fixed recent window, how many resolve back to
-- MASTER via (a) FEEDBACK_ID [auh_explore.sql pattern] vs (b) consumer_id_hashed
-- + TREATMENT_ID [auh_tracking.sql / imt_pipeline.py pattern]. Same window for
-- all three counts so (b) and (c) are directly comparable against (a)'s denominator.
-- NOTE: a matched count ABOVE Q3a's denominator = join fan-out (duplicate keys
-- on the MASTER side) — that's a grain finding, record it, don't dismiss it.
-- ---------------------------------------------------------------------------

-- Q3a: EVENT total rows in window (denominator)
SELECT
    COUNT(*)                        AS event_rows_window
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_dt_tm >= DATE '2026-04-01'
  AND disposition_dt_tm <  DATE '2026-07-01';

-- Q3b: EVENT rows matching MASTER via FEEDBACK_ID
SELECT
    COUNT(*)                        AS event_rows_matched_feedback_id
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON e.FEEDBACK_ID = m.FEEDBACK_ID
WHERE e.disposition_dt_tm >= DATE '2026-04-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01';

-- Q3c: EVENT rows matching MASTER via consumer_id_hashed + TREATMENT_ID
SELECT
    COUNT(*)                        AS event_rows_matched_consumer_treatment
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE e.disposition_dt_tm >= DATE '2026-04-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01';


-- ---------------------------------------------------------------------------
-- Q4a: Unsubscribe (disposition_cd=4) key coverage — EVENT alone, no join
-- ---------------------------------------------------------------------------
-- Proves: of unsub events in the window (same as Q3), how many carry non-null
-- TREATMENT_ID / consumer_id_hashed on the EVENT row itself. No join, so these
-- counts cannot be inflated by fan-out — this is the true code-4 row count.
-- ---------------------------------------------------------------------------

SELECT
    COUNT(*)                        AS unsub_rows_total,
    COUNT(TREATMENT_ID)             AS unsub_rows_with_treatment_id,
    COUNT(consumer_id_hashed)       AS unsub_rows_with_consumer_id
FROM DTZV01.VENDOR_FEEDBACK_EVENT
WHERE disposition_cd = 4
  AND disposition_dt_tm >= DATE '2026-04-01'
  AND disposition_dt_tm <  DATE '2026-07-01';

-- ---------------------------------------------------------------------------
-- Q4b: Unsub attribution — resolved to MASTER / client, same window
-- ---------------------------------------------------------------------------
-- Proves: how many unsub events resolve to a MASTER send record and a CLNT_NO.
-- Compare unsub_rows_joined against Q4a's unsub_rows_total: a higher number
-- here = MASTER-side fan-out (duplicate consumer_id_hashed+TREATMENT_ID keys).
-- Uses the consumer_id_hashed+TREATMENT_ID path (Q3c); re-run via FEEDBACK_ID
-- if Q3 shows that path has better coverage.
-- ---------------------------------------------------------------------------

SELECT
    COUNT(*)                        AS unsub_rows_joined,
    COUNT(DISTINCT m.CLNT_NO)       AS distinct_clients_via_master
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2026-04-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01';


-- ---------------------------------------------------------------------------
-- Q5: Unsubs by campaign MNE, trailing 12 months
-- ---------------------------------------------------------------------------
-- Proves: which campaigns (MNE = SUBSTR(TREATMENT_ID, 8, 3), same convention as
-- SUBSTR(TACTIC_ID, 8, 3) elsewhere in the repo) carry the unsubscribe volume.
-- Joined via consumer_id_hashed+TREATMENT_ID (Q3c path); MASTER is the only
-- table carrying TREATMENT_ID's full string reliably as a send-level record.
-- Month dimension kept in the output — pool downstream, not in extraction.
-- Row counts inherit any fan-out found in Q4b; distinct_clients is fan-out-safe.
-- ---------------------------------------------------------------------------

SELECT
    date_trunc('month', e.disposition_dt_tm) AS unsub_month,
    SUBSTR(m.TREATMENT_ID, 8, 3)    AS mne,
    COUNT(*)                        AS unsub_rows,
    COUNT(DISTINCT m.CLNT_NO)       AS distinct_clients
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  e.consumer_id_hashed = m.consumer_id_hashed
    AND e.TREATMENT_ID       = m.TREATMENT_ID
WHERE e.disposition_cd = 4
  AND e.disposition_dt_tm >= DATE '2025-07-01'
  AND e.disposition_dt_tm <  DATE '2026-07-01'
GROUP BY date_trunc('month', e.disposition_dt_tm), SUBSTR(m.TREATMENT_ID, 8, 3)
ORDER BY unsub_month, unsub_rows DESC;
