-- =============================================================================
-- Vendor feedback retention probe — how far back does coverage actually go?
-- =============================================================================
-- Settles the MASTER/EVENT retention question empirically instead of from
-- memory (Andre believes 2023; an earlier repo note said mid-2025). Quarterly
-- grain, MASTER and EVENT run SEPARATELY — no join, since retention is a
-- property of each table on its own, not of the attribution chain between them.
--
-- MASTER has no send-date column (schemas/vendor_feedback_tables_schema.md,
-- "Hard facts") -> load_tm is the best available time proxy. It marks when the
-- row entered our warehouse, not when the email was sent — flagged below, not
-- swept under the rug.
-- EVENT has a true event timestamp (disposition_dt_tm) for every journey stage,
-- including disposition_cd=1 (sent), so its min/max is a real coverage bound.
--
-- Distinct-client counts are NOT directly comparable across the two blocks:
-- MASTER counts CLNT_NO (resolved), EVENT counts consumer_id_hashed (EVENT has
-- no CLNT_NO of its own — see join map, §3 of the knowledge doc).
--
-- Deliberately UNWINDOWED (no 2024-01-01 floor) — this IS the retention check;
-- per knowledge-doc §7, join-coverage/retention checks run unwindowed (the
-- standard floor would fake the answer we're trying to get).
-- Counts + distinct clients + min/max only. No rates, no division.
--
-- Answers ONE decision: is a 12-month lookback (pack 19) fully covered for
-- unsubs in the spotlight quarter — i.e. does data reach back that far?
-- If either block returns more than ~20 quarters, that IS the finding
-- (coverage goes back further than expected) — do not truncate the WHERE
-- clause to force the row count down.
--
-- Reviewed 2026-07-22 for Teradata-direct: the quarter-bucket arithmetic below
-- (EXTRACT + integer division) is portable as written — EXTRACT(MONTH ...)
-- returns INTEGER on both engines, and INTEGER/INTEGER truncates identically
-- in Trino and Teradata, so month 1-12 maps to quarter 1-4 the same way on
-- either side. No functional change needed; only the engine tag was wrong.
-- ENGINE: Teradata-direct.
-- =============================================================================


-- Block 1 — MASTER, quarterly grain (load_tm = retention proxy, not a send date)
SELECT
    EXTRACT(YEAR FROM m.load_tm) * 10
      + ((EXTRACT(MONTH FROM m.load_tm) - 1) / 3 + 1) AS yyyyq,
    COUNT(*)                    AS n_rows,
    COUNT(DISTINCT m.CLNT_NO)   AS n_distinct_clients,
    MIN(m.load_tm)              AS min_load_tm,
    MAX(m.load_tm)              AS max_load_tm
FROM DTZV01.VENDOR_FEEDBACK_MASTER m
GROUP BY 1
ORDER BY 1;


-- Block 2 — EVENT, quarterly grain (disposition_dt_tm = true event timestamp)
SELECT
    EXTRACT(YEAR FROM e.disposition_dt_tm) * 10
      + ((EXTRACT(MONTH FROM e.disposition_dt_tm) - 1) / 3 + 1) AS yyyyq,
    COUNT(*)                              AS n_rows,
    COUNT(DISTINCT e.consumer_id_hashed)  AS n_distinct_clients,
    MIN(e.disposition_dt_tm)              AS min_disposition_dt,
    MAX(e.disposition_dt_tm)              AS max_disposition_dt
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
GROUP BY 1
ORDER BY 1;
