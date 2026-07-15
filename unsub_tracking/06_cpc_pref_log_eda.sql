-- =============================================================================
-- CPC Preference Log — first-look EDA (schema discovery ONLY)
-- =============================================================================
-- DDWV01.CPC_RB_PREF_LOG — client preference (do-not-contact) log. Source of
-- truth all campaigns abide by (per team, 2026-07-15). Known so far:
--   - PREF_ID = preference identifier (data dictionary col); code 1014 with
--     CPC = 'N' means out of ALL marketing for the RBC entity.
--   - Log grain (changes over time) — presumed, NOT verified.
-- Purpose here: discover the real schema before writing anything else. We have
-- ZERO verified columns — do NOT extend this pack until S0 output is reviewed
-- (SEND_DT/FEEDBACK_ID lesson: never query assumed columns).
--
-- Downstream intent (next pack, after S0):
--   1. Unsub -> CPC linkage: code-4 clients whose CPC flag changed within N
--      days (validates unsub counts against the trusted source; gives CPC its
--      "why" + campaign attribution).
--   2. Population lost: active base vs opted-out, trend YoY, by source.
--
-- ENGINE: Teradata-direct.
-- =============================================================================

-- S0a: column catalog + sample values
SELECT TOP 5 * FROM DDWV01.CPC_RB_PREF_LOG;

-- S0b: raw size
SELECT CAST(COUNT(*) AS BIGINT) AS cpc_log_rows
FROM DDWV01.CPC_RB_PREF_LOG;

-- After S0 output: add code distribution (PREF_ID x flag value), grain check
-- (rows per client x pref), date-column profiling, and the 1014 slice.
