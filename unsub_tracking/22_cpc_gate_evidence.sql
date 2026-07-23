-- 22_cpc_gate_evidence.sql — 22-A: who writes the bridged flips; 22-B: gate-leak test (Andre's design). ENGINE: Teradata-direct.
-- Reuses 21b's B-reverse pipeline verbatim (vt_unsub_evt12/vt_unsub_resolved 2-pass chunk) + 21a/21b's state-recon pattern.
-- vt_params: trend_start/window_start = editable, one place; floor_dt = hard floor (2024-01-01); asof = implicit run date.
-- Fresh session recommended; pre-clean DROPs follow — 'does not exist' errors on a fresh session are harmless.

-- pre-clean: clear leftover volatile tables from a prior/aborted run
DROP TABLE vt_email_resolved;
DROP TABLE vt_email_evt3;
DROP TABLE vt_cpc_gate_flags;
DROP TABLE vt_cpc_gate_wstart;
DROP TABLE vt_vendor_unsub_hist;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_evt12;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_params;


-- editable: trend_start (22-A) + window_start (22-B) are the ONLY window edit points; floor_dt = hard floor
CREATE VOLATILE TABLE vt_params AS (
    SELECT DATE '2025-07-01' AS trend_start, DATE '2026-04-01' AS window_start, CURRENT_DATE AS asof, DATE '2024-01-01' AS floor_dt
) WITH DATA PRIMARY INDEX (trend_start) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_params COLUMN (trend_start);


-- ===== VOLATILES + STATS — 22-A pieces first, then 22-B pieces =====

-- vt_cpc_flips: flips TO 5002 on 1002/1012/1014 since trend_start, APP_SYS_CD carried -- copied verbatim from 21b
CREATE VOLATILE TABLE vt_cpc_flips AS (
    SELECT
        c.CLNT_NO,
        c.PREF_ID,
        c.CHG_TMSTMP,
        c.APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF_LOG c
    CROSS JOIN vt_params vp
    WHERE c.PREF_ID IN (1002, 1012, 1014)
      AND c.CLNT_CONSENT_TYP = 5002
      AND c.CHG_TMSTMP >= vp.trend_start
      AND c.CHG_TMSTMP >= vp.floor_dt      -- floor: belt-and-suspenders
      AND c.CHG_TMSTMP <  vp.asof
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_flips COLUMN (CLNT_NO);


-- vt_unsub_evt12: STAGE 1 — tiny EVENT-only probe, disp=4, 12mo bounded -- copied verbatim from 21b
CREATE VOLATILE TABLE vt_unsub_evt12 AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= ADD_MONTHS(vp.asof, -12)   -- 12mo lookback start
      AND e.disposition_dt_tm >= vp.floor_dt                 -- floor: belt-and-suspenders
      AND e.disposition_dt_tm <  vp.asof
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_evt12 COLUMN (consumer_id_hashed, TREATMENT_ID);


-- vt_unsub_resolved: STAGE 2, chunked into 2 sequential MASTER passes -- copied verbatim from 21b (only MASTER touch for 22-A)
-- pass 1/2: load_tm asof-13mo to asof-6mo
CREATE VOLATILE TABLE vt_unsub_resolved AS (
    SELECT DISTINCT
        m.CLNT_NO,
        u.disposition_dt_tm AS unsub_tm
    FROM vt_unsub_evt12 u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
    CROSS JOIN vt_params vp
    WHERE m.load_tm >= ADD_MONTHS(vp.asof, -13)
      AND m.load_tm >= ADD_MONTHS(vp.floor_dt, -1)
      AND m.load_tm <  ADD_MONTHS(vp.asof, -6)
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

-- pass 2/2: load_tm asof-6mo to asof+1mo
INSERT INTO vt_unsub_resolved
    SELECT DISTINCT
        m.CLNT_NO,
        u.disposition_dt_tm AS unsub_tm
    FROM vt_unsub_evt12 u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
    CROSS JOIN vt_params vp
    WHERE m.load_tm >= ADD_MONTHS(vp.asof, -6)
      AND m.load_tm >= ADD_MONTHS(vp.floor_dt, -1)
      AND m.load_tm <  ADD_MONTHS(vp.asof, 1);

COLLECT STATISTICS ON vt_unsub_resolved COLUMN (CLNT_NO);


-- vt_vendor_unsub_hist: undeduped vendor unsub events, ~12mo, derived FROM vt_unsub_resolved (no further MASTER access)
CREATE VOLATILE TABLE vt_vendor_unsub_hist AS (
    SELECT CLNT_NO, unsub_tm
    FROM vt_unsub_resolved
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vendor_unsub_hist COLUMN (CLNT_NO);


-- vt_cpc_gate_wstart: state AS-OF window_start (not asof) for 1002/1012/1014 -- reuses vt_cpc_latest logic, bound swapped
CREATE VOLATILE TABLE vt_cpc_gate_wstart AS (
    WITH ranked AS (
        SELECT
            c.CLNT_NO,
            c.PREF_ID,
            c.CLNT_CONSENT_TYP,
            ROW_NUMBER() OVER (PARTITION BY c.CLNT_NO, c.PREF_ID
                               ORDER BY c.CHG_TMSTMP DESC) AS rn
        FROM DDWV01.CPC_RB_PREF_LOG c
        CROSS JOIN vt_params vp
        WHERE c.PREF_ID IN (1002, 1012, 1014)
          AND c.CHG_TMSTMP < vp.window_start
    )
    SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP
    FROM ranked
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_gate_wstart COLUMN (CLNT_NO);


-- vt_cpc_gate_flags: per-client out-flags on 1002/1012/1014 as-of window_start -- feeds 22-B main cut + exclusivity cut
CREATE VOLATILE TABLE vt_cpc_gate_flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1002,
        MAX(CASE WHEN PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1012,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1014
    FROM vt_cpc_gate_wstart
    GROUP BY 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_gate_flags COLUMN (CLNT_NO);


-- vt_email_evt3: STAGE 1 — tiny EVENT-only probe, disp=1 (sent), bounded to the 3mo test window
CREATE VOLATILE TABLE vt_email_evt3 AS (
    SELECT
        e.consumer_id_hashed,
        e.TREATMENT_ID,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 1
      AND e.disposition_dt_tm >= vp.window_start
      AND e.disposition_dt_tm >= vp.floor_dt                       -- floor: belt-and-suspenders
      AND e.disposition_dt_tm <  ADD_MONTHS(vp.window_start, 3)    -- window_start + 3mo (test window end)
) WITH DATA PRIMARY INDEX (consumer_id_hashed, TREATMENT_ID) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_email_evt3 COLUMN (consumer_id_hashed, TREATMENT_ID);


-- vt_email_resolved: STAGE 2 — single MASTER pass, load_tm bound to window ±1mo (5mo span, under chunk threshold)
CREATE VOLATILE TABLE vt_email_resolved AS (
    SELECT DISTINCT m.CLNT_NO
    FROM vt_email_evt3 u
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = u.consumer_id_hashed
        AND m.TREATMENT_ID       = u.TREATMENT_ID
    CROSS JOIN vt_params vp
    WHERE m.load_tm >= ADD_MONTHS(vp.window_start, -1)
      AND m.load_tm >= vp.floor_dt                                 -- floor: belt-and-suspenders
      AND m.load_tm <  ADD_MONTHS(vp.window_start, 4)              -- window end (+3mo) + 1mo pad
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_email_resolved COLUMN (CLNT_NO);


-- ===== 22-A: writer attribution for the bridged flips =====
-- 22-A DECISION: are the ~140 bridged flips SFMC(7020) pipe or assisted-channel coincidence?

WITH nearest_prior_bridge AS (
    SELECT
        f.PREF_ID,
        f.APP_SYS_CD,
        f.CHG_TMSTMP,
        h.unsub_tm,
        ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO, f.PREF_ID, f.CHG_TMSTMP ORDER BY h.unsub_tm DESC) AS rn
    FROM vt_cpc_flips f
    INNER JOIN vt_vendor_unsub_hist h                -- INNER: bridged set only (has a qualifying prior unsub)
        ON  h.CLNT_NO  = f.CLNT_NO
        AND h.unsub_tm <  f.CHG_TMSTMP
    WHERE f.PREF_ID IN (1002, 1012, 1014)
),
bridged AS (
    SELECT
        PREF_ID,
        APP_SYS_CD,
        CAST(CHG_TMSTMP AS DATE) - CAST(unsub_tm AS DATE) AS gap_days
    FROM nearest_prior_bridge
    WHERE rn = 1
)
-- editable: gap bands collapsed to 0-1 / 2-7 / 8-30 / 31+ to keep rows low
SELECT
    PREF_ID,
    APP_SYS_CD,
    SUM(CASE WHEN gap_days BETWEEN 0  AND 1  THEN 1 ELSE 0 END) AS gap_0_1d,
    SUM(CASE WHEN gap_days BETWEEN 2  AND 7  THEN 1 ELSE 0 END) AS gap_2_7d,
    SUM(CASE WHEN gap_days BETWEEN 8  AND 30 THEN 1 ELSE 0 END) AS gap_8_30d,
    SUM(CASE WHEN gap_days >= 31              THEN 1 ELSE 0 END) AS gap_31p_d,
    CAST(COUNT(*) AS BIGINT) AS bridged_flips
FROM bridged
GROUP BY 1, 2
ORDER BY 1, 7 DESC;


-- ===== 22-B: the gate-leak test (Andre's design) =====
-- 22-B DECISION: do explicitly opted-out clients still receive email? raw leak rate = upper bound (purpose fields empty, service mail included — state this in the label)

-- 22-B main cut: flagged (state=5002 as-of window_start) vs received email in [window_start, window_start+3mo)
WITH gate_long AS (
    SELECT CLNT_NO, 1002 AS PREF_ID FROM vt_cpc_gate_flags WHERE out_1002 = 1
    UNION ALL
    SELECT CLNT_NO, 1012 AS PREF_ID FROM vt_cpc_gate_flags WHERE out_1012 = 1
    UNION ALL
    SELECT CLNT_NO, 1014 AS PREF_ID FROM vt_cpc_gate_flags WHERE out_1014 = 1
),
baseline_none AS (
    -- baseline pool = clients WITH some 1002/1012/1014 history but none = 5002, 1-in-10 MOD slice (context only)
    SELECT CLNT_NO
    FROM vt_cpc_gate_flags
    WHERE out_1002 = 0 AND out_1012 = 0 AND out_1014 = 0
      AND MOD(CLNT_NO, 10) = 0                        -- editable: slice modulus (1-in-10), baseline row only
)
SELECT
    CAST(PREF_ID AS VARCHAR(30)) AS gate_cohort,
    CAST(COUNT(*) AS BIGINT) AS flagged_clients,
    CAST(SUM(CASE WHEN er.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS received_email_in_window
FROM gate_long g
LEFT JOIN vt_email_resolved er ON er.CLNT_NO = g.CLNT_NO
GROUP BY 1
UNION ALL
SELECT
    CAST('NONE_baseline_1in10' AS VARCHAR(30)) AS gate_cohort,
    CAST(COUNT(*) AS BIGINT) AS flagged_clients,
    CAST(SUM(CASE WHEN er.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS received_email_in_window
FROM baseline_none b
LEFT JOIN vt_email_resolved er ON er.CLNT_NO = b.CLNT_NO
ORDER BY 1;


-- 22-B second cut: same cohorts split by out-flag EXCLUSIVITY (descriptive, partial bundle-confound control)
WITH excl_long AS (
    SELECT CLNT_NO, 1002 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM vt_cpc_gate_flags WHERE out_1002 = 1
    UNION ALL
    SELECT CLNT_NO, 1012 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM vt_cpc_gate_flags WHERE out_1012 = 1
    UNION ALL
    SELECT CLNT_NO, 1014 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM vt_cpc_gate_flags WHERE out_1014 = 1
)
SELECT
    PREF_ID,
    CASE WHEN flag_count = 1 THEN 'only_this_flag' ELSE 'multi_flag' END AS exclusivity,
    CAST(COUNT(*) AS BIGINT) AS flagged_clients,
    CAST(SUM(CASE WHEN er.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS received_email_in_window
FROM excl_long e
LEFT JOIN vt_email_resolved er ON er.CLNT_NO = e.CLNT_NO
GROUP BY 1, 2
ORDER BY 1, 2;


-- eof: drop volatile tables (children before parent)
DROP TABLE vt_email_resolved;
DROP TABLE vt_email_evt3;
DROP TABLE vt_cpc_gate_flags;
DROP TABLE vt_cpc_gate_wstart;
DROP TABLE vt_vendor_unsub_hist;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_evt12;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_params;
