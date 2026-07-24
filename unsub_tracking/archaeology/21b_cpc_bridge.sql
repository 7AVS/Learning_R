-- 21b_cpc_bridge.sql — expensive — run in a FRESH session, ideally alone; if killed, note which statement was last to complete
-- Split from 21_cpc_study_consolidated.sql. Unsub resolution pipeline + blocks B-main/B-reverse/O. ENGINE: Teradata-direct.
-- vt_params re-declared (session-scoped); trend_start = ONLY window edit point; floor_dt = hard floor (2024-01-01).
-- Lookback 12mo (was 24) — CPU governor constraint; 181+ band right-censored.

-- pre-clean: clear leftover volatile tables from a prior/aborted run
DROP TABLE vt_cpc_latest;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_vendor_unsub;
DROP TABLE vt_vendor_unsub_hist;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_evt12;
DROP TABLE vt_params;


-- editable: trend_start is the ONLY window edit point; asof is implicit run date; floor_dt = hard floor, no scan reaches earlier (per Andre)
CREATE VOLATILE TABLE vt_params AS (
    SELECT DATE '2025-07-01' AS trend_start, CURRENT_DATE AS asof, DATE '2024-01-01' AS floor_dt
) WITH DATA PRIMARY INDEX (trend_start) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_params COLUMN (trend_start);


-- vt_cpc_latest: re-declared here (session-scoped) for O's cpc_flags -- state recon: no floor (latest row may be old)
CREATE VOLATILE TABLE vt_cpc_latest AS (
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
          AND c.CHG_TMSTMP < vp.asof
    )
    SELECT CLNT_NO, PREF_ID, CLNT_CONSENT_TYP
    FROM ranked
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_latest COLUMN (CLNT_NO);


-- vt_cpc_flips: re-declared here (session-scoped) for B-reverse
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


-- vt_unsub_evt12: STAGE 1 of the MASTER pass — tiny EVENT-only probe, disp=4, 12mo bounded (staged pattern: 19 VT1)
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


-- vt_unsub_resolved: STAGE 2, chunked into 2 sequential MASTER passes (this is the only MASTER touch in the file)
-- pass 1/2: MASTER resolved in 2 sequential passes — each under the CPU governor's budget (load_tm asof-13mo to asof-6mo)
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

-- pass 2/2: same table, load_tm asof-6mo to asof+1mo
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


-- vt_vendor_unsub: first vendor unsub per client, trailing 12mo, derived FROM vt_unsub_resolved (no further MASTER access) -- feeds B-main + O is_unsub
CREATE VOLATILE TABLE vt_vendor_unsub AS (
    WITH ranked AS (
        SELECT
            r.CLNT_NO,
            r.unsub_tm AS first_unsub_tm,
            ROW_NUMBER() OVER (PARTITION BY r.CLNT_NO ORDER BY r.unsub_tm ASC) AS rn
        FROM vt_unsub_resolved r
        CROSS JOIN vt_params vp
        WHERE r.unsub_tm >= ADD_MONTHS(vp.asof, -12)   -- trailing 12mo start
          AND r.unsub_tm <  vp.asof
    )
    SELECT CLNT_NO, first_unsub_tm
    FROM ranked
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vendor_unsub COLUMN (CLNT_NO);


-- vt_vendor_unsub_hist: undeduped vendor unsub events, ~12mo lookback (was 24), derived FROM vt_unsub_resolved (no further MASTER access) -- feeds B-reverse nearest-prior gap analysis
CREATE VOLATILE TABLE vt_vendor_unsub_hist AS (
    SELECT CLNT_NO, unsub_tm
    FROM vt_unsub_resolved
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vendor_unsub_hist COLUMN (CLNT_NO);


-- ===== BLOCK B: bridge timing — how fast, and how completely, does a vendor unsub reach CPC =====

-- B-main: for vendor first-unsubs (trailing 12mo), days-gap to the first matching CPC 5002 flip, per PREF_ID
WITH bridge_events AS (
    SELECT
        v.CLNT_NO,
        c.PREF_ID,
        c.CHG_TMSTMP,
        v.first_unsub_tm,
        ROW_NUMBER() OVER (PARTITION BY v.CLNT_NO, c.PREF_ID ORDER BY c.CHG_TMSTMP ASC) AS rn
    FROM vt_vendor_unsub v
    INNER JOIN DDWV01.CPC_RB_PREF_LOG c
        ON  c.CLNT_NO = v.CLNT_NO
        AND c.PREF_ID IN (1002, 1012, 1014)
        AND c.CLNT_CONSENT_TYP = 5002
        AND c.CHG_TMSTMP > v.first_unsub_tm
)
SELECT
    PREF_ID,
    -- editable: day-gap bands
    SUM(CASE WHEN CHG_TMSTMP <  first_unsub_tm + INTERVAL '2'  DAY THEN 1 ELSE 0 END) AS gap_0_1d,
    SUM(CASE WHEN CHG_TMSTMP >= first_unsub_tm + INTERVAL '2'  DAY
             AND CHG_TMSTMP <  first_unsub_tm + INTERVAL '8'  DAY THEN 1 ELSE 0 END) AS gap_2_7d,
    SUM(CASE WHEN CHG_TMSTMP >= first_unsub_tm + INTERVAL '8'  DAY
             AND CHG_TMSTMP <  first_unsub_tm + INTERVAL '31' DAY THEN 1 ELSE 0 END) AS gap_8_30d,
    SUM(CASE WHEN CHG_TMSTMP >= first_unsub_tm + INTERVAL '31' DAY
             AND CHG_TMSTMP <  first_unsub_tm + INTERVAL '91' DAY THEN 1 ELSE 0 END) AS gap_31_90d,
    SUM(CASE WHEN CHG_TMSTMP >= first_unsub_tm + INTERVAL '91' DAY THEN 1 ELSE 0 END) AS gap_91p_d,
    CAST(COUNT(*) AS BIGINT) AS bridged_clients
FROM bridge_events
WHERE rn = 1
GROUP BY 1
ORDER BY 1;


-- B-reverse: for each CPC 5002 flip on 1002/1012/1014 since trend_start, nearest-prior vendor unsub gap distribution
-- B-reverse DECISION: does a hidden sync exist? tight recurring gap = batch middleman; no_prior dominant = no pipe
WITH nearest_prior AS (
    SELECT
        f.PREF_ID,
        f.CHG_TMSTMP,
        h.unsub_tm,
        ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO, f.PREF_ID, f.CHG_TMSTMP ORDER BY h.unsub_tm DESC) AS rn
    FROM vt_cpc_flips f
    LEFT JOIN vt_vendor_unsub_hist h
        ON  h.CLNT_NO  = f.CLNT_NO
        AND h.unsub_tm <  f.CHG_TMSTMP
    WHERE f.PREF_ID IN (1002, 1012, 1014)
),
gapped AS (
    SELECT
        PREF_ID,
        CAST(CHG_TMSTMP AS DATE) - CAST(unsub_tm AS DATE) AS gap_days   -- NULL unsub_tm -> NULL gap_days (no prior found)
    FROM nearest_prior
    WHERE rn = 1
)
SELECT
    PREF_ID,
    -- editable: gap bands (7-21d fine-grained: batch-cadence hunt)
    SUM(CASE WHEN gap_days = 0                 THEN 1 ELSE 0 END) AS gap_0d,
    SUM(CASE WHEN gap_days = 1                 THEN 1 ELSE 0 END) AS gap_1d,
    SUM(CASE WHEN gap_days BETWEEN 2   AND 3   THEN 1 ELSE 0 END) AS gap_2_3d,
    SUM(CASE WHEN gap_days BETWEEN 4   AND 7   THEN 1 ELSE 0 END) AS gap_4_7d,
    SUM(CASE WHEN gap_days BETWEEN 8   AND 14  THEN 1 ELSE 0 END) AS gap_8_14d,
    SUM(CASE WHEN gap_days BETWEEN 15  AND 21  THEN 1 ELSE 0 END) AS gap_15_21d,
    SUM(CASE WHEN gap_days BETWEEN 22  AND 30  THEN 1 ELSE 0 END) AS gap_22_30d,
    SUM(CASE WHEN gap_days BETWEEN 31  AND 60  THEN 1 ELSE 0 END) AS gap_31_60d,
    SUM(CASE WHEN gap_days BETWEEN 61  AND 90  THEN 1 ELSE 0 END) AS gap_61_90d,
    SUM(CASE WHEN gap_days BETWEEN 91  AND 180 THEN 1 ELSE 0 END) AS gap_91_180d,
    SUM(CASE WHEN gap_days >= 181               THEN 1 ELSE 0 END) AS gap_181p_d,
    SUM(CASE WHEN gap_days IS NULL              THEN 1 ELSE 0 END) AS no_prior_unsub_found,
    CAST(AVG(gap_days) AS DECIMAL(10,1)) AS avg_gap_days_found,
    CAST(COUNT(*) AS BIGINT) AS cpc_flips
FROM gapped
GROUP BY 1
ORDER BY 1;


-- ===== BLOCK O: overlap redo — reachability loss across mechanisms, with the 1014 blank-fix applied =====

WITH cpc_flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5002            THEN 1 ELSE 0 END) AS out_1002,
        MAX(CASE WHEN PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002            THEN 1 ELSE 0 END) AS out_1012,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5002            THEN 1 ELSE 0 END) AS out_1014_explicit,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP IN (5002, 5003)   THEN 1 ELSE 0 END) AS out_1014_effective
    FROM vt_cpc_latest
    GROUP BY 1
),
combined AS (
    SELECT
        COALESCE(u.CLNT_NO, c.CLNT_NO)                     AS CLNT_NO,
        CASE WHEN u.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END  AS is_unsub,
        COALESCE(c.out_1002, 0)                            AS out_1002,
        COALESCE(c.out_1012, 0)                            AS out_1012,
        COALESCE(c.out_1014_explicit, 0)                   AS out_1014_explicit,
        COALESCE(c.out_1014_effective, 0)                  AS out_1014_effective
    FROM vt_vendor_unsub u
    FULL OUTER JOIN cpc_flags c ON c.CLNT_NO = u.CLNT_NO
)
-- note: is_unsub = trailing-12mo FLOW; out_* = current-stock STATE -- state-vs-flow mix, fine for overlap reading (per 08)
SELECT
    is_unsub, out_1002, out_1012, out_1014_explicit, out_1014_effective,
    CAST(COUNT(*) AS BIGINT) AS clients
FROM combined
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC, 5 DESC;


-- eof: drop volatile tables (children before parent)
DROP TABLE vt_cpc_latest;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_vendor_unsub;
DROP TABLE vt_vendor_unsub_hist;
DROP TABLE vt_unsub_resolved;
DROP TABLE vt_unsub_evt12;
DROP TABLE vt_params;
