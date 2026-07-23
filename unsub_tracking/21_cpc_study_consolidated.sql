-- 21_cpc_study_consolidated.sql — CPC landscape (Z), vendor-bridge timing (B), overlap redo w/ 1014 fix (O), enforcement field-fill probe (E)
-- Schema: schemas/cpc_rb_pref_log_schema.md (5001 Yes/5002 No/5003 blank; blank=NO only for 1014/1015). ENGINE: Teradata-direct.
-- vt_params below is the ONLY place to edit trend_start; asof = run date (CURRENT_DATE), all window ends exclusive of asof.
-- Pre-clean DROPs immediately follow; 'does not exist' errors on a fresh session are harmless.

-- pre-clean: clear leftover volatile tables from a prior/aborted run
DROP TABLE vt_cpc_latest;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_vendor_unsub;
DROP TABLE vt_vendor_unsub_hist;
DROP TABLE vt_params;

-- editable: trend_start is the ONLY window edit point; asof is implicit run date
CREATE VOLATILE TABLE vt_params AS (
    SELECT DATE '2025-07-01' AS trend_start, CURRENT_DATE AS asof
) WITH DATA PRIMARY INDEX (trend_start) ON COMMIT PRESERVE ROWS;


-- vt_cpc_latest: current state per (CLNT_NO, PREF_ID) for 1002/1012/1014, feeds Z1 + O
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


-- vt_cpc_flips: flips TO 5002 on 1002/1012/1014 since trend_start, feeds Z2 + Z3 + B-reverse
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
      AND c.CHG_TMSTMP <  vp.asof
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_cpc_flips COLUMN (CLNT_NO);


-- vt_vendor_unsub: first vendor unsub per client, trailing 12mo, feeds B-main + O is_unsub (idiom copied from 19 VT2)
CREATE VOLATILE TABLE vt_vendor_unsub AS (
    WITH events AS (
        SELECT e.consumer_id_hashed, e.TREATMENT_ID, e.disposition_dt_tm
        FROM DTZV01.VENDOR_FEEDBACK_EVENT e
        CROSS JOIN vt_params vp
        WHERE e.disposition_cd = 4
          AND e.disposition_dt_tm >= ADD_MONTHS(vp.asof, -12)   -- trailing 12mo start
          AND e.disposition_dt_tm <  vp.asof
    ),
    resolved AS (
        SELECT
            m.CLNT_NO,
            u.disposition_dt_tm,
            ROW_NUMBER() OVER (PARTITION BY m.CLNT_NO ORDER BY u.disposition_dt_tm ASC) AS rn
        FROM events u
        INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
            ON  m.consumer_id_hashed = u.consumer_id_hashed
            AND m.TREATMENT_ID       = u.TREATMENT_ID
        CROSS JOIN vt_params vp
        WHERE m.load_tm >= ADD_MONTHS(vp.asof, -13)   -- trailing-12mo start - 1mo margin
          AND m.load_tm <  ADD_MONTHS(vp.asof, 1)      -- asof + 1mo margin
    )
    SELECT CLNT_NO, disposition_dt_tm AS first_unsub_tm
    FROM resolved
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vendor_unsub COLUMN (CLNT_NO);


-- vt_vendor_unsub_hist: undeduped vendor unsub events, 24mo lookback from asof, feeds B-reverse nearest-prior gap analysis
CREATE VOLATILE TABLE vt_vendor_unsub_hist AS (
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    CROSS JOIN vt_params vp
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= ADD_MONTHS(vp.asof, -24)   -- 24mo lookback from asof
      AND e.disposition_dt_tm <  vp.asof
      AND m.load_tm           >= ADD_MONTHS(vp.asof, -25)   -- 24mo span - 1mo margin
      AND m.load_tm           <  ADD_MONTHS(vp.asof, 1)      -- asof + 1mo margin
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_vendor_unsub_hist COLUMN (CLNT_NO);


-- ===== BLOCK Z: zoom-out landscape — how much is lost right now, is it accelerating, who writes it =====

-- Z1: stock as-of now by PREF_ID x consent type. 1014 blank=NO -> row-holders only here; effective-No for 1014 = 5002+5003 of row-holders (never-answered clients have no row and can't be counted from the log)
SELECT
    PREF_ID,
    CLNT_CONSENT_TYP,
    CAST(COUNT(*) AS BIGINT) AS clients
FROM vt_cpc_latest
GROUP BY 1, 2
ORDER BY 1, 2;


-- Z2: monthly flow of new 5002 flips since trend_start, per PREF_ID (predates the 2024-03 HSBC spike documented in 07)
SELECT
    EXTRACT(YEAR FROM CHG_TMSTMP) * 100 + EXTRACT(MONTH FROM CHG_TMSTMP) AS flip_month_yyyymm,
    PREF_ID,
    CAST(COUNT(*) AS BIGINT) AS new_optouts
FROM vt_cpc_flips
GROUP BY 1, 2
ORDER BY 1, 2;


-- Z3: writer attribution for the same flips — which APP_SYS_CD produces the opt-outs, per PREF_ID
SELECT
    PREF_ID,
    APP_SYS_CD,
    CAST(COUNT(*) AS BIGINT) AS optout_flips
FROM vt_cpc_flips
GROUP BY 1, 2
ORDER BY 1, 3 DESC;


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
    SUM(CASE WHEN gap_days BETWEEN 181 AND 365 THEN 1 ELSE 0 END) AS gap_181_365d,
    SUM(CASE WHEN gap_days >= 366               THEN 1 ELSE 0 END) AS gap_366p_d,
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


-- ===== BLOCK E: enforcement probe — are purpose/initiator fields filled enough to ever separate marketing vs service sends =====
-- source copied from 12_switch_enforcement_test.sql E3b (lines 190-209); contact_purps_typ/cntct_evnt_initiator confirmed cols #25/#8 in schemas/vendor_feedback_tables_schema.md

-- E1: send-row fill rate by contact_purps_typ, trailing 3mo (NULL forms its own GROUP BY bucket)
SELECT
    m.contact_purps_typ,
    CAST(COUNT(*) AS BIGINT) AS send_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
CROSS JOIN vt_params vp
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= ADD_MONTHS(vp.asof, -3)
  AND e.disposition_dt_tm <  vp.asof
GROUP BY 1
ORDER BY 2 DESC;


-- E2: send-row fill rate by cntct_evnt_initiator, trailing 3mo (NULL forms its own GROUP BY bucket)
SELECT
    m.cntct_evnt_initiator,
    CAST(COUNT(*) AS BIGINT) AS send_rows
FROM DTZV01.VENDOR_FEEDBACK_EVENT e
INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
    ON  m.consumer_id_hashed = e.consumer_id_hashed
    AND m.TREATMENT_ID       = e.TREATMENT_ID
CROSS JOIN vt_params vp
WHERE e.disposition_cd = 1
  AND e.disposition_dt_tm >= ADD_MONTHS(vp.asof, -3)
  AND e.disposition_dt_tm <  vp.asof
GROUP BY 1
ORDER BY 2 DESC;


-- eof: drop volatile tables (children before parent)
DROP TABLE vt_cpc_latest;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_vendor_unsub;
DROP TABLE vt_vendor_unsub_hist;
DROP TABLE vt_params;
