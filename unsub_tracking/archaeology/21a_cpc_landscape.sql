-- 21a_cpc_landscape.sql — cheap — CPC log + sliced probes only; delivers the zoom-out slide
-- Split from 21_cpc_study_consolidated.sql. Blocks Z1/Z2/Z3 + E1/E2. ENGINE: Teradata-direct.
-- vt_params: trend_start = ONLY window edit point; asof = run date; floor_dt = hard floor (2024-01-01).
-- Pre-clean DROPs follow; 'does not exist' errors on a fresh session are harmless.

-- pre-clean: clear leftover volatile tables from a prior/aborted run
DROP TABLE vt_cpc_latest;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_params;


-- editable: trend_start is the ONLY window edit point; asof is implicit run date; floor_dt = hard floor, no scan reaches earlier (per Andre)
CREATE VOLATILE TABLE vt_params AS (
    SELECT DATE '2025-07-01' AS trend_start, CURRENT_DATE AS asof, DATE '2024-01-01' AS floor_dt
) WITH DATA PRIMARY INDEX (trend_start) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_params COLUMN (trend_start);


-- vt_cpc_latest: current state per (CLNT_NO, PREF_ID) for 1002/1012/1014, feeds Z1 -- state recon: no floor (latest row may be old)
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


-- vt_cpc_flips: flips TO 5002 on 1002/1012/1014 since trend_start, feeds Z2 + Z3
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


-- ===== BLOCK E: enforcement probe — are purpose/initiator fields filled enough to ever separate marketing vs service sends =====
-- source copied from 12_switch_enforcement_test.sql E3b (lines 190-209); contact_purps_typ/cntct_evnt_initiator confirmed cols #25/#8 in schemas/vendor_feedback_tables_schema.md

-- E1: send-row fill rate by contact_purps_typ, trailing 3mo, 1-in-10 client slice -- fill-RATE estimate, counts are 1/10 scale (NULL forms its own GROUP BY bucket)
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
  AND e.disposition_dt_tm >= vp.floor_dt            -- floor: belt-and-suspenders
  AND e.disposition_dt_tm <  vp.asof
  AND m.load_tm           >= vp.floor_dt            -- floor: belt-and-suspenders (MASTER side had no bound at all)
  AND MOD(m.CLNT_NO, 10) = 0                         -- editable: slice modulus (1-in-10)
GROUP BY 1
ORDER BY 2 DESC;


-- E2: send-row fill rate by cntct_evnt_initiator, trailing 3mo, 1-in-10 client slice -- fill-RATE estimate, counts are 1/10 scale (NULL forms its own GROUP BY bucket)
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
  AND e.disposition_dt_tm >= vp.floor_dt            -- floor: belt-and-suspenders
  AND e.disposition_dt_tm <  vp.asof
  AND m.load_tm           >= vp.floor_dt            -- floor: belt-and-suspenders (MASTER side had no bound at all)
  AND MOD(m.CLNT_NO, 10) = 0                         -- editable: slice modulus (1-in-10)
GROUP BY 1
ORDER BY 2 DESC;


-- eof: drop volatile tables
DROP TABLE vt_cpc_latest;
DROP TABLE vt_cpc_flips;
DROP TABLE vt_params;
