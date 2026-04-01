-- =============================================================================
-- VBA/VBU Vintage Queries
-- =============================================================================
--
-- Requested by: Daniel Chin, 2026-03-18
-- Author:       Andre (from Daniel's framework)
--
-- Structure:
--   Query 1 — Population counts by MNE, fiscal quarter, test group (T vs C)
--   Query 2 — Full vintage with success join (CTE-based)
--   Query 3 — Monthly trend of leads by MNE
--
-- CTE design:
--   population  — T and C populations from tactic event history
--   success     — PLACEHOLDER — swap when success library is ready
--
-- Table:
--   DG6V01.TACTIC_EVNT_IP_AR_HIST
--
-- MNE filter:
--   SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
--   Excludes MNE in (PER, COL, MCR, OPP) and SUBSTR(TACTIC_ID, 8, 1) <> 'J'
--   (Daniel's standard exclusions carried forward for safety)
--
-- Fiscal year convention:
--   FY starts in November. Nov/Dec roll into next fiscal year.
--   Nov/Dec/Jan = Q1, Feb/Mar/Apr = Q2, May/Jun/Jul = Q3, Aug/Sep/Oct = Q4
--
-- Test vs Control:
--   TREATMENT_ID is used to distinguish treatment (T) from control (C).
--   Convention: TREATMENT_ID ending in '_C' or containing 'CTRL' = control.
--   Adjust the CASE logic below if VBA/VBU uses a different naming scheme.
--
-- Success library:
--   NOT YET AVAILABLE. The success CTE is a placeholder structure.
--   When the success definition is finalized, replace the success CTE
--   with the real join logic (table, key, window, event filter).
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Population Counts by MNE, Fiscal Quarter, Test Group
-- ---------------------------------------------------------------------------
-- Purpose: Denominator counts — how many leads per MNE, quarter, T vs C.
-- No success join needed here; pure population sizing.
-- ---------------------------------------------------------------------------

SELECT
    SUBSTR(TACTIC_ID, 8, 3)                            AS mne,

    -- Fiscal year/quarter: Nov/Dec = next FY Q1, Jan = same-year Q1
    CASE
        WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (11, 12)
            THEN EXTRACT(YEAR FROM TREATMT_STRT_DT) + 1
        ELSE EXTRACT(YEAR FROM TREATMT_STRT_DT)
    END
    || CASE
        WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (11, 12, 1)  THEN 'Q1'
        WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (2, 3, 4)    THEN 'Q2'
        WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (5, 6, 7)    THEN 'Q3'
        WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (8, 9, 10)   THEN 'Q4'
        ELSE 'ERR'
    END                                                 AS fiscal_qtr,

    -- Test vs Control: adjust pattern if VBA/VBU uses different naming
    CASE
        WHEN TREATMENT_ID LIKE '%\_C' ESCAPE '\'
          OR TREATMENT_ID LIKE '%CTRL%'
            THEN 'C'
        ELSE 'T'
    END                                                 AS test_group,

    COUNT(DISTINCT CLNT_NO)                             AS leads

FROM DG6V01.TACTIC_EVNT_IP_AR_HIST

WHERE
    -- VBA and VBU mnemonics only
    SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')

    -- Daniel's standard exclusions (redundant given the IN filter above,
    -- but kept for safety / copy-paste reuse with other MNEs)
    AND SUBSTR(TACTIC_ID, 8, 3) NOT IN ('PER', 'COL', 'MCR', 'OPP')
    AND SUBSTR(TACTIC_ID, 8, 1) <> 'J'

    -- Date range: FY2026 Q1 onward (Nov 2025 start). Adjust as needed.
    AND TREATMT_STRT_DT >= DATE '2025-11-01'

GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- ---------------------------------------------------------------------------
-- QUERY 2: Full Vintage — Population + Success (CTE-based)
-- ---------------------------------------------------------------------------
-- Purpose: Response rate by MNE, fiscal quarter, and test group.
--          LEFT JOINs population to success so non-responders are preserved.
--
-- TODO:    Replace the success CTE with the real success definition
--          once the success library is ready.
-- ---------------------------------------------------------------------------

WITH population AS (
    -- -----------------------------------------------------------------
    -- CTE 1: Population — all T and C leads for VBA/VBU
    -- -----------------------------------------------------------------
    SELECT
        CLNT_NO,
        TACTIC_ID,
        TREATMENT_ID,
        TREATMT_STRT_DT,
        TST_GRP_CD,

        SUBSTR(TACTIC_ID, 8, 3)                        AS mne,

        -- Fiscal year/quarter
        CASE
            WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (11, 12)
                THEN EXTRACT(YEAR FROM TREATMT_STRT_DT) + 1
            ELSE EXTRACT(YEAR FROM TREATMT_STRT_DT)
        END
        || CASE
            WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (11, 12, 1)  THEN 'Q1'
            WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (2, 3, 4)    THEN 'Q2'
            WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (5, 6, 7)    THEN 'Q3'
            WHEN EXTRACT(MONTH FROM TREATMT_STRT_DT) IN (8, 9, 10)   THEN 'Q4'
            ELSE 'ERR'
        END                                             AS fiscal_qtr,

        -- Test vs Control
        CASE
            WHEN TREATMENT_ID LIKE '%\_C' ESCAPE '\'
              OR TREATMENT_ID LIKE '%CTRL%'
                THEN 'C'
            ELSE 'T'
        END                                             AS test_group

    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST

    WHERE
        SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
        AND SUBSTR(TACTIC_ID, 8, 3) NOT IN ('PER', 'COL', 'MCR', 'OPP')
        AND SUBSTR(TACTIC_ID, 8, 1) <> 'J'
        AND TREATMT_STRT_DT >= DATE '2025-11-01'
),

success AS (
    -- -----------------------------------------------------------------
    -- CTE 2: Success events — PLACEHOLDER
    -- -----------------------------------------------------------------
    -- >>> SWAP THIS CTE WHEN SUCCESS LIBRARY IS READY <<<
    --
    -- Expected contract:
    --   - Join key:    CLNT_NO
    --   - Window:      success event occurs within N days of TREATMT_STRT_DT
    --   - Output cols: CLNT_NO, success_dt (date the success event occurred)
    --
    -- Placeholder below uses a fake table and 90-day window.
    -- Replace SUCCESS_TABLE, SUCCESS_DATE_COL, and the window with actuals.
    -- -----------------------------------------------------------------
    SELECT
        p.CLNT_NO,
        s.SUCCESS_DATE_COL                              AS success_dt
    FROM population p
    INNER JOIN SUCCESS_SCHEMA.SUCCESS_TABLE s
        ON  s.CLNT_NO = p.CLNT_NO
        AND s.SUCCESS_DATE_COL BETWEEN p.TREATMT_STRT_DT
                                    AND p.TREATMT_STRT_DT + INTERVAL '90' DAY
)

-- Main query: join population LEFT JOIN success, compute response rates
SELECT
    p.mne,
    p.fiscal_qtr,
    p.test_group,

    COUNT(DISTINCT p.CLNT_NO)                           AS leads,
    COUNT(DISTINCT s.CLNT_NO)                           AS responders,

    -- Response rate as decimal (multiply by 100 in reporting layer)
    CAST(
        COUNT(DISTINCT s.CLNT_NO) * 1.0
        / NULLIF(COUNT(DISTINCT p.CLNT_NO), 0)
        AS DECIMAL(7, 4)
    )                                                   AS response_rate

FROM population p
LEFT JOIN success s
    ON s.CLNT_NO = p.CLNT_NO

GROUP BY
    p.mne,
    p.fiscal_qtr,
    p.test_group

ORDER BY
    p.mne,
    p.fiscal_qtr,
    p.test_group;


-- ---------------------------------------------------------------------------
-- QUERY 3: Monthly Trend of Leads by MNE
-- ---------------------------------------------------------------------------
-- Purpose: Time-series view — how many distinct leads per month per MNE.
--          Useful for spotting deployment cadence and volume shifts.
-- ---------------------------------------------------------------------------

SELECT
    SUBSTR(TACTIC_ID, 8, 3)                            AS mne,

    -- Year-month string for charting
    CAST(EXTRACT(YEAR FROM TREATMT_STRT_DT) AS VARCHAR(4))
    || '-'
    || LPAD(CAST(EXTRACT(MONTH FROM TREATMT_STRT_DT) AS VARCHAR(2)), 2, '0')
                                                        AS year_month,

    COUNT(DISTINCT CLNT_NO)                             AS leads

FROM DG6V01.TACTIC_EVNT_IP_AR_HIST

WHERE
    SUBSTR(TACTIC_ID, 8, 3) IN ('VBA', 'VBU')
    AND SUBSTR(TACTIC_ID, 8, 3) NOT IN ('PER', 'COL', 'MCR', 'OPP')
    AND SUBSTR(TACTIC_ID, 8, 1) <> 'J'
    AND TREATMT_STRT_DT >= DATE '2025-11-01'

GROUP BY 1, 2
ORDER BY 1, 2;
