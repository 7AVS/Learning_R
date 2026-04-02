-- =============================================================================
-- VBU Validation — Original Vintage Queries (Transcribed)
-- =============================================================================
--
-- Source: VBU Validation workbook (sql tab), Daniel Chin
-- Transcribed: 2026-04-02 from screenshots
-- Platform: Teradata SQL (original runs in Teradata, not Starburst)
--
-- This is the REFERENCE copy of Daniel's VBU vintage framework.
-- The VBA adaptation is in vba_vintage_curves.sql.
--
-- Tables:
--   DG6V01.tactic_evnt_ip_ar_hist — tactic event history
--   d3cv12a.cr_crd_rpts_acct — credit card account snapshot
--   D3CV12A.dly_full_portfolio — daily portfolio (product changes)
--   SYS_CALENDAR.CALENDAR — Teradata system calendar
--
-- Key design decisions in this query:
--   - tst_grp_cd positions 6-8 = FROM product code
--   - TACTIC_DECISN_VRB_INFO first N chars must match tactic_id (validation)
--   - addnl_data_dt used with ADD_MONTHS(-1) + LAST_DAY for ME snapshot
--   - Product changes: exclude no-ops, flip-backs, and prior AIB holders
--   - Vintage = days from treatment start (clamped 0-90)
--   - "Any" = any product change; "Primary" = change to AIB excluding prior
--   - SYS_CALENDAR scaffold zero-fills missing vintage days
--   - Cumulative via window function (ROWS UNBOUNDED PRECEDING)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- QUERY 1: Summary — Leads, Responders, Rates by Fiscal Quarter + Test Group
-- ---------------------------------------------------------------------------

WITH base AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        E.treatmt_end_dt                           AS Treat_End_DT,
        E.addnl_data_dt,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt BETWEEN DATE '2025-09-01' AND DATE '2025-11-30'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
        AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, LENGTH(CAST(E.tactic_id AS VARCHAR(50))))
),
elig AS (
    /* Deployed accounts with launch product snapshot */
    SELECT
        b.clnt_no,
        b.tactic_id,
        b.tst_grp_cd,
        b.Treat_Start_DT,
        b.Treat_End_DT,
        A.acct_no,
        SUBSTR(b.tst_grp_cd, 6, 3)                AS FROM_Product_Code,
        A.prod_cd_current                          AS Prod_ME_Before_Launch
    FROM base b
    JOIN d3cv12a.cr_crd_rpts_acct A
        ON A.clnt_no = b.clnt_no
        AND A.ME_dt = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
        AND (
            (A.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
            OR (A.prod_cd_current IN ('C00', 'C01', 'C02') AND b.tst_grp_cd = 'XX')
        )
        AND A.status = 'OPEN'
),
acct_changes AS (
    /* In-window product changes; exclude no-ops and flip-backs */
    SELECT
        e.clnt_no,
        e.tactic_id,
        e.tst_grp_cd,
        e.Treat_Start_DT,
        e.Treat_End_DT,
        e.acct_no,
        d.visa_prod_cd                             AS New_Product,
        d.DT_record_ext                            AS Dt_Prod_Change
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON d.acct_no = e.acct_no
        AND d.DT_record_ext BETWEEN (e.Treat_Start_DT - INTERVAL '1' DAY)
                                AND (e.Treat_End_DT + INTERVAL '5' DAY)
        AND d.visa_prod_cd <> e.Prod_ME_Before_Launch
        AND d.visa_prod_cd <> e.FROM_Product_Code
),
prior_target AS (
    /* Accounts already at AIB before start (exclude from primary) */
    SELECT DISTINCT e.clnt_no, e.tactic_id, e.acct_no
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON d.acct_no = e.acct_no
        AND d.visa_prod_cd = 'AIB'
        AND d.dt_record_ext < e.Treat_Start_DT
),
responders_any_client AS (
    SELECT DISTINCT clnt_no, tactic_id FROM acct_changes
),
responders_primary_client AS (
    SELECT DISTINCT a.clnt_no, a.tactic_id
    FROM acct_changes a
    LEFT JOIN prior_target p
        ON p.clnt_no = a.clnt_no AND p.tactic_id = a.tactic_id AND p.acct_no = a.acct_no
    WHERE p.acct_no IS NULL
        AND a.New_Product = 'AIB'
)
SELECT
    SUBSTR(b.tactic_id, 8, 3)                     AS MNE,
    b.tst_grp_cd,
    b.Treat_Start_DT,
    CASE
        WHEN MONTH(b.Treat_Start_DT) IN (11, 12) THEN CAST(YEAR(b.Treat_Start_DT) + 1 AS VARCHAR(4))
        ELSE CAST(YEAR(b.Treat_Start_DT) AS VARCHAR(4))
    END
    || CASE
        WHEN MONTH(b.Treat_Start_DT) IN (11, 12, 1) THEN 'Q1'
        WHEN MONTH(b.Treat_Start_DT) IN (2, 3, 4)    THEN 'Q2'
        WHEN MONTH(b.Treat_Start_DT) IN (5, 6, 7)    THEN 'Q3'
        WHEN MONTH(b.Treat_Start_DT) IN (8, 9, 10)   THEN 'Q4'
        ELSE 'error'
    END                                            AS yearqtr,
    COUNT(DISTINCT b.clnt_no)                      AS leads,
    COUNT(DISTINCT ra.clnt_no)                     AS successes_any,
    COUNT(DISTINCT rp.clnt_no)                     AS successes_primary,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT ra.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS success_rate_any,
    CASE WHEN COUNT(DISTINCT b.clnt_no) = 0 THEN 0
        ELSE CAST(COUNT(DISTINCT rp.clnt_no) AS DECIMAL(18,6))
             / CAST(COUNT(DISTINCT b.clnt_no) AS DECIMAL(18,6))
    END                                            AS success_rate_primary
FROM base b
LEFT JOIN responders_any_client ra
    ON ra.clnt_no = b.clnt_no AND ra.tactic_id = b.tactic_id
LEFT JOIN responders_primary_client rp
    ON rp.clnt_no = b.clnt_no AND rp.tactic_id = b.tactic_id
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;


-- ---------------------------------------------------------------------------
-- QUERY 2: Vintage Curves — Daily + Cumulative (0-90 days)
-- ---------------------------------------------------------------------------
-- Client-level vintage: any vs primary (AIB)
-- VBU only, Sep-Nov 2025 deployments
-- ---------------------------------------------------------------------------

WITH base AS (
    SELECT DISTINCT
        E.clnt_no,
        CAST(E.tactic_id AS VARCHAR(50))           AS tactic_id,
        E.treatmt_strt_dt                          AS Treat_Start_DT,
        E.treatmt_end_dt                           AS Treat_End_DT,
        E.addnl_data_dt,
        E.tst_grp_cd
    FROM DG6V01.tactic_evnt_ip_ar_hist E
    WHERE E.treatmt_strt_dt BETWEEN DATE '2025-09-01' AND DATE '2025-11-30'
        AND SUBSTR(E.tactic_id, 8, 3) = 'VBU'
        AND SUBSTR(E.tactic_id, 8, 1) <> 'J'
        AND CAST(E.tactic_id AS VARCHAR(50)) = SUBSTR(CAST(E.tactic_decisn_vrb_info AS VARCHAR(200)), 1, LENGTH(CAST(E.tactic_id AS VARCHAR(50))))
),
elig AS (
    SELECT
        b.clnt_no,
        b.tactic_id,
        b.tst_grp_cd,
        b.Treat_Start_DT,
        b.Treat_End_DT,
        A.acct_no,
        SUBSTR(b.tst_grp_cd, 6, 3)                AS FROM_Product_Code,
        A.prod_cd_current                          AS Prod_ME_Before_Launch
    FROM base b
    JOIN d3cv12a.cr_crd_rpts_acct A
        ON A.clnt_no = b.clnt_no
        AND A.ME_dt = LAST_DAY(ADD_MONTHS(b.addnl_data_dt, -1))
        AND (
            (A.prod_cd_current = SUBSTR(b.tst_grp_cd, 6, 3) AND b.tst_grp_cd <> 'XX')
            OR (A.prod_cd_current IN ('C00', 'C01', 'C02') AND b.tst_grp_cd = 'XX')
        )
        AND A.status = 'OPEN'
),
acct_changes AS (
    SELECT
        e.clnt_no,
        e.tactic_id,
        e.Treat_Start_DT,
        e.Treat_End_DT,
        e.acct_no,
        e.Prod_ME_Before_Launch,
        e.FROM_Product_Code,
        d.visa_prod_cd                             AS New_Product,
        d.DT_record_ext                            AS Dt_Prod_Change
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON d.acct_no = e.acct_no
        AND d.DT_record_ext BETWEEN (e.Treat_Start_DT - INTERVAL '1' DAY)
                                AND (e.Treat_End_DT + INTERVAL '5' DAY)
        AND d.visa_prod_cd <> e.Prod_ME_Before_Launch
        AND d.visa_prod_cd <> e.FROM_Product_Code
),
prior_target AS (
    SELECT DISTINCT e.clnt_no, e.tactic_id, e.acct_no
    FROM elig e
    JOIN D3CV12A.dly_full_portfolio d
        ON d.acct_no = e.acct_no
        AND d.visa_prod_cd = 'AIB'
        AND d.dt_record_ext < e.Treat_Start_DT
),
earliest_any_by_client AS (
    SELECT
        a.clnt_no,
        a.tactic_id,
        a.Treat_Start_DT,
        a.Treat_End_DT,
        MIN(a.Dt_Prod_Change)                      AS First_Change_DT
    FROM acct_changes a
    GROUP BY 1, 2, 3, 4
),
earliest_primary_by_client AS (
    SELECT
        a.clnt_no,
        a.tactic_id,
        a.Treat_Start_DT,
        a.Treat_End_DT,
        MIN(a.Dt_Prod_Change)                      AS First_Change_DT
    FROM acct_changes a
    LEFT JOIN prior_target p
        ON p.clnt_no = a.clnt_no
        AND p.tactic_id = a.tactic_id
        AND p.acct_no = a.acct_no
    WHERE a.New_Product = 'AIB'
        AND p.acct_no IS NULL
    GROUP BY 1, 2, 3, 4
),
vintages_any AS (
    SELECT
        e.clnt_no,
        e.tactic_id,
        e.Treat_Start_DT,
        e.Treat_End_DT,
        CAST(CASE
            WHEN e.First_Change_DT < e.Treat_Start_DT THEN 0
            ELSE e.First_Change_DT - e.Treat_Start_DT
        END AS INTEGER)                            AS vintage
    FROM earliest_any_by_client e
    WHERE CAST(CASE
            WHEN e.First_Change_DT < e.Treat_Start_DT THEN 0
            ELSE e.First_Change_DT - e.Treat_Start_DT
        END AS INTEGER) BETWEEN 0 AND 90
),
vintages_primary AS (
    SELECT
        e.clnt_no,
        e.tactic_id,
        e.Treat_Start_DT,
        e.Treat_End_DT,
        CAST(CASE
            WHEN e.First_Change_DT < e.Treat_Start_DT THEN 0
            ELSE e.First_Change_DT - e.Treat_Start_DT
        END AS INTEGER)                            AS vintage
    FROM earliest_primary_by_client e
    WHERE CAST(CASE
            WHEN e.First_Change_DT < e.Treat_Start_DT THEN 0
            ELSE e.First_Change_DT - e.Treat_Start_DT
        END AS INTEGER) BETWEEN 0 AND 90
),
cohort AS (
    SELECT
        SUBSTR(b.tactic_id, 8, 3)                 AS mne,
        b.Treat_Start_DT,
        b.Treat_End_DT,
        COUNT(DISTINCT b.clnt_no)                  AS leads
    FROM base b
    GROUP BY 1, 2, 3
),
successes_any AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily_any
    FROM vintages_any v
    GROUP BY 1, 2, 3, 4
),
successes_primary AS (
    SELECT
        SUBSTR(v.tactic_id, 8, 3)                 AS mne,
        v.Treat_Start_DT,
        v.Treat_End_DT,
        v.vintage,
        COUNT(DISTINCT v.clnt_no)                  AS success_daily_primary
    FROM vintages_primary v
    GROUP BY 1, 2, 3, 4
),
scaffold AS (
    SELECT
        c.mne,
        c.Treat_Start_DT,
        c.Treat_End_DT,
        c.leads,
        CAST(cal.calendar_date - c.Treat_Start_DT AS INTEGER) AS vintage
    FROM cohort c
    JOIN SYS_CALENDAR.CALENDAR cal
        ON cal.calendar_date BETWEEN c.Treat_Start_DT
            AND CASE
                WHEN (c.Treat_Start_DT + INTERVAL '90' DAY) <= (c.Treat_End_DT + INTERVAL '5' DAY)
                THEN (c.Treat_Start_DT + INTERVAL '90' DAY)
                ELSE (c.Treat_End_DT + INTERVAL '5' DAY)
            END
)
SELECT
    s.mne,
    s.Treat_Start_DT,
    s.Treat_End_DT,
    s.vintage,
    s.leads,
    COALESCE(a.success_daily_any, 0)               AS success_daily_any,
    SUM(COALESCE(a.success_daily_any, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum_any,
    COALESCE(p.success_daily_primary, 0)           AS success_daily_primary,
    SUM(COALESCE(p.success_daily_primary, 0)) OVER (
        PARTITION BY s.mne, s.Treat_Start_DT, s.Treat_End_DT
        ORDER BY s.vintage
        ROWS UNBOUNDED PRECEDING
    )                                              AS success_cum_primary
FROM scaffold s
LEFT JOIN successes_any a
    ON a.mne = s.mne
    AND a.Treat_Start_DT = s.Treat_Start_DT
    AND a.Treat_End_DT = s.Treat_End_DT
    AND a.vintage = s.vintage
LEFT JOIN successes_primary p
    ON p.mne = s.mne
    AND p.Treat_Start_DT = s.Treat_Start_DT
    AND p.Treat_End_DT = s.Treat_End_DT
    AND p.vintage = s.vintage
ORDER BY s.mne, s.Treat_Start_DT, s.Treat_End_DT, s.vintage;
