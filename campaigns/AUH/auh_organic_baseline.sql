/*
=============================================================================
AUH — Organic Authorized User Addition Rate by Fiscal Quarter
=============================================================================

PURPOSE:
  Measures the organic rate at which clients add authorized users, split by
  Rewards vs. NonRewards card holders, by fiscal quarter. This provides the
  baseline response rate inputs required for the AUH campaign MDE calculation.

METHODOLOGY:
  Unit of analysis: client

  Rewards classification (client-level, Rewards precedence):
    - Rewards: any open card with product code in ('AVP','GCP','GPR','IAV','MC2','MC4')
    - NonRewards: ONLY open cards in ('PLT','CLO','MC1','MCP','VPR'), no Rewards card
    - Mixed-portfolio clients (both types) → classified as Rewards

  Fiscal year ends October:
    Q1 = November – January
    Q2 = February – April
    Q3 = May – July
    Q4 = August – October

  Base (denominator):
    Distinct clients who had at least one qualifying open card on at least one
    day during the fiscal quarter, AND were NOT under active AUH campaign
    treatment on that day. The organic check happens at the daily level —
    a client qualifies for the quarter's organic base if there exists at least
    one day in the quarter where they had an open card AND no AUH treatment.

  Success (numerator):
    Distinct clients from the organic base who had an AU addition event
    (dtl_evnt_typ_cd = 191, ADD_RELTN_CD = 3) at any point during that
    fiscal quarter. No same-day matching requirement — quarterly grain only.

  AUH exclusion:
    Clients are excluded on any day they fall within an active AUH treatment
    window (treatmt_strt_dt <= snapshot_date <= treatmt_end_dt) in
    DG6V01.TACTIC_EVNT_IP_AR_HIST, tactic_id LIKE '%AUH%'.
    Null end dates are treated as open-ended (coalesced to DATE '2999-12-31').

DATE RANGE: FY2025 Q1 (Nov 2024) through FY2026 Q4 (Oct 2026)
  Portfolio snapshot dates: DATE '2024-10-31' through DATE '2026-10-30'
  (DT_RECORD_EXT + 1 = effective event date, so DT_RECORD_EXT range is
   DATE '2024-10-31' = DATE '2024-11-01' - 1 through
   DATE '2026-10-30' = DATE '2026-10-31' - 1)

TABLES:
  D3CV12A.DLY_FULL_PORTFOLIO      — daily card portfolio snapshot
  DG6V01.TACTIC_EVNT_IP_AR_HIST  — AUH treatment windows
  D3CV12A.CR_CRD_ACCT_EVNT_DLY   — credit card account events (AU additions)

=============================================================================
*/

WITH

-- ---------------------------------------------------------------------------
-- CTE 1: fiscal_quarters
-- Define start and end dates for each fiscal quarter in scope.
-- Fiscal year ends October: Q1=Nov-Jan, Q2=Feb-Apr, Q3=May-Jul, Q4=Aug-Oct.
-- ---------------------------------------------------------------------------
fiscal_quarters AS (
    SELECT DATE '2024-11-01' AS q_start, DATE '2025-01-31' AS q_end, 'FY2025 Q1' AS fiscal_period
    UNION ALL
    SELECT DATE '2025-02-01', DATE '2025-04-30', 'FY2025 Q2'
    UNION ALL
    SELECT DATE '2025-05-01', DATE '2025-07-31', 'FY2025 Q3'
    UNION ALL
    SELECT DATE '2025-08-01', DATE '2025-10-31', 'FY2025 Q4'
    UNION ALL
    SELECT DATE '2025-11-01', DATE '2026-01-31', 'FY2026 Q1'
    UNION ALL
    SELECT DATE '2026-02-01', DATE '2026-04-30', 'FY2026 Q2'
    UNION ALL
    SELECT DATE '2026-05-01', DATE '2026-07-31', 'FY2026 Q3'
    UNION ALL
    SELECT DATE '2026-08-01', DATE '2026-10-31', 'FY2026 Q4'
),

-- ---------------------------------------------------------------------------
-- CTE 2: elig_accounts
-- Open qualifying cards with product type flags and fiscal quarter assigned.
-- One row per client-date-product. Uses DT_RECORD_EXT + 1 as the effective
-- snapshot date. Filtered to the full analysis window.
-- ---------------------------------------------------------------------------
elig_accounts AS (
    SELECT
        (c.DT_RECORD_EXT + 1)                           AS evnt_dt,
        c.clnt_no,
        CASE
            WHEN UPPER(c.visa_prod_cd) IN ('AVP','GCP','GPR','IAV','MC2','MC4')
                THEN 'Rewards'
            WHEN UPPER(c.visa_prod_cd) IN ('PLT','CLO','MC1','MCP','VPR')
                THEN 'NonRewards'
            ELSE NULL
        END                                              AS prod_type,
        fq.fiscal_period
    FROM D3CV12A.DLY_FULL_PORTFOLIO c
    INNER JOIN fiscal_quarters fq
        ON (c.DT_RECORD_EXT + 1) BETWEEN fq.q_start AND fq.q_end
    WHERE
        -- Restrict DT_RECORD_EXT to the full analysis window
        c.DT_RECORD_EXT >= DATE '2024-11-01' - 1
        AND c.DT_RECORD_EXT <= DATE '2026-10-31' - 1
        -- Open cards only
        AND UPPER(c.status) = 'OPEN'
        -- Only qualifying product codes (Rewards or NonRewards)
        AND UPPER(c.visa_prod_cd) IN (
            'AVP','GCP','GPR','IAV','MC2','MC4',   -- Rewards
            'PLT','CLO','MC1','MCP','VPR'           -- NonRewards
        )
),

-- ---------------------------------------------------------------------------
-- CTE 3: auh_treatments
-- AUH campaign treatment windows per client.
-- Null end dates coalesced to a far-future date (open-ended treatments).
-- ---------------------------------------------------------------------------
auh_treatments AS (
    SELECT
        h.clnt_no,
        h.treatmt_strt_dt,
        COALESCE(h.treatmt_end_dt, DATE '2999-12-31') AS treatmt_end_dt
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST h
    WHERE
        h.tactic_id LIKE '%AUH%'
),

-- ---------------------------------------------------------------------------
-- CTE 4: elig_organic_days
-- Client-day combinations where the client had an open qualifying card AND
-- was NOT under active AUH treatment on that day.
-- This is the organic eligibility check at the daily level.
-- ---------------------------------------------------------------------------
elig_organic_days AS (
    SELECT
        ea.evnt_dt,
        ea.clnt_no,
        ea.prod_type,
        ea.fiscal_period
    FROM elig_accounts ea
    WHERE NOT EXISTS (
        SELECT 1
        FROM auh_treatments h
        WHERE h.clnt_no = ea.clnt_no
          AND ea.evnt_dt BETWEEN h.treatmt_strt_dt AND h.treatmt_end_dt
    )
),

-- ---------------------------------------------------------------------------
-- CTE 5: base_dedup
-- One row per client-quarter with Rewards-precedence classification.
-- A client with ANY Rewards day in the quarter = Rewards for that quarter.
-- A client with only NonRewards days = NonRewards.
-- This de-duplicates and applies the Rewards-precedence rule at quarterly grain.
-- ---------------------------------------------------------------------------
base_dedup AS (
    SELECT
        fiscal_period,
        clnt_no,
        -- Rewards precedence: if client had any Rewards day in the quarter → Rewards
        MAX(CASE WHEN prod_type = 'Rewards' THEN 1 ELSE 0 END) AS has_rewards_day
    FROM elig_organic_days
    GROUP BY
        fiscal_period,
        clnt_no
),

-- ---------------------------------------------------------------------------
-- CTE 6: organic_base
-- Final organic base: one row per client-quarter with segment assignment.
-- ---------------------------------------------------------------------------
organic_base AS (
    SELECT
        fiscal_period,
        clnt_no,
        CASE
            WHEN has_rewards_day = 1 THEN 'Rewards'
            ELSE 'NonRewards'
        END AS segment
    FROM base_dedup
),

-- ---------------------------------------------------------------------------
-- CTE 7: au_events
-- AU addition events from the account events table, with fiscal quarter
-- assigned. Filters to dtl_evnt_typ_cd = 191 AND ADD_RELTN_CD = 3 (AU adds).
-- ---------------------------------------------------------------------------
au_events AS (
    SELECT
        e.evnt_dt,
        e.clnt_no,
        fq.fiscal_period
    FROM D3CV12A.CR_CRD_ACCT_EVNT_DLY e
    INNER JOIN fiscal_quarters fq
        ON e.evnt_dt BETWEEN fq.q_start AND fq.q_end
    WHERE
        e.evnt_dt > DATE '2024-11-01'
        AND e.dtl_evnt_typ_cd = 191
        AND e.ADD_RELTN_CD = 3
),

-- ---------------------------------------------------------------------------
-- CTE 8: succ_candidates
-- Distinct client-quarter pairs where an AU addition event occurred.
-- De-duplication: one row per client per quarter (multiple events → one success).
-- ---------------------------------------------------------------------------
succ_candidates AS (
    SELECT DISTINCT
        fiscal_period,
        clnt_no
    FROM au_events
),

-- ---------------------------------------------------------------------------
-- CTE 9: base_success_joined
-- Join organic base to success candidates at the quarter + client level.
-- A client counts as a success only if they were in the organic base for
-- that quarter AND had at least one AU event during that quarter.
-- ---------------------------------------------------------------------------
base_success_joined AS (
    SELECT
        ob.fiscal_period,
        ob.clnt_no,
        ob.segment,
        CASE WHEN sc.clnt_no IS NOT NULL THEN 1 ELSE 0 END AS is_success
    FROM organic_base ob
    LEFT JOIN succ_candidates sc
        ON ob.fiscal_period = sc.fiscal_period
        AND ob.clnt_no      = sc.clnt_no
)

-- ---------------------------------------------------------------------------
-- FINAL SELECT
-- Aggregate to fiscal quarter level, split by Rewards vs. NonRewards.
-- ---------------------------------------------------------------------------
SELECT
    bsj.fiscal_period                                               AS Fiscal_Period,

    -- Base counts (denominator)
    COUNT(DISTINCT CASE WHEN segment = 'Rewards'    THEN clnt_no END)
                                                                    AS Base_Rewards_Organic,
    COUNT(DISTINCT CASE WHEN segment = 'NonRewards' THEN clnt_no END)
                                                                    AS Base_NonRewards_Organic,

    -- Success counts (numerator)
    COUNT(DISTINCT CASE WHEN segment = 'Rewards'    AND is_success = 1 THEN clnt_no END)
                                                                    AS Success_Rewards_Organic,
    COUNT(DISTINCT CASE WHEN segment = 'NonRewards' AND is_success = 1 THEN clnt_no END)
                                                                    AS Success_NonRewards_Organic

FROM base_success_joined bsj

GROUP BY
    bsj.fiscal_period

ORDER BY
    -- Order chronologically: derive sort key from fiscal period string
    -- FY2025 Q1 → 202501, FY2025 Q4 → 202504, FY2026 Q1 → 202601, etc.
    CAST(SUBSTR(bsj.fiscal_period, 3, 4) AS INTEGER) * 10
    + CAST(SUBSTR(bsj.fiscal_period, 9, 1) AS INTEGER)
;
