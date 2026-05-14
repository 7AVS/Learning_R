-- AUH Pre-Measurement Sanity Check (v2)
-- Layered with experiment design: arm x model x action/control.
--
-- Group derivation (validated sources):
--   arm_label   = first 2 chars of TST_GRP_CD
--     NR -> NonReward         (Phase 1 + continuation in Phase 2; no P2 DOE)
--     RN -> Rewards_NoOffer   (P2 DOE Arm 2 - Comm Only)
--     RO -> Rewards_Offer     (P2 DOE Arm 1 - Comm+Offer)
--   model_label = derived by deployment
--     P1 (2026042AUH, Daniel doc): NRGA -> Web, NRR -> Random, NRS -> Model
--     P2+ (Robin's lookup): third char of prefix: R -> Random, M -> Model, W -> Web
--   ac_temp     = _C suffix -> Control, else Action  (TEMP; Robin did not
--                 explicitly confirm but the convention persists in Phase 2 codes)
--
-- DOE source: campaigns/AUH/auh_phase2_doe.md (Phase 2 rewards only,
-- 539,620 total, 70/30 A:C, 50/50 arm split).


-- ============================================================
-- SECTION A: Group profile
-- Lead counts per (deployment x arm x model x ac_temp).
-- The "design audit" — everything else builds on this view.
-- ============================================================

-- A1: Leads per cell per deployment
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    arm_label,
    model_label,
    ac_temp,
    COUNT(*) AS leads
FROM (
    SELECT
        TACTIC_ID, TREATMT_STRT_DT, TST_GRP_CD,
        CASE SUBSTR(TST_GRP_CD, 1, 2)
            WHEN 'NR' THEN 'NonReward'
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE 'Unknown'
        END AS arm_label,
        CASE TACTIC_ID
            WHEN '2026042AUH' THEN
                CASE
                    WHEN TST_GRP_CD LIKE 'NRGA%' THEN 'Web'
                    WHEN TST_GRP_CD LIKE 'NRR%'  THEN 'Random'
                    WHEN TST_GRP_CD LIKE 'NRS%'  THEN 'Model'
                    ELSE 'Unknown'
                END
            ELSE
                CASE SUBSTR(TST_GRP_CD, 3, 1)
                    WHEN 'R' THEN 'Random'
                    WHEN 'M' THEN 'Model'
                    WHEN 'W' THEN 'Web'
                    ELSE 'Unknown'
                END
        END AS model_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) x
GROUP BY TACTIC_ID, TREATMT_STRT_DT, arm_label, model_label, ac_temp
ORDER BY TACTIC_ID, arm_label, model_label, ac_temp;


-- ============================================================
-- SECTION B: Granularity (documentation — was SC1)
-- ============================================================

-- B1: rows vs distinct evnt/account/client per deployment
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    COUNT(*)                                                AS rows_,
    COUNT(DISTINCT TACTIC_EVNT_ID)                          AS distinct_evnt_ids,
    COUNT(DISTINCT CAST(TACTIC_EVNT_ID AS DECIMAL(20,0)))   AS distinct_acct_nos,
    COUNT(DISTINCT CLNT_NO)                                 AS distinct_clnt_nos
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
GROUP BY TACTIC_ID, TREATMT_STRT_DT
ORDER BY TACTIC_ID;


-- ============================================================
-- SECTION C: Within-deployment duplications (documentation — was SC2/SC3/SC7)
-- ============================================================

-- C1: Clients appearing in >1 row within the same deployment
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    COUNT(*)                AS clients_with_multiple_rows,
    SUM(per_client_rows)    AS total_dup_rows,
    MAX(per_client_rows)    AS max_rows_per_client
FROM (
    SELECT TACTIC_ID, TREATMT_STRT_DT, CLNT_NO, COUNT(*) AS per_client_rows
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY TACTIC_ID, TREATMT_STRT_DT, CLNT_NO
    HAVING COUNT(*) > 1
) x
GROUP BY TACTIC_ID, TREATMT_STRT_DT
ORDER BY TACTIC_ID;


-- C2: Accounts appearing in >1 row within the same deployment
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    COUNT(*)                AS accounts_with_multiple_rows,
    SUM(per_acct_rows)      AS total_dup_rows,
    MAX(per_acct_rows)      AS max_rows_per_account
FROM (
    SELECT TACTIC_ID, TREATMT_STRT_DT,
           CAST(TACTIC_EVNT_ID AS DECIMAL(20,0)) AS acct_no,
           COUNT(*) AS per_acct_rows
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY TACTIC_ID, TREATMT_STRT_DT, CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))
    HAVING COUNT(*) > 1
) x
GROUP BY TACTIC_ID, TREATMT_STRT_DT
ORDER BY TACTIC_ID;


-- C3: Full-row inspection for clients with multiple rows in Phase 2
SELECT *
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE TACTIC_ID = '2026119AUH'
  AND CLNT_NO IN (
      SELECT CLNT_NO
      FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
      WHERE TACTIC_ID = '2026119AUH'
      GROUP BY CLNT_NO
      HAVING COUNT(*) > 1
  );


-- ============================================================
-- SECTION D: Cross-deployment overlap (improved — adds cell slicing)
-- ============================================================

-- D1: Clients in N deployments (was SC4)
SELECT deployments_per_client, COUNT(*) AS clients
FROM (
    SELECT CLNT_NO, COUNT(DISTINCT TACTIC_ID) AS deployments_per_client
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY CLNT_NO
) x
GROUP BY deployments_per_client
ORDER BY deployments_per_client;


-- D2: Accounts in N deployments (was SC5)
SELECT deployments_per_account, COUNT(*) AS accounts
FROM (
    SELECT CAST(TACTIC_EVNT_ID AS DECIMAL(20,0)) AS acct_no,
           COUNT(DISTINCT TACTIC_ID) AS deployments_per_account
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))
) x
GROUP BY deployments_per_account
ORDER BY deployments_per_account;


-- D3: Cross-wave clients — where they landed in P2 (by P2 cell)
-- For the ~73K clients in both P1 and P2: counts per P2 (arm, model, ac).
-- Improves on old SC4 by showing distribution across P2 cells.
SELECT
    p2.arm_label,
    p2.model_label,
    p2.ac_temp,
    COUNT(*) AS cross_wave_clients
FROM (
    SELECT CLNT_NO
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026042AUH'
) p1
INNER JOIN (
    SELECT
        CLNT_NO,
        CASE SUBSTR(TST_GRP_CD, 1, 2)
            WHEN 'NR' THEN 'NonReward'
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE 'Unknown'
        END AS arm_label,
        CASE SUBSTR(TST_GRP_CD, 3, 1)
            WHEN 'R' THEN 'Random'
            WHEN 'M' THEN 'Model'
            WHEN 'W' THEN 'Web'
            ELSE 'Unknown'
        END AS model_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026119AUH'
) p2 ON p1.CLNT_NO = p2.CLNT_NO
GROUP BY p2.arm_label, p2.model_label, p2.ac_temp
ORDER BY p2.arm_label, p2.model_label, p2.ac_temp;


-- ============================================================
-- SECTION E: P1 -> P2 transition matrix at cell grain (improved SC6)
-- ============================================================

-- E1: Paired view (P1 arm/model/ac x P2 arm/model/ac), client counts
SELECT
    p1.arm_label     AS p1_arm,
    p1.model_label   AS p1_model,
    p1.ac_temp       AS p1_ac,
    p2.arm_label     AS p2_arm,
    p2.model_label   AS p2_model,
    p2.ac_temp       AS p2_ac,
    COUNT(*) AS clients
FROM (
    SELECT
        CLNT_NO,
        CASE SUBSTR(TST_GRP_CD, 1, 2)
            WHEN 'NR' THEN 'NonReward' ELSE 'Unknown' END AS arm_label,
        CASE
            WHEN TST_GRP_CD LIKE 'NRGA%' THEN 'Web'
            WHEN TST_GRP_CD LIKE 'NRR%'  THEN 'Random'
            WHEN TST_GRP_CD LIKE 'NRS%'  THEN 'Model'
            ELSE 'Unknown'
        END AS model_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026042AUH'
) p1
INNER JOIN (
    SELECT
        CLNT_NO,
        CASE SUBSTR(TST_GRP_CD, 1, 2)
            WHEN 'NR' THEN 'NonReward'
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE 'Unknown'
        END AS arm_label,
        CASE SUBSTR(TST_GRP_CD, 3, 1)
            WHEN 'R' THEN 'Random'
            WHEN 'M' THEN 'Model'
            WHEN 'W' THEN 'Web'
            ELSE 'Unknown'
        END AS model_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026119AUH'
) p2 ON p1.CLNT_NO = p2.CLNT_NO
GROUP BY p1.arm_label, p1.model_label, p1.ac_temp,
         p2.arm_label, p2.model_label, p2.ac_temp
ORDER BY clients DESC;


-- ============================================================
-- SECTION F: SRM (Sample Ratio Mismatch)
-- F2 compares observed vs DOE-designed cell counts (rewards arms).
-- F1 checks the high-level 50/50 arm split.
-- F3 checks A/C ratio consistency (internal — for NonReward where no DOE).
-- F4 sums the chi-square contribution for the overall test statistic.
-- Compute p-value externally (Excel: CHISQ.DIST.RT(stat, df)).
-- ============================================================

-- F1: Arm-level 50/50 split (Rewards arms only — DOE Arm 1 vs Arm 2)
SELECT
    arm_label,
    COUNT(*)            AS observed_n,
    269810              AS designed_n,
    COUNT(*) - 269810   AS diff,
    (COUNT(*) - 269810) * 1.0 / SQRT(269810.0)                  AS std_residual,
    POWER(COUNT(*) - 269810, 2) * 1.0 / 269810.0                AS chi_sq_contrib
FROM (
    SELECT
        CASE SUBSTR(TST_GRP_CD, 1, 2)
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE NULL
        END AS arm_label
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE TACTIC_ID = '2026119AUH'
      AND SUBSTR(TST_GRP_CD, 1, 2) IN ('RN', 'RO')
) x
GROUP BY arm_label
ORDER BY arm_label;


-- F2: Cell-level observed vs designed (12 rewards cells from DOE)
WITH observed AS (
    SELECT
        arm_label, model_label, ac_temp,
        COUNT(*) AS observed_n
    FROM (
        SELECT
            CASE SUBSTR(TST_GRP_CD, 1, 2)
                WHEN 'RN' THEN 'Rewards_NoOffer'
                WHEN 'RO' THEN 'Rewards_Offer'
                ELSE 'Unknown'
            END AS arm_label,
            CASE SUBSTR(TST_GRP_CD, 3, 1)
                WHEN 'R' THEN 'Random'
                WHEN 'M' THEN 'Model'
                WHEN 'W' THEN 'Web'
                ELSE 'Unknown'
            END AS model_label,
            CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE TACTIC_ID = '2026119AUH'
          AND SUBSTR(TST_GRP_CD, 1, 2) IN ('RN', 'RO')
    ) x
    GROUP BY arm_label, model_label, ac_temp
),
designed AS (
              SELECT 'Rewards_Offer'   AS arm_label, 'Web'    AS model_label, 'Action'  AS ac_temp, 45328 AS designed_n
    UNION ALL SELECT 'Rewards_Offer',                'Web',                   'Control',            19426
    UNION ALL SELECT 'Rewards_Offer',                'Model',                 'Action',             44240
    UNION ALL SELECT 'Rewards_Offer',                'Model',                 'Control',            18960
    UNION ALL SELECT 'Rewards_Offer',                'Random',                'Action',             99299
    UNION ALL SELECT 'Rewards_Offer',                'Random',                'Control',            42557
    UNION ALL SELECT 'Rewards_NoOffer',              'Web',                   'Action',             45328
    UNION ALL SELECT 'Rewards_NoOffer',              'Web',                   'Control',            19426
    UNION ALL SELECT 'Rewards_NoOffer',              'Model',                 'Action',             44240
    UNION ALL SELECT 'Rewards_NoOffer',              'Model',                 'Control',            18960
    UNION ALL SELECT 'Rewards_NoOffer',              'Random',                'Action',             99299
    UNION ALL SELECT 'Rewards_NoOffer',              'Random',                'Control',            42557
)
SELECT
    d.arm_label,
    d.model_label,
    d.ac_temp,
    d.designed_n,
    COALESCE(o.observed_n, 0)                                                     AS observed_n,
    COALESCE(o.observed_n, 0) - d.designed_n                                      AS diff,
    (COALESCE(o.observed_n, 0) - d.designed_n) * 1.0 / SQRT(d.designed_n * 1.0)   AS std_residual,
    POWER(COALESCE(o.observed_n, 0) - d.designed_n, 2) * 1.0 / d.designed_n       AS chi_sq_contrib
FROM designed d
LEFT JOIN observed o
       ON d.arm_label   = o.arm_label
      AND d.model_label = o.model_label
      AND d.ac_temp     = o.ac_temp
ORDER BY d.arm_label, d.model_label, d.ac_temp;


-- F3: A/C ratio consistency per (arm x model) cell — internal check
-- (Use when no DOE exists. Outputs observed A:C ratio per cell;
--  if the design rule is "everyone gets 70/30", these should all be 0.70.)
SELECT
    arm_label,
    model_label,
    SUM(CASE WHEN ac_temp = 'Action'  THEN 1 ELSE 0 END)                                AS action_n,
    SUM(CASE WHEN ac_temp = 'Control' THEN 1 ELSE 0 END)                                AS control_n,
    COUNT(*)                                                                            AS total_n,
    SUM(CASE WHEN ac_temp = 'Action'  THEN 1 ELSE 0 END) * 1.0 / COUNT(*)               AS pct_action
FROM (
    SELECT
        TACTIC_ID,
        CASE SUBSTR(TST_GRP_CD, 1, 2)
            WHEN 'NR' THEN 'NonReward'
            WHEN 'RN' THEN 'Rewards_NoOffer'
            WHEN 'RO' THEN 'Rewards_Offer'
            ELSE 'Unknown'
        END AS arm_label,
        CASE TACTIC_ID
            WHEN '2026042AUH' THEN
                CASE
                    WHEN TST_GRP_CD LIKE 'NRGA%' THEN 'Web'
                    WHEN TST_GRP_CD LIKE 'NRR%'  THEN 'Random'
                    WHEN TST_GRP_CD LIKE 'NRS%'  THEN 'Model'
                    ELSE 'Unknown'
                END
            ELSE
                CASE SUBSTR(TST_GRP_CD, 3, 1)
                    WHEN 'R' THEN 'Random'
                    WHEN 'M' THEN 'Model'
                    WHEN 'W' THEN 'Web'
                    ELSE 'Unknown'
                END
        END AS model_label,
        CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
) x
GROUP BY arm_label, model_label
ORDER BY arm_label, model_label;


-- F4: Overall chi-square statistic for F2 (rewards SRM)
-- Sum the cell-level chi_sq contributions. df = 12 - 1 = 11.
-- Compute p-value externally: CHISQ.DIST.RT(stat, 11) in Excel.
WITH observed AS (
    SELECT
        arm_label, model_label, ac_temp,
        COUNT(*) AS observed_n
    FROM (
        SELECT
            CASE SUBSTR(TST_GRP_CD, 1, 2)
                WHEN 'RN' THEN 'Rewards_NoOffer'
                WHEN 'RO' THEN 'Rewards_Offer'
                ELSE 'Unknown'
            END AS arm_label,
            CASE SUBSTR(TST_GRP_CD, 3, 1)
                WHEN 'R' THEN 'Random'
                WHEN 'M' THEN 'Model'
                WHEN 'W' THEN 'Web'
                ELSE 'Unknown'
            END AS model_label,
            CASE WHEN TRIM(TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS ac_temp
        FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
        WHERE TACTIC_ID = '2026119AUH'
          AND SUBSTR(TST_GRP_CD, 1, 2) IN ('RN', 'RO')
    ) x
    GROUP BY arm_label, model_label, ac_temp
),
designed AS (
              SELECT 'Rewards_Offer'   AS arm_label, 'Web'    AS model_label, 'Action'  AS ac_temp, 45328 AS designed_n
    UNION ALL SELECT 'Rewards_Offer',                'Web',                   'Control',            19426
    UNION ALL SELECT 'Rewards_Offer',                'Model',                 'Action',             44240
    UNION ALL SELECT 'Rewards_Offer',                'Model',                 'Control',            18960
    UNION ALL SELECT 'Rewards_Offer',                'Random',                'Action',             99299
    UNION ALL SELECT 'Rewards_Offer',                'Random',                'Control',            42557
    UNION ALL SELECT 'Rewards_NoOffer',              'Web',                   'Action',             45328
    UNION ALL SELECT 'Rewards_NoOffer',              'Web',                   'Control',            19426
    UNION ALL SELECT 'Rewards_NoOffer',              'Model',                 'Action',             44240
    UNION ALL SELECT 'Rewards_NoOffer',              'Model',                 'Control',            18960
    UNION ALL SELECT 'Rewards_NoOffer',              'Random',                'Action',             99299
    UNION ALL SELECT 'Rewards_NoOffer',              'Random',                'Control',            42557
)
SELECT
    SUM(POWER(COALESCE(o.observed_n, 0) - d.designed_n, 2) * 1.0 / d.designed_n) AS chi_sq_total,
    12 - 1 AS df
FROM designed d
LEFT JOIN observed o
       ON d.arm_label   = o.arm_label
      AND d.model_label = o.model_label
      AND d.ac_temp     = o.ac_temp;
