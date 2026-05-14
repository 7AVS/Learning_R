-- AUH Pre-Measurement Sanity Check
-- Establish deployment grain (client vs account) and detect overlap
-- across waves BEFORE running any conversion measurement.
-- All AUH tactic_ids captured via SUBSTR. No labels, no classifications.


-- SC1: Grain summary per deployment
-- Compare row_count vs distinct evnt_ids vs distinct accounts vs distinct clients.
-- Equal across all four = 1:1 grain. Mismatches reveal where dups live.
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    COUNT(*)                                                AS rows,
    COUNT(DISTINCT TACTIC_EVNT_ID)                          AS distinct_evnt_ids,
    COUNT(DISTINCT CAST(TACTIC_EVNT_ID AS DECIMAL(20,0)))   AS distinct_acct_nos,
    COUNT(DISTINCT CLNT_NO)                                 AS distinct_clnt_nos
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
GROUP BY TACTIC_ID, TREATMT_STRT_DT
ORDER BY TACTIC_ID;


-- SC2: Within-deployment client duplications
-- Clients appearing in >1 row within the same TACTIC_ID
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    COUNT(*)                AS clients_with_multiple_rows,
    SUM(per_client_rows)    AS total_dup_rows,
    MAX(per_client_rows)    AS max_rows_per_client
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        CLNT_NO,
        COUNT(*) AS per_client_rows
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY TACTIC_ID, TREATMT_STRT_DT, CLNT_NO
    HAVING COUNT(*) > 1
) x
GROUP BY TACTIC_ID, TREATMT_STRT_DT
ORDER BY TACTIC_ID;


-- SC3: Within-deployment account duplications
-- Accounts appearing in >1 row within the same TACTIC_ID
SELECT
    TACTIC_ID,
    TREATMT_STRT_DT,
    COUNT(*)                AS accounts_with_multiple_rows,
    SUM(per_acct_rows)      AS total_dup_rows,
    MAX(per_acct_rows)      AS max_rows_per_account
FROM (
    SELECT
        TACTIC_ID,
        TREATMT_STRT_DT,
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0)) AS acct_no,
        COUNT(*) AS per_acct_rows
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY TACTIC_ID, TREATMT_STRT_DT, CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))
    HAVING COUNT(*) > 1
) x
GROUP BY TACTIC_ID, TREATMT_STRT_DT
ORDER BY TACTIC_ID;


-- SC4: Cross-deployment client overlap
-- Distribution: how many clients appear in 1, 2, 3+ deployments
SELECT
    deployments_per_client,
    COUNT(*) AS clients
FROM (
    SELECT
        CLNT_NO,
        COUNT(DISTINCT TACTIC_ID) AS deployments_per_client
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY CLNT_NO
) x
GROUP BY deployments_per_client
ORDER BY deployments_per_client;


-- SC5: Cross-deployment account overlap
-- Distribution: how many accounts appear in 1, 2, 3+ deployments
SELECT
    deployments_per_account,
    COUNT(*) AS accounts
FROM (
    SELECT
        CAST(TACTIC_EVNT_ID AS DECIMAL(20,0)) AS acct_no,
        COUNT(DISTINCT TACTIC_ID) AS deployments_per_account
    FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
    WHERE SUBSTR(TACTIC_ID, 8, 3) = 'AUH'
    GROUP BY CAST(TACTIC_EVNT_ID AS DECIMAL(20,0))
) x
GROUP BY deployments_per_account
ORDER BY deployments_per_account;
