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


-- SC6: Phase 1 -> Phase 2 config transition matrix for cross-wave clients
-- Pairs each cross-wave client's P1 row with their P2 row, side by side.
-- ac_temp = Action/Control via _C suffix (TEMP — Daniel's P1 convention,
-- assumed to hold for P2; not yet explicitly confirmed by Robin).
SELECT
    p1.TREATMT_MN                              AS p1_treatmt_mn,
    p1.TST_GRP_CD                              AS p1_tst_grp_cd,
    CASE WHEN TRIM(p1.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS p1_ac_temp,
    p1.RPT_GRP_CD                              AS p1_rpt_grp_cd,
    SUBSTR(p1.TACTIC_DECISN_VRB_INFO, 121, 8)  AS p1_channel,
    SUBSTR(p1.TACTIC_DECISN_VRB_INFO,  21, 3)  AS p1_prod_cd,
    p2.TREATMT_MN                              AS p2_treatmt_mn,
    p2.TST_GRP_CD                              AS p2_tst_grp_cd,
    CASE WHEN TRIM(p2.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS p2_ac_temp,
    p2.RPT_GRP_CD                              AS p2_rpt_grp_cd,
    SUBSTR(p2.TACTIC_DECISN_VRB_INFO, 121, 8)  AS p2_channel,
    SUBSTR(p2.TACTIC_DECISN_VRB_INFO,  21, 3)  AS p2_prod_cd,
    COUNT(*)                                   AS clients
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST p1
INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST p2
    ON p1.CLNT_NO = p2.CLNT_NO
WHERE p1.TACTIC_ID = '2026042AUH'
  AND p2.TACTIC_ID = '2026119AUH'
GROUP BY
    p1.TREATMT_MN, p1.TST_GRP_CD,
    CASE WHEN TRIM(p1.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END,
    p1.RPT_GRP_CD,
    SUBSTR(p1.TACTIC_DECISN_VRB_INFO, 121, 8),
    SUBSTR(p1.TACTIC_DECISN_VRB_INFO,  21, 3),
    p2.TREATMT_MN, p2.TST_GRP_CD,
    CASE WHEN TRIM(p2.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END,
    p2.RPT_GRP_CD,
    SUBSTR(p2.TACTIC_DECISN_VRB_INFO, 121, 8),
    SUBSTR(p2.TACTIC_DECISN_VRB_INFO,  21, 3)
ORDER BY clients DESC;


-- SC7: Full row dump for the Phase 2 client(s) with multiple accounts
-- SC1/SC2 surfaced 1 client with 2 rows in 2026119AUH on different accounts.
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


-- SC8: A/C distribution among the 73K cross-wave clients (P1 x P2 matrix)
-- TEMP labels via _C suffix convention (needs explicit P2 confirmation).
-- Answers "of the ~73K cross-wave clients, how many were P1-control vs P1-action,
-- and how did each subset land in P2?"
SELECT
    CASE WHEN TRIM(p1.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS p1_ac_temp,
    CASE WHEN TRIM(p2.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END AS p2_ac_temp,
    COUNT(*) AS clients
FROM DG6V01.TACTIC_EVNT_IP_AR_HIST p1
INNER JOIN DG6V01.TACTIC_EVNT_IP_AR_HIST p2
    ON p1.CLNT_NO = p2.CLNT_NO
WHERE p1.TACTIC_ID = '2026042AUH'
  AND p2.TACTIC_ID = '2026119AUH'
GROUP BY
    CASE WHEN TRIM(p1.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END,
    CASE WHEN TRIM(p2.TST_GRP_CD) LIKE '%\_C' ESCAPE '\' THEN 'Control' ELSE 'Action' END
ORDER BY p1_ac_temp, p2_ac_temp;
