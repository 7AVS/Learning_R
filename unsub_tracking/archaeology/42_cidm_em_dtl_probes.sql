-- 42 — verification probes: DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL (Email Eligibility Detail)
-- The candidate base for the emailable-base waterfall (its own doc: "can also be used
-- for a waterfall analysis"). Run top to bottom, one statement at a time.
-- Each probe answers ONE question, stated above it.


/* ============================================================================
[1] What does a row look like? (columns, values, grain by eye)
============================================================================ */
SELECT * FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL SAMPLE 10;


/* ============================================================================
[2] Freshness + history: is LOAD_DT a daily snapshot, monthly, or one-off?
============================================================================ */
SELECT MIN(LOAD_DT) AS earliest_load,
       MAX(LOAD_DT) AS latest_load,
       COUNT(DISTINCT LOAD_DT) AS n_loads,
       CAST(COUNT(*) AS BIGINT) AS n_rows_total
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL;

-- rows per load, last 10 loads: stable ~14-15MM per load = full-base snapshots
SELECT LOAD_DT, CAST(COUNT(*) AS BIGINT) AS n_rows,
       COUNT(DISTINCT CLNT_NO) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
GROUP BY 1
ORDER BY 1 DESC;


/* ============================================================================
[3] Grain proof on the LATEST load: one row per client, or duplicates?
============================================================================ */
SELECT dup_rows, COUNT(*) AS n_clients
FROM (
    SELECT CLNT_NO, COUNT(*) AS dup_rows
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
    GROUP BY 1
) t
GROUP BY 1 ORDER BY 1;


/* ============================================================================
[4] The waterfall raw material: every indicator's Y/N/null split on the latest load.
    This one output IS the decomposition of "why clients are in or out of the base".
============================================================================ */
SELECT DELIVERABLE_EM_ADDR_IND, CPC1012_IND, EMAIL_KILL_CLNT_IND,
       SPAM_COMPLAINT_EM_IND, VALID_EM_ADDR_IND, EM_ELIGIBLE_IND,
       CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 7 DESC;


/* ============================================================================
[5] The headline number: emailable base on the latest load (mock expects ~14-15MM)
============================================================================ */
SELECT EM_ELIGIBLE_IND, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
GROUP BY 1;


/* ============================================================================
[6] Cross-check vs what we know: does CPC1012_IND agree with the CPC table?
    Standing 1012 = explicit No in CHC (the table CIDM reads) vs the kill count
    implied by CPC1012_IND = 'N' (or however it encodes - see [4] first).
============================================================================ */
SELECT CAST(COUNT(*) AS BIGINT) AS n_standing_1012_no_in_CHC
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002;

SELECT CPC1012_IND, CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
GROUP BY 1;


/* ============================================================================
[7] Equivalence spot-check: CHC vs CPC_RB_PREF (documented mirrors) - same standing
    1012 counts?
============================================================================ */
SELECT 'DG6V01.CPC_CLNT_PREF_CHC' AS source_table,
       CAST(COUNT(*) AS BIGINT) AS n_1012_explicit_no
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002
UNION ALL
SELECT 'DDWV01.CPC_RB_PREF',
       CAST(COUNT(*) AS BIGINT)
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002;


/* ============================================================================
[8] The Borealis PRODUCTS rule (cpc_products_cd.sql), cloned - standing view.
    Per product switch: how production's rule reads the standing book.
    FALSE = do-not-contact for that product; blank = contactable unless employee.
============================================================================ */
SELECT PREF_ID,
       CASE WHEN CLNT_CONSENT_TYP = 5002                            THEN 'FALSE - explicit No (5002)'
            WHEN CLNT_CONSENT_TYP = 5003
                 AND EMP_ID IS NOT NULL
                 AND EMP_ID NOT IN (999999999999999, 999999999)     THEN 'FALSE - blank + real EMP_ID'
            WHEN CLNT_CONSENT_TYP = 5003                            THEN 'TRUE - blank (contactable default)'
            ELSE                                                         'TRUE - other consent value' END AS borealis_reading,
       COUNT(DISTINCT CLNT_NO) AS n_clients
FROM DG6V01.CPC_CLNT_PREF_CHC
WHERE PREF_ID IN (1004,1006,1010,1020,1021,1023,1024,1025,1026,1027,1028,1030,1031,1034,1044)
GROUP BY 1, 2
ORDER BY 1, 2;


/* ============================================================================
[9] Product switches - FLOW: monthly writes to No, 18-month frame.
    (From CPC_RB_PREF - the proven mirror with the write timestamp. Read as consent
    erosion per product, NOT product-specific choices - most are branch bundles.)
============================================================================ */
SELECT TRIM(EXTRACT(YEAR FROM CAST(CHG_TMSTMP AS DATE))) || '-' ||
         TRIM(CASE WHEN EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)) < 10 THEN '0' ELSE '' END) ||
         TRIM(EXTRACT(MONTH FROM CAST(CHG_TMSTMP AS DATE)))  AS chg_month,
       PREF_ID,
       COUNT(*) AS n_writes_to_no
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID IN (1004,1006,1010,1020,1021,1023,1024,1025,1026,1027,1028,1030,1031,1034,1044)
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-02-01'
GROUP BY 1, 2
ORDER BY 1, 2;


/* ============================================================================
[10] Client-level match: EM_DTL's 1012 flag vs the CPC table's most recent position.
     Clean derivation = explicit-No rows all flagged Y, everything else N;
     off-diagonal cells = population scoping or timing to explain.
============================================================================ */
SELECT COALESCE(c.cpc_standing, 'no 1012 row in CPC')            AS cpc_most_recent_position,
       COALESCE(e.CPC1012_IND, 'not in EM_DTL')                  AS em_dtl_1012_flag,
       CAST(COUNT(*) AS BIGINT)                                  AS n_clients
FROM (
    SELECT CLNT_NO,
           CASE WHEN CLNT_CONSENT_TYP = 5002 THEN 'explicit No (5002)'
                WHEN CLNT_CONSENT_TYP = 5003 THEN 'blank (5003)'
                ELSE                              'Yes / other' END AS cpc_standing
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
) c
FULL OUTER JOIN (
    SELECT CLNT_NO, CPC1012_IND
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
) e ON e.CLNT_NO = c.CLNT_NO
GROUP BY 1, 2
ORDER BY 3 DESC;


/* ============================================================================
[10d] The one-number discrepancy: clients whose LATEST 1012 position in
      CPC_RB_PREF is explicit No (5002) AND who sit in EM_DTL flagged N
      (not suppressed - CIDM would email them). One row, three counts.
============================================================================ */
SELECT CAST(SUM(CASE WHEN c.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)  AS n_cpc_latest_5002,
       CAST(SUM(CASE WHEN e.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)  AS n_em_dtl_flag_N,
       CAST(SUM(CASE WHEN c.CLNT_NO IS NOT NULL
                      AND e.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)  AS n_discrepancy_5002_and_N
FROM (
    SELECT CLNT_NO
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
        AND CLNT_CONSENT_TYP = 5002
) c
FULL OUTER JOIN (
    SELECT CLNT_NO
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
      AND CPC1012_IND = 'N'
) e ON e.CLNT_NO = c.CLNT_NO;


/* ============================================================================
[10b] Grain probe before [10c]: does any client carry MORE than one 1012 row in
      CPC_RB_PREF? dup_rows = 1 only -> table is one-row-per-client on 1012.
============================================================================ */
SELECT dup_rows, COUNT(*) AS n_clients
FROM (
    SELECT CLNT_NO, COUNT(*) AS dup_rows
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
    GROUP BY 1
) t
GROUP BY 1 ORDER BY 1;


/* ============================================================================
[10c] The discrepancy cross-tab, dedup-hardened: EM_DTL 1012 flag vs the client's
      MOST RECENT 1012 row in CPC_RB_PREF (QUALIFY keeps newest by CHG_TMSTMP).
      Discrepancy cells to watch:
        explicit No x N            -> CIDM would email an opted-out client
        Yes x Y                    -> wrongly suppressed
        explicit No x not in EM_DTL-> opted-out clients the gate table doesn't carry
      Tie caveat: identical CHG_TMSTMP rows tie arbitrarily; if [10b] shows dupes,
      add a deterministic tie-breaker (prefer 5002) before quoting numbers.
============================================================================ */
SELECT COALESCE(c.cpc_1012_latest, 'no 1012 row in CPC_RB_PREF')  AS cpc_rb_pref_latest_position,
       COALESCE(e.CPC1012_IND, 'not in EM_DTL')                   AS em_dtl_1012_flag,
       CAST(COUNT(*) AS BIGINT)                                   AS n_clients
FROM (
    SELECT CLNT_NO,
           CASE WHEN CLNT_CONSENT_TYP = 5001 THEN 'Yes (5001)'
                WHEN CLNT_CONSENT_TYP = 5002 THEN 'explicit No (5002)'
                WHEN CLNT_CONSENT_TYP = 5003 THEN 'blank (5003)'
                ELSE 'other consent value' END AS cpc_1012_latest
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
) c
FULL OUTER JOIN (
    SELECT CLNT_NO, CPC1012_IND
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
) e ON e.CLNT_NO = c.CLNT_NO
GROUP BY 1, 2
ORDER BY 3 DESC;
