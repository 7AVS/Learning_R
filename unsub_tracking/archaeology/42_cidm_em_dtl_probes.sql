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
[11] Volumes side by side: 1012 opted-out in CPC_RB_PREF vs EM_DTL flag N.
     (EM_DTL note: opted-out = CPC1012_IND 'Y'; 'N' = contactable side.)
============================================================================ */
SELECT CAST('CPC_RB_PREF: 1012 = 5002' AS VARCHAR(40)) AS source_table,
       CAST(COUNT(*) AS BIGINT) AS n_clients
FROM DDWV01.CPC_RB_PREF
WHERE PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002

UNION ALL

SELECT 'EM_DTL: CPC1012_IND = N',
       CAST(COUNT(*) AS BIGINT)
FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
  AND CPC1012_IND = 'N';


/* ============================================================================
[12] EMAIL DRAFT to the CIDM steward (copy below, fill in the name)
==============================================================================

Subject: EM eligibility - two questions on how EM_DTL is built

Hi [name],

I'm reconciling our emailable-base numbers against CIDM_CHANNEL_ELIG_EM_DTL and
the EM Eligibility spec, and I'd like to confirm two things:

1. Is the consent piece of EM_DTL built only from DG6V01.CPC_CLNT_PREF_CHC
   (Pref 1012), or do other sources or processes also feed the table? I hit a
   case where a client shows ineligible in EM_DTL while their 1012 in CPC is
   open, so I want to make sure I'm not missing an input.

2. The spec says EM channel eligibility must be used together with EM Contact
   Eligibility (%emcontacteligible). What does that check contain, and where
   does it live? I want to know what I'd miss by reading EM_DTL alone.

Thanks!
Andre

============================================================================ */


/* ============================================================================
[13] Mismatch examples, 10 each. A: CPC latest 1012 = 5002 but EM_DTL flag = N
     (CIDM would email an opted-out client). B: CPC latest = 5001 but flag = Y
     (wrongly suppressed). Empty result = clean mirror (expected from [10]);
     rows returned -> CHG_TMSTMP/APP_SYS_CD show timing drift vs structural.
============================================================================ */
SELECT c.CLNT_NO, c.CLNT_CONSENT_TYP, c.CHG_TMSTMP, c.APP_SYS_CD, e.CPC1012_IND
FROM (
    SELECT CLNT_NO, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
) c
JOIN DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL e
  ON e.CLNT_NO = c.CLNT_NO
 AND e.LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
WHERE c.CLNT_CONSENT_TYP = 5002
  AND e.CPC1012_IND = 'N'
SAMPLE 10;

SELECT c.CLNT_NO, c.CLNT_CONSENT_TYP, c.CHG_TMSTMP, c.APP_SYS_CD, e.CPC1012_IND
FROM (
    SELECT CLNT_NO, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
) c
JOIN DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL e
  ON e.CLNT_NO = c.CLNT_NO
 AND e.LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
WHERE c.CLNT_CONSENT_TYP = 5001
  AND e.CPC1012_IND = 'Y'
SAMPLE 10;


/* ============================================================================
[14] Opposite direction: sample EM_DTL by flag value, look up each client's
     LATEST 1012 row (max CHG_TMSTMP) in CPC_CLNT_PREF_CHC - the table the
     spec cites. Expected: Y sample -> 5002/employee-blank; N sample ->
     5001/blank/no-row (nulls on CPC columns = client has no 1012 row).
============================================================================ */
SELECT e.CLNT_NO, e.CPC1012_IND, c.CLNT_CONSENT_TYP, c.CHG_TMSTMP, c.APP_SYS_CD
FROM (
    SELECT CLNT_NO, CPC1012_IND
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
      AND CPC1012_IND = 'Y'
    SAMPLE 10
) e
LEFT JOIN (
    SELECT CLNT_NO, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
    FROM DG6V01.CPC_CLNT_PREF_CHC
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
) c ON c.CLNT_NO = e.CLNT_NO;

SELECT e.CLNT_NO, e.CPC1012_IND, c.CLNT_CONSENT_TYP, c.CHG_TMSTMP, c.APP_SYS_CD
FROM (
    SELECT CLNT_NO, CPC1012_IND
    FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL
    WHERE LOAD_DT = (SELECT MAX(LOAD_DT) FROM DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL)
      AND CPC1012_IND = 'N'
    SAMPLE 10
) e
LEFT JOIN (
    SELECT CLNT_NO, CLNT_CONSENT_TYP, CHG_TMSTMP, APP_SYS_CD
    FROM DG6V01.CPC_CLNT_PREF_CHC
    WHERE PREF_ID = 1012
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) = 1
) c ON c.CLNT_NO = e.CLNT_NO;
