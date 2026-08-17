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
