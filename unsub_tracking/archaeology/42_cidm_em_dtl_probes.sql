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


/* ============================================================================
[15] METADATA SEARCH — RUNS IN STARBURST (Trino SQL), NOT Teradata Studio.
     information_schema stores table names LOWERCASE: always lower() + lowercase
     patterns. Swap the catalog prefix to sweep each catalog you can see:
     edl0_im / dw00_im / dw00_im_qid / tu30_sa_crc0_bfs.
     Keyword sweep for the backfeed hunt: %unsub% %sfmc% %exact% %optout%
     %opt_out% %consent% %pref% %backfeed%.
     LEADS already spotted in edl0_im (from editor history 2026-08-17):
       prod_uq20_digital.sf_unsubscribe          <- SFMC-shaped unsub table, probe first
       prod_dvh0_avion.avion_houselist_universe_ema...
       prod_brt0_ess.lai0__intelligent_email_manag...
============================================================================ */
SELECT table_catalog, table_schema, table_name
FROM edl0_im.information_schema.tables
WHERE lower(table_name) LIKE '%backfeed%'
   OR lower(table_name) LIKE '%unsub%'
   OR lower(table_name) LIKE '%output%';

-- by column name (add AND table_schema = '...' if slow)
SELECT table_schema, table_name, column_name
FROM edl0_im.information_schema.columns
WHERE lower(column_name) LIKE '%unsub%';

-- first look at the sf_unsubscribe lead
SELECT * FROM edl0_im.prod_uq20_digital.sf_unsubscribe LIMIT 10;


/* ============================================================================
[16] sf_unsubscribe = SFMC tracking-extract unsub EVENT log (Starburst/Trino).
     Fields decoded 2026-08-17: accountid = SFMC business unit (MID);
     oybaccountid = on-your-behalf parent/child BU; jobid = send job;
     listid = audience list (77 ~ All Subscribers); batchid = batch in job;
     subscriberid = SFMC internal id; subscriberkey = subscriber key -
     9-digit, CLNT_NO-shaped [PROBE 2 tests this]; eventdate = unsub moment;
     isunique = first unsub per subscriber per job; domain = email domain.
     [1] freshness/size; [2] subscriberkey = CLNT_NO?; [3] monthly volume vs
     VENDOR_FEEDBACK ~35K/mo - same feed or dead archive?
============================================================================ */
SELECT MIN(eventdate) AS earliest, MAX(eventdate) AS latest, COUNT(*) AS n_rows
FROM edl0_im.prod_uq20_digital.sf_unsubscribe;

SELECT * FROM edl0_im.prod_uq20_digital.sf_unsubscribe
WHERE subscriberkey = '427966379';

-- eventdate is VARCHAR (mixed ISO formats) - slice it as text, never cast/compare to DATE
SELECT substr(eventdate, 1, 7) AS month, COUNT(*) AS n_unsubs
FROM edl0_im.prod_uq20_digital.sf_unsubscribe
WHERE substr(eventdate, 1, 10) >= '2025-07-01'
GROUP BY 1 ORDER BY 1;
-- [16-1] RAN 2026-08-17: earliest 2018-10-11, latest 2026-08-16 (LIVE, daily),
-- n_rows 2,485,941 (~26K/mo avg ~ vendor feed order). [16-2] RAN: Andre's own
-- 2026-08-05 unsub returned; subscriberkey = CLNT_NO confirmed.


/* ============================================================================
[17] TACTIC ATTRIBUTION HUNT (Starburst). Andre's unsub URL carries
     PmvTctID=2026201VRE + jid; sf_unsubscribe carries jobid. Route 1: a
     sf_sent/sf_job sibling with jobid + email/tactic name -> jobid joins every
     unsub to its sending tactic. Route 2: any table capturing PmvTctID.
     (Note: VENDOR_FEEDBACK_EVENT disp-4 already carries TREATMENT_ID - this
     adds an independent, client-keyed SFMC-side route.)
============================================================================ */
SELECT table_name
FROM edl0_im.information_schema.tables
WHERE table_schema = 'prod_uq20_digital'
ORDER BY 1;

SELECT table_schema, table_name, column_name
FROM edl0_im.information_schema.columns
WHERE lower(column_name) LIKE '%tct%'
   OR lower(column_name) LIKE '%tactic%'
   OR lower(column_name) LIKE '%treatment%';


/* ============================================================================
[18] sf_unsubscribe runs ~90-100K rows/mo (RAN 2026-08-17: 2025-10 96,951 /
     2025-11 102,848 / 2025-12 91,795 / 2026-01 93,457 / 2026-02 90,245 /
     2026-03 96,269 / 2026-04 91,147 / 2026-05 64,981 / 2026-06 73,672 /
     2026-07 62,103 / 2026-08p 29,740) = ~3x the vendor feed (~27-35K/mo).
     Also a level DROP from 2026-05. Split the gap: repeat events per client?
     other business units? Then compare n_clients vs vendor monthly.
============================================================================ */
SELECT substr(eventdate, 1, 7) AS month,
       COUNT(*) AS n_rows,
       COUNT(DISTINCT subscriberkey) AS n_clients,
       SUM(CASE WHEN lower(isunique) = 'true' THEN 1 ELSE 0 END) AS n_isunique
FROM edl0_im.prod_uq20_digital.sf_unsubscribe
WHERE substr(eventdate, 1, 10) >= '2025-07-01'
GROUP BY 1 ORDER BY 1;

SELECT accountid, oybaccountid, COUNT(*) AS n_rows,
       COUNT(DISTINCT subscriberkey) AS n_clients
FROM edl0_im.prod_uq20_digital.sf_unsubscribe
WHERE substr(eventdate, 1, 10) >= '2025-07-01'
GROUP BY 1, 2 ORDER BY 3 DESC;


/* ============================================================================
[19] THE BLIND-SPOT NUMBER (federated Starburst - one statement, two catalogs).
     Of distinct clients who unsubscribed in SFMC in the last 12mo, how many
     does CIDM's gate table count emailable TODAY? Headline = the eligible-Y row:
     the selection gate would pick them again. (CPC = golden source; unsubs
     never reach it - this states the gap in the gate's own terms.)
============================================================================ */
WITH unsubs AS (
    SELECT DISTINCT subscriberkey AS clnt_no
    FROM edl0_im.prod_uq20_digital.sf_unsubscribe
    WHERE substr(eventdate, 1, 10) >= '2025-08-01'
)
SELECT COALESCE(e.em_eligible_ind, 'not in EM_DTL') AS em_dtl_eligibility_today,
       COUNT(*) AS n_unsubscribed_clients
FROM unsubs u
LEFT JOIN (
    SELECT clnt_no, em_eligible_ind
    FROM dw00_im.dtztau.cidm_channel_elig_em_dtl
    WHERE load_dt = (SELECT MAX(load_dt) FROM dw00_im.dtztau.cidm_channel_elig_em_dtl)
) e ON CAST(e.clnt_no AS VARCHAR) = u.clnt_no
GROUP BY 1;


/* ============================================================================
[20] Attribution decider - column lists of the three extracts that matter.
     sf_rbc_sendlog with a tactic/treatment/campaign column next to jobid =
     unsub-to-tactic attribution exists client-keyed end to end; else parked.
============================================================================ */
SELECT table_name, column_name, ordinal_position
FROM edl0_im.information_schema.columns
WHERE table_schema = 'prod_uq20_digital'
  AND table_name IN ('sf_rbc_sendlog', 'sf_sent', 'sf_subscribers')
ORDER BY table_name, ordinal_position;


/* ============================================================================
[21] ATTRIBUTION ROUTE CONFIRMED (tactic-column search RAN 2026-08-17, 45 rows):
     sf_rbc_sendlog has pmv1_tactic_id + pmv1_treatment_mnemonic ("Pmv" = the
     PmvTctID URL param) -> sf_unsubscribe x sendlog on jobid = unsub-to-tactic,
     client-keyed, SFMC-side. sf_emailmarketingcampaigndata = tactic dimension
     (tactic_id, cell code, event type, mnemonic, start/end dates).
     CAUTION: dated slice (sf_rbc_sendlog_march_21_31) + _test variants exist -
     prove the main sendlog's coverage window before relying on it.
============================================================================ */
SELECT column_name, ordinal_position
FROM edl0_im.information_schema.columns
WHERE table_schema = 'prod_uq20_digital' AND table_name = 'sf_rbc_sendlog'
ORDER BY ordinal_position;

SELECT * FROM edl0_im.prod_uq20_digital.sf_rbc_sendlog LIMIT 10;


/* ============================================================================
[23] THE INVERSE LINKAGE (Teradata-direct) - 34b asked "of CPC flips, how many had
     a prior SF unsub"; this asks the reverse: of every SF/vendor unsubscriber
     (disp 4, 2024-01 ->), did the 7020 backfeed EVER close their CPC 1012 - and
     with what lag? Expect row 1 (~95%+ NO write) = the missing link, rows 3-4 =
     the working trickle (next-day write-through). Caveats: CPC side = standing
     table (overwritten 7020 writes drop out - measured negligible); multi-unsub
     clients anchored to their FIRST unsub in frame.
============================================================================ */
WITH u AS (
    SELECT m.CLNT_NO, MIN(CAST(e.disposition_dt_tm AS DATE)) AS unsub_dt
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN (SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO
                FROM DTZV01.VENDOR_FEEDBACK_MASTER
                WHERE SUBSTR(TREATMENT_ID, 1, 7) BETWEEN '2023274' AND '2026212'
                  AND CLNT_NO IS NOT NULL) m
      ON  m.consumer_id_hashed = e.consumer_id_hashed
      AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2024-01-01'
      AND e.disposition_dt_tm <  DATE '2026-08-01'
      AND CHARACTER_LENGTH(TRIM(e.TREATMENT_ID)) = 10
      AND SUBSTR(e.TREATMENT_ID, 1, 7) BETWEEN '0000000' AND '9999999'
    GROUP BY 1
),
w AS (
    SELECT CLNT_NO, CAST(CHG_TMSTMP AS DATE) AS write_dt
    FROM DDWV01.CPC_RB_PREF
    WHERE PREF_ID = 1012
      AND CLNT_CONSENT_TYP = 5002
      AND APP_SYS_CD = 7020
)
SELECT CASE WHEN w.CLNT_NO IS NULL              THEN '1. NO 7020 CPC write - the missing link'
            WHEN w.write_dt <  u.unsub_dt       THEN '2. 7020 write BEFORE this unsub (older opt-out)'
            WHEN w.write_dt -  u.unsub_dt <= 1  THEN '3. written within 0-1 days (backfeed worked)'
            WHEN w.write_dt -  u.unsub_dt <= 7  THEN '4. written within 2-7 days'
            ELSE                                     '5. written 8+ days later'
       END                                          AS outcome,
       CAST(COUNT(*) AS BIGINT)                     AS n_clients,
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(5,1)) AS pct
FROM u
LEFT JOIN w ON w.CLNT_NO = u.CLNT_NO
GROUP BY 1
ORDER BY 1;


/* ============================================================================
[22] Unsub -> tactic via the sendlog, using the HEFMOMENTS team's PRODUCTION
     join spec (found 2026-08-17, PR #658 rbc-to/a5w0-hef_mortgage_moments):
     events join sendlog ON jobid+listid+batchid+subscriberid=subid; clients
     join ON CLNT_NO = SUBSCRIBERKEY (their code - confirms our key proof).
     [22a] sample: unsubs with sending tactic. [22b] match rate - does the
     sendlog cover the recent window (dated slices exist, coverage unproven)?
============================================================================ */
SELECT u.subscriberkey                AS clnt_no,
       u.eventdate                    AS unsub_time,
       u.jobid,
       s.pmv1_tactic_id,
       s.pmv1_treatment_mnemonic,
       s.send_date,
       s.send_classification
FROM edl0_im.prod_uq20_digital.sf_unsubscribe u
LEFT JOIN edl0_im.prod_uq20_digital.sf_rbc_sendlog s
       ON  u.jobid        = s.jobid
       AND u.listid       = s.listid
       AND u.batchid      = s.batchid
       AND u.subscriberid = s.subid
WHERE substr(u.eventdate, 1, 10) >= '2026-07-01'
LIMIT 100;

SELECT COUNT(*)                                              AS n_unsubs,
       SUM(CASE WHEN s.jobid IS NOT NULL THEN 1 ELSE 0 END)  AS n_matched_to_sendlog
FROM edl0_im.prod_uq20_digital.sf_unsubscribe u
LEFT JOIN edl0_im.prod_uq20_digital.sf_rbc_sendlog s
       ON  u.jobid        = s.jobid
       AND u.listid       = s.listid
       AND u.batchid      = s.batchid
       AND u.subscriberid = s.subid
WHERE substr(u.eventdate, 1, 10) >= '2026-07-01';
