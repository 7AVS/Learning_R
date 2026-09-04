-- !! READS DDWV01.CPC_RB_PREF_LOG, WHICH IS BROKEN (~1% of writes). DO NOT RUN. Historical only. Andre 2026-09-04 !!
-- volatile tables vt_unsub_base + vt_unsub_first + vt_q2_send_detail + vt_q2_sends + vt_dns_1002 + vt_gate_cohorts + vt_postunsub_sends persist per session; DROP all seven at end
-- rerun after failure: run all seven DROPs first
-- run ONE statement at a time
-- heavy builds (vt_unsub_base, vt_q2_send_detail) are chunked into monthly bites - if a statement dies, rerun ONLY that CREATE/INSERT, nothing else is lost; 35 statements total in this file (was 31)
-- Universe: NBA campaign email (SFMC vendor feed); CPC opt-out = explicit No on switches 1002 (entity DNS), 1012 (banking email), 1014 (marketing sharing) - the 3 email-relevant of ~40 codes.

-- SETUP pass 1/4: unsub base - trailing-12-month resolved unsub events, MASTER load_tm chunk 1 (2025-06-01 to 2025-10-01); EVENT disposition window (2025-07-01 to 2026-07-01) held constant across all 4 passes; now also carries TREATMENT_ID for Evidence 8's unsub_mne
CREATE VOLATILE TABLE vt_unsub_base AS (
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm,
        m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-07-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2025-06-01'
      AND m.load_tm           <  DATE '2025-10-01'
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

-- SETUP pass 2/4: same table, MASTER load_tm chunk 2 (2025-10-01 to 2026-02-01)
INSERT INTO vt_unsub_base
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm,
        m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-07-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2025-10-01'
      AND m.load_tm           <  DATE '2026-02-01';

-- SETUP pass 3/4: same table, MASTER load_tm chunk 3 (2026-02-01 to 2026-05-01)
INSERT INTO vt_unsub_base
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm,
        m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-07-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2026-02-01'
      AND m.load_tm           <  DATE '2026-05-01';

-- SETUP pass 4/4: same table, MASTER load_tm chunk 4 (2026-05-01 to 2026-08-01)
INSERT INTO vt_unsub_base
    SELECT DISTINCT
        m.CLNT_NO,
        e.disposition_dt_tm AS unsub_tm,
        m.TREATMENT_ID
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd = 4
      AND e.disposition_dt_tm >= DATE '2025-07-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2026-05-01'
      AND m.load_tm           <  DATE '2026-08-01';

COLLECT STATISTICS ON vt_unsub_base COLUMN (CLNT_NO);


-- SETUP dedup: first unsub per client - runs AFTER both passes are loaded, derived from vt_unsub_base (no further MASTER access); now also derives unsub_mne = SUBSTR(TREATMENT_ID,8,3) from the winning row for Evidence 8
CREATE VOLATILE TABLE vt_unsub_first AS (
    WITH ranked AS (
        SELECT
            CLNT_NO,
            unsub_tm,
            TREATMENT_ID,
            ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY unsub_tm ASC, TREATMENT_ID ASC) AS rn
        FROM vt_unsub_base
    )
    SELECT CLNT_NO, unsub_tm, SUBSTR(TREATMENT_ID, 8, 3) AS unsub_mne
    FROM ranked
    WHERE rn = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_unsub_first COLUMN (CLNT_NO);


-- SETUP 3 pass 1/3: heavy send scan, April (EVENT 2026-04-01 to 2026-05-01, MASTER load_tm 2026-03-01 to 2026-06-01, +/-1mo margin around the send month); everything downstream derives from this table (EVENT disp IN (1,5); not cohort-restricted, serves all consumers)
CREATE VOLATILE TABLE vt_q2_send_detail AS (
    SELECT
        m.CLNT_NO,
        SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
        e.disposition_cd,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd IN (1, 5)
      AND e.disposition_dt_tm >= DATE '2026-04-01'
      AND e.disposition_dt_tm <  DATE '2026-05-01'
      AND m.load_tm           >= DATE '2026-03-01'
      AND m.load_tm           <  DATE '2026-06-01'
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

-- SETUP 3 pass 2/3: same table, May (EVENT 2026-05-01 to 2026-06-01, MASTER load_tm 2026-04-01 to 2026-07-01); EVENT windows are disjoint per pass (Apr/May/Jun each fall in exactly one pass), so no (EVENT,MASTER) match can be produced twice even though MASTER windows overlap between passes
INSERT INTO vt_q2_send_detail
    SELECT
        m.CLNT_NO,
        SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
        e.disposition_cd,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd IN (1, 5)
      AND e.disposition_dt_tm >= DATE '2026-05-01'
      AND e.disposition_dt_tm <  DATE '2026-06-01'
      AND m.load_tm           >= DATE '2026-04-01'
      AND m.load_tm           <  DATE '2026-07-01';

-- SETUP 3 pass 3/3: same table, June (EVENT 2026-06-01 to 2026-07-01, MASTER load_tm 2026-05-01 to 2026-08-01)
INSERT INTO vt_q2_send_detail
    SELECT
        m.CLNT_NO,
        SUBSTR(m.TREATMENT_ID, 8, 3) AS mne,
        e.disposition_cd,
        e.disposition_dt_tm
    FROM DTZV01.VENDOR_FEEDBACK_EVENT e
    INNER JOIN DTZV01.VENDOR_FEEDBACK_MASTER m
        ON  m.consumer_id_hashed = e.consumer_id_hashed
        AND m.TREATMENT_ID       = e.TREATMENT_ID
    WHERE e.disposition_cd IN (1, 5)
      AND e.disposition_dt_tm >= DATE '2026-06-01'
      AND e.disposition_dt_tm <  DATE '2026-07-01'
      AND m.load_tm           >= DATE '2026-05-01'
      AND m.load_tm           <  DATE '2026-08-01';

COLLECT STATISTICS ON vt_q2_send_detail COLUMN (CLNT_NO);

-- SETUP 4/4: vt_q2_sends derived from vt_q2_send_detail (disp=1 clients only) - volatile-to-volatile, no EVENT/MASTER access; same name+stats so Evidence 4/5/SUMMARY are untouched
CREATE VOLATILE TABLE vt_q2_sends AS (
    SELECT DISTINCT CLNT_NO
    FROM vt_q2_send_detail
    WHERE disposition_cd = 1
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_q2_sends COLUMN (CLNT_NO);


-- EVIDENCE 1: two consent worlds, monthly volumes (email unsubs outnumber CPC opt-outs ~35x)
-- window: Jul 2025 - Jun 2026 for both series
SELECT
    CAST('email_unsub' AS VARCHAR(30)) AS consent_world,
    EXTRACT(YEAR FROM unsub_tm) * 100 + EXTRACT(MONTH FROM unsub_tm) AS month_yyyymm,
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) AS clients
FROM vt_unsub_first
GROUP BY 1, 2
UNION ALL
SELECT
    'cpc_optout',
    EXTRACT(YEAR FROM CHG_TMSTMP) * 100 + EXTRACT(MONTH FROM CHG_TMSTMP),
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT)
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014)
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-07-01'
  AND CHG_TMSTMP <  DATE '2026-07-01'
GROUP BY 1, 2
ORDER BY 1, 2;


-- EVIDENCE 2: the blind gate (~99.6% of unsubscribers have no explicit CPC opt-out)
-- unsubs: Jul 2025 - Jun 2026; consent standing: latest answer ever (any time, before or after the unsub - deliberately generous to CPC); before/after split vs each client's first unsub
WITH cpc_latest AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        CHG_TMSTMP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
    -- consent standing = latest answer ever recorded (full history; small table - deliberate exception to the 2024 scan floor)
),
cpc_optout_detail AS (
    SELECT CLNT_NO, PREF_ID, CHG_TMSTMP
    FROM cpc_latest
    WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002
),
cpc_optout AS (
    SELECT DISTINCT CLNT_NO
    FROM cpc_optout_detail
),
-- multi-switch clients: a client can hold a qualifying (latest-state) opt-out on more than one switch;
-- count them ONCE using the EARLIEST qualifying CHG_TMSTMP for the before/after test below
cpc_optout_earliest AS (
    SELECT CLNT_NO, MIN(CHG_TMSTMP) AS optout_chg_tmstmp
    FROM cpc_optout_detail
    GROUP BY CLNT_NO
),
flagged AS (
    SELECT
        u.CLNT_NO,
        CASE WHEN co.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS has_cpc_optout,
        CASE WHEN ce.optout_chg_tmstmp IS NOT NULL AND ce.optout_chg_tmstmp <  u.unsub_tm THEN 1 ELSE 0 END AS optout_before_unsub,
        CASE WHEN ce.optout_chg_tmstmp IS NOT NULL AND ce.optout_chg_tmstmp >= u.unsub_tm THEN 1 ELSE 0 END AS optout_after_unsub
    FROM vt_unsub_first u
    LEFT JOIN cpc_optout co ON co.CLNT_NO = u.CLNT_NO
    LEFT JOIN cpc_optout_earliest ce ON ce.CLNT_NO = u.CLNT_NO
)
SELECT CAST('unsub_clients_total' AS VARCHAR(30)) AS metric, CAST(COUNT(*) AS BIGINT) AS clients FROM flagged
UNION ALL
SELECT 'with_explicit_cpc_optout', CAST(SUM(has_cpc_optout) AS BIGINT) FROM flagged
UNION ALL
SELECT 'without_explicit_cpc_optout', CAST(SUM(1 - has_cpc_optout) AS BIGINT) FROM flagged
UNION ALL
SELECT 'optout_recorded_before_unsub', CAST(SUM(optout_before_unsub) AS BIGINT) FROM flagged
UNION ALL
SELECT 'optout_recorded_after_unsub', CAST(SUM(optout_after_unsub) AS BIGINT) FROM flagged
ORDER BY 1;


-- EVIDENCE 3: no bridge (CPC opt-outs are not triggered by email unsubs; only trace = SFMC on 1012, ~15/yr)
-- window: CPC flips since Jul 2025, matched against any prior unsub (no cap on how far back the unsub can be)
WITH cpc_flips AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        APP_SYS_CD,
        CHG_TMSTMP
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
      AND CLNT_CONSENT_TYP = 5002
      AND CHG_TMSTMP >= DATE '2025-07-01'
),
nearest_prior AS (
    SELECT
        f.PREF_ID,
        f.APP_SYS_CD,
        u.CLNT_NO AS matched_unsub,
        ROW_NUMBER() OVER (PARTITION BY f.CLNT_NO, f.PREF_ID, f.CHG_TMSTMP ORDER BY u.unsub_tm DESC) AS rn
    FROM cpc_flips f
    LEFT JOIN vt_unsub_first u
        ON  u.CLNT_NO  = f.CLNT_NO
        AND u.unsub_tm <  f.CHG_TMSTMP
)
SELECT
    PREF_ID,
    APP_SYS_CD,
    CASE WHEN matched_unsub IS NOT NULL THEN 'Y' ELSE 'N' END AS had_prior_unsub,
    CAST(COUNT(*) AS BIGINT) AS flips
FROM nearest_prior
WHERE rn = 1
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC;


-- EVIDENCE 4: the leaking gate (clients with explicit email opt-outs still receive campaign email)
-- window: opted out before Apr 1 2026 -> did they get campaign email Apr-Jun 2026?
WITH cpc_gate AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
      AND CHG_TMSTMP < DATE '2026-04-01'
    -- standing as of Apr 1, 2026 = latest answer ever recorded before that date
),
gate_flags AS (
    SELECT
        CLNT_NO,
        MAX(CASE WHEN PREF_ID = 1002 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1002,
        MAX(CASE WHEN PREF_ID = 1012 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1012,
        MAX(CASE WHEN PREF_ID = 1014 AND CLNT_CONSENT_TYP = 5002 THEN 1 ELSE 0 END) AS out_1014
    FROM cpc_gate
    WHERE rn = 1
    GROUP BY 1
),
gate_long AS (
    SELECT CLNT_NO, 1002 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM gate_flags WHERE out_1002 = 1
    UNION ALL
    SELECT CLNT_NO, 1012 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM gate_flags WHERE out_1012 = 1
    UNION ALL
    SELECT CLNT_NO, 1014 AS PREF_ID, (out_1002 + out_1012 + out_1014) AS flag_count FROM gate_flags WHERE out_1014 = 1
)
SELECT
    CAST(PREF_ID AS VARCHAR(30)) AS pref_id,
    CASE WHEN flag_count = 1 THEN 'only_this_flag' ELSE 'multi_flag' END AS exclusivity,
    CAST(COUNT(*) AS BIGINT) AS optout_clients,
    CAST(SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT) AS got_email_apr_jun
FROM gate_long g
LEFT JOIN vt_q2_sends s ON s.CLNT_NO = g.CLNT_NO
GROUP BY 1, 2
UNION ALL
SELECT
    'ALL_SWITCHES',
    'any_flag',
    CAST(COUNT(*) AS BIGINT),
    CAST(SUM(CASE WHEN s.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END) AS BIGINT)
FROM (SELECT DISTINCT CLNT_NO FROM gate_long) g
LEFT JOIN vt_q2_sends s ON s.CLNT_NO = g.CLNT_NO
ORDER BY 1, 2;


-- EVIDENCE 5: does the email channel itself honor unsubscribes? (claim: unsub IS enforced at the vendor - the disconnect is the bank's record, not the send)
-- window: unsubscribed Jul 2025 - Mar 2026 -> any campaign email Apr-Jun 2026?
WITH cohort AS (
    SELECT CLNT_NO
    FROM vt_unsub_first
    WHERE unsub_tm < DATE '2026-04-01'
)
SELECT CAST('unsub_before_apr_clients' AS VARCHAR(30)) AS metric, CAST(COUNT(*) AS BIGINT) AS clients FROM cohort
UNION ALL
SELECT 'got_email_apr_jun', CAST(COUNT(*) AS BIGINT)
FROM cohort c
INNER JOIN vt_q2_sends s ON s.CLNT_NO = c.CLNT_NO
ORDER BY 1;


-- EVIDENCE 6: which campaigns' mail reaches 1002 (entity do-not-solicit) clients? (rules out 'it's just transactional' - every row is a campaign deployment)
-- cohort: latest 1002 standing before Apr 1, 2026 = 5002 (mirrors E4's cpc_gate, 1002 only; full-history standing, Option-A exception - small table)
CREATE VOLATILE TABLE vt_dns_1002 AS (
    WITH cpc_gate_1002 AS (
        SELECT
            CLNT_NO,
            CLNT_CONSENT_TYP,
            ROW_NUMBER() OVER (PARTITION BY CLNT_NO ORDER BY CHG_TMSTMP DESC) AS rn
        FROM DDWV01.CPC_RB_PREF_LOG
        WHERE PREF_ID = 1002
          AND CHG_TMSTMP < DATE '2026-04-01'
    )
    SELECT CLNT_NO
    FROM cpc_gate_1002
    WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_dns_1002 COLUMN (CLNT_NO);

-- sends: from vt_q2_send_detail (disp=1 filter), restricted to the 1002 cohort - reuses the heavy send-detail build above (3 monthly passes), no fresh EVENT/MASTER scan
-- window: campaign email Apr-Jun 2026, by mne = SUBSTR(TREATMENT_ID, 8, 3)
SELECT TOP 20
    s.mne,
    CAST(COUNT(DISTINCT s.CLNT_NO) AS BIGINT) AS clients,
    CAST(COUNT(*) AS BIGINT) AS send_rows
FROM vt_q2_send_detail s
INNER JOIN vt_dns_1002 d ON d.CLNT_NO = s.CLNT_NO
WHERE s.disposition_cd = 1
GROUP BY 1
ORDER BY 2 DESC;


-- EVIDENCE 7: which campaigns' mail reaches clients standing opted-out, per switch (1002 entity / 1012 email / 1014 sharing / 1006 credit-card content)
-- cohort: latest answer ever recorded before Apr 1, 2026 = 5002, one row per (CLNT_NO, PREF_ID) - same Option-A idiom as E4/E6
CREATE VOLATILE TABLE vt_gate_cohorts AS (
    WITH cpc_gate_all AS (
        SELECT
            CLNT_NO,
            PREF_ID,
            CLNT_CONSENT_TYP,
            ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
        FROM DDWV01.CPC_RB_PREF_LOG
        WHERE PREF_ID IN (1002, 1012, 1014, 1006)
          AND CHG_TMSTMP < DATE '2026-04-01'
    )
    SELECT CLNT_NO, PREF_ID
    FROM cpc_gate_all
    WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_gate_cohorts COLUMN (CLNT_NO);

-- note: 1006 = product-content preference (blank/no-record = implicit YES; only an explicit No counts here) - the topic-gate test
-- send detail with mne: reads vt_q2_send_detail directly (disp=1 filter folded into the join) - no separate build, no fresh EVENT/MASTER scan
SELECT
    CAST(g.PREF_ID AS VARCHAR(30)) AS pref_id,
    CAST(s.mne AS VARCHAR(30)) AS mne,
    CAST(COUNT(DISTINCT g.CLNT_NO) AS BIGINT) AS clients
FROM vt_gate_cohorts g
INNER JOIN vt_q2_send_detail s ON s.CLNT_NO = g.CLNT_NO AND s.disposition_cd = 1
GROUP BY 1, 2
QUALIFY ROW_NUMBER() OVER (PARTITION BY g.PREF_ID ORDER BY COUNT(DISTINCT g.CLNT_NO) DESC) <= 12
ORDER BY 1, 3 DESC;


-- post-unsub send detail: derived from vt_q2_send_detail, restricted to the pre-Apr unsub cohort - no fresh EVENT/MASTER scan, carries mne + disposition for Evidence 8's waterfall
CREATE VOLATILE TABLE vt_postunsub_sends AS (
    SELECT DISTINCT
        p.CLNT_NO,
        p.mne,
        p.disposition_cd,
        p.disposition_dt_tm
    FROM vt_q2_send_detail p
    INNER JOIN (SELECT CLNT_NO FROM vt_unsub_first WHERE unsub_tm < DATE '2026-04-01') c
        ON c.CLNT_NO = p.CLNT_NO
) WITH DATA PRIMARY INDEX (CLNT_NO) ON COMMIT PRESERVE ROWS;

COLLECT STATISTICS ON vt_postunsub_sends COLUMN (CLNT_NO);

-- EVIDENCE 8: decompose the 25,721 post-unsub receivers - how much survives the benign explanations (red-team round 2)
-- method: exclusions applied cumulatively in the order below (CASL-window lag -> hardbounce -> CPC re-consent -> same/cross-campaign residual); hardbounce match is at the mne grain (proxy for same TREATMENT_ID); 14 calendar days = 10-business-day CASL proxy
WITH cohort AS (
    -- pre-Apr unsub cohort with its triggering mne, copied from Evidence 5 / vt_unsub_first
    SELECT CLNT_NO, unsub_tm, unsub_mne
    FROM vt_unsub_first
    WHERE unsub_tm < DATE '2026-04-01'
),
sent AS (
    -- this cohort's disp=1 sends, joined to the client's own unsub_tm/unsub_mne
    SELECT
        p.CLNT_NO,
        p.mne,
        p.disposition_dt_tm,
        c.unsub_tm,
        c.unsub_mne
    FROM vt_postunsub_sends p
    INNER JOIN cohort c ON c.CLNT_NO = p.CLNT_NO
    WHERE p.disposition_cd = 1
),
bounced AS (
    -- (client, mne) pairs with a disp=5 in the same feed - the hardbounce proxy, mne grain not literal TREATMENT_ID
    SELECT DISTINCT CLNT_NO, mne
    FROM vt_postunsub_sends
    WHERE disposition_cd = 5
),
sent_flagged AS (
    SELECT
        s.CLNT_NO,
        s.mne,
        s.unsub_mne,
        CASE WHEN s.disposition_dt_tm <= s.unsub_tm + INTERVAL '14' DAY THEN 1 ELSE 0 END AS in_casl_window,
        CASE WHEN b.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS mne_bounced
    FROM sent s
    LEFT JOIN bounced b ON b.CLNT_NO = s.CLNT_NO AND b.mne = s.mne
),
client_rollup AS (
    SELECT
        CLNT_NO,
        MIN(in_casl_window) AS all_in_casl_window,
        MIN(mne_bounced)    AS all_bounced
    FROM sent_flagged
    GROUP BY CLNT_NO
),
step2 AS (
    -- survives excl #1: at least one send outside the 14-day CASL window
    SELECT CLNT_NO
    FROM client_rollup
    WHERE all_in_casl_window = 0
),
step3 AS (
    -- survives excl #2: not every send hardbounced (client_rollup's all_bounced uses the client's FULL send history, not just the step2 survivors)
    SELECT s2.CLNT_NO
    FROM step2 s2
    INNER JOIN client_rollup cr ON cr.CLNT_NO = s2.CLNT_NO
    WHERE cr.all_bounced = 0
),
reconsent AS (
    -- caveat: catches CPC-side re-consent only; SFMC-side resubscribes are not visible in this feed
    SELECT DISTINCT p.CLNT_NO
    FROM DDWV01.CPC_RB_PREF_LOG p
    INNER JOIN cohort c ON c.CLNT_NO = p.CLNT_NO
    WHERE p.PREF_ID IN (1002, 1012)
      AND p.CLNT_CONSENT_TYP = 5001
      AND p.CHG_TMSTMP >  c.unsub_tm
      AND p.CHG_TMSTMP >= DATE '2025-07-01'
      AND p.CHG_TMSTMP <  DATE '2026-07-01'
),
step4 AS (
    -- survives excl #3: no CPC-side re-consent after unsub
    SELECT s3.CLNT_NO
    FROM step3 s3
    LEFT JOIN reconsent r ON r.CLNT_NO = s3.CLNT_NO
    WHERE r.CLNT_NO IS NULL
),
same_campaign_residual AS (
    -- residual split uses only sends that survived excl #1-2 (outside CASL window, not bounced) - the sends that actually count as real receipt
    SELECT DISTINCT sf.CLNT_NO
    FROM sent_flagged sf
    INNER JOIN step4 s4 ON s4.CLNT_NO = sf.CLNT_NO
    WHERE sf.in_casl_window = 0
      AND sf.mne_bounced = 0
      AND sf.mne = sf.unsub_mne
),
cross_campaign_residual AS (
    SELECT s4.CLNT_NO
    FROM step4 s4
    LEFT JOIN same_campaign_residual scr ON scr.CLNT_NO = s4.CLNT_NO
    WHERE scr.CLNT_NO IS NULL
)
SELECT CAST('0 unsubscribed before Apr 2026 (cohort)' AS VARCHAR(60)) AS step, CAST(COUNT(*) AS BIGINT) AS clients FROM cohort
UNION ALL
SELECT '1 gross: received any send Apr-Jun', CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT) FROM sent
UNION ALL
SELECT '2 excl. sends within 14 days of unsub (CASL proxy)', CAST(COUNT(*) AS BIGINT) FROM step2
UNION ALL
SELECT '3 excl. clients whose every send hardbounced (mne proxy)', CAST(COUNT(*) AS BIGINT) FROM step3
UNION ALL
SELECT '4 excl. CPC-side re-consents after unsub', CAST(COUNT(*) AS BIGINT) FROM step4
UNION ALL
SELECT '5 residual: cross-campaign only (different mne)', CAST(COUNT(*) AS BIGINT) FROM cross_campaign_residual
UNION ALL
SELECT '6 residual: same campaign as unsubbed (in-program leak)', CAST(COUNT(*) AS BIGINT) FROM same_campaign_residual
ORDER BY 1;


-- SUMMARY: the story in one table - each row's proof lives in Evidence 1-8
-- E2's full-history standing logic (cpc_latest/cpc_optout_detail/cpc_optout/cpc_optout_earliest/flagged) is copied verbatim from Evidence 2 above - do not re-derive it, keep in sync if E2 changes
-- E5's pre-Apr unsub cohort CTE is copied verbatim from Evidence 5 above (widened to 3 cols so it also backs E8's rows below - same population, no behavior change for rows 9-10)
-- E8's waterfall chain (sent/bounced/sent_flagged/client_rollup/step2/step3/reconsent/step4/same_campaign_residual/cross_campaign_residual) is copied verbatim from Evidence 8 above - keep in sync if E8 changes
-- reuses vt_unsub_first, vt_q2_sends, vt_dns_1002, vt_postunsub_sends; the only fresh scans are the small bounded CPC_RB_PREF_LOG reads E1/E2/E8 already do - no new EVENT/MASTER scans
WITH cpc_latest AS (
    SELECT
        CLNT_NO,
        PREF_ID,
        CLNT_CONSENT_TYP,
        CHG_TMSTMP,
        ROW_NUMBER() OVER (PARTITION BY CLNT_NO, PREF_ID ORDER BY CHG_TMSTMP DESC) AS rn
    FROM DDWV01.CPC_RB_PREF_LOG
    WHERE PREF_ID IN (1002, 1012, 1014)
    -- consent standing = latest answer ever recorded (full history; small table - deliberate exception to the 2024 scan floor)
),
cpc_optout_detail AS (
    SELECT CLNT_NO, PREF_ID, CHG_TMSTMP
    FROM cpc_latest
    WHERE rn = 1 AND CLNT_CONSENT_TYP = 5002
),
cpc_optout AS (
    SELECT DISTINCT CLNT_NO
    FROM cpc_optout_detail
),
cpc_optout_earliest AS (
    SELECT CLNT_NO, MIN(CHG_TMSTMP) AS optout_chg_tmstmp
    FROM cpc_optout_detail
    GROUP BY CLNT_NO
),
flagged AS (
    SELECT
        u.CLNT_NO,
        CASE WHEN co.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS has_cpc_optout,
        CASE WHEN ce.optout_chg_tmstmp IS NOT NULL AND ce.optout_chg_tmstmp <  u.unsub_tm THEN 1 ELSE 0 END AS optout_before_unsub,
        CASE WHEN ce.optout_chg_tmstmp IS NOT NULL AND ce.optout_chg_tmstmp >= u.unsub_tm THEN 1 ELSE 0 END AS optout_after_unsub
    FROM vt_unsub_first u
    LEFT JOIN cpc_optout co ON co.CLNT_NO = u.CLNT_NO
    LEFT JOIN cpc_optout_earliest ce ON ce.CLNT_NO = u.CLNT_NO
),
cohort AS (
    -- pre-Apr unsub cohort, copied from Evidence 5; widened to unsub_tm + unsub_mne so it also backs Evidence 8's chain below
    SELECT CLNT_NO, unsub_tm, unsub_mne
    FROM vt_unsub_first
    WHERE unsub_tm < DATE '2026-04-01'
),
sent AS (
    -- copied verbatim from Evidence 8
    SELECT
        p.CLNT_NO,
        p.mne,
        p.disposition_dt_tm,
        c.unsub_tm,
        c.unsub_mne
    FROM vt_postunsub_sends p
    INNER JOIN cohort c ON c.CLNT_NO = p.CLNT_NO
    WHERE p.disposition_cd = 1
),
bounced AS (
    SELECT DISTINCT CLNT_NO, mne
    FROM vt_postunsub_sends
    WHERE disposition_cd = 5
),
sent_flagged AS (
    SELECT
        s.CLNT_NO,
        s.mne,
        s.unsub_mne,
        CASE WHEN s.disposition_dt_tm <= s.unsub_tm + INTERVAL '14' DAY THEN 1 ELSE 0 END AS in_casl_window,
        CASE WHEN b.CLNT_NO IS NOT NULL THEN 1 ELSE 0 END AS mne_bounced
    FROM sent s
    LEFT JOIN bounced b ON b.CLNT_NO = s.CLNT_NO AND b.mne = s.mne
),
client_rollup AS (
    SELECT
        CLNT_NO,
        MIN(in_casl_window) AS all_in_casl_window,
        MIN(mne_bounced)    AS all_bounced
    FROM sent_flagged
    GROUP BY CLNT_NO
),
step2 AS (
    SELECT CLNT_NO
    FROM client_rollup
    WHERE all_in_casl_window = 0
),
step3 AS (
    SELECT s2.CLNT_NO
    FROM step2 s2
    INNER JOIN client_rollup cr ON cr.CLNT_NO = s2.CLNT_NO
    WHERE cr.all_bounced = 0
),
reconsent AS (
    SELECT DISTINCT p.CLNT_NO
    FROM DDWV01.CPC_RB_PREF_LOG p
    INNER JOIN cohort c ON c.CLNT_NO = p.CLNT_NO
    WHERE p.PREF_ID IN (1002, 1012)
      AND p.CLNT_CONSENT_TYP = 5001
      AND p.CHG_TMSTMP >  c.unsub_tm
      AND p.CHG_TMSTMP >= DATE '2025-07-01'
      AND p.CHG_TMSTMP <  DATE '2026-07-01'
),
step4 AS (
    SELECT s3.CLNT_NO
    FROM step3 s3
    LEFT JOIN reconsent r ON r.CLNT_NO = s3.CLNT_NO
    WHERE r.CLNT_NO IS NULL
),
same_campaign_residual AS (
    SELECT DISTINCT sf.CLNT_NO
    FROM sent_flagged sf
    INNER JOIN step4 s4 ON s4.CLNT_NO = sf.CLNT_NO
    WHERE sf.in_casl_window = 0
      AND sf.mne_bounced = 0
      AND sf.mne = sf.unsub_mne
),
cross_campaign_residual AS (
    SELECT s4.CLNT_NO
    FROM step4 s4
    LEFT JOIN same_campaign_residual scr ON scr.CLNT_NO = s4.CLNT_NO
    WHERE scr.CLNT_NO IS NULL
)
-- row 1: one side of Evidence 1's two-consent-worlds comparison, 12-mo total (no monthly division - reader divides by 12)
SELECT
    CAST('email unsubs, 12-mo total' AS VARCHAR(40)) AS what,
    CAST('Jul 2025 - Jun 2026' AS VARCHAR(30)) AS time_window,
    CAST(COUNT(*) AS BIGINT) AS clients,
    CAST(NULL AS BIGINT) AS of_population
FROM vt_unsub_first
UNION ALL
-- row 2: other side of the same comparison, bounded CPC scan mirrors Evidence 1's cpc branch
SELECT
    'cpc opt-out flips, 12-mo total',
    'Jul 2025 - Jun 2026',
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT),
    NULL
FROM DDWV01.CPC_RB_PREF_LOG
WHERE PREF_ID IN (1002, 1012, 1014)
  AND CLNT_CONSENT_TYP = 5002
  AND CHG_TMSTMP >= DATE '2025-07-01'
  AND CHG_TMSTMP <  DATE '2026-07-01'
UNION ALL
-- row 3: population anchor for the blind-gate rows below (Evidence 2)
SELECT
    'unsubscribers, total',
    'Jul 2025 - Jun 2026',
    CAST(COUNT(*) AS BIGINT),
    NULL
FROM vt_unsub_first
UNION ALL
-- row 4: Evidence 2's with_explicit_cpc_optout
SELECT
    'w/ explicit CPC opt-out',
    'any time',
    CAST(SUM(has_cpc_optout) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM vt_unsub_first)
FROM flagged
UNION ALL
-- row 5: Evidence 2's before/after split, before
SELECT
    'opt-out recorded before unsub',
    'any time',
    CAST(SUM(optout_before_unsub) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM vt_unsub_first)
FROM flagged
UNION ALL
-- row 6: Evidence 2's before/after split, after
SELECT
    'opt-out recorded after unsub',
    'any time',
    CAST(SUM(optout_after_unsub) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM vt_unsub_first)
FROM flagged
UNION ALL
-- row 7: Evidence 6's cohort population, standing before Apr 1 2026
SELECT
    'do-not-solicit 1002, standing',
    'before Apr 1 2026',
    CAST(COUNT(*) AS BIGINT),
    NULL
FROM vt_dns_1002
UNION ALL
-- row 8: Evidence 6's cohort restricted to clients who received a campaign email (all-MNE version of the E6 breakout)
SELECT
    '1002 standing, got campaign email',
    'Apr - Jun 2026',
    CAST(COUNT(*) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM vt_dns_1002)
FROM vt_dns_1002 d
INNER JOIN vt_q2_sends s ON s.CLNT_NO = d.CLNT_NO
UNION ALL
-- row 9: Evidence 5's cohort population
SELECT
    'unsubscribed before Apr 2026',
    'Jul 2025 - Mar 2026',
    CAST(COUNT(*) AS BIGINT),
    NULL
FROM cohort
UNION ALL
-- row 10: Evidence 5's got_email_apr_jun
SELECT
    'unsub pre-Apr, got campaign email',
    'Apr - Jun 2026',
    CAST(COUNT(*) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM cohort)
FROM cohort c
INNER JOIN vt_q2_sends s ON s.CLNT_NO = c.CLNT_NO
UNION ALL
-- row 11: Evidence 8's row 1 (gross post-unsub receivers)
SELECT
    'post-unsub receivers, gross',
    'Apr - Jun 2026',
    CAST(COUNT(DISTINCT CLNT_NO) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM cohort)
FROM sent
UNION ALL
-- row 12: Evidence 8's row 6 (residual that survives all four exclusions AND matches the unsub mne)
SELECT
    'post-unsub receivers, in-program leak',
    'Apr - Jun 2026',
    CAST(COUNT(*) AS BIGINT),
    (SELECT CAST(COUNT(*) AS BIGINT) FROM cohort)
FROM same_campaign_residual
ORDER BY CASE what
    WHEN 'email unsubs, 12-mo total'           THEN 1
    WHEN 'cpc opt-out flips, 12-mo total'      THEN 2
    WHEN 'unsubscribers, total'                THEN 3
    WHEN 'w/ explicit CPC opt-out'             THEN 4
    WHEN 'opt-out recorded before unsub'       THEN 5
    WHEN 'opt-out recorded after unsub'        THEN 6
    WHEN 'do-not-solicit 1002, standing'       THEN 7
    WHEN '1002 standing, got campaign email'   THEN 8
    WHEN 'unsubscribed before Apr 2026'        THEN 9
    WHEN 'unsub pre-Apr, got campaign email'   THEN 10
    WHEN 'post-unsub receivers, gross'         THEN 11
    WHEN 'post-unsub receivers, in-program leak' THEN 12
END;


DROP TABLE vt_postunsub_sends;
DROP TABLE vt_gate_cohorts;
DROP TABLE vt_dns_1002;
DROP TABLE vt_q2_sends;
DROP TABLE vt_q2_send_detail;
DROP TABLE vt_unsub_first;
DROP TABLE vt_unsub_base;
