-- =============================================================================
-- O2P Population Reconciliation — why our counts run higher than the colleague's
-- =============================================================================
-- Engine: Teradata (the tactic-event table is EDW/Teradata)
-- Tactic: 202609902P  (= 2026099 O2P)
--
-- THE GAP WE'RE EXPLAINING (our tracker vs their report):
--   total leads   : ours 804,552  vs theirs 764,310   (+40,242  ≈ +5.0%)
--   mobile leads  : ours 520,649  vs theirs 439,000   (+81,649  ≈ +18.6%)
--
-- TWO HYPOTHESES:
--   H1  total gap ≈ control holdout. Our population includes TG7 (CONTROL);
--       their "decisioned leads" may be TEST-only. 40,242 / 804,552 = 5.0%.
--   H2  mobile gap ≈ channel definition. We filter O2P mobile with
--       TACTIC_CELL_CD LIKE '%IM%' (broad); the CTU tracker uses the deploy
--       flag SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MB%'. Different scopes.
--
-- Run R1–R3, then read against the four target numbers above.
-- COUNTS ONLY. (TG7 = CONTROL for O2P, per o2p_colleague_success_logic.md.)
-- =============================================================================


-- R1 — TOTAL population split by test/control. Does TEST-only = 764,310?
SELECT
    CASE WHEN TST_GRP_CD = 'TG7' THEN 'CONTROL' ELSE 'TEST' END  AS arm,
    COUNT(DISTINCT CLNT_NO)                                      AS clients
FROM dg6v01.TACTIC_EVNT_IP_AR_HIST
WHERE TACTIC_ID = '202609902P'
GROUP BY 1
ORDER BY 1;
-- (Sum of the two rows should reproduce our 804,552. If TEST = ~764,310 → H1 holds.)


-- R2 — MOBILE population under the two competing definitions, by arm.
-- Compare each column against theirs (439,000) to see which definition + arm matches.
SELECT
    CASE WHEN TST_GRP_CD = 'TG7' THEN 'CONTROL' ELSE 'TEST' END                                       AS arm,
    COUNT(DISTINCT CLNT_NO)                                                                            AS all_clients,
    COUNT(DISTINCT CASE WHEN TRIM(TACTIC_CELL_CD) LIKE '%IM%' THEN CLNT_NO END)                        AS mob_cellcd_im,
    COUNT(DISTINCT CASE WHEN SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MB%' THEN CLNT_NO END)       AS mob_vrb_mb,
    COUNT(DISTINCT CASE WHEN TRIM(TACTIC_CELL_CD) LIKE '%IM%'
                         AND SUBSTR(TACTIC_DECISN_VRB_INFO,121,30) LIKE '%MB%' THEN CLNT_NO END)       AS mob_both
FROM dg6v01.TACTIC_EVNT_IP_AR_HIST
WHERE TACTIC_ID = '202609902P'
GROUP BY 1
ORDER BY 1;
-- mob_cellcd_im (TEST+CONTROL) should reproduce our 520,649.
-- Watch whether mob_vrb_mb or mob_both, and/or TEST-only, lands near 439,000.


-- R3 — distinct TACTIC_CELL_CD values and their client counts.
-- Reveals which cells '%IM%' is sweeping in (and any non-mobile cells it catches).
SELECT
    TACTIC_CELL_CD,
    COUNT(DISTINCT CLNT_NO)                                      AS clients,
    MAX(CASE WHEN TRIM(TACTIC_CELL_CD) LIKE '%IM%' THEN 1 ELSE 0 END)  AS caught_by_im_filter
FROM dg6v01.TACTIC_EVNT_IP_AR_HIST
WHERE TACTIC_ID = '202609902P'
GROUP BY TACTIC_CELL_CD
ORDER BY clients DESC;
