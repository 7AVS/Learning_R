-- preflight5.sql
-- Two questions, in order. The second cannot be asked until the first is answered.
--
-- Q19: is the vendor tables' TREATMENT_ID the same identifier as TACTIC_ID in
--      DTZV01.TACTIC_EVNT_IP_AR_H60M? Every join in unsub_tracking/ assumes it is.
--      UNSUB_TRACKING_KNOWLEDGE.md:135,157 assert it. Section 9 Open Questions item 1 records
--      that the script written to prove it (archaeology/03_tactic_join_channel_validation.sql,
--      checks J1-J4) was never reviewed. It has been an assumption for the life of this folder.
--
-- Q20+: if the identifiers match, define the campaign universe from the DEPLOYMENTS ACTUALLY RUN
--      in the window, instead of parsing TREATMENT_ID strings for a year and a Julian day. That
--      string heuristic was wrong: a 2020 floor discarded 2018319KVM, a live 2018-vintage id with
--      3,080,273 sends, and any wider floor is equally arbitrary.
--
-- VERIFIED COLUMNS (from repo usage, not guessed):
--   TACTIC_ID, TREATMT_STRT_DT, TREATMT_END_DT, CLNT_NO  (UNSUB_TRACKING_KNOWLEDGE.md:142-144)
--   Grain: one row per client x tactic deployment. No status/active flag exists - deployment
--   membership is a TREATMT_STRT_DT range and nothing else.
-- ENGINE: Teradata-direct. Every query returns <= 20 rows.


-- =========================================================================================
-- Q19. THE FOUNDATION TEST. Do the two identifier sets overlap?
--   in_both high        -> same namespace, the join is sound, proceed to Q20.
--   in_both near zero   -> different namespaces. SUBSTR(TREATMENT_ID,8,3) has been extracting an
--                          MNE from a field that only resembles a tactic id, and every
--                          per-campaign number in this folder is void.
-- =========================================================================================
SELECT SUM(CASE WHEN v_flag = 1 AND t_flag = 1 THEN 1 ELSE 0 END) AS in_both,
       SUM(CASE WHEN v_flag = 1 AND t_flag = 0 THEN 1 ELSE 0 END) AS vendor_only,
       SUM(CASE WHEN v_flag = 0 AND t_flag = 1 THEN 1 ELSE 0 END) AS tactic_only
FROM (
    SELECT COALESCE(v.id, t.id)                          AS id,
           MAX(CASE WHEN v.id IS NULL THEN 0 ELSE 1 END) AS v_flag,
           MAX(CASE WHEN t.id IS NULL THEN 0 ELSE 1 END) AS t_flag
    FROM (SELECT DISTINCT TREATMENT_ID AS id
          FROM DTZV01.VENDOR_FEEDBACK_EVENT
          WHERE disposition_cd = 1
            AND disposition_dt_tm >= DATE '2025-08-01'
            AND disposition_dt_tm <  DATE '2026-08-01') v
    FULL OUTER JOIN (SELECT DISTINCT TACTIC_ID AS id
                     FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
                     WHERE TREATMT_STRT_DT >= DATE '2025-08-01'
                       AND TREATMT_STRT_DT <  DATE '2026-08-01') t
      ON t.id = v.id
    GROUP BY 1
) x;


-- =========================================================================================
-- Q20 v2. Weight the same question by VOLUME.
-- v1 threw 2646: the LEFT JOIN ran against 289M raw send rows when it only needs 11,636.
-- Aggregate the vendor side to one row per TREATMENT_ID FIRST, then join. Same answer, tiny cost.
-- =========================================================================================
SELECT CASE WHEN t.TACTIC_ID IS NOT NULL THEN 'matches a deployment in window'
            ELSE 'NO matching deployment' END AS status,
       COUNT(*)          AS distinct_ids,
       SUM(e.send_rows)  AS send_rows,
       SUM(e.clients)    AS client_send_pairs
FROM (SELECT TREATMENT_ID,
             COUNT(*)                           AS send_rows,
             COUNT(DISTINCT consumer_id_hashed) AS clients
      FROM DTZV01.VENDOR_FEEDBACK_EVENT
      WHERE disposition_cd = 1
        AND disposition_dt_tm >= DATE '2025-08-01'
        AND disposition_dt_tm <  DATE '2026-08-01'
      GROUP BY 1) e
LEFT JOIN (SELECT DISTINCT TACTIC_ID
           FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
           WHERE TREATMT_STRT_DT >= DATE '2025-08-01'
             AND TREATMT_STRT_DT <  DATE '2026-08-01') t
  ON t.TACTIC_ID = e.TREATMENT_ID
GROUP BY 1;


-- =========================================================================================
-- Q21. THE 2018 MYSTERY. Is 2018319KVM a real deployment still running, or vendor-side residue?
-- No row in the tactic table means it is not a campaign and the whitelist removes it with no
-- argument. Recent TREATMT_STRT_DTs mean it is a live evergreen and the id is simply reused.
-- No date filter here on purpose - we want its whole history.
-- =========================================================================================
SELECT TACTIC_ID,
       MIN(TREATMT_STRT_DT)     AS first_start,
       MAX(TREATMT_STRT_DT)     AS last_start,
       MAX(TREATMT_END_DT)      AS last_end,
       COUNT(DISTINCT CLNT_NO)  AS clients
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TACTIC_ID IN ('2018319KVM', 'CABVRSN1', 'DEFAULT', '2019105THA', '21010AOT4B')
GROUP BY 1
ORDER BY 1;


-- =========================================================================================
-- Q22. The whitelist itself, sized. How many deployments ran in the window and how many
-- mnemonics do they carry? This is the campaign universe the analysis should be scoped to.
-- =========================================================================================
SELECT COUNT(DISTINCT TACTIC_ID)                  AS deployments_in_window,
       COUNT(DISTINCT SUBSTR(TACTIC_ID, 8, 3))    AS distinct_mnemonics,
       MIN(TREATMT_STRT_DT)                       AS earliest_start,
       MAX(TREATMT_STRT_DT)                       AS latest_start
FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
WHERE TREATMT_STRT_DT >= DATE '2025-08-01'
  AND TREATMT_STRT_DT <  DATE '2026-08-01';


-- =========================================================================================
-- Q23. Is the whitelist too strict? Q20 left 44,446,410 sends (14.3%) unmatched across 743 ids,
-- and only 29 of those are junk - the other 714 are properly formed tactic ids.
--
-- Likely cause: filtering on TREATMT_STRT_DT inside the window excludes a deployment that
-- launched BEFORE it and kept mailing through it. The correct test is "active during the window",
-- not "started during the window":
--     TREATMT_STRT_DT <  WIN_CEIL
--     AND (TREATMT_END_DT >= WIN_FLOOR OR TREATMT_END_DT IS NULL)
--
-- Compare against Q20. If unmatched volume drops sharply, use the overlap test as the whitelist.
-- If it barely moves, the 714 are residue like 2018319KVM and the strict test was right.
-- =========================================================================================
SELECT CASE WHEN t.TACTIC_ID IS NOT NULL THEN 'matches a deployment ACTIVE in window'
            ELSE 'NO matching deployment' END AS status,
       COUNT(*)          AS distinct_ids,
       SUM(e.send_rows)  AS send_rows
FROM (SELECT TREATMENT_ID, COUNT(*) AS send_rows
      FROM DTZV01.VENDOR_FEEDBACK_EVENT
      WHERE disposition_cd = 1
        AND disposition_dt_tm >= DATE '2025-08-01'
        AND disposition_dt_tm <  DATE '2026-08-01'
      GROUP BY 1) e
LEFT JOIN (SELECT DISTINCT TACTIC_ID
           FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
           WHERE TREATMT_STRT_DT < DATE '2026-08-01'
             AND (TREATMT_END_DT >= DATE '2025-08-01' OR TREATMT_END_DT IS NULL)) t
  ON t.TACTIC_ID = e.TREATMENT_ID
GROUP BY 1;


-- =========================================================================================
-- Q24. Name what is STILL unmatched under the overlap test. If the top rows are DEFAULT and
-- CABVRSN1 the whitelist is doing its job. If real-looking tactic ids remain, they need
-- explaining before anything is excluded on their account.
-- =========================================================================================
SELECT TOP 15
       e.TREATMENT_ID,
       SUBSTR(TRIM(e.TREATMENT_ID), 8, 3) AS mne_substring,
       e.send_rows
FROM (SELECT TREATMENT_ID, COUNT(*) AS send_rows
      FROM DTZV01.VENDOR_FEEDBACK_EVENT
      WHERE disposition_cd = 1
        AND disposition_dt_tm >= DATE '2025-08-01'
        AND disposition_dt_tm <  DATE '2026-08-01'
      GROUP BY 1) e
LEFT JOIN (SELECT DISTINCT TACTIC_ID
           FROM DTZV01.TACTIC_EVNT_IP_AR_H60M
           WHERE TREATMT_STRT_DT < DATE '2026-08-01'
             AND (TREATMT_END_DT >= DATE '2025-08-01' OR TREATMT_END_DT IS NULL)) t
  ON t.TACTIC_ID = e.TREATMENT_ID
WHERE t.TACTIC_ID IS NULL
ORDER BY e.send_rows DESC;
