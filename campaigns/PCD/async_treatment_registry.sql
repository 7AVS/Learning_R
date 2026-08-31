-- Async treatment registry — PCD
-- Engine: Starburst (Trino). Reference VALUES block, not a standalone runnable query.
--
-- Why this exists (2026-08-27): ION+ leaked into the async banner population because
-- cohort_arm in async_banner_vintage_tracker.sql / async_banner_summary.sql /
-- pcd_success_validation.sql keyed ONLY on the 7 strategy codes packed in
-- tactic_decisn_vrb_info (position 3: MSC8YUS3, MAO28CJ5, MAO2EDB1, MFB8L6X6,
-- MFB8UJPY, MFB9BX97, MFB9HYQ7). The ION+ 21K offer (PPCDA7AA, Aug-4-2026
-- deployment) and its no-offer holdout controls (PPCD86AA, PPCD76AA) run on
-- THE SAME strategy codes as the async banner deployments, so the strategy-code
-- allowlist alone cannot separate them from the async population.
--
-- The rule going forward: strategy code stays the randomisation unit (it defines
-- the DOE cell — do not touch that CASE), but the DEPLOYMENT is gated on treatment
-- code (treatmt_mn) on top of it. is_async=1 rows below are the only treatment
-- codes that belong in the async banner population.
--
-- The deployment team supplies the treatment IDs (test AND control) for each new
-- wave before it can be added to this registry — do not infer is_async from the
-- offer description or from strategy code alone.
--
-- treatmt_mn is documented on the curated table DL_MR_PROD.cards_pcd_ongoing_decis_resp
-- (schemas/pcd_curated_schemas.md:39, char(8)) and confirmed against these exact values
-- in campaigns/PCD/async_w4_ion_pivot_and_slide_2026-08-27.md (Andre's Offer_Description x
-- Treatmt_mn x Strategy_Seg_Cd pivot). NOT YET CONFIRMED as a column on
-- DG6V01.TACTIC_EVNT_IP_AR_HIST — see async_treatment_registry_STATUS note at the
-- bottom of this file before wiring this registry into the three tracker/summary/
-- validation queries.

SELECT * FROM (
    VALUES
        ('PPCD87AA', 'Avion VI Privilege 35K + 20K net-spend',            1, 'W1-W3 async banner; test and control share this code, arm from tst_grp_cd'),
        ('PPCD13AA', 'Infinite Avion 15K',                                1, 'W1-W3 async banner; test and control share this code, arm from tst_grp_cd'),
        ('PPCD84AA', 'Infinite Avion from ION/IOP 15K',                   1, 'W1-W3 async banner; test and control share this code, arm from tst_grp_cd'),
        ('PPCDA7AA', 'ION+ 21K net-spend $1.5K/6mo + 7K anniversary',     0, 'Aug-4-2026 deployment on the same strategy codes; NOT a banner; test-only'),
        ('PPCD86AA', 'ION+ from ION, no offer',                          0, 'ION+ control (separate holdout), control-only'),
        ('PPCD76AA', 'ION+ no offer',                                     0, 'ION+ control (separate holdout), control-only')
) AS t (treatmt_mn, deployment, is_async, note);

-- ---------------------------------------------------------------------------
-- STATUS — 2026-08-27, blocks wiring this into the tracker/summary/validation SQL
-- ---------------------------------------------------------------------------
-- async_banner_vintage_tracker.sql, async_banner_summary.sql, and
-- pcd_success_validation.sql all query DG6V01.TACTIC_EVNT_IP_AR_HIST directly
-- (not the curated table) and derive their strategy-code arm from
-- tactic_decisn_vrb_info by position, not from a treatmt_mn column select.
-- pcd_tactic_field_mapping.sql exists specifically because dedicated curated-table
-- columns (confirmed: strategy_seg_cd) are NOT present on the raw tactic event
-- table and had to be byte-position-located inside tactic_decisn_vrb_info.
-- PCD_2026111_README.md confirms the same pattern in the other direction
-- ("tst_grp_cd lives on the tactic event table, NOT on the curated table").
-- No file in this repo selects treatmt_mn FROM DG6V01.TACTIC_EVNT_IP_AR_HIST, and
-- no file confirms that the existing `product_mnemonic` parse (position 4 of the
-- space-split tactic_decisn_vrb_info string, used in all three target files) is
-- the same value as treatmt_mn. Do not add `AND treatmt_mn IN (...)` to a query
-- against DG6V01.TACTIC_EVNT_IP_AR_HIST on the assumption that this column exists
-- there — it is only confirmed on DL_MR_PROD.cards_pcd_ongoing_decis_resp.
--
-- Two ways to close this before gating the three files:
--   1. Confirm empirically whether tactic_decisn_vrb_info position 4 IS treatmt_mn:
--      SELECT DISTINCT element_at(split(regexp_replace(trim(tactic_decisn_vrb_info),' +',' '),' '),4) AS candidate,
--             tst_grp_cd
--      FROM DG6V01.TACTIC_EVNT_IP_AR_HIST
--      WHERE tactic_id IN ('2026111PCD','2026125PCD')
--      -- if the distinct values returned are exactly the registry's treatmt_mn set, position 4 = treatmt_mn.
--   2. If not, join to DL_MR_PROD.cards_pcd_ongoing_decis_resp on clnt_no (+ acct_no)
--      to pull treatmt_mn directly instead of filtering the raw tactic table.
