-- Source-table catalog for the async-banner measurement stack.
-- Use this as a checklist when scanning HDFS for equivalent datasets so the
-- pipeline can be moved off Starburst/Trino over Teradata federation
-- (currently slow / spool-heavy).
--
-- Tables are referenced by one or more of:
--   async_banner_vintage_tracker.sql  (engagement + responders, daily + cumulative)
--   async_banner_summary.sql          (engagement + responders, totals to date)
--   pcd_success_validation.sql        (PCD responder count)
--   ctu_success_validation.sql        (CTU responder count)
--   o2p_success_validation.sql        (O2P responder count)


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ 1. COHORT — shared by all three campaigns (PCD, CTU, O2P)                  ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

-- DG6V01.TACTIC_EVNT_IP_AR_HIST
--   Engine     : Teradata (via DG6V01 schema)
--   Used in    : every file above (cohort definition)
--   Purpose    : tactic deployment / recipient event log. Drives the cohort.
--   Columns we pull:
--     - tactic_id                       (campaign filter)
--     - clnt_no                         (cohort key)
--     - visa_acct_no                    (PCD card-acct join key for dly_full_portfolio)
--     - treatmt_strt_dt, treatmt_end_dt (treatment window)
--     - tactic_decisn_vrb_info          (packed string — async-allowlist at position 3,
--                                        from-product code at chars 42-44 for PCD,
--                                        mobile channel flag at chars 121-150 for CTU)
--     - tst_grp_cd                      (test/control suffix T/C for PCD; TG4/TG7 for O2P)
--     - rpt_grp_cd                      (O2P async eligibility — PO2P* allowlist)
--     - tactic_cell_cd                  (O2P channel served — %MB% for is_mobile)


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ 2. ENGAGEMENT — shared by all three campaigns (GA4)                        ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

-- edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced
--   Engine     : Starburst / Trino (EDL catalog)
--   Used in    : tracker + summary (engagement events)
--   Purpose    : GA4 view_promotion / select_promotion events per client per banner.
--   Partition  : varchar year / month / day  (always filter year='2026' AND month IN (...))
--   Columns we pull:
--     - event_date, event_name
--     - it_item_id (CTU='i_300102', O2P='i_298045')
--     - it_item_name (PCD: 4 RBC_PCD_* values)
--     - it_creative_name (suffix '%n_no%' splits click_p vs click_n)
--     - up_srf_id2_value, ep_srf_id2  (BIGINT cast → clnt_no; PCD uses both via COALESCE)
--   Note       : 'reduced' variant. There is also a non-reduced
--                tsz_00198_data_ga4_ecommerce on the same catalog (used by older EDA files).


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ 3. SUCCESS — PCD only                                                      ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

-- D3CV12A.dly_full_portfolio
--   Engine     : Teradata (D3CV12A = visa-cards catalog; p3c)
--   Used in    : tracker, summary, pcd_success_validation
--   Purpose    : event-driven portfolio log. Used to detect a product change on a
--                visa acct during the treatment window (visa_prod_cd <> from-product).
--   Grain      : event-driven, NOT daily. dt_record_ext is the event date.
--   Columns we pull:
--     - acct_no                (join to TACTIC_EVNT_IP_AR_HIST.visa_acct_no)
--     - dt_record_ext          (event date)
--     - visa_prod_cd           (new product code)
--   Note       : Single-scan only. Multi-scan volatile builds blow spool.


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ 4. SUCCESS — CTU only (chequing-account switch chain)                      ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

-- ddwv01.clnt_ar_reltn_dly
--   Engine     : Teradata
--   Used in    : tracker, summary, ctu_success_validation
--   Purpose    : client ↔ arrangement (account) link, daily snapshot. Used to get the
--                client's chequing ar_id as of (treatmt_strt_dt - 1).
--   Columns we pull:
--     - clnt_no, ar_id, snap_dt, dw_srvc_id   (filter dw_srvc_id = 1 = personal)

-- ddwv01.ar_static_dly
--   Engine     : Teradata
--   Used in    : tracker, summary, ctu_success_validation
--   Purpose    : arrangement-level static attributes, daily snapshot. Used to confirm
--                acct_typ = 13 (chequing), acct_cls IN (0, 8, 9, 10), open_cls_sts = 'O'.
--   Columns we pull:
--     - ar_id, snap_dt, srvc_id, open_cls_sts, acct_typ, acct_cls

-- ddwv01.deposit_account_dly
--   Engine     : Teradata
--   Used in    : tracker, summary, ctu_success_validation
--   Purpose    : deposit-account daily attributes. Carries flt_pr_tm_trnsctn, the fee
--                plan / transaction tier used to label from_product (chequing tier).
--   Columns we pull:
--     - ar_id, snap_dt, dw_srvc_id, flt_pr_tm_trnsctn

-- ddwv01.dep_acct_sw_dly
--   Engine     : Teradata
--   Used in    : tracker, summary, ctu_success_validation
--   Purpose    : deposit-account switch events (the actual conversion event for CTU).
--   Columns we pull:
--     - ar_id, acct_sw_proc_dt, acct_sw_proc_tm, rec_typ_cd
--     - from_acct_typ, from_acct_clss, from_fee_opt  (→ pba_acct_lkup for from-product name)
--     - to_acct_typ, to_acct_clss, to_fee_opt        (→ pba_acct_lkup for to-product name)

-- ddwv01.pba_acct_lkup
--   Engine     : Teradata
--   Used in    : tracker, summary, ctu_success_validation
--   Purpose    : code → product-name lookup. Maps (acct_typ_cd, acct_clss_cd,
--                srvc_fee_opt_cd) to prod_en_nm. Filter pda_typ_cd = 'C' for consumer.
--   Columns we pull:
--     - acct_typ_cd, acct_clss_cd, srvc_fee_opt_cd, pda_typ_cd, snap_dt, prod_en_nm
--   Note       : multi-snap_dt; we take MAX(snap_dt) within cohort window.


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ 5. SUCCESS — O2P only (credit-application chain)                           ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
-- Schema is DDWV01. The four tables form a chain that needs all four joined to
-- get from clnt_no to a completed-approved primary card application.

-- DDWV01.CR_APP_CLNT_RELTN
--   Purpose : maps clnt_no → cr_app_id (which apps a client is on).
--   Columns : clnt_no, cr_app_id, cr_app_clnt_seq_no, sys_src_id

-- DDWV01.OVRL_CR_APP
--   Purpose : application header. Used to filter app_typ = 'P' (primary).
--   Columns : cr_app_id, sys_src_id, app_typ

-- DDWV01.CR_APP_CLNT_PROD_RELTN
--   Purpose : maps the client side of the app to the product side.
--   Columns : cr_app_id, cr_app_clnt_seq_no, cr_app_prod_seq_no, sys_src_id

-- DDWV01.CR_APP_PROD
--   Purpose : product attributes per application. Carries app date, completion date,
--             status, and product type.
--   Columns we pull:
--     - cr_app_id, cr_app_prod_seq_no, sys_src_id
--     - prod_app_dt              (application date — window filter)
--     - prod_app_compl_dt        (completion date — must be non-null)
--     - appl_for_prod_typ        (filter to '40','41','43' — card products)
--     - prod_app_sts_cd          (filter to approved/completed: 32,37,45,47,51,56,62)


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║ Summary: tables to find in HDFS                                            ║
-- ╠═════════════════════════════════════════════════════════════════════════════╣
-- ║  COHORT (1)                                                                ║
-- ║    DG6V01.TACTIC_EVNT_IP_AR_HIST                                           ║
-- ║                                                                            ║
-- ║  ENGAGEMENT (1)                                                            ║
-- ║    edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce_reduced    ║
-- ║                                                                            ║
-- ║  PCD SUCCESS (1)                                                           ║
-- ║    D3CV12A.dly_full_portfolio                                              ║
-- ║                                                                            ║
-- ║  CTU SUCCESS (5)                                                           ║
-- ║    ddwv01.clnt_ar_reltn_dly                                                ║
-- ║    ddwv01.ar_static_dly                                                    ║
-- ║    ddwv01.deposit_account_dly                                              ║
-- ║    ddwv01.dep_acct_sw_dly                                                  ║
-- ║    ddwv01.pba_acct_lkup                                                    ║
-- ║                                                                            ║
-- ║  O2P SUCCESS (4)                                                           ║
-- ║    DDWV01.CR_APP_CLNT_RELTN                                                ║
-- ║    DDWV01.OVRL_CR_APP                                                      ║
-- ║    DDWV01.CR_APP_CLNT_PROD_RELTN                                           ║
-- ║    DDWV01.CR_APP_PROD                                                      ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
-- Total: 12 distinct EDW tables + 1 EDL/Trino table = 13 sources.
