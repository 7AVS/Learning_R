# Cards Pod — Active Workstreams

Last updated: 2026-09-03

## ⚡ HOT — pick up here (session 2026-08-25→31)

### ★ VBU propensity model holdout test (DESIGN SHIPPED 2026-09-03 — green-lit test, our split rec delivered)
- Design locked + red-teamed (4 blind reviewers, all fixes applied): client-level random **70/30**,
  holdout = no VBU touch in any channel, arms persist across **2 waves**, read ~80d post-deploy.
  Score-band reads analytic (bands 1–4 individual, 5–9 bottom bucket; no band logic in decisioning).
  Cost ~102–160 forgone upgrades/wave (Jun/Jul basis, scales with wave size).
- Docs all pushed (`campaigns/VBA_VBU/propensity_experiment/`): Confluence page PASTED
  (vbu_propensity_confluence.html), DOE report, one-pager, exec email draft (send pending),
  split explainer xlsx, deployment summary xlsx. Every number window-attributed (Jan–Aug 2026
  history; Jun/Jul per-wave basis).
- Key facts banked: program monthly since ≥Jan 2026; mature baselines NR 1.25–1.99% /
  R_55 2.36–3.56% (swing = band mix), Jun/Jul anchor 1.79%/2.40%; control ≈0 (3/~7,550 NM
  Jan–Aug); BAU NM draw is SCORE-LINKED (d1 ~8% vs d4 ~4%; Jan–Mar d9-10 NC≥COMM) → fresh
  draw required, day-1 SRM per band. Full pivots transcribed in folder (workstation_pivot*.md).
- OPEN (people-side): CIDM confirmations (fresh random draw, no-touch, arm persistence, how NM
  is drawn), TST_GRP_CD pre-build, NIBT $/upgrade from finance, unsub guardrail threshold,
  randomization owner + freeze date, launch wave date. Memory: project_vbu_propensity_experiment_status.

### A. VBA/VBU Q3 scorecard reconciliation (ACTIVE — blocked on probes Andre runs)
- Q3 Commentary email (2026-08-25): 2 open asks tagged to Andre — VBA ITA impact + MCB on TPA; VBU MC6→MCB. ANSWERS DRAFTED & VERIFIED: `campaigns/VBA_VBU/q3_commentary_answers_2026-08-27.md` (VBA: ITA = 60% of pool at +0.08pp n.s. → pooled 0.87→0.41pp, TPA flat 0.90pp, MCB immaterial; VBU: MC6→MCB = 48% of leads at +0.36pp → 1.93→1.36pp, excl. it −4%). Replies ready to send (+ maturity line, final after Sep 30).
- MBR scorecard REPRODUCED: NBO Monthly Summary, campaign END month (May+Jun+Jul = 278,609 action / 0.16pp). Andre's PPK = start-month curated table (293,404 / 0.45pp). Remaining gap: NBO slice is ~3.5% smaller and NAC control 2,158 vs ledger 5,275 (May) → some decisioning field filters it. PROBES PUSHED, NOT RUN: `vba_nbo_dashboard_filter_probe.sql` (11 blocks) + `vba_decisioning_field_probe.sql` (46 blocks, classification in `vba_decisioning_fields.md`). Target: subset = 92,792/2,158 (end-May). Also open: earlier scorecard 184,521/0.0% (third source, unexplained); VBU NBO end-month check (target 87,203); PCL NIBT 156 vs 138 email thread (untouched).
- Ledgers: `vba_vbu_monthly_ledger.sql` (two queries, cohort-month rollup, start+end month side by side). Cube queries with Q1-Q8 questions embedded: `nbo_vba_q3_cube.sql`, `vbu_q3_cube.sql` (VBU verified: control='Action'/'Control', responder 3-way label LIKE '1.%', year_mon_start 'YYYY-MM').
- `references/campaign_query_cards.md` = per-table query cards (VBA/VBU/CRV ready; PCD/PCQ arm NOT on curated table → `campaigns/_templates/arm_probe_pcd_pcq.sql` pushed, not run).

### B. PCD Async Banner Wave 4 (RESOLVED — slide copy final)
- W4 (Aug 4) = TWO experiments: legacy offers 70/30 (0.57% vs 0.37%, +0.20pp, tracking W3) + NEW ION+ 21K cell 90/10 (48K test, 2.01% vs 1.07% no-offer control, +0.94pp, z=6.1, 69% of conversions). Pooled curve = mix artefact — never present. Final slide copy: `campaigns/ASYNC/async_banner_slide_text_2026-08-25.md` (W1/W2 frozen as seen; W3 finalized 201 incr/$936K; NIBT = $388/mo = $4,656/yr per incr client). Slide fixes flagged 2026-08-28: pp-not-bps, ION+ 452 not 552, W2 $90K, W3 row +30%, legend rename.
- Evidence: `campaigns/ASYNC/async_banner_wave4_pivot_2026-08-25.md` (all pivots + SRM z-table + vintage by offer) and `async_is_mb_by_tactic_2026-08-25.md` (TREATMT_MN = offer; is_mb by offer: 13AA 99%/87AA 25%/84AA 10%).
- OPEN: (1) ION+ control = no banner at all, or ION+ banner without bonus? (changes what +0.94pp measures); (2) is_mb semantics — if "got banner", ION/IOP cell is ~90% non-banner inside "async" (W1-W4 unchanged, doesn't affect W4 slide); (3) repo async SQL defines cohort by strategy-code list only + 2 files with typo'd lists (`async_banner_vintage_ab.sql:219`, `value_capture_report_v3.sql:154`); W2/W3 slide numbers not reproducible from repo SQL.


## Priority Stack

### 0c. Vintages One-Stop Shop (2026-07-22, BUILT & PUSHED — commit 04a6b9f)
- **What:** `vintages/` = standardized simple vintage set. 14 Teradata-direct files: 7 campaigns
  {CRV, PCL, PCQ, PCD, AUH, VBA, VBU} × {monthly, quarterly}. One output contract:
  `campaign, cohort, arm_raw, arm, vintage_day, cohort_size, cum_responses` (counts only,
  arms VARCHAR(30)). Simple version per Andre: arms only (Action/Control; PCQ Champion/Challenger),
  one success metric, no slicers. `vintages/README.md` = full inventory + validation checklist.
- **Construction (Andre's spec):** DENOMINATOR deduped per bin — client once per bin, anchored/armed
  at FIRST in-bin deployment. NUMERATOR not deduped — one success max per deployment window,
  last-touch attribution. Therefore quarterly cum_responses = Σ monthly; quarterly cohort_size ≤
  Σ monthly (gap = repeat-contact clients). Monthly/quarterly twins differ ONLY in bin expression.
- **Key changes vs old files:** CRV success = RAW `cards_crv_install_details` (off datalab curated
  flag) + canon GREATEST clamp, cap per cohort×arm; PCL anchor → `treatmt_strt_dt` (Andre override);
  VBA/VBU success = Casper+SCOT union; deleted 5 dead `*_monthly_cohort.sql` + absorbed
  `crv_vintage_monthly_raw.sql` + deduped root `imt_pipeline*.py`.
- **OPEN — Andre must answer (all flagged [VERIFY] in files/README):**
  1. SCOT path `edl0_im...` reachable from Teradata-direct? If not → VBA/VBU run via Starburst.
  2. AUH `_C` = Control — confirm (documented as unconfirmed working assumption).
  3. VBU success: Casper/SCOT application signal (current build) vs product-change (Daniel's original).
  4. Before first use: PCL strt_dt reliability check (query in file top) + CRV raw-vs-curated
     reconciliation vs `campaigns/CRV/vintage_reconciliation/crv_vintage_v1_datalab.sql`.
- **Later:** converge to one parameterized SQL per engine; migrate success defs to
  measurement_events_v2 where codes exist (see 0d).

### 0d. measurement_events_v2 Event Catalog (2026-07-22, Q0 RUN)
- **What:** Andre ran the event_cd list (Starburst, `event_date > 2026-01-01`) → **42 codes**,
  transcribed to `schemas/measurement_events_v2_event_catalog.md` (**gitignored/local-only —
  won't travel via git**). Card codes: `p_card_installmt_purch` (CRV, confirmed),
  `p_card_credlmt_inc` (PCL/CLI cand.), `p_card_apply`/`p_card_open`/`p_card_actvn` (PCQ cands.),
  `p_card_upgrade` (PCD or VBU cand.). **No AUH code — AUH likely cannot migrate.**
- **Next:** re-run Q0 with volume + date-range columns (`schemas/measurement_events_v2_eda.py`);
  resolve 3 OCR [VERIFY] codes; then per-campaign ACCOUNT-level validation (event fires where
  curated responder=1).

### 0e. Unsub outputs captured (2026-07-22)
- Email funnel by mne×cohort (202501–202607), first-unsub table, vendor-table quarterly profiling —
  transcribed to `unsub_tracking/results_2026-07-22_transcriptions.md` (**gitignored/local-only**).
  Finding: reliable vendor history starts ~2019Q3 (bounds pack 19 lookback). Numbers are phone-photo
  OCR — re-extract in-env before publishing.

### 0a. Value Capture Reporting Pipeline v1 (2026-07-17, reworked SQL-only same day)
- **What:** New root-level `value_capture/` folder builds rows for a partner team's Q2
  entity-reporting Excel table. Fixed interchange contract (`mne, test_desc, trt_start_dt,
  trt_end_dt, success_name, stratum, cohort_month, test_clients, test_successes, control_clients,
  control_successes` — counts only, unique clients) so future teammate blocks can target it
  independently of their internal logic/engine. **SQL-only per Andre's direction — no Python, no
  Excel workbook.**
- **Built:** `value_capture/README.md` (contract + final-column mapping), two per-cohort SQL blocks
  (`blocks/pcl_sales_modal_block.sql`, `blocks/pcq_ms_block.sql` — strict re-grains/re-aggregations
  of existing production queries, no new measurement logic, unchanged from v1), and
  `value_capture_report.sql` — ONE Trino/Starburst query that ports both blocks' logic inline,
  arm-maps PCQ's raw `test_group_latest` codes in SQL (with an unmapped-code guard row, not a
  silent drop), pools cohort_months/deciles, and computes stratified lift/SE/z/p-value/significance
  natively (`normal_cdf`), returning ~3-4 decision-sized rows. Stats formulas hand-verified against
  the known n1=1000,x1=60,n0=1000,x0=40 case (lift 2.00pp, z=2.052, p=0.0402, Y) by transcribing the
  exact SQL arithmetic into Python.
- **Open decisions inherited (not resolved here):** PCQ Period-ASC gating and assignment-vs-delivery
  population split — see `campaigns/sales_modal/README.md`. `value_capture_report.sql` ships both
  gated/ungated PCQ rows as separate test contrasts; Andre picks which goes to the partner sheet.
- **Next:** teammates' two remaining blocks; Andre runs the two SQL blocks and pastes real output
  into INPUT; delete the EXAMPLE verification rows before submitting to the partner.

### 0. Sales Modal Consolidation (2026-07-16)
- **What:** Consolidated PCL/PCQ/PCD sales-modal work, previously scattered across
  `campaigns/PCL_PLI/sales_modal/` and `campaigns/PCQ/modal_sales/`, into `campaigns/sales_modal/`
  (`pcl/`, `pcq/`, `pcd/`, `shared/`). Old folders removed; see `campaigns/sales_modal/README.md` for
  the full file-by-file production/superseded breakdown.
- **Built:** Two shared parameterized templates in `shared/` — `ms_population_success_template.sql`
  (Teradata-direct, generalizes PCQ's tactic-event + curated two-hop MS pattern for PCQ/PCL) and
  `ga4_modal_exposure_template.sql` (Trino, generalizes PCL's GA4 exposure/dismiss classification).
  Also added `pcd/pcd_modal_creative_split.sql` — splits PCD's SalesModal creative out of the pooled
  async-banner tracker read (does not modify the original tracker).
- **Flagged, pending Andre sign-off:** two PROPOSED CANON rules on PCQ MS measurement (Period-ASC
  numerator gate; `ms_targeted` as the population split, not `test_group_latest`) — two existing PCQ
  files don't apply one or both. Full detail in the shared template header + README.
- **Known-stale:** `pcl/build_modal_exposure_summary.py` + `.xlsx` hardcode wrong-id ~0.7% reach
  numbers; real reach is ~76% per `p9_vcl_full_measurement.sql`. Needs regeneration.
- **Next:** Andre sign-off on the two proposed canon rules; regenerate the exposure summary xlsx.

### 1. PCD Async Banner Tracking (HIGH)
- **Ask:** Daily stats for LOB — available leads, banner views, clicks, CTR
- **Jira:** NBA-12268
- **Requestor:** Daniel Chin
- **Tactic ID:** `2026111PCD`
- **Status:** LIVE (~5 weeks post-launch). Schema catalogued (`schemas/pcd_curated_schemas.md`, local). EDA built (`campaigns/PCD/pcd_2026111_curated_eda.sql`, 35 self-contained queries A–I). Vintage curves SQL built (`campaigns/PCD/pcd_2026111_vintage.sql`, long-format slicer pattern, single Starburst query). Full session context in `campaigns/PCD/PCD_2026111_README.md`.
- **Tables:** `DL_MR_PROD.cards_pcd_ongoing_decis_resp` (curated), `DG6V01.TACTIC_EVNT_IP_AR_HIST` (for tst_grp_cd), `edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce` (GA4)
- **Launch:** ~2026-04-20 (confirmed live)
- **Next:** (a) recover `tst_grp_cd → action_control` mapping for non-T/C codes; (b) capture schema columns past `clicked_mb`; (c) run diagnostics + vintage in work env, validate curve shapes
- **Blocked on:** None active. Mapping list pending from Andre's external doc.
- **2026-05-29:** Async banner interim deck reframed conservative — all 3 campaign slides (PCD/O2P/CTU) + slide-2 setup table + OBF caveat finalized in `campaigns/ASYNC/async_banner_slides_mockup.html` (v2). Stance: interim = directional only under O'Brien–Fleming; no "channel works" claims. O2P = context only (control not a valid benchmark, compass argument kept off-slide). CTU observational; randomised A/C DoE launches 9 Jun. Incremental reported per campaign (~60 PCD, ~1,850 O2P vs non-async, ~64 CTU), never pooled. NEXT: replace placeholder numbers with real query output; fold into PPT.

### 2. AUH Experiment Design / Control Split (HIGH)
- **Ask:** Prepare for tomorrow's experiment design discussion
- **Status:** Phase 1 data extracted (tactic 2026042AUH, ~190K pop, 6 test groups). MNE confirmed = AUH. Exploratory SQL built (`campaigns/AUH/auh_explore.sql`). Control split doc on Andre's machine — not yet in repo.
- **Phase 1:** Complete (2/12 - 3/12/2026), non-rewards, email only
- **Phase 2:** Apr 30 launch, rewards cards, email + OLB, 539.6K pop, 10% control
- **Next:** Bring control split doc into repo. Update auh_explore.sql with confirmed codes. Prepare Phase 2 DOE.

### 3. PLI New Deployment Control Split (HIGH)
- **Ask:** Design control split for upcoming PLI deployment
- **Status:** Not started. Need waterfall numbers.
- **Table:** cards_pli_decision_resp
- **Next:** Get waterfall from delivery team. Design control allocation.

### 4. Daily Campaign Tracking Harness (ONGOING)
- **Ask:** Repeatable daily metrics for AUH, PCD, PLI
- **Status:** No automation. Manual queries only.
- **Need:** SQL templates per campaign → export → report. Ideally Tableau but short-term manual.
- **Next:** Build after PCD codes confirmed and AUH control split done.

### 5. IPC Confluence Page (MEDIUM)
- **Ask:** Build Confluence page for IPC campaign
- **Note:** IPC/IRI = International Money Transfer, NOT cards. Different pod. Keep separate.
- **Status:** Not started
- **Next:** Decide if this lives in cards/ or separate project.

### 6. Universal Templates (FOUNDATION)
- **Ask:** Transcribe DoE template, one-pager, experimentation process into reusable artifacts
- **Source:** 14 pics captured 2026-03-17 (Value Capture Framework PPT, DoE Report Template Word, Experimentation Process v1.2 Word)
- **Status:** Pics classified, not yet transcribed
- **Next:** Transcribe into campaigns/_templates/

---

## Blocked Items
| Item | Blocked on | Owner | Since |
|------|-----------|-------|-------|
| PCD action_control mapping | Mapping list (Andre's external doc) | Andre | 2026-05-26 |
| AUH Phase 2 execution | Execution person unassigned | Tracy/pod | 2026-03-16 |

## Key Dates
| Date | Event |
|------|-------|
| 2026-03-18 | AUH experiment design discussion |
| ~2026-04-20 | PCD async banner launch (postponed from Mar 25) |
| 2026-04-24 | PI Planning |
| 2026-04-30 | AUH Phase 2 launch |
| 2026-05 | TPA IO deployment, PLI sales modal |

## Campaign Quick Reference
| Code | MNE | Tactic Pattern | Table |
|------|-----|----------------|-------|
| CLI | (TBD) | (TBD) | (TBD — not catalogued) |
| AUH | AUH | 2026042AUH | DG6V01.TACTIC_EVNT_IP_AR_HIST + D3CV12A.ACCT_CRD_OWN_DLY_DELTA |
| PCD | PCD | 2026111PCD | cards_pcd_ongoing_decis_resp + TACTIC_EVNT_IP_AR_HIST + ga4_ecommerce |
| PLI | PCL | (TBD) | cards_pli_decision_resp |
| TPA | PCQ | (TBD) | cards_tpa_pcq_decision_resp |
