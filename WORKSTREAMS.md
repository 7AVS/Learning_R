# Cards Pod — Active Workstreams

Last updated: 2026-05-29

## Priority Stack

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
- **2026-05-29:** Async banner interim deck reframed conservative — all 3 campaign slides (PCD/O2P/CTU) + slide-2 setup table + OBF caveat finalized in `campaigns/PCD/async_banner_slides_mockup.html` (v2). Stance: interim = directional only under O'Brien–Fleming; no "channel works" claims. O2P = context only (control not a valid benchmark, compass argument kept off-slide). CTU observational; randomised A/C DoE launches 9 Jun. Incremental reported per campaign (~60 PCD, ~1,850 O2P vs non-async, ~64 CTU), never pooled. NEXT: replace placeholder numbers with real query output; fold into PPT.

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
