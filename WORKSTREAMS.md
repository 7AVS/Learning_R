# Cards Pod — Active Workstreams

Last updated: 2026-03-17

## Priority Stack

### 1. PCD Async Banner Tracking (HIGH)
- **Ask:** Daily stats for LOB — available leads, banner views, clicks, CTR
- **Jira:** NBA-12268
- **Requestor:** Daniel Chin
- **Status:** Exploratory SQL built (`campaigns/PCD/pcd_async_banner_explore.sql`), waiting on confirmed banner codes (it_item_name, it_item_id, it_creative_name) from Melissa Baker
- **Tables:** ga4_ecommerce (banner events), cards_pcd_ongoing_decis_resp (population)
- **Launch:** ~2026-03-25
- **Next:** Run Query 1 to discover existing PCD banner events. Follow up with Melissa.

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
| PCD banner tracking | Banner codes (GA tag) | Melissa Baker | 2026-03-16 |
| AUH Phase 2 execution | Execution person unassigned | Tracy/pod | 2026-03-16 |

## Key Dates
| Date | Event |
|------|-------|
| 2026-03-18 | AUH experiment design discussion |
| ~2026-03-25 | PCD async banner launch |
| 2026-04-24 | PI Planning |
| 2026-04-30 | AUH Phase 2 launch |
| 2026-05 | TPA IO deployment, PLI sales modal |

## Campaign Quick Reference
| Code | MNE | Tactic Pattern | Table |
|------|-----|----------------|-------|
| CLI | (TBD) | (TBD) | (TBD — not catalogued) |
| AUH | AUH | 2026042AUH | DG6V01.TACTIC_EVNT_IP_AR_HIST + D3CV12A.ACCT_CRD_OWN_DLY_DELTA |
| PCD | (TBD) | (TBD) | cards_pcd_ongoing_decis_resp + ga4_ecommerce |
| PLI | PCL | (TBD) | cards_pli_decision_resp |
| TPA | PCQ | (TBD) | cards_tpa_pcq_decision_resp |
