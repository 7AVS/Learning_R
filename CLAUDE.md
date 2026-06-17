# Cards Pod — Project Instructions

## What This Is
Measurement and analytics infrastructure for the Cards pod (NBA). Contains schemas, exploratory SQL, pipeline code, campaign artifacts, and experiment documentation.

## Campaigns
| Folder | MNE | Campaign | Status |
|--------|-----|----------|--------|
| CLI | TBD | Credit Limit Increase | Always-on, Priority #1. Barely catalogued — EDA only |
| AUH | AUH | Authorized Users | Phase 1 complete. Phase 2 (Apr 30): organic baseline built, recommendation emails drafted, second deployment recommended |
| PCD | TBD | Product Card Upgrade | Async banner interim deck (PCD/O2P/CTU) built — conservative/OBF framing locked 2026-05-29. Awaiting real query output; CTU randomised A/C DoE launches 9 Jun. |
| PCL | PCL | Pre-Approved Limit Increase (PLI) | MDE calculators delivered (v1+v2), recommendation email sent, mobile expansion DOE finalized |
| PCQ | PCQ | Credit Card Acquisition (TPA) | DM test designed, MDE calculators built (v1+v2), recommendation email drafted, awaiting business decision on decile scope |
| IMT | IPC/IRI | International Money Transfer | Not Cards pod — different team. Folder exists for IPC/IRI results reference only |
| VBA_VBU | — | Visa Benefit Add / Upgrade | Early stage — vintage SQL only |

## Folder Structure
- `/schemas/` — Table schemas (.md), Python pipelines, EDA scripts
- `/campaigns/<CODE>/` — Per-campaign artifacts (DOE, tracking, SQL, audit). Folder names = MNE codes (PCL not PLI, PCQ not TPA)
- `/campaigns/_templates/` — Universal templates: DOE report, one-pager, experimentation process, campaign tracker (transcribed from pics)
- `/campaigns/IMT/` — IPC/IRI reference only (not Cards pod)
- `/pics/` — Raw photos from meetings, screenshots, emails

## Key Tables
- **Teradata (EDW):** dw00_jm.dl_mr_prod.cards_pcd_ongoing_decis_resp, dw00_im.dl_mr_prod.cards_pli_decision_resp, dw00_im.dl_mr_prod.cards_tpa_pcq_decision_resp
- **Teradata (Tactic history):** DTZV01.TACTIC_EVNT_IP_AR_H60M
- **Teradata (Events):** DDWV01.EXT_CDP_CHNL_EVNT
- **Teradata (Email):** DTZV01.VENDOR_FEEDBACK_MASTER, DTZV01.VENDOR_FEEDBACK_EVENT
- **Trino (GA4):** edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_ecommerce, edl0_im.prod_yg80_pcbsharedzone.tsz_00198_data_ga4_narrow
- **Segment:** DG6V01.CLNT_DERIV_DTA_HIST

## SQL Conventions
- **CANON: `references/query_engine_guidelines.md`** — the three environments (Starburst/Trino, Teradata-direct, YARN Spark), per-engine syntax rules, federation pushdown rules, partition pruning. Check every new query against it before shipping.
- GA4 tables are in Trino (catalog: edl0_im). Use Trino SQL syntax.
- Campaign/EDW tables (Teradata schemas) are normally queried THROUGH Starburst federation → **write Trino syntax** (no QUALIFY, no TOP, no NULLIFZERO, strict typing). Teradata-native syntax only when running Teradata-direct (volatile tables / spool control).
- GA4 partitioned by year/month/day (varchar). Always filter year AND month (event_date alone does not prune).
- TACTIC_ID structure: positions 8-10 = MNE (campaign mnemonic, e.g. AUH, PCL, PCQ, IRI/IPC for IMT).

## Artifacts Per Campaign
Each campaign under `/campaigns/<CODE>/` should have:
1. **DOE design** — experiment design, population, control %, power analysis
2. **Tracking** — response/performance tracking (codes, metrics, windows)
3. **Exploratory SQL** — table discovery and validation queries
4. **Pipeline** — production measurement code (when ready)
5. **Audit trail** — decisions, changes, approvals

## Working Style
- Pics arrive in /pics/ — sort and transcribe into proper artifacts
- Templates may be universal (all campaigns) or campaign-specific
- Harnesses = reusable code templates, built incrementally as we learn
- Always delegate multi-file work to agents
- Reference cards-pod-context.md for team, stakeholders, meeting history

---

## Operating Philosophy — The Value-Finder Workbench

**Mission:** find cross-campaign, client-level value and ship defensible causal findings fast. This is not a reporting function — it's the team that turns campaign data into *"here's the next test, here's where the value is."*

**The method** *(detail in `career_strategy/02_strategy_test_catalogue.md`)*:
- **Borrowed randomization is the primitive.** Any campaign with a randomized control is a natural experiment we repurpose to measure what it wasn't designed for — causal value without waiting to design and deploy a new RCT. Ships paired with its guard: *never condition on a post-treatment variable.*
- **Run the menu, don't reinvent.** A standing catalogue of causal levers (overlap, sequencing, channel, rank, saturation) + an explanatory layer (descriptive trees, decile concentration, selection guards). Point a known pattern at a new campaign; don't hand-build a 1,000-line one-off.
- **The loop:** borrowed randomization → causal lever → explanatory pass → next hypothesis. Every finding seeds the next.

**Test velocity is the product.** Bias to more tests, not more validation. Validate just enough to design a precise experiment or protect a real decision — then ship. Over-proving on data is the trap that kills throughput.

**Build for handoff, not for hoarding.** Every harness and analysis is built to be run by someone else: parameterized, documented, self-serve. We own the *method*, not the *execution* — so the work scales past one person's hours and never becomes a single point of failure on critical-path delivery.

**Standardize the repeatable.** Cohort building, success lookups (Success Library), vintage, MDE calculators — harnesses with editable parameters, not code retyped per campaign.

---

## Intent (Layer 3 — Why This Workbench Exists)

**Purpose:** Experiment design and campaign measurement for the NBA Cards pod. We work both ends of the campaign workflow — designing experiments up front, measuring outcomes at the end — to provide statistically sound recommendations that drive iteration speed.

**Good work has three qualities, in priority order:**
1. **Accurate** — the math is sound, attribution is correct, success detection is valid
2. **Clear** — the goal, methodology, and findings are understandable to non-statistical stakeholders
3. **Timely** — delivered within the sprint window. We are on the critical path — late work is a showstopper for other teams.

**Tradeoffs (Pre-decided):**
- Rigor where it protects a decision, speed everywhere else. High-scrutiny or irreversible calls (experiment design, success definition, exec-facing numbers) get full rigor — a wrong answer there is worse than a right answer late. Directional internal reads ship fast; over-proving on data is the trap that kills test velocity. (See Operating Philosophy above.)
- Breadth/Depth is not a binary — all campaigns run in parallel at different lifecycle stages. Each gets attention proportional to its current sprint needs.
- Templates are semi-reusable — same philosophy, different parameters per campaign. Build harnesses with editable variables, not rigid pipelines.

**Not our job:**
- Deploying campaigns or uploading tactic configurations
- Managing data pipelines or the tactic event base
- Making business strategy decisions (we provide recommendations, stakeholders decide)
- We participate in experiment design and measure outcomes. We don't push buttons.

## Judgment (Layer 4 — Risk and Authority)

**Statistical authority: Claude is the expert here.** Andre knows the business domain and basic analytical concepts. Claude owns statistical correctness. This means:
- ALWAYS question methodology if something looks off — do not just execute
- Flag assumptions in experiment designs before they get locked into production
- If a control split looks underpowered, say so. If an MDE assumption seems aggressive, challenge it.
- Push back when the math doesn't hold, even if Andre says "just do it"
- Explain statistical concepts in plain language — Andre is building his understanding

**High-stakes areas (mistakes here are expensive):**
- **Experiment design (MDE, control splits):** If wrong, 2+ weeks of downstream production work is wasted. This is the highest-stakes output.
- **Success/conversion definition:** How we define and detect conversion determines everything downstream. Attribution logic must be airtight.
- **Measurement code accuracy:** Wrong numbers → wrong recommendations → wrong business decisions.

**No production access risk:** Claude generates code, Andre runs it in a separate professional environment. Claude cannot accidentally execute against production data. No guardrails needed on code generation — but the code itself must be correct because Andre trusts it.

## Coherence (Layer 5 — Cross-Session Continuity)

**The primary coherence problem is context loss between sessions, not behavioral drift.** When a session ends or context compresses, continuity breaks. To manage this:
- **WORKSTREAMS.md is the source of truth** for what's active, blocked, and upcoming. Update it when work progresses.
- **cards-pod-context.md** holds team, stakeholder, and meeting history context. Reference it when needed.
- **Per-campaign folders** (`campaigns/<CODE>/`) hold all artifacts for that campaign. Always check what exists before starting work.
- When starting a new session in this workbench, read WORKSTREAMS.md first to re-establish context.
- Campaign artifacts will vary — there is no fixed template roadmap. Some campaigns need MDE docs, some need vintage curves, some need PowerPoint packs, some need shared notebooks. Follow the campaign's needs, not a rigid structure.

## Evaluation (Layer 6 — How We Know The Work Is Right)

### Experiment Design Validation
- MDE must be within baseline response rate expectations
- Key parameters (control %, confidence level, minimum detectable effect) should be editable so stakeholders can explore aggressive vs. conservative splits
- Before locking a design: verify population size supports the MDE, confirm success metric definition, validate that control/test splits are clean

### Measurement Validation (Step-by-Step, Non-Negotiable)
This workbench follows a strict build-and-verify methodology:
1. **One table at a time.** Build one source table, verify the numbers, do EDA, understand the mnemonics and what the data represents.
2. **Then the next.** Move to the next table only after the first is validated.
3. **Then the join logic.** Define how tables relate — account level vs. client level, matching windows, transaction detection parameters, card types.
4. **Validate each merge.** After each join, check row counts, verify the logic didn't explode or collapse unexpectedly.
5. **Never run the final query blind.** The full merged/detection/measurement query is the LAST step, built from validated building blocks.
6. **Cross-reference results.** Compare against:
   - Third-party sources or independent reference tables
   - Daily cumulative window curves vs. rolled-up cohort summaries (these must match)
   - Known baseline numbers or prior deployment results
7. **Sanity checks are mandatory**, not optional. If a number looks surprising, investigate before reporting.

## Teradata Quirks & Hard Rules (Learned from Experience)
1. **TDWM `sys_calendar` Blocker:** Unconstrained product joins (CROSS JOIN) against `sys_calendar.calendar` will block in TDWM ("F-uncnstrm PJ … rowest"). **Solution**: The day-spine and the joining cells must be placed in `VOLATILE TABLE`s with `COLLECT STATISTICS` before the cross join.
2. **Volatile Table Session Persistence:** `VOLATILE TABLE`s persist in the session. When creating scripts, do not use volatile tables for everything or it will cause "table already exists" errors when users rerun failed scripts. Use CTEs (`WITH`) for all data prep, and ONLY use volatile tables where strictly required by TDWM (e.g., the spine and cells).
3. **UNION ALL Implicit Truncation:** In Teradata, the character length of a column built via `UNION ALL` is strictly defined by the length of the string in the *first* `SELECT` statement. If the first block is `'overall'` (7 chars), all subsequent strings (like `'model_score_decile'`) will be truncated. **Solution**: Always explicitly cast strings in the first `SELECT` of a `UNION ALL` (e.g., `CAST('overall' AS VARCHAR(50))`).
