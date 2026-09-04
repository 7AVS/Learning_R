# Unsub Tracking — Knowledge Base (migration doc)

Everything learned building the bank-wide (NBA-wide) email unsubscription deep-dive.
Written 2026-07-14 for migration to a new environment. Repo folder: `unsub_tracking/`.

Folder layout (2026-07-24): archaeology/ = exploratory packs 01-22 (results catalogued below); museum/ = presentation-grade evidence queries.

---

## LOCKED FACTS — read before asking anything (2026-07-25)

Re-explained across at least five sessions because none of it was written down. It is written down now.
Do not re-litigate. Do not ask again.

1. **NEVER query `DDWV01.CPC_RB_PREF_LOG`. The log is broken (Andre 2026-09-04).** It carries
   ~1% of the email-origin (7020) consent writes: pack 61 v1 read it and found 324 RBC-wide /
   70 Avion closures over May-2025..Jun-2026 where the standing table holds 17,127 Avion
   closures. Every CPC read (state, writes, revocations, as-of-date) goes to
   `DDWV01.CPC_RB_PREF` (writes, with `APP_SYS_CD`) or `DDWV01.CPC_RB_PREF_MTHLY` (month-end
   snapshots). If a query, agent, or memory file names `CPC_RB_PREF_LOG`, stop and fix it
   before running. This supersedes every reference to "the log" / `CPC_RB_PREF_LOG` below —
   they describe the archaeology as it was built, not what to run today.

2. **What this project is.** Learning how these systems work. **We are not claiming anything.** Not
   hunting for a suppression gate, not proving who is or isn't reached, not building a compliance case.
   The one established finding: **there is no bridge between the two systems** (vendor/SFMC unsubscribes
   and the CPC preference log). That is the finding. Nothing further is asserted.

3. **We track all three switches — 1002, 1012, 1014 — as a set.** Do not isolate one. Do not ask which
   "should be the gate." The question is not which switch gates email.

4. **1014 is the most important, and here is why:** it is the **parameter used in the client decisioning
   tree in the design of every campaign** — campaign teams read 1014 to select clients for their
   communications. This is operational fact from Andre, and it outranks the EDW dictionary's nominal
   framing of 1014 as a cross-entity sharing consent. Consistent with §0.2 above: *"PREF_ID 1014 with
   CPC='N' = out of ALL RBC marketing."*

   → Therefore the comments in `museum/cpc_evidence_hdfs.py` lines 9-10 and 104 calling 1014
   *"CONTEXT ONLY / NOT a same-entity email gate"* are the dictionary's framing, not this project's.
   They are what pushed four red-team rounds and the deck toward a 1002 lead. Do not inherit them.

5a. **NO ROW ≠ BLANK (ruled by Andre 2026-07-25).** `CPC_RB_PREF_LOG` is a log of events. If a client has
   **no row** on a switch, no consent record for them exists anywhere in the corporation — they are a
   *true blank*, and they are **NOT an opt-out**. Exclude them from every "No" population.
   A client who **has a row** whose value is blank (5003) or No (5002) **is** counted as an opt-out on
   1014/1015. Why some systems write a blank row rather than leaving the client absent is not understood —
   see the open archaeology question on blank writers.
   → Therefore E2b's `standing_no_under_rule = explicit_no + blank` is **CORRECT**. 1014 among the
   319,733 unsubscribers = **89,518** (912 explicit + 88,606 blank). The 223,373 no-row clients stay out.

5. **Blank = No on 1014 and 1015 ONLY.** Blank = Yes on 1002/1006/1012 (§16 addendum line 767,
   §17-D line 814). `museum/cpc_evidence.sql` breaks this — `CLNT_CONSENT_TYP = 5002` in seven places,
   understating every 1014 figure. `cpc_evidence_hdfs.py` and `archaeology/23_cpc_landscape.py`
   (`is_no()`) implement it correctly.

6. **Folder rule — museum is FINAL ONLY.** `museum/` holds the finished, consolidated version of what
   goes into the deck: the evidence code, the deck, the README. It is a compilation of what the
   archaeology already settled — only what matters, nothing exploratory. **Never write a new
   investigative script into `museum/`.** Anything that explores, tests, checks, or answers a new
   question goes in `archaeology/` as the next `NN_topic.py`. It graduates into museum only when it is
   final, and only by replacing or extending the existing museum file — not by adding a sibling.

   Museum output must be usable **by an analyst**: tables and DataFrames that can be read, exported and
   plotted. Not print statements, not formatted console text, not a wall of hard-coded strings. If the
   only way to get a number out is to read it off a screenshot, the code is not finished.

7. **Never ask Andre to re-run something before grepping this file AND `RESULTS_CATALOG.md`.** He runs
   everything in a closed environment and results return only as photos of the screen, which do not
   survive a session. Any result he shares gets transcribed **in the same turn** into
   `unsub_tracking/RESULTS_CATALOG.md` — **ONE document, appended as a dated section. Never a new file
   per batch.** Record the cross-checks that prove the read (a sum matching a figure already in canon).
   Scripts with no catalogued output are unfinished.

8. **Holdout = channel slot XX (0 sends across 2.77M holdout decisions).** Action cells carry a
   channel code (`EM` for email); holdout/control cells carry none — the slot reads `XX`. Confirmed
   empirically 2026-09-04 (Pack 54 v3.1, §21). `TST_GRP_CD` is NOT a reliable arm indicator — no
   standard convention across MNEs (PCD alone has 81 distinct codes) — never use it to derive
   Action/Control.

---

## 0. REQUIRED OUTCOMES (locked 2026-07-15, team-confirmed)

1. **Value of an unsub** (exec ask): not all unsubs weigh the same. Segment unsubscribers by **TIBC × age** (TIBC = Transaction/Investment/Borrowing/Credit product-category counts, UCP), apply a per-segment LTV determined by us — "T-only at age 20 = decades of future NIBT lost." Deliverable: LTV given up, campaign by campaign; localization matrix showing where the problem is real vs where raw counts overstate it. UCP `Profitability` field to vet (risk: current-year contribution understates young clients). Enrichment runs Spark-side (UCP, merge clnt_no + MONTH_END_DATE).
2. **Population lost to campaigns over time — anchored on CPC, not vendor feedback.** Source of truth: `DDWV01.CPC_RB_PREF_LOG` (client preference log; PREF_ID 1014 with CPC='N' = out of ALL RBC marketing). Reason: business partners (Avion) distrust unsub data (suspected double counts). Deliverable: of ~15MM active clients, how many lost, trend vs year ago, campaign source via our unsub attribution chain.

**FINDING (2026-07-15 run, D1/D2): email unsubs do NOT write to CPC.** 649,885 distinct unsub clients since 2024-01-01; only 417 (0.06%) had ANY CPC change within 7 days, scattered across unrelated codes with mixed values = background noise. Conclusion: disposition_cd=4 is a VENDOR-level one-click unsub — it never touches bank consent; unsubbed clients remain CPC-contactable. Consequences: (1) "CPC validates unsubs" is dead as designed — the double-count objection is answered instead by methodology: DISTINCT clients, FIRST unsub (649,885 is deduplicated by construction); (2) population lost = TWO separate metrics: email-channel lost (vendor unsubs, campaign-attributable via our chain) and bank-consent lost (CPC opt-out trend by PREF_ID, NOT campaign-attributable via unsubs); (3) CONFIRMED with 90-day window: 2,161 of 649,885 (0.33%) — still coincidence-level; no slow batch pipe. Tested-and-refuted hypothesis — share with team before further builds.
**Switch independence (09 run, 2026-07-15):** (a) writes arrive in bundles — since 2024 ex-HSBC: 1.76M single-switch saves / 1.48M partial / 1.85M full form saves (6+ switches, one microsecond). (b) Dominant bundle = ONBOARDING BLANK-STAMPING: all top-20 same-timestamp pairs are 5003+5003 across product prefs + 1014 (~1.66M clients) — the system instantiates the whole switchboard as "never answered" at relationship open; clients don't choose at onboarding. No 5002+5002 pairs in top 20 → no mass opt-out cascade. (c) Contradiction census: of 50,738 entity-opted-out (1002=No), 3,996 hold explicit YES underneath (storage fully independent; hierarchy lives in suppression engines only) while 47,974 also carry explicit NOs (the opt-out FORM soft-cascades, the system doesn't enforce); 85,726 clients have explicit switch values with 1002 never set. Cross-validation: census 5002 rows sum = 50,738 = stock query exactly. RULE: read each switch independently; apply precedence only at evaluation; never infer one switch from another.

### MASTER SWITCH MAP — what each switch actually controls (the classification that matters)

Three functional classes. **For unsub/reachability work, only the CHANNEL-CLOSERS matter.**

| Class | Switches | Blank default | Closes a contact channel? |
|---|---|---|---|
| **CHANNEL-CLOSERS (Banking)** | **1002** entity DNS (master — closes ALL) · 1007 mail · 1008 phone · 1009 online · **1012 E-MAIL** (evidence-resolved, see below) · 1013 F2F · 1048 ATM · **vendor unsub (outside CPC — closes email at the ESP)** | allowed (open) | **YES** |
| **SHARING-ONLY per dictionary — ENFORCEMENT UNVERIFIED** | 1014 share-for-marketing · 1015 share-for-service · 1036 personalization · 1057 DI SfM · 1016 credit bureau | **1014/1015: NO**; others allowed | Dictionary says NO (data usage only) — but team lore says 1014=N = "out of all marketing". Which switches actually stop email = `12_switch_enforcement_test.sql` (state-before-window × received-email cross-tab, 1007 as negative control). **12 ran 2026-07-16** (E1+E2 recovered from photos 2026-07-23, §14) — but the raw cross-tab shows a bundle/selection confound, not a clean per-switch enforcement signal. Do not present the sharing-only classification as fact; still unresolved. |
| **TOPIC/CONTENT** (limit what, not whether) | products 1004/1006/1010/1023/1024/1025/1026/1044 (+ business codes) · services 1020-1022/1042-1043 · newsletters 1045/1046/1047 (email *content* subscriptions, not the channel) | allowed | NO (topic only) |

**The email-reachability set = vendor unsub + 1012 + 1002.** Everything else is noise for channel questions.

**1012 = Banking E-Mail — RESOLVED BY EVIDENCE (2026-07-15, W4):** Exact Target (the email ESP, APP_SYS_CD 7020) writes `1012=5002` as its dominant output — 11,702 rows, 2014→2026-06, still active. An email platform doesn't write a Mobile switch. Newer catalog agrees; the 2007 dictionary page (1012=Mobile) is stale.

**ESP pipe — final form of the finding:** the Exact Target→CPC pipe EXISTS but is a trickle: ~80 writes/month vs ~35K unsubs/month (~0.2%). It carries deeper opt-outs (preference-center actions: 1012, newsletters 1046/1045, even 54× entity 1002), NOT one-click unsubs. Phrase as "the pipe exists but doesn't carry unsubs," not "no pipe."

**Who writes what (W1-W3, 2026-07-15):** machines instantiate, humans flip. All full-form (6+) bundles are machine-written (7999 default-system; 1.6M of them internally mixed-code). Human channels write single switches (branch 7001: 427K singles vs 4K bundles). First-touch: 7999 (5.0M clients) + batch 7006 (1.4M) = administrative entry at onboarding; OLB (546K) top self-serve entry. Log universe ≈ **7.7M distinct clients** (supersedes earlier ~3.75M estimate from misread digits). Undocumented system codes seen at volume: **7033 (1.6M rows), 7053, 7028** — dictionary stale.

Full cube extract (switch × position × system × save-shape): `11_cpc_master_cube.sql`.

**Cube pivot findings (Andre's pivot, 2026-07-15 late):**
- **Two onboarding flavors:** mass blank-stamp = 5003 × 7999 (1.4–1.66M clients per switch) AND an explicit-Yes capture cohort = 5001 × 7033 (~229,265 clients, written to 1002 + all channel doors — except 1012).
- **7053 = a single-switch Yes engine** (1012: 14,458 · 1015: 13,210 · 1014: 8,294 · smaller others) — consent-capture flavor, possibly CASL express-consent flows. Earlier read of the 14,458 as an opt-out was WRONG — it is 5001 (Yes).
- **Single-switch email opt-outs 2024+:** 1,386 via 7020 Exact Target (cross-checks W4 ~80/mo) + ~93 branch. Tiny either way.
- **1012 is excluded from every mass-stamp block** (both onboarding flavors skip it) — only consent flows (7033/7053 Yes) and the ESP (7020 No) ever write it. Third independent evidence 1012 = email, and it means 1012's stored values are all signal, no administrative noise.
- **7033 and 7053 are undocumented system codes** (dictionary stale). PARKED: both write consents (Yes), not channel closures — ignorable for reachability work; relevant only for "who explicitly consented" questions.
- **Branch full-form opt-outs** (the soft cascade's mechanism): ~3.3–3.5K clients per switch at 5002 × 7001 in the 6+ block.
- Excel practice: the long extract IS the cube; pivots are 2D slices — use slicers (bundle/system/consent) to rotate, one small pivot per question, not nested mega-grids.

**CPC interpretation (critical):** rows are change events in EITHER direction (5001 Yes / 5002 No / 5003 blank; some 5001s are process-driven, e.g. 1036 auto-Yes at OLB enrol). Presence in table ≠ opt-out. Opted-out population = clients whose LATEST row per (client, pref) is 5002; absence = blank default (YES except 1014/1015 = NO). Stock+flow queries: `07_cpc_optout_stock_trend.sql`.

These two are guaranteed Power Pack slides; core vintages come after.

## 1. Mission & design

Holistic unsubscription tracking — not per-campaign. Two axes per unsubscriber:
- **Horizontal (timeline):** contact history before the unsub — how many contacts, cadence, and the exact deployment that triggered it.
- **Vertical (breadth):** how many distinct campaigns (MNEs) the client participated in.

**Statistical guardrail (non-negotiable):** unsubscriber-only analysis is selection-biased.
"Contacts before unsub" must be a hazard curve — P(unsub at contact n | still subscribed at n) — with at-risk denominators (clients who reached n contacts and did NOT unsub). Same for breadth: unsub rate per breadth level, not the breadth distribution of unsubscribers. Deployment attribution is *recorded* (see §3) — clean descriptively, but framed as diagnostic, not causal.

Targeted ≠ sent: the tactic table logs the decision; vendor feedback logs delivery. A client can be decisioned into email and never receive one (suppression, bounce). Targeting is the denominator concept; disposition 1 is the delivery reality.

---

## 2. Tables & schemas (empirically verified 2026-07-14)

Engine for all of this: **Teradata-direct** (EDW). Two-part names, no catalog prefix.
Full column catalogs: `schemas/vendor_feedback_tables_schema.md` (transcribed from Teradata metadata; source pics `pics/PXL_20260714_1604*.jpg`).

### DTZV01.VENDOR_FEEDBACK_EVENT — 9 columns (journey log)
`consumer_id_hashed (varchar), srvc_provdr_nm char(8), legal_entity_cd char(5), source_evnt_id varchar(30), disposition_dt_tm timestamp(6), disposition_tm_zone char(3), disposition_cd smallint, treatment_id varchar(50), load_tm timestamp(6)`

- **Grain = journey log.** One send → multiple rows sharing `(consumer_id_hashed, treatment_id)`, one per stage, each timestamped.
- `disposition_cd` (confirmed AUH Phase 1): **1=sent, 2=opened, 3=clicked, 4=unsubscribed, 5=hardbounce, 6=complaint.** 1→2→3 sequential; 4/5/6 outcome events.
- No `CLNT_NO` — resolve client through MASTER.
- Never count raw rows for funnel questions — collapse to send-journey grain first (§5).

### DTZV01.VENDOR_FEEDBACK_MASTER — 29 columns (send master)
Key cols: `treatment_id varchar(50) (=TACTIC_ID), clnt_no integer, consumer_id_hashed, email_addr, email_subj_line varchar(300), email_lang_cd, channel_type_cd char(3), cntct_mthd_typ char(3), category_cd, sub_category_cd, product_code, treatment_exp_dt (expiry, NOT send date), priority_score, card_no, load_tm` (full 29 in schema doc; col #17 `app_` vs `opp_product_typ_code` unresolved — photos conflict).

- Believed one row per client × email send — grain NOT yet verified (check via 01 pack Q3/Q4 fan-out).
- Rich analysis fields discovered: `email_subj_line` (creative-level unsub analysis), `channel_type_cd`/`cntct_mthd_typ` (channel candidates), `priority_score` (NBA priority at send).

### DG6V01.TACTIC_EVNT_IP_AR_HIST — decisioning / tactic history
(Alt name: DTZV01.TACTIC_EVNT_IP_AR_H60M — same logical table, 60-month rolling.)
Confirmed columns: `CLNT_NO, TACTIC_ID, TST_GRP_CD, RPT_GRP_CD, TACTIC_CELL_CD, TREATMT_STRT_DT, TREATMT_END_DT, TREATMT_MN, TACTIC_DECISN_VRB_INFO, ADDNL_DECISN_DATA1`.

- Grain: one row per client × tactic deployment (decision record).
- `TACTIC_ID` positions 8–10 = campaign MNE: `SUBSTR(TACTIC_ID, 8, 3)`.
- `TACTIC_DECISN_VRB_INFO` = packed string. **Never GROUP BY the raw column** — only a SUBSTR. Position 121 len 30 is the known marker slot (PCQ modal sales: `SUBSTR(...,121,30) LIKE '%MS%'`). Layout may be campaign-specific — verify per MNE before universal extraction.
- No native channel column (see §6).

---

## 3. Join map (the chain that makes attribution work)

```
EVENT (disposition, when)                    ← the unsub signal (disposition_cd=4)
  │  consumer_id_hashed + treatment_id       ← ONLY valid EVENT↔MASTER path
MASTER (clnt_no, creative, channel fields)   ← client + send context
  │  treatment_id = TACTIC_ID  AND  clnt_no = CLNT_NO   ← CLNT_NO REQUIRED (else fan-out to every decisioned client)
TACTIC_EVNT_IP_AR_HIST (TREATMT_STRT_DT, arm, MNE, packed info)  ← send timing + 60-mo contact history
```

**Attribution is directly recorded:** an unsub EVENT row carries the `treatment_id` of the send whose link was clicked — no last-touch inference needed. (Verify % non-null via 01 pack Q4a.)

**TACTIC_ID is unique per deployment (Andre, 2026-07-16):** the ID encodes MNE + julian date, so it is time-bound — each wave mints a new TACTIC_ID — and a client never duplicates on one TACTIC_ID. Consequences: `(TACTIC_ID, CLNT_NO)` is unique; **NO time-window conditions are needed in any join** (the exact key pins the deployment instance); all window logic was removed from 05/16/17 same day. Date floors in WHERE clauses are scan pruning only.

**Minimal field sets (all this pack needs):** MASTER → `consumer_id_hashed`, `TREATMENT_ID`, `CLNT_NO` (3 fields: composite key in, client number out). EVENT → `consumer_id_hashed`, `TREATMENT_ID`, `disposition_cd`, `disposition_dt_tm` (4 fields: key, what happened, when).

**Dead ends — columns that DO NOT EXIST (cost us a broken first run):**
- `SEND_DT` (MASTER) — send timing only via the decisioning join above
- `FEEDBACK_ID` (both) — auh_explore.sql's join was never valid
- `EVENT_TYPE` (EVENT)
Old repo code (auh_explore.sql, parts of imt pipeline) references these — do not trust it over this doc / the schema doc.

---

## 4. MNE tracking scope (Andre, 2026-07-14)

Extraction stays ALL-MNE (the MNE falls out of `SUBSTR(treatment_id,8,3)` — no filter needed at extract time); this list is the REPORTING scope. Filter downstream with an exact IN-list, never substrings.

| Group | MNE | Description (from env screenshots) | Status |
|---|---|---|---|
| Cards | PCQ | Cards Acquisition | confirmed in env list |
| Cards | PCL | Proactive Credit Limit Increase | confirmed |
| Cards | PCD | Credit Card Upgrade | confirmed |
| Cards | AUH | Authorized User | confirmed |
| Cards | CLI | Card Limit Increase Nurture | confirmed |
| Cards | MVP | Card Acquisition Nurture | confirmed |
| Cards | CRV | (Cards pod campaign, runs today) | KEEP (Andre 2026-07-14); not in env email list — verify presence in data |
| PBA | CTU | Chequing Account Right Fit | confirmed |
| Personal Lending | O2P | Pre-approved Overdraft Opportunity | confirmed |
| Payments | VDT | Activation Trigger | confirmed |
| Payments | VUI | Usage Trigger | confirmed |
| Payments | VUT | Wallet Provisioning | confirmed |
| Payments | VDA | BFCM Acquisition | confirmed (in env list) |
| Payments | VAW | (debit campaign, known from dashboard_sas work) | ⚠ not in env list — confirm |
| Payments | VCN | (debit campaign, known from dashboard_sas work) | ⚠ not in env list — confirm |
| Personal Loans | RCU | — | KEEP (Andre 2026-07-14); verify presence in data |
| Personal Loans | RCL | — | KEEP (Andre 2026-07-14); verify presence in data |

VVD is NOT an MNE (Andre 2026-07-14) — do not track.

Draft IN-list (fix the ⚠ entries before production use):
`('PCQ','PCL','PCD','AUH','CLI','MVP','CRV','CTU','O2P','VDT','VUI','VUT','VDA','VAW','VCN','RCU','RCL')`

### Full MNE dictionary (transcribed from env screenshots, pics/PXL_20260714_1647*/1648*.jpg)
Context: the screenshots came from an in-environment agent run answering "List all MNEs targeting EM (email)" — i.e., this is likely the email-targeting universe, not all NBA.

DAR Chequing Dormant Account (PBA-Echo) · CLI Card Limit Increase Nurture (Cards) · MVP Card Acquisition Nurture (Cards) · MNP Mortgage Nurture (HEF) · MAF Mortgage First Anniversary (HEF) · MSW Mortgage Switch (HEF) · NMO New Mortgage Opportunity (HEF) · MEC Mortgage Engagement (HEF) · AUS RBC Bank Access USA (US Banking) · AUT RBC Bank Access USA Trigger (US Banking) · MOS Investment Advice (PSI) · OFC/MWA/BPI/FND/ACK PBA Onboarding (PBA) · IMF International Money Transfer (PBA) · CRE Credit Education (PBA) · FRE Fraud Education (PBA) · PIE Investment Education (PSI) · PIH Investment Recommendation (PSI) · TFP TFSA/RRSP PAC set up (PSI) · TAO TFSA Acquisition (PSI) · PRA Chequing Restraint (PBA-Echo) · PPQ Chequing Account Acquisition (PBA) · PFS Chequing Funding (PBA-Echo) · CHQ Chequing Funding (PBA-Echo) ⚠ PFS/CHQ both read "Chequing Funding" across the two pics — verify · CTU Chequing Account Right Fit (PBA) · IDE Direct Investing Acct Acquisition (PSI) · GIS GIC Acquisition (PSI) · ESV HISA Savings Acquisition (PSI) · NBI Next Best Insurance Offer (INS) · NBR Loan Protector for Loan/RCL clients (INS) · PCQ Cards Acquisition (Cards) · PCL Proactive Credit Limit Increase (Cards) · PCD Credit Card Upgrade (Cards) · IOC Investment Offer Confirmation (PSI) · IOR Investment Offer Reminder (PSI) · AUH Authorized User (Cards) · MIR Mortgage Payment Increase Review (HEF) · MFO Mortgage Flood Prevention Offer (HEF) · CMU Accessory Dwelling Unit Opportunity (HEF) · MWP Mortgage Welcome Program (HEF) · MFY New Mortgage Touchpoint 3mo (HEF) · MAR Mortgage Auto Renewal Trigger (HEF) · RPB Chequing Retention (PBA-Echo) · VBP Balance Protector Acquisition (INS) · IPC IMT Proactive (Payments) · IRI IMT Reactive (Payments) · VDA BFCM Acquisition (Payments) · VDT Activation Trigger (Payments) · VUI Usage Trigger (Payments) · VUT Wallet Provisioning (Payments) · O2P Pre-approved Overdraft Opportunity (Personal Lending)

---

## 5. disposition_cd usage — journey patterns

Canonical patterns in `04_journey_query_patterns.sql`:
- **P1 (workhorse):** collapse to send-journey grain — GROUP BY `(consumer_id_hashed, treatment_id)`, `MAX(CASE WHEN disposition_cd = k …)` per stage flag, `MIN(CASE … disposition_dt_tm)` for first-occurrence times. Base for funnels, unsub-given-open, time-to-unsub.
- **P2:** campaign funnel — MNE × month × sequential stage counts, built on P1. Counts only; divide downstream.
- **P3:** ordered path — `ROW_NUMBER() OVER (PARTITION BY consumer, treatment ORDER BY disposition_dt_tm)`. Always scope to a treatment; unscoped = full-table window sort.
- **V1 (run before trusting funnels):** sequentiality violations — `unsub_without_sent` (preference-center opt-outs), `click_without_open` (image-blocking kills open pixel — expected, treat opens as a floor), `open_before_sent` (timezone/ordering).

---

## 6. Email-channel identification (CONFIRMED — env EDA 2026-07-15)

Validated in Andre's environment (window treatmt_strt_dt 2025-07-01→2026-07-01; ground truth = disposition_cd=1 sent universe, 303 distinct MNEs). Source pics `pics/PXL_20260715_1526*.jpg`.

**Production rule (184-MNE scope) — a tactic row is email-decisioned if EITHER fires; no MNE IN-list needed, the signal is the filter:**
```sql
   SUBSTR(t.tactic_decisn_vrb_info, 121, 30) LIKE '%EM%'      -- Priority 1: 55 MNEs
OR UPPER(COALESCE(t.addnl_decisn_data1,'')) LIKE '%EM%'       -- Priority 2: 129 MNEs
```

**Edge cases OUT of production scope (10 MNEs):** slot 101 of VRB_INFO (HPO, OII, OTC, RMG, SLC, VRE, WWC), TACTIC_CELL_CD (HPE, ZFE), TREATMT_MN (REM).
**Unresolvable (73 MNEs):** ~68 appear in vendor feedback with ZERO tactic rows in the window (K* block + others — likely a different decisioning system; no tactic-side denominator possible → use the EVENT-only view, 02 tracker); 5 in the tactic table with no EM signal in any field (ACF, BBP, BPU, VO3, ZXX).

Env reference files (Andre's environment, not this repo): `unsw_email_back.sql` (8-statement EDA S1–S8 that derived the rules), `tact/mme_channel_map_final.csv` (complete MNE→detection_field lookup, 267 rows), `email_funnel_by_cohort.sql` (env production funnel).
`governance/channel_codes.md`: EM=Email; IM=online banner, MB=mobile banner, CC, DM.
⚠ TODO: confirm none of the 17 tracked MNEs (esp. RCU, RCL, VAW, VCN) sit in the 68-MNE unresolvable block — check mme_channel_map_final.csv.

## 7. Engine rules & gotchas (Teradata-direct)

- `EXISTS` is only legal as a WHERE/ON predicate — NOT in select lists / CASE expressions. Use LEFT JOIN on DISTINCT projections for match-flag counting.
- `CAST(COUNT(*) AS BIGINT)` on any potentially-huge count (plain COUNT overflows, error 2616, >~2.1B rows).
- `SELECT TOP 5 *` for column discovery (works on the DTZV01 view layer; `HELP TABLE` may not). `TOP` is fine Teradata-direct; it is NOT Trino.
- Month buckets: `EXTRACT(YEAR)*100 + EXTRACT(MONTH)` (yyyymm) — type-agnostic (DATE or TIMESTAMP).
- CTEs (`WITH`) for prep; volatile tables ONLY where TDWM forces them (unconstrained product joins vs sys_calendar — not needed in these packs).
- Counts only in SQL — no rate divisions (divide in Excel/pandas downstream).
- History floor `DATE '2024-01-01'` on analysis scans (data reaches ~2018). EXCEPTION: join-coverage checks (03 J2/J3) run unwindowed — MASTER has no date column, so windowing the tactic side fakes join failures.
- Time grain in every extract output (month at minimum); pool downstream, never in extraction.

## 8. File index (repo `unsub_tracking/`)

| File | Purpose |
|---|---|
| `01_vendor_feedback_eda.sql` (+.py, unmaintained) | Table validation: catalogs, volumes, disposition mix, EVENT↔MASTER coverage, unsub attribution coverage, unsubs by MNE |
| `02_campaign_unsub_tracker.sql` (+.py, unmaintained) | League table: MNE × month × disposition counts from EVENT alone (no join) + NULL-treatment guard |
| `03_tactic_join_channel_validation.sql` | MASTER↔tactic join coverage + grain (J1–J4); EM channel-marker discovery (C1–C5) |
| `04_journey_query_patterns.sql` | disposition_cd usage patterns P1–P3 + sequentiality validation V1 |
| `05_email_journey_by_mne_cohort.sql` | THE volume summary: decisioned-email denominator (two-field rule) + client-distinct funnel per MNE × cohort month; 30-day disposition window per deployment (editable assumption) |
| `06_cpc_pref_log_eda.sql` | CPC decision queries: D1 which code unsubs flip, D2 unsub↔CPC linkage rate (RESULT: no pipe — 0.06%/0.33%) |
| `07_cpc_optout_stock_trend.sql` | CPC opt-out stock (latest-state) + monthly flow + timeline cube extract (Q4) |
| `08_reachability_overlap.sql` | Cross-tab unsub × 1002 × 1012 × 1014 flags — overlap/union of exit mechanisms. **Status (confirmed 2026-07-23, §14):** no run output found anywhere in the photo backlog — still UNRUN or unphotographed. |
| `09_cpc_switch_independence.sql` | Bundle sizes, same-timestamp pair matrix, contradiction census |
| `10_cpc_writes_by_system.sql` | APP_SYS_CD overlay: volume/bundle-shape/first-touch by system + Exact Target profile |
| `11_cpc_master_cube.sql` | THE cube extract: switch × position × system × save-shape (in-env pivot base) |
| `12_switch_enforcement_test.sql` | Which switch ACTUALLY stops email — state-before-window × received-email, 1007 negative control (settles 1014 dictionary-vs-lore). **RAN 2026-07-16, results recovered from photos 2026-07-23** — E1 (all-switch scan) + E2 (16-combo cross-tab) done, E3 (purpose-split) inconclusive; window unconfirmed; does NOT settle the 1014 dictionary-vs-lore question yet (§14) |
| `13_unsub_value_spine.sql` | Value spine: S1 first-unsub per client (in-env extract; embedded verbatim in 15 — never needs a standalone run) + S2 tracked-MNE league table |
| `14_cpc_optout_campaign_proximity.sql` | Did campaign sends precede CPC 1002 opt-outs? Backward proximity with base-rate control |
| `15_unsub_value_enrichment.py` | Spark/UCP (allowed .py — Lumina side): spine → TIBC×age segment matrix by trigger MNE + PROF_TOT_ANNUAL vetting |
| `16_population_lost_trend.sql` | Month × MNE, ALL MNEs, long format: em_clients_sent (disposition 1) + clients_first_unsub + tracked flag — Excel-pivot extract. **v4 (2026-07-23):** `clients_sent` re-booked to deployment month (was disposition month) — v3 rates were unusable for month-end deployers AUH/PCD (§13) |
| `17_em_decision_vendor_coverage.sql` | EM-decisioned → vendor coverage: sent_in_window/decisioned ratio, Cards five (CRV/PCL/PCQ/PCD/AUH) — 91–98% headline (§11, 2026-07-16). **RE-RAN 2026-07-22** with full per-MNE × month detail now transcribed (§13) |
| `18_vendor_retention_probe.sql` (Teradata-direct) | Quarterly rows/distinct-clients/min-max for MASTER (load_tm proxy) and EVENT (disposition_dt_tm) separately, unwindowed — settles how far back coverage goes. **RAN 2026-07-22** — retention resolved: ~7-yr rolling window, 12-mo lookback fully covered (§13) |
| `19_unsub_journey_lookback.sql` (Teradata-direct) | THE journey number: first-unsub cohort vs symmetric send-indexed stayed baseline, 12-mo lookback contacts + distinct MNEs, cohort_group × cohort_month summary. v1 (Trino, `APPROX_PERCENTILE`) errored 3706 running Teradata-direct in-env 2026-07-22 — converted; percentiles → banded distribution (see §12) |
| `21a_cpc_landscape.sql` (Teradata-direct) | SPLIT off the planned `21_cpc_study_consolidated.sql` — cheap, CPC-log-only half: Z1 stock, Z2 monthly flip trend, Z3 writer (APP_SYS_CD) attribution, E1/E2 purpose-field fill-rate. **RAN 2026-07-23** (§14-D) |
| `21b_cpc_bridge.sql` (Teradata-direct) | SPLIT off the planned `21_cpc_study_consolidated.sql` — expensive half, run alone in a fresh session: unsub-resolution pipeline feeding B-main/B-reverse (vendor-unsub↔CPC-flip gap timing) + O (5-flag reachability overlap). **RAN 2026-07-23** (§14-D) |
| `22_cpc_gate_evidence.sql` (Teradata-direct) | 22-A: writer attribution for B-reverse's bridged flips (closes Z3's open join). 22-B: gate-leak test — clients flagged out (1002/1012/1014 = No as-of window_start) vs received-email-in-window, main cut + exclusivity cut. **RAN 2026-07-23 — both decisions closed** (§14-E) |
| `museum/cpc_evidence.sql` (+ `museum/README.md`) (Teradata-direct) | The 6-evidence standalone proof pack for the CPC consent claim (two consent worlds, blind gate w/ before-after split, no bridge, leaking gate, plus red-team-driven suppression-scope and MNE-mix checks) — self-contained rerun of 21b/22's findings. **COMPLETE RUN 2026-07-24 (second run)** — all 6 Evidence blocks + summary now populated, Option A (consent standing full-history), deck numbers FINAL (§14-F, §14-G) |
| `cpc_gates_static.html` | one-screen static diagram: gate hierarchy + population Venn (shareable) |
| `UNSUB_TRACKING_KNOWLEDGE.md` | this doc |

Python note: `.py` versions discontinued at Andre's request (2026-07-14); SQL is the deliverable. The `.py` pattern, if ever needed again: pre-initialized `EDW` connector, `EDW.cursor()` → fetchall → DataFrame.

## 9. Open questions

1. Run outputs of packs 01–03 not yet reviewed (join coverage %, fan-out/grain buckets, channel-marker distributions, unsub attribution %).
2. MASTER grain unverified (one row per client × send?).
3. Semantics of disposition 4: one-click unsub vs preference-center vs list-level — determines whether an unsub kills all email or one program. Also whether repeated unsubs per client appear.
4. MASTER col #17: `app_` vs `opp_product_typ_code`.
5. ⚠ MNE presence: **VAW, VCN, CLI show ZERO rows in 16's output (2026-07-16)** — no sends, no unsubs since 2024-01 → apparently not in the vendor email universe. CLI absence is new information (always-on Cards priority #1 with no vendor email footprint — confirm with Andre whether CLI email exists at all). CRV/RCU/RCL confirmed present. PFS vs CHQ duplicate description still open.
6. Wedge decision (saturation evidence vs campaign league table vs standing monitor) — base layer built to serve all three.
7. Retention window of vendor feedback tables (Q1c/Q2b outputs will show).
8. VUT anomaly in 16: unsubs visible (~190 in one month) with little/no sends — verify sent-event coverage for VUT deployments.

## 10. Run results — 16 population lost trend (2026-07-16)

Source: Excel pivot of 16's output, filtered `tracked_mne=Y` (pic `pics/PXL_20260716_180439893.jpg`; phone photo, middle months hidden by scroll — numbers directional until in-env export).

- Ran end-to-end, full span 202401→202607. Two pivot sections: `first_unsub` and `has sent - from deployment` (= em_clients_sent).
- **PCQ is the biggest tracked burner and growing**: ~350–400 first-unsubs/mo in 2024 → ~600–820/mo in 2026; sends ~475–640K/mo.
- **PCL steepest rise**: double digits/mo in 2024 → ~270–435/mo in 2026 (sends ~420–575K/mo).
- PCD rising (~50–85 → ~90–250/mo); RCU steady ~50–190/mo; CRV low double digits; RCL single digits; VDT/VUI/VDA small.
- MVP and AUH first-unsubs only appear in recent months (late email starters).
- **Do NOT compute per-month rates naively**: numerator is booked to the UNSUB month, denominator to the SEND month — a campaign can book unsubs in a month it didn't deploy (sends=0 rows with unsubs>0 are expected, not a bug). Rate needs deployment-anchored alignment (05's per-deployment window) or annual aggregation.
- Scale context: tracked first-unsubs sum to roughly 1–2K/mo against ~35K/mo bank-wide → our tracked campaigns are a small share of total email burn (quantify with the `other_mne` rows, not visible in this pivot).

## 11. Run results — 17 EM-decisioned → vendor coverage (2026-07-16, Andre verbal)

Scope: CRV/PCL/PCQ/PCD/AUH, cohorts ≥ 2025-01-01. **sent_in_window / decisioned = 91–98%** across the five Cards MNEs. Verdict: the decisioned→vendor chain is essentially complete for Cards —
- vendor feedback is a valid measurement base for these campaigns (no material logging holes);
- the 2–9% gap = send-time suppression/throttling (expected, not data loss);
- the two-field EM-decisioning rule holds up (a leaky rule would show a much lower ratio).
Per-MNE/per-month detail and the in_master split not transcribed — headline only.

## 12. Power Pack Q3 spotlight — "Anatomy of an Unsub" design LOCKED (2026-07-22)

Three numbers, locked scope:
1. **Share of quarterly unsubs with Cards last-touch** — already built (`16_population_lost_trend.sql` v2). Not touched by this lock.
2. **Journey** — contacts in the 12 months before first-unsub vs. a contacted-but-stayed baseline. New build: `19_unsub_journey_lookback.sql`. Baseline is a **symmetric send-indexed risk set** (index on the client's own last send in the window, NOT a fixed calendar cutoff) — avoids conditioning bias from indexing only the unsub group on an event.
3. **Value matrix** — TIBC × **TENURE** (not age) profile of unsubbers. Framed as "value now unreachable by email." Revolver/transactor explicitly dropped as a segmentation axis for this spotlight.

Other decisions folded in:
- **CPC cold-open cut (Andre's call):** the CPC-anchored "population lost" narrative (§0 outcome 2) is NOT one of this spotlight's three numbers — parked separately, not part of the cold open.
- **Tenure over age:** `TENURE_RBC_YEARS` is a confirmed UCP field (`schemas/ucp_business_curated_fields.md` L41-42, corroborated `campaigns/CRV/ucp_profiling/profile_4groups.py` L31) and was already being pulled/used in `15_unsub_value_enrichment.py` before this lock — swap, not a new dependency.
- **New retention check:** `18_vendor_retention_probe.sql` settles how far back MASTER/EVENT coverage goes (quarterly, unwindowed) — feeds the "is 12 months of lookback covered" question behind #2.
- Packs 18 and 19 added to the file index (§8); 13 and 16 untouched.

**2026-07-22 engine fix — both packs run Teradata-direct, not Trino:** 18 and 19 were
drafted with a `-- ENGINE: Starburst/Trino` header even though they touch only DTZV01
tables (single-source EDW, same as siblings 13/16). Andre ran 19 Teradata-direct and hit
**error 3706** ("Data type lookback_contacts does not match a defined type name") —
Teradata's signature error for an unknown function, here `APPROX_PERCENTILE` (Trino-only,
no Teradata equivalent). Both files converted in place:
- **19:** `APPROX_PERCENTILE(25/50/75)` on `lookback_contacts` and `lookback_mnes` replaced
  with a **banded distribution** (exact counts of clients per band, per cohort_group ×
  cohort_month) — median is read off the bands instead. Bands (editable assumption, set in
  the final SELECT's CASE WHEN): contacts = 0 / 1 / 2 / 3-4 / 5-6 / 7-9 / 10-14 / 15+; mnes
  = 0 / 1 / 2 / 3 / 4 / 5+. Added `AVG` of both (CAST to `DECIMAL(10,1)`) alongside the
  bands — safe Teradata-direct since the 9881 pushdown-ROUND hazard is a Starburst
  artifact that never fires on a native Teradata session. Also fixed a latent UNION ALL
  truncation bug in the `population` CTE: `'unsub'` (5 chars) as the first UNION branch
  would have silently truncated `'stayed'` (6 chars) to `'staye'` per the CLAUDE.md hard
  rule — both branches now `CAST(... AS VARCHAR(10))`. The 12-month lookback join swapped
  Trino's `p.index_dt - INTERVAL '12' MONTH` for `ADD_MONTHS(CAST(p.index_dt AS DATE), -12)`
  (Teradata-native; the CAST-to-DATE-before-arithmetic idiom matches pack 14's
  `CAST(o.CHG_TMSTMP AS DATE) - 30`).
- **18:** no real Trino-only construct was present — the quarter-bucket arithmetic
  (`EXTRACT(...) * 10 + ((EXTRACT(MONTH ...) - 1) / 3 + 1)`) is portable as written
  (INTEGER/INTEGER truncates the same way on both engines). Only the header tag was wrong;
  fixed to `ENGINE: Teradata-direct`, no query-body changes.
- Both headers' engine tags now read `Teradata-direct`; Trino/pushdown-specific caveats
  (APPROX_PERCENTILE caution, Starburst pushdown-guard comment) rewritten to Teradata-direct
  framing (spool guard instead of pushdown guard).

## 13. Run results — 18 vendor retention probe + 17 re-run (2026-07-22)

### Pack 18 — vendor retention probe RESULTS (run 2026-07-22, Teradata-direct)
Source pics: `pics/PXL_20260722_233811045.jpg` (MASTER tab), `pics/PXL_20260722_233958940.jpg` (EVENT tab).

- **MASTER (load_tm proxy):** coverage 2019Q3 → today. Earliest load_tm **2019-07-22 20:29 — exactly 7 years before run date** → strongly suggests a **rolling 7-year retention window**, not table birth. Recent quarters ~80–128M rows, ~10M distinct clients/quarter. 2026Q3 partial (10.7M rows, 4.55M clients through 2026-07-22).
- **EVENT (disposition_dt_tm):** coverage back to 2018Q2 but sparse/unreliable before 2019Q4 (20182: 101K rows; 20183: 295 rows; 20192: 9K rows — clearly a partial early feed). Solid from 2019Q4 on. Recent quarters ~120–180M rows, ~11M distinct clients. 2026Q3 partial through 2026-07-22.
- OCR caution: EVENT 20193 row transcribed as n_rows 4,628,956 < n_distinct 6,323,849 — impossible, phone-photo misread, treat that cell as unreliable.
- **DECISION RESOLVED:** the 12-month lookback for pack 19 is fully covered for any plausible spotlight cohort. Earlier "mid-2025 retention" note and the "2023" belief (§9 Q7) were both wrong — coverage is ~7 years.

### Pack 17 — decisioned→vendor funnel, per-MNE × month RESULTS (run 2026-07-22, Cards five, 202501–202607)
Source pics (scrolled views of one grid, cross-checked): `pics/PXL_20260722_233607118.jpg`, `pics/PXL_20260722_233625550.jpg`, `pics/PXL_20260722_233634398.jpg`. Columns: mne, cohort_yyyymm, clients_decisioned_em, in_master, sent, opened, clicked, unsub, hardbounce, complaint.

Numbers are **directional from phone OCR** — overlapping shots disagreed on a few cells (a PCL row read 202501 in one shot, 202601 in another; a couple of unsub cells shifted by one row between reads). Ranges (`~lo–hi`) mark the disagreement; slide-final numbers must come from the in-env export.

| MNE | Month | Dec | Master | Sent | Open | Click | Unsub | HB | Compl |
|---|---|---|---|---|---|---|---|---|---|
| AUH | 202602 | 94,846 | 86,575 | 86,575 | 56,032 | 324 | 147 | 69 | 3 |
| AUH | 202604 | 661,591 | 555,967 | 555,967 | 360,808 | 2,389 | 765 | 301 | 12 |
| CRV | 202501 | 52,745 | 24,567 | 24,567 | 15,003 | 223 | 26 | 9 | 0 |
| CRV | 202502 | 57,235 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| CRV | 202503 | 63,871 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| CRV | 202504 | 57,748 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| CRV | 202505 | 51,806 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| CRV | 202506 | 55,392 | 40,171 | 40,171 | 23,593 | 350 | 81 | 41 | 1 |
| CRV | 202507 | 20,646 | 15,196 | 15,196 | 8,938 | 117 | 25 | 13 | 0 |
| CRV | 202508 | 49,703 | 39,175 | 39,175 | 24,431 | 279 | 107 | 58 | 2 |
| CRV | 202509 | 60,427 | 53,986 | 53,986 | 31,069 | 520 | 167 | 70 | 1 |
| CRV | 202510 | 59,205 | 49,435 | 49,435 | 29,993 | 445 | 121 | 74 | 1 |
| CRV | 202511 | 59,144 | 52,846 | 52,846 | 33,183 | 453 | 117 | 89 | 2 |
| CRV | 202512 | 58,606 | 51,612 | 51,612 | 30,913 | 463 | 148 | 91 | 2 |
| CRV | 202601 | 57,858 | 49,283 | 49,283 | 30,812 | 501 | 167 | 84 | 2 |
| CRV | 202602 | 60,355 | 56,422 | 56,422 | 37,780 | 420 | 140 | 74 | 2 |
| CRV | 202603 | 76,043 | 70,751 | 70,751 | 44,877 | 609 | 184 | 76 | 1 |
| CRV | 202604 | 59,696 | 55,638 | 55,638 | 33,329 | 394 | 106 | 59 | 0 |
| CRV | 202605 | 56,161 | 49,126 | 49,126 | 33,217 | 342 | 81 | 40 | 1 |
| CRV | 202606 | 72,190 | 53,141 | 53,141 | 29,819 | 384 | 55 | 58 | 0 |
| CRV | 202607 | 40,885 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| PCD | 202501 | 410,427 | 383,705 | 383,705 | 261,797 | 17,013 | 150 | 274 | 13 |
| PCD | 202502 | 455,880 | 426,915 | 426,915 | 292,341 | 20,503 | 126 | 291 | 15 |
| PCD | 202503 | 447,749 | 420,463 | 420,463 | 266,389 | 21,098 | 170 | 291 | 9 |
| PCD | 202504 | 259,627 | 243,476 | 243,476 | 164,822 | 10,436 | 111 | 251 | 5 |
| PCD | 202505 | 451,149 | 423,571 | 423,571 | 294,561 | 21,126 | 244 | 478 | 7 |
| PCD | 202506 | 420,082 | 392,697 | 392,697 | 262,160 | 14,731 | 363 | 372 | 9 |
| PCD | 202507 | 453,788 | 421,844 | 421,844 | 284,023 | 16,956 | 853 | 438 | 10 |
| PCD | 202508 | 342,576 | 317,116 | 317,116 | 209,537 | 12,187 | 801 | 423 | 5 |
| PCD | 202509 | 411,126 | 380,070 | 380,070 | 260,132 | 16,729 | 748 | 342 | 9 |
| PCD | 202510 | 429,228 | 394,905 | 394,905 | 259,457 | 17,462 | 728 | 340 | 9 |
| PCD | 202511 | 127,047 | 116,637 | 116,637 | 80,447 | 4,416 | 197 | 98 | 1 |
| PCD | 202512 | 486,351 | 448,928 | 448,928 | 315,630 | 20,037 | 819 | 384 | 11 |
| PCD | 202601 | 326,672 | 300,050 | 300,050 | 212,422 | 17,554 | 535 | 232 | 11 |
| PCD | 202602 | 369,123 | 337,606 | 337,606 | 234,266 | 16,590 | 618 | 230 | 12 |
| PCD | 202603 | 573,375 | 523,827 | 523,827 | 362,475 | 25,665 | 914 | 353 | 10 |
| PCD | 202604 | 335,381 | 303,977 | 303,977 | 209,248 | 11,128 | 429 | 151 | 5 |
| PCD | 202605 | 339,381 | 306,638 | 306,638 | 215,070 | 15,650 | 418 | 227 | 2 |
| PCD | 202606 | 413,173 | 372,171 | 372,171 | 261,834 | 16,578 | 388 | 269 | 10 |
| PCD | 202607 | 244,841 | 219,081 | 219,081 | 141,029 | 6,438 | 83 | 129 | 1 |
| PCL* | 202501 | 854,240 | 845,933 | 845,933 | 484,377 | 55,839 | 435 | 595 | ~20 |
| PCL* | 202502 | 472,945 | 468,381 | 468,381 | 275,713 | 18,554 | ~297 | ~327 | ~12 |
| PCL* | 202503 | 433,133 | 429,049 | 429,049 | 237,708 | 23,525 | ~297 | ~327 | ~12 |
| PCL* | 202504 | 519,774 | 514,768 | 514,768 | 288,235 | 26,019 | 221 | 253 | 6 |
| PCL* | 202505 | 620,383 | 614,204 | 614,204 | 337,726 | 26,783 | 286 | 355 | 10 |
| PCL* | 202506 | 507,237 | 501,952 | 501,952 | 284,024 | 21,220 | 334 | 437 | 5 |
| PCL* | 202507 | 713,603 | 703,685 | 703,685 | 418,068 | 36,736 | 581 | 322 | 3 |
| PCL* | 202508 | 498,706 | 492,960 | 492,960 | 245,845 | 23,102 | 1,579 | 615 | 11 |
| PCL* | 202509 | 534,024 | 527,351 | 527,351 | 301,820 | 27,543 | ~1,076–1,141 | ~470–799 | ~4–9 |
| PCL* | 202510 | 497,200 | 489,468 | 489,468 | 276,373 | 24,642 | ~1,060–1,141 | ~605–799 | ~9–10 |
| PCL* | 202511 | 537,507 | 530,201 | 530,201 | 331,363 | 43,976 | ~1,060–1,093 | ~605–655 | ~7–10 |
| PCL* | 202512 | 634,078 | 624,815 | 624,815 | 369,932 | 37,521 | ~1,093–1,296 | ~655–816 | ~6–7 |
| PCL* | 202601 | 783,382 | 771,046 | 771,046 | 454,584 | 38,298 | ~1,296–1,524 | ~816–903 | ~6–11 |
| PCL* | 202602 | 481,256 | 473,220 | 473,220 | 274,998 | 26,774 | ~984–1,524 | ~496–903 | ~4–11 |
| PCL* | 202603 | 573,045 | 562,891 | 562,891 | 309,574 | 27,821 | ~984–1,046 | ~447 | ~3–4 |
| PCL* | 202604 | 577,889 | 567,377 | 567,377 | 325,223 | 28,781 | 881 | 402 | ~3–9 |
| PCL* | 202605 | 550,780 | 540,602 | 540,602 | 303,760 | 24,596 | 602 | 397 | ~6–9 |
| PCL* | 202606 | 585,309 | 574,179 | 574,179 | 314,283 | 29,729 | 392 | 400 | ~5–6 |
| PCL* | 202607 | 444,355 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| PCQ | 202501 | 505,008 | 476,191 | 476,191 | 298,472 | 7,698 | 391 | ~652 | ~20 |
| PCQ | 202502 | 549,813 | 518,634 | 518,634 | 316,230 | 7,460 | ~391–425 | ~652–769 | ~18–20 |
| PCQ | 202503 | 522,234 | 492,147 | 492,147 | 276,839 | 6,517 | ~398–425 | ~717–769 | ~18–20 |
| PCQ | 202504 | 536,000 | 508,762 | 508,762 | 286,840 | 5,894 | ~398–563 | ~717–870 | ~19–20 |
| PCQ | 202505 | 531,482 | 504,471 | 504,471 | 289,273 | 5,620 | ~540–563 | ~740–870 | ~14–19 |
| PCQ | 202506 | 506,899 | 478,750 | 478,750 | 273,142 | 5,374 | ~540–666 | ~621–740 | ~13–14 |
| PCQ | 202507 | 485,324 | 446,906 | 446,906 | 259,713 | 4,576 | 1,050 | ~621–676 | ~11–13 |
| PCQ | 202508 | 480,443 | 448,176 | 448,176 | 258,210 | 5,351 | ~1,050–1,399 | ~676–842 | ~11–18 |
| PCQ | 202509 | 487,178 | 458,320 | 458,320 | 281,433 | 5,440 | ~1,193–1,399 | ~842–1,027 | ~18–27 |
| PCQ | 202510 | 515,491 | 479,370 | 479,370 | 292,306 | 5,654 | 1,174 | ~929–1,027 | ~22–27 |
| PCQ | 202511 | 557,684 | 517,749 | 517,749 | 321,095 | 14,598 | 1,377 | ~929–1,214 | ~22–24 |
| PCQ | 202512 | 567,988 | 533,166 | 533,166 | 324,131 | 7,868 | 1,464 | 1,095 | 22 |
| PCQ | 202601 | 536,613 | 503,316 | 503,316 | 298,209 | 4,924 | 1,295 | 953 | 16 |
| PCQ | 202602 | 594,348 | 551,781 | 551,781 | 317,752 | 14,009 | 1,451 | 852 | 25 |
| PCQ | 202603 | 595,044 | 556,129 | 556,129 | 302,423 | 8,406 | 1,359 | 641 | 14 |
| PCQ | 202604 | 599,479 | 559,295 | 559,295 | 315,637 | 6,436 | 1,295 | 543 | 15 |
| PCQ | 202605 | 222,201 | 206,970 | 206,970 | 113,360 | 1,872 | 442 | 179 | 6 |
| PCQ | 202606 | 721,489 | 657,880 | 657,880 | 381,038 | 9,903 | 1,204 | 899 | 23 |
| PCQ | 202607 | 357,771 | 328,227 | 328,227 | 184,132 | 2,788 | 198 | 371 | 2 |

*PCL rows carry the most cross-shot OCR disagreement — treat all PCL cells as directional pending in-env export.

**Findings:**
1. **PCL unsub step-change at 202508:** ~300–600/mo (202501–202507) → 1,579 (202508), sustained ~1,000–1,500/mo after. PCQ shows a similar climb (~400/mo → ~1,000–1,400/mo) starting ~202507–202509. Hypothesis to investigate: what deployed/changed Aug–Sep 2025 — open question, not answered.
2. **CRV vendor black hole:** 202502–202505 have 51–64K decisioned/mo but ZERO vendor rows (in_master=0) — vendor gap or CRV email not routed through this vendor those months. Exclude/footnote in any CRV trend.
3. **clients_in_master == clients_sent in every single row** — funnel step degenerate; presence in MASTER implies sent.
4. **July 2026 (202607) feed is partial and per-campaign:** PCD flowing, CRV/PCL zero → spotlight quarter must stop at 202606.
5. **Scale check for the spotlight "share" number:** Cards-five unsubs sum ≈1.5–4K/mo vs ~35K/mo program-wide (§10 finding) → Cards' share of program unsubs is plausibly ~5–10%; if 16 v2 confirms, the slide story flips from "Cards drive unsubs" to "Cards are a minor contributor — the problem is program-level." Awaiting 16 v2 for last-touch confirmation.
6. **AUH appears only episodically** (202602, 202604) — consistent with phased deployments, not monthly cadence.

### 16 v3 rollup (a) first read (2026-07-23, pics/PXL_20260723_000436256.jpg)

Cards-five × month first-unsub + sent table ran (16 v3, rollup (a), Teradata-direct).

**BOOKING MISMATCH found:** `clients_sent` was booked to the vendor's `disposition_dt_tm` calendar month while `clients_first_unsub` was booked to the triggering-deployment month — two different clocks on the same row. For campaigns deploying near month-end this splits one deployment's sends across the month boundary from its unsubs: AUH 202604 shows 356 unsubs vs only 8 sent, and 202605 shows 0 unsubs vs 555,967 sent — same deployment, torn in half by the axis mismatch. PCD's sent column alternates ~84K/598K/157K/633K month-to-month for the same reason. **Monthly unsub/sent rate math from this v3 output is unusable for AUH/PCD.** → **v4 fix:** `clients_sent` re-booked to the deployment month using the identical mechanism `clients_first_unsub` already uses (see `16_population_lost_trend.sql` `sent_raw`/`sent_a` CTEs, 2026-07-23).

Preliminary findings below are **directional, pending v4 re-run** — do not cite as final:
- **PCQ first-unsub/sent rate ≈ 0.10–0.14%/mo**, roughly 2–3× PCL's rate (~0.04–0.06%/mo). CRV/PCL/PCQ deploy mid-month often enough that the axis mismatch is smaller — these three reads are directionally OK; AUH/PCD are not.
- **Cards-five first-unsubs ≈ 1,200–1,800/mo** vs program-wide ~26K+/mo → Cards' share of program unsubs ≈ 5–7%, consistent with the §13 Pack-17 scale check (finding 5). Awaiting rollup (b) confirmation on the v4 re-run before this share is final.

## 14. CPC study reopened — recovered artifacts (2026-07-23)

The 2026-07-15/16 photo backlog (29 shots) was fully reviewed and identified. This recovers Pack 12's E1/E2/E3 results — run 2026-07-16, never catalogued until now — and surfaces an adjacent MarTech initiative relevant to the CPC↔vendor-unsub boundary (§0 finding).

### A. MTEC-12644 "Real-time CPC Unsubscribe" (Confluence, transcribed from `pics/PXL_20260715_2315*.jpg` / `2316*.jpg`)

- Project: MarTech, author T.S. (MarTech; diagram owner H., first name only on page — full names withheld from repo, known to Andre), last updated 2026-06-18. Jira MTEC-12644 itself inaccessible (the page says so). Blueprint PARTIALLY complete — objectives/functional-reqs/assumptions sections are still template placeholders.
- **Business context:** the existing daily batch integration for RBC email opt-out is being enhanced with a REAL-TIME capability. Drivers: Gmail's Feb-2024 inbox-provider unsub requirements; decoupling from ESPs (reusable across Salesforce, Sendgrid, AWS); decoupling the surrogate vendor ID from API consumers.
- **Key quotes:** "Preserving the batch process ensures a 100% match between RBC and SFMC unsubscribe data as of a given date/time." — and — "The Client Communication domain (PRSO application) has no access to channel consent today."
- **Architecture** (Lucid diagram, `pics/PXL_20260715_231554794.jpg`): SFMC → Email → Public Secure Opt-Out Page → (surrogate ID + address) → new PRSO Opt-Out API, sitting between the CPC-CC API and CPC-PC API → CPC-PC; new PRSO ID Table (future). Existing daily batch path (files → CPC-CC API → Daily Delta cylinders) explicitly unchanged.
  - **In scope:** S4 Multi-Org only; PRSO onboarded to CPC-CC APIs; new client + non-client public unsubscribe pages (CASL-compliant, API refs `C000-CPC-CC-CustPrefMod` / `C000-CPC-PC-CustContactPrefMod`); new external PRSO API.
  - **Out of scope:** CPC-CC opt-out to SRF# (future phase); changes to the existing batch; changes to CPC C000 APIs.
  - **NFRs:** <1 TPS, ≤2s response, 24/7, external Apigee, PingF auth.
- **INTERPRETATION (flag as interpretation, not fact):** together with the 0.06% CPC linkage (§0 D2) and the ~80/mo Exact Target trickle (§0 ESP-pipe finding), the coherent reading is that email unsubs live in the ESP/SFMC suppression world (batch-file sync) and do NOT write to `CPC_RB_PREF_LOG` at scale today; CPC channel consent is a separate universe PRSO cannot read; MTEC-12644 builds the real-time bridge. Direction of the "100% match" batch claim (RBC→SFMC vs SFMC→RBC) is AMBIGUOUS in the source doc — do not state as fact. **UPDATE (2026-07-23, §14-D):** the "does this batch write to CPC" half is now EMPIRICALLY CONFIRMED, not just a coherent reading — Z2/Z3/B-reverse/O (packs 21a/21b) show no batch-writer footprint and no sync-cadence clustering. Direction of the "100% match" claim itself remains unconfirmed; sharpened MarTech ask: confirm nothing syncs today + get the MTEC-12644 go-live date.

### B. Pack 12 results RECOVERED from photos — E1 + E2 ran 2026-07-16, never catalogued until now

Pack 12 (`12_switch_enforcement_test.sql`) DID run. Its output sat in phone photos, uncatalogued, until this recovery pass.

**E1 — all-switch scan** (`pics/PXL_20260716_001838168.jpg`): for every switch, clients whose latest state was No (5002) BEFORE the window, vs. how many of those received an email IN the window. Full 33 rows (rate computed for readability, not part of the source screenshot):

| Switch | Clients (No/5002 pre-window) | Received email in window | Rate |
|---|---|---|---|
| -1 (baseline, all) | 7,563,199 | 3,688,588 | 48.8% |
| 1002 | 50,271 | 9,676 | 19.2% |
| 1004 | 47,095 | 9,576 | 20.3% |
| 1006 | 48,600 | 9,798 | 20.2% |
| 1007 | 47,984 | 9,345 | 19.5% |
| 1008 | 47,482 | 10,067 | 21.2% |
| 1009 | 49,068 | 9,905 | 20.2% |
| 1010 | 46,240 | 9,182 | 19.9% |
| 1012 | 33,964 | 10,232 | 30.1% |
| 1013 | 48,075 | 9,351 | 19.5% |
| 1014 | 80,750 | 21,195 | 26.3% |
| 1015 | 151,708 | 39,917 | 26.3% |
| 1016 | 1,454,381 | 647,683 | 44.5% |
| 1020 | 17,000 | 1,583 | 9.3% |
| 1021 | 18,237 | 1,655 | 9.1% |
| 1022 | 17,004 | 1,569 | 9.2% |
| 1023 | 46,378 | 9,244 | 19.9% |
| 1024 | 47,910 | 9,370 | 19.6% |
| 1025 | 48,558 | 9,606 | 19.8% |
| 1026 | 47,809 | 9,392 | 19.6% |
| 1027 | 1,314 | 103 | 7.8% |
| 1028 | 1,312 | 100 | 7.6% |
| 1030 | 1,312 | 102 | 7.8% |
| 1031 | 1,310 | 98 | 7.5% |
| 1032 | 1,235 | 76 | 6.2% |
| 1033 | 1,229 | 75 | 6.1% |
| 1034 | 1,316 | 103 | 7.8% |
| 1036 | 145,602 | 47,932 | 32.9% |
| 1042 | 40,239 | 11,002 | 27.3% |
| 1044 | 46,186 | 9,155 | 19.8% |
| 1045 | 125 | 70 | 56.0%† |
| 1046 | 2,419 | 1,835 | 75.9%† |
| 1048 | 47,822 | 9,272 | 19.4% |

†1045/1046 denominators are tiny (125, 2,419) — treat as noise, not signal.

**E1 reading:** baseline receive rate ≈48.8%; nearly ALL No-switches cluster ~19–30% regardless of channel relevance (1007 mail 19.5% vs 1012 email 30.1% — a channel-specific switch and an irrelevant one land in the same band). Consistent with a bundle/selection confound (same clients hit many switches at once, per the §0 pack-09 bundle finding) rather than each switch individually gating email. Repeated ~46–50K holder counts recurring across many unrelated switches = the same bundled opt-out population showing up again and again.

**E2 — 16-combo cross-tab** (`pics/PXL_20260716_000048758.jpg`, `000523483.jpg`, `001843490.jpg` — three separate captures, values identical/stable across all three): `out_1002 × out_1012 × out_1014 × out_1007_dm_control` (flag order as labeled in the query) vs. clients / received_email_in_window. Full 16 rows:

| 1002 | 1012 | 1014 | 1007 (DM ctrl) | Clients | Received email | Rate |
|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 3,999,323 | 2,304,010 | 57.6% |
| 0 | 0 | 0 | 1 | 395 | 137 | 34.7% |
| 0 | 0 | 1 | 0 | 34,096 | 12,795 | 37.5% |
| 0 | 0 | 1 | 1 | 61 | 27 | 44.3% |
| 0 | 1 | 0 | 0 | 12,228 | 5,956 | 48.7% |
| 0 | 1 | 0 | 1 | 1,362 | 408 | 30.0% |
| 0 | 1 | 1 | 0 | 157 | 61 | 38.9% |
| 0 | 1 | 1 | 1 | 62 | 25 | 40.3% |
| 1 | 0 | 0 | 0 | 993 | 256 | 25.8% |
| 1 | 0 | 0 | 1 | 1,411 | 514 | 36.4% |
| 1 | 0 | 1 | 0 | 2,475 | 384 | 15.5% |
| 1 | 0 | 1 | 1 | 25,237 | 4,740 | 18.8% |
| 1 | 1 | 0 | 0 | 529 | 178 | 33.6% |
| 1 | 1 | 0 | 1 | 964 | 441 | 45.7% |
| 1 | 1 | 1 | 0 | 170 | 110 | 64.7% |
| 1 | 1 | 1 | 1 | 18,492 | 3,053 | 16.5% |

Flag order convention: columns read `1002, 1012, 1014, 1007` left to right; 1 = switch was No (out) before the window, 0 = not.

**CAVEAT (unverified):** none of the E1/E2 screenshots show the underlying SQL or the date window — window boundaries are unconfirmed (likely Q2-2026 per the file's design intent, but mark unverified, not confirmed). No enforcement conclusion should be drawn from E1/E2 alone until the window is confirmed and a proper controlled comparison (not a raw cross-tab) is run.

**E3 — purpose-split** (per photo IDs `pics/PXL_20260716_001847332.jpg`, `001852198.jpg`): attempted split by `CONTACT_PURPS_TYP` / `CNTCT_EVNT_INITIATOR` (marketing vs. service). Fields came back MOSTLY EMPTY — one summary showed ~155K send-rows against only ~9.6K distinct clients with a populated purpose field. Reading: purpose fields are likely too sparse to support a marketing-vs-service split as currently populated; needs a fill-rate probe before use in any enforcement redesign.

**Also recovered:** `pics/PXL_20260715_234124009.jpg` is an Excel "cpc cube" pivot (`PREF_ID × CLNT_CONSENT_TYP × APP_SYS_CD`) — NOT the E2 table as earlier assumed. Notable legible cells: 1012/5002/7020 = 1,386; 1012/5001/7999 = 14,458; the 1016 row dominates volumes overall. Partial read only — needs an in-env export if this cube is ever needed for real.

### C. Photo-index note

The 2026-07-15/16 photo backlog (29 shots) is now fully identified: MTEC-12644 Confluence set (5 shots, §A), CPC query results (E1/E2/E3 + cube pivots + bundle/system scans, §B and the §0 cube-pivot findings), and the RBC consent-code dictionary page (`pics/PXL_20260715_223246054.jpg`).

**§8 flag update:** pack 08 (`08_reachability_overlap.sql`) — still no run output found anywhere in the backlog; remains UNRUN or unphotographed. Pack 12 (`12_switch_enforcement_test.sql`) — E1+E2 results RECOVERED above (directional, window unconfirmed); E3 ran but purpose fields came back empty, inconclusive. Neither settles the §0 "1014 dictionary-vs-lore" enforcement question — see the updated MASTER SWITCH MAP cross-reference in §0.

### D. Run results — 21a/21b (2026-07-23)

The planned `21_cpc_study_consolidated.sql` was SPLIT before running into `21a_cpc_landscape.sql` (cheap, CPC-log-only) and `21b_cpc_bridge.sql` (expensive, unsub-resolution + bridge pipeline, run alone in a fresh session) — see §8. **Both RAN 2026-07-23.** Source pics: `pics/PXL_20260723_2240*.jpg`/`2241*.jpg` (21a), `pics/PXL_20260723_2252*.jpg` (21b).

**Z2 — monthly flip trend (1002/1012/1014 → 5002, 202507–202607):** stable, no trend across the 13-month window. 1002 ≈126–230/mo (13-mo total 2,168); 1012 ≈38–189/mo (total 1,541); 1014 ≈366–907/mo (total 8,642). 202607 is a partial month. Total CPC opt-outs across all three switches ≈1K/mo against ~35K/mo vendor unsubs (§0 finding) → gap ≈35×. **Correction:** an earlier "150×" figure in circulation was 1002-only; the all-three-switch ratio is ≈35×.

**Z3 — writer attribution (flips by APP_SYS_CD):**

| PREF_ID | 7001 (branch) | 7003 (contact centre) | 7006 (batch) | 7020 (SFMC/ESP) | 7033 | 7053 |
|---|---|---|---|---|---|---|
| 1002 | 1,160 | 644 | 147 | 1 | — | 216 |
| 1012 | 689 | 578 | 29 | 201 | — | 44 |
| 1014 | 5,756 | 539 | 219 | — (no row) | 2,128 | — |

READING: humans (branch 7001 + contact-centre 7003) write the vast majority of flips on all three switches. SFMC/7020 is small and 1012-only (≈16/mo, consistent with §0's Exact Target trickle). No batch writer: 7006 is trivial everywhere and 99999 is absent entirely → no hidden sync process exists on the writer side.

**E1/E2 — purpose-fill probes (1-in-10 client slice, trailing 3mo, ≈16.08M sampled send-rows):** `CONTACT_PURPS_TYP` is a single distinct value, NULL — 100% empty. `CNTCT_EVNT_INITIATOR` is constant `'1'`. **DECISION:** a marketing-vs-service split is IMPOSSIBLE from the vendor MASTER as currently populated — pack 12's E3 route is permanently closed; the switch-enforcement question (§0 MASTER SWITCH MAP, 1014 dictionary-vs-lore) requires a matched-control design, or stays out of scope.

**B-reverse — CPC flip → nearest prior vendor unsub (12-mo lookback, no fixed window — Andre's design):**

| PREF_ID | cpc_flips | no_prior_unsub_found | bridged | bridged % | avg gap (bridged) |
|---|---|---|---|---|---|
| 1002 | 2,168 | 2,158 | 10 | 0.5% | ~52–101d |
| 1012 | 1,541 | 1,511 | 30 | 1.9% | ~52–101d |
| 1014 | 8,642 | 8,542 | 100 | 1.2% | ~52–101d |

Bridged gaps are SMEARED across every band (avg 52–101 days) — no cluster at a batch-cadence interval. **Cross-check:** B-reverse's `cpc_flips` totals match Z2's monthly sums EXACTLY for 1002 and 1012 — two independently-built blocks agree. The 1014 `cpc_flips` figure photo-misread as 3,642; corrected to 8,642 via the Z2 monthly sum plus the bridged arithmetic (8,642 − 8,542 = 100 ✓). **VERDICT: no hidden sync/middleman** — confirmed from both the writer side (Z3) and the timing side (B-reverse).

**O — overlap** (trailing-12mo first-unsubs × CPC latest-state flags; `is_unsub × out_1002 × out_1012 × out_1014_explicit × out_1014_effective` — `effective=0` forces `explicit=0`, pruning the theoretical 32 combos to the 24 actually produced):

- Unsub side (312,376 clients total): 223,851 all-flags-zero + 87,182 only-1014-effective (blank default, not an explicit action) → **≈99.6% of unsubscribers hold no explicit CPC opt-out.**
- Full 24-combo cross-tab (is_unsub / out_1002 / out_1012 / out_1014_explicit / out_1014_effective → clients; explicit=1 forces effective=1, so 24 valid combos = complete; source pic PXL_20260723_225232706.jpg):
  Unsub rows: (1,1,1,1,1)=123, (1,1,1,0,1)=1, (1,1,1,0,0)=14, (1,1,0,1,1)=210, (1,1,0,0,1)=13, (1,1,0,0,0)=9, (1,0,1,1,1)=5, (1,0,1,0,1)=5, (1,0,1,0,0)=423, (1,0,0,1,1)=540, (1,0,0,0,1)=87,182, (1,0,0,0,0)=223,851. Non-unsub rows: (0,1,1,1,1)=18,300, (0,1,1,0,1)=52, (0,1,1,0,0)=1,368, (0,1,0,1,1)=27,474, (0,1,0,0,1)=1,001, (0,1,0,0,0)=1,346, (0,0,1,1,1)=208, (0,0,1,0,1)=205, (0,0,1,0,0)=12,613, (0,0,0,1,1)=34,770, (0,0,0,0,1)=3,467,252, (0,0,0,0,0)=583,632.
- Non-unsub side: 3,467,252 only-1014-effective (blank default) + 583,632 all-zero (= explicit 1014 Yes) + small opt-out cells (e.g. 1002-explicit combos ≈18–27K).
- Full 24-row table IS reproduced above (added 2026-07-23 from photo transcript); treat as photo-read until in-env export confirms.

**HEADLINE:** the consent gate does not know about ≈312K/yr clients who told the vendor to stop — they remain "marketable" per CPC.

**Resolves §14-A's open interpretation:** whatever the MTEC-12644 doc's "100% match" batch does, it does NOT write vendor unsubs into `CPC_RB_PREF_LOG` — confirmed empirically by all four evidence lines above (Z2, Z3, B-reverse, O), not merely inferred as before. Direction of the "100% match" claim itself is still unconfirmed. Sharpened MarTech question: confirm nothing syncs today + get the MTEC-12644 go-live date.

### E. Run results — 22 gate evidence (2026-07-23)

`22_cpc_gate_evidence.sql` ran end-to-end 2026-07-23, closing both decisions the file was built for (22-A: who writes the bridged flips; 22-B: gate-leak test). Source pics: uploads 2026-07-23 16:28 (phone).

**22-B — gate-leak test.** State as-of 2026-04-01 (`window_start`), email received in the 3-month window Apr–Jun 2026. Main cut:

| gate_cohort | flagged_clients | received_email_in_window | rate |
|---|---|---|---|
| 1002 | 49,407 | 9,491 | 19.2% |
| 1012 | 33,051 | 9,975 | 30.2% |
| 1014 | 79,298 | 20,766 | 26.2% |
| NONE_baseline_1in10 | 394,840 | 228,664 | 57.9% |

Exclusivity cut (splits each flagged cohort by whether the client also carries another gate flag — partial bundle-confound control, per the switch-independence pattern in §0):

| PREF_ID | exclusivity | flagged_clients | received_email_in_window | rate |
|---|---|---|---|---|
| 1002 | multi_flag | 47,077 | 8,746 | 18.6% |
| 1002 | only_this_flag | 2,330 | 745 | 32.0% |
| 1012 | multi_flag | 19,865 | 3,755 | 18.9% |
| 1012 | only_this_flag | 13,186 | 6,220 | **47.2%** |
| 1014 | multi_flag | 45,856 | 8,229 | 17.9% |
| 1014 | only_this_flag | 33,442 | 12,537 | 37.5% |

READINGS:
1. Main-cut rates reproduce the recovered pack-12 E1 rates (§14-B) almost exactly — 30.1/26.2/19.2 there vs 30.2/26.2/19.2 here — two independently-windowed runs (E1's unconfirmed window vs 22-B's confirmed 2026-04-01 anchor) agree to within noise.
2. The exclusivity cut removes the bundle confound and the leak WORSENS, not improves: 1012-only (the purest single-switch email opt-out population) receives email at 47.2% — above several multi-flag cells and not far below the 57.9% baseline. The email-specific switch barely moves campaign email delivery once bundling is stripped out.
3. Construction note: the vendor feed is built from NBA campaign TREATMENT_IDs — these are marketing sends by construction, so "it's all service mail, not marketing" is not an available explanation for the leak. Residual caveat: a minority of MNEs may carry non-promotional content (per the MNE dictionary, §4); one follow-up worth doing is which specific MNEs reach flagged clients.
4. Scale: ~9.5K entity-DNS (1002) clients received a campaign email in the quarter despite being flagged out at the master switch.

**VERDICT (22-B):** gate leak PROVEN — descriptive/compliance-flavored finding (state-vs-outcome cross-tab, not a controlled experiment; cause unattributed). Either targeting/suppression logic does not consult CPC at send time, or CPC→SFMC suppression-list sync is partial/broken — this run does not distinguish the two. Direction symmetry with §14-D noted: consent fails to flow BOTH ways — vendor unsubs don't reach CPC (§14-D), and CPC opt-outs don't reliably suppress vendor sends (here).

**22-A — bridged-flip writer attribution.** Joins B-reverse's bridged-flip client list (§14-D) back to writer (`APP_SYS_CD`), banded by gap-to-prior-unsub. Full 7 rows:

| PREF_ID | APP_SYS_CD | gap 0-1d | gap 2-7d | gap 8-30d | gap 31+d | bridged_flips |
|---|---|---|---|---|---|---|
| 1002 | 7003 (contact centre) | 1 | 0 | 2 | 6 | 9 |
| 1002 | 7001 (branch) | 0 | 0 | 0 | 1 | 1 |
| 1012 | 7020 (SFMC/ESP) | 8 | 4 | 1 | 2 | **15** |
| 1012 | 7003 (contact centre) | 1 | 1 | 2 | 6 | 10 |
| 1012 | 7001 (branch) | 0 | 1 | 0 | 4 | 5 |
| 1014 | 7001 (branch) | 1 | 7 | 20 | 66 | 94 |
| 1014 | 7003 (contact centre) | 1 | 0 | 1 | 4 | 6 |

Row totals tie exactly to B-reverse's bridged counts (§14-D): 1002 = 9+1 = 10 ✓; 1012 = 15+10+5 = 30 ✓; 1014 = 94+6 = 100 ✓.

**VERDICT (22-A):** the only genuine automated crossing is SFMC/7020 on 1012 — ~15/yr, clustered same-day-to-week (12 of the 15 land in the 0-1d/2-7d bands). Every other bridge (all of 1002, all of 1014, and the non-7020 rows on 1012) is assisted-channel (branch/contact-centre) at long gaps, mostly 31+ days — consistent with a person acting on a customer request weeks after the client had already unsubscribed by email, not a pipe. Closes 22-A's decision: bridged flips are not evidence of a hidden sync; the one real automated crossing is tiny and email-specific.

**Engine note:** one statement in this run hit TDWM error 3149 ("F-uncnstrn PJ rowtest" filter violation) — `CROSS JOIN vt_params` without collected statistics on that volatile table means the optimizer can't prove it's a 1-row join, so it gets treated as an unconstrained product join. Fix: `COLLECT STATISTICS` on `vt_params` — being applied to 21a/21b/22 (none of the three currently collect stats on it in the checked-in SQL).

**Photo note:** the rotated printout photo of the O cross-tab (§14-D) was re-received during this pass; the 2026-07-23 screen read already on file stands as the source of record — no re-transcription performed.

**2026-07-24 rerun (Andre, clean session):** 22-A reproduced EXACTLY (all 7 rows digit-identical); 22-B rates identical (1012-only 47.1%, counts +0.1% drift = warehouse load timing). Claims CONFIRMED, provisional status lifted. Museum evidence file: `museum/cpc_evidence.sql`.

### F. Museum final run — cpc_evidence.sql (2026-07-24, Option A: consent standing full-history)

`museum/cpc_evidence.sql` ran standalone in-env 2026-07-24, four Evidence blocks (E1–E4). Source: Andre's phone uploads 2026-07-24 09:40–09:41. This run set the consent-standing parameter to **Option A — full history** (no floor date), superseding an earlier same-day floored pass whose narrower numbers appear below only as a comparison check.

**E1 — monthly volumes, Jul2025–Jun2026 (12 months, cpc_optout vs email_unsub):**

| Month | cpc_optout | email_unsub |
|---|---|---|
| 2025-07 | 838 | 26,673 |
| 2025-08 | 823 | 30,920 |
| 2025-09 | 716 | 28,839 |
| 2025-10 | 780 (or 730) | 29,320 |
| 2025-11 | 636 | 28,164 |
| 2025-12 | 700 | 27,271 |
| 2026-01 | 723 | 26,119 |
| 2026-02 | 656 | 27,970 |
| 2026-03 | 963 | 28,318 |
| 2026-04 | 894 | 24,951 |
| 2026-05 | 758 | 18,746 |
| 2026-06 | 565 (or 561) | 22,471 |

email_unsub column: reader dropped a digit this pass, but the series matches the prior verified read (§14-D Z2 cross-check lineage) — treat as confirmed, not re-derived. Gap ≈35× (cpc_optout vs email_unsub), consistent with Z2's ≈35× finding.

**E2 — blind gate (full-history standing):** `unsub_clients_total` 319,733; `with_explicit_cpc_optout` 1,387; `without` 318,846 (1,387 + 318,846 = 320,233 — internal sum off by ~500 vs the 319,733 total, one cell has ±500 OCR ambiguity; the headline rate is unaffected). **99.6% of unsubscribers have no explicit CPC opt-out.** Comparison: the earlier same-day floored pass had shown 725/99.8% — Option A restores the true full-history standing (larger explicit-opt-out count, rate ticks down but conclusion unchanged). 1,387 is consistent with the archaeology O cross-tab's explicit-opt-out cell sum (§14-D, ≈1,343 across the unsub-side explicit combos) — same order of magnitude, independently reproduced.

**E3 — no-bridge by writer:** qualitatively confirmed AGAIN — `had_prior=N` dominates every switch, `had_prior=Y` rows are tiny, writers are 7001/7003/7033/7006 with 7020 small. Photo is cell-grade unreliable on this dense output (dropped digits, one misgrouped row, unresolved 7033-vs-7053 split on 1002) — no numbers transcribed from it. **DECK RULE:** cite archaeology's verified writer numbers (§14-D Z3 + §14-E 22-A — digit-identical across two independent runs) for any per-writer figure; museum E3 itself is pending one clean in-env export before it can serve as the record.

**E4 — leaking gate (full-history cohorts):**

| PREF_ID | exclusivity | flagged_clients | received_email_in_window | rate |
|---|---|---|---|---|
| 1002 | multi_flag | 47,711 (or 47,111) | 8,750 | 18.3% (or 18.6%) |
| 1002 | only_this_flag | 2,333 | 746 | 32.0% |
| 1012 | multi_flag | 19,889 | 3,779 | 19.0% |
| 1012 | only_this_flag | 13,714 (or 13,214) | 6,224 | ~45–47% |
| 1014 | multi_flag | 45,988 | 8,231 | 17.9% |
| 1014 | only_this_flag | 33,342 | 12,540 | 37.6% |

Per-switch multi/only splits and rates match §14-E's 22-B exclusivity cut almost exactly (that run's 1002/1012/1014 rows) — same population, independently reproduced. **ALL_SWITCHES (union across 1002/1012/1014, deduplicated):** 96,317 optout_clients / 28,342 got_email_apr_jun = **29.4% leak**. This is the one genuinely new number this run adds: Option A restores the true full-history union (~96K clients) against a floored pass that had shown only ~42.9K — the per-switch rates were already stable and predicted this; the aggregate leak rate (29.4%) is now confirmed at full scale.

**Close:** museum numbers are now FINAL and deck-grade for E1, E2, and E4. E3 has no deck-grade museum number — any slide use of per-writer figures draws on archaeology's verified numbers (§14-D Z3, §14-E 22-A).

### G. Museum complete run -- all evidence + new findings (2026-07-24, second run)

`museum/cpc_evidence.sql` ran again in-env 2026-07-24, this pass producing all six Evidence blocks (E1-E6) plus the summary block for the first time. Source: Andre's phone uploads 2026-07-24 11:39-11:40 (six photos).

**E1 -- re-confirmed.** Same series as Section 14-F's E1 (monthly cpc_optout vs email_unsub). Reader dropped a digit again this pass; series validated against the prior verified reads (Section 14-F, Section 14-D Z2) -- treat as confirmed, not re-derived.

**E2 -- blind gate, now with a before/after split (closes red-team objection #5, any-time logic, Section 15-B):**

| Metric | Value |
|---|---|
| unsub_clients_total | 319,733 |
| with_explicit_cpc_optout | 1,387 |
| without_explicit_cpc_optout | 318,346 |
| optout_recorded_before_unsub | 1,252 |
| optout_recorded_after_unsub | 135 |

Sums check exactly: 1,387 + 318,346 = 319,733; 1,252 + 135 = 1,387. This corrects Section 14-F's flagged ~500 OCR ambiguity on the "without" cell -- 318,846 there was a digit-flip; 318,346 is the number.

READING: propagation ceiling = 135 clients/yr (0.04% of the unsub population) -- the largest number of vendor unsubs that could plausibly trace to a CPC opt-out recorded after the fact. The 1,252 before-cases are clients who were already opted out at CPC before they also unsubscribed at the vendor -- from the client's-eye view, they said no once, kept getting mail, then said no again at the vendor (the gate-leak population, seen from the consent side rather than the delivery side). Red-team objection #5 (any-time logic) is now answered with data, not just design intent.

**E5 (NEW) -- vendor-side suppression is not channel-wide (closes red-team objection #4, Section 15-B):** 248,130 clients unsubscribed Jul 2025-Mar 2026; 25,721 of them (10.4%) received a campaign email Apr-Jun 2026. Cohort arithmetic checks out (~9 months x ~26.6K/mo unsub rate, consistent with Section 14-F E1's monthly series). Photo was dim on this block -- values assigned by magnitude, then arithmetic-validated against the monthly series, not read digit-by-digit. Candidate mechanisms (open, not claimed as the answer): program-level suppression scope (unsub kills one program, not all), multi-address clients, re-consent overriding a prior opt-out (ties to red-team objection #6, still open).

**E6 (NEW) -- transactional escape closed, with campaign names (closes red-team objection #3, Section 15-B):** top campaigns reaching 1002 (entity do-not-solicit) clients Apr-Jun 2026, by distinct clients and send-rows:

| MNE | Clients | Send rows |
|---|---|---|
| OCF | 5,081 | 6,327 |
| VRE | 2,831 | 1,687(?) |
| FWC | 2,355 | 2,434 |
| API | 1,739 | 1,943 |
| VRG | 1,573 | 1,609 |
| VME | 1,395 | 1,974 |
| PPQ | 1,378 | 1,785 |
| RFT | 1,285 | 1,938 |
| KFI | 1,174 | 2,122 |
| TAO | 966 | 3,169 |
| AUS | 777 | 777 |
| GIS | 706 | 706 |
| PCD | 697 | 697 |
| PCL | 691 | 1,505 |
| ESV | 665 | 1,480 |
| PAL | 562 | 852 |
| PCQ | 525 | 964 |
| AUH | 518 | 1,152 |

Plus one unreadable row and one row photo-read with MNE='1' -- an OCR artifact, not a real MNE code; verify both on export. Cards campaigns present: PCD, PCL, PCQ, AUH. VRE and VRG recur here -- the same unidentified campaign family seen in the multi-unsub pairs elsewhere in this doc; a dictionary lookup on VRE/VRG is still pending.

**E3 -- legible this pass.** Writer story confirmed at the Y/N (had_prior) grain, matching Section 14-F's qualitative read. Bridged counts approx 12/26/~118 per switch (1002/1012/1014 order) -- consistent with 22-A's digit-confirmed 10/30/100 (Section 14-E) within OCR noise. The one automated (7020/SFMC) crossing on 1012 read as 16 rows this pass vs 22-A's confirmed 15 -- same order of magnitude, small-number noise. Section 14-F's deck rule stands: cite archaeology's Section 14-D Z3 / Section 14-E 22-A numbers as the record; museum E3 is now qualitatively confirmed but still not the digit-grade source.

**E4 -- re-shot too pixelated to read.** No numbers taken from this pass; the prior triple-confirmed numbers (Section 14-E, Section 14-F) stand as the record.

**Three-layer synthesis (deck spine), assembled from this run:**
1. Unsub -> 10.4% of unsubscribers still get a campaign email the following quarter (E5).
2. 1002 standing -> 19.2% of entity-do-not-solicit clients get mailed, now with named campaigns responsible (E1/Section 14-F, E6).
3. The two systems (vendor suppression and CPC) share only ~0.4% overlap and a 0.04% propagation ceiling (E2) -- they do not talk to each other.

One-liner: "No single system in the chain treats no as no, and the systems do not talk to each other."

**VERDICT:** museum evidence pack is COMPLETE -- all 6 Evidence blocks plus the summary now populated in one run. Deck numbers are final pending only the E6 unreadable-row / MNE='1' cleanup on next export.

## 15. Red-team review and response (2026-07-24)

### A. Method

An Opus agent role-played a senior campaign orchestrator (deployment/suppression owner) and was given ONLY the 2-slide deck draft cold — no analysis context, no access to §0–§14 — and asked to scrutinize it as an adversarial reviewer would before wider circulation.

### B. Objections raised (numbered, compressed) and disposition

| # | Objection | Disposition |
|---|---|---|
| 1 | 35× is a friction artifact — comparing a one-click vendor unsub against a deliberate multi-step preference-centre trip, not like-for-like consent strength | Fair. Slide 1 reframed to scope contrast (friction, not enforcement) instead of a bare ratio. |
| 2 | Switch fusion: 1014 = sharing consent per dictionary, 1012 possibly servicing-related; only 1002 (entity do-not-solicit) is unambiguous | ACCEPTED. Deck re-led with 1002 alone (19.2%, ~9.5K clients/quarter); 1012/1014 demoted to "pending switch-mapping confirmation." |
| 3 | Transactional-mail contamination — "received email" side may include service mail, not marketing | Countered: feed built from NBA campaign TREATMENT_IDs, marketing by construction (§14-E pt.3). Per-MNE verification promoted to REQUIRED → new **Evidence 6** (MNE mix reaching 1002-flagged clients). |
| 4 | Which suppression list does send-time decisioning actually read — CPC directly, or an SFMC-side list synced from CPC? | Unanswered — added to outreach questions (§C). New **Evidence 5** (unsub-before-Apr × received-after; expected LOW if vendor suppression works). |
| 5 | Panel A uses any-time logic (state ever true), not before/after | Already addressed — E2's before/after split covers this; rerun pending under the new full-history parameter (§14-F). |
| 6 | Re-consent / program-level express-consent could legitimately override a general opt-out — the "leak" may be correct behavior, not a bug | Open. Domain answer needed (Andre or outreach), not a data answer. |
| 7 | Opt-out recency distribution — "flagged" is treated as one bucket regardless of how long ago the opt-out happened | Open — cheap future cut, not blocking this deck. |
| 8 | "Says who CPC gates campaign email?" — deck was implicitly presupposing the design intent it was trying to test | Wording rule adopted: never presuppose design intent. Reframed to "consent does not translate into delivery." |
| 9 | Political: reads as a compliance finding; risky to circulate widely without a compliance pre-brief | Adopted — compliance pre-brief before wide circulation. |

### C. Outreach questions (consolidated, now 5)

a. Where do SFMC unsub batch files land — CPC-CC or CPC-PC?
b. Name of the existing daily batch job (the one MTEC-12644, §14-A, says is preserved unchanged)?
c. What does send-time suppression actually read — SFMC subscriber status, or CPC switches?
d. Which switch is the lawful campaign-email suppression key, and is there a documented SLA for how fast it's honored?
e. Does program-level express consent override a general opt-out?

### D. Resulting artifact changes

- Deck re-cut: slide 1 = scope contrast, not a bare 35× ratio (obj. 1); slide 2 re-led with 1002, 1012/1014 demoted to pending (obj. 2).
- `museum/cpc_evidence.sql` gains two new blocks: **Evidence 5** (which suppression list send-time reads — obj. 4) and **Evidence 6** (MNE mix reaching 1002-flagged clients — obj. 3).
- E2 5-row rerun pending (before/after split under the full-history parameter — obj. 5, ties to §14-F).
- Deck carries placeholders pending E5/E6 — not final until those land.
- Objections 6/7 logged open, not yet built against; objections 8/9 are wording/process rules, already in effect.

## 16. APP_SYS_CD official mapping — EDW Data Dictionary (2026-07-25)

Source: EDW Data Dictionary page for CPC_RB_PREF_LOG.APP_SYS_CD (Andre screenshots, pics/PXL_20260725_app_sys_cd_dictionary_1of2.jpg + 2of2.jpg). Column = "which application/system was used to update the statement option / populate the value". INTEGER(10), Confidential. Also appears in ARNGMNT_OPTION_ACTVY / ARNGMNT_OPTION tables.

**CPC-Consents valid values (the section that governs our consent rows):**
| code | system | note |
|---|---|---|
| 7001 | Sales Platform | **branch or Service Delivery staff** — assisted-channel inference now CONFIRMED |
| 7002 | Client Source | Direct Investing staff |
| 7003 | Royal Direct | **Contact Centre staff** — call-centre inference CONFIRMED |
| 7004 | OnLine Banking | self-serve digital |
| 7006 | RBC Banking | **INTERNAL processing** — STaR UI (stand-alone URL), batch maintenance / purge. NOT a client action |
| 7009 | Sapient/Bridgetrack | |
| 7015 | RCT / LINX desktop | |
| 7017 | D&H/AMIA/CMG | Telemarketer, NOV-2011 |
| 7020 | Exact Target | = SFMC (already known) |
| 7021 | TSYS | Total Systems, JUN-2012 |
| 7024 / 7025 / 7026 | VOX / ZEDD / APAC | Telemarketing vendors, JAN-2015 |
| 99999 | Batch update process | SRF, consolidation |

**CPC-PC valid values (different meanings per source system!):** 7020 = Exact Target, 7025 = CASL Tool, 7030 = ADHOC Data Source, default NULL. Same code ≠ same meaning across CPC-Consents vs CPC-PC — never join meanings across source systems.

**Gap:** codes **7033** (2,039 flips on 1014) and **7053** (217 on 1002, 45 on 1012) observed in our E3 output are NOT in this dictionary page (list ends 7030 → 7999). Either the page is stale (entries reference 2011-2015 era) or newer codes are undocumented. Residual ask if it matters for the deck; otherwise label "undocumented code" in the appendix.

**Impact:** outreach question (f) CLOSED. Deck writer-appendix can use official names. E3 interpretation refined: 7001/7003 = staff-assisted CONFIRMED; **7006 flips are internal batch/maintenance writes, not client decisions** — exclude or footnote 7006 when talking about "client-initiated" opt-outs (1002: 144, 1012: 29, 1014: 216 of the 12-mo No-flips).

### §16 addendum — CLNT_CONSENT_TYP blank rule, official dictionary text (Andre read-out 2026-07-25)

EDW dictionary description (near-verbatim from Andre's reading; pic pending): field "indicates the client's consent preference to be contacted for this preference or not". **Blank** = "client has never explicitly indicated yes or no, or we are unable to confirm the client has signed the appropriate agreement with usage consent." Rule: **"A blank value is treated as a YES for all preferences EXCEPT Share for Services across RBC and Share for Marketing across RBC — for these consents a blank value must be treated as a NO."**

Implications (2026-07-25):
- DEFAULT IS PERMISSIVE: never-answered = operational YES on 1002/1006/1012 (and all non-share switches). The 318,346 unsubscribers with no explicit CPC opt-out are read by the consent system as CONTACTABLE — sharpens the zoom-in headline from "gate is blind" to "gate defaults the unheard no to yes".
- Explicit-5002 cohorts are EXACT (not merely conservative) on 1002/1006/1012 — blank is affirmatively Yes there, so explicit No = the complete legal opt-out population. 1014 keeps the floor/iceberg framing (81,699 explicit vs ~3.64M rule-implied incl. 3,557,099 standing blanks).
- The 7999 blank stream (started 2019, D1) writes a value with OPPOSITE legal meaning per switch: Yes-equivalent on 1002/1006/1012, No-equivalent on 1014/1015. Outreach-grade question.
- E11 blank-bridge test reads per switch: only 1014/1015 post-unsub blank-writes could constitute honoring an unsub.

### §16 addendum 2 — PREF_ID taxonomy, official dictionary text (Andre read-out 2026-07-25; pics pending)

Four groups of consent/preferences: (1) Consent for Entity Marketing & Information Usage, (2) Consent for Communication Channels, (3) Product preferences, (4) Services preferences.

Group 1 codes named: entity do-not-solicit per entity — 1001 RBC Direct Investing, **1002 RBC Royal Bank ("equivalent to Do Not Solicit for each entity")**, 1016 Bank and Credit Bureau (Andre read-out, verify). Information usage (sharing): **1014 Share for Marketing across RBC**, 1015 Share for Services across RBC, 1036 Share for Online Personalization across RBC, 1057 DI Share for Marketing. Regulatory line: "all processes which use client data must honor these indicators"; example given: bank clients No on 1014 must be excluded from a DIRECT INVESTING offer list.

Group 2: per-entity contact-channel consents (two telephone consents = internal units). 1012 presumed here (verify on pic).

IMPLICATIONS:
- **1014 = CROSS-ENTITY sharing gate, not a same-entity email gate** → red-team objection #2 VINDICATED by dictionary text. 1014-flagged clients receiving RBC bank campaign email ≠ documented violation of 1014. Deck already re-led on 1002; purge any residual 1014-as-breach language. Open outreach nuance (don't claim): if NBA targeting consumes cross-entity data, 1014 may bind on data-USE side.
- 1002 deck-lead framing now dictionary-backed ("do not solicit", regulatory-must-honor group).
- OPEN: which group holds 1006 (our "credit-card content" gate)? If group 3/4 (product preference, not consent), E7 breadth framing adjusts.
- Suppression-key hierarchy for deck appendix: 1002 entity DNS + 1012 channel consent stack; sharing switches are data-use gates.

---

## 17. CPC × Unsub archaeology — consolidated findings & interpretation guide (2026-07-25)

Reservoir-based re-derivation (Spark off HDFS, governor-free) that supersedes the launch-era CPC numbers in §14 where they conflict. Pipeline: `museum/cpc_reservoir_extract.py` (Teradata→parquet) + `archaeology/23_cpc_landscape.py` (Spark T1–T4). Everything below traces to a run Andre executed 2026-07-25; screenshots transcribed same day.

### A. The story in one line
CPC (the consent centre) and the vendor send system are **two unsynced worlds with no closed enforcement loop between them.** A client's consent state — whether an unsub or a do-not-solicit flag — does not reliably reach what the send system does. This is bigger than the original "does an unsub write to CPC" question (answer: no, and that's not the interesting part).

### B. The reservoir (what the findings run on)
- `unsub_base` — 319,733 distinct unsub clients (EVENT disposition_cd=4, unsub Jul25–Jun26).
- `cpc_pref` — 13,831,133 rows, **only 4 switches landed: 1002/1006/1012/1014**, full history (per-switch pull; 1015 NOT landed).
- `q2_recipients` — 10,329,138 distinct Apr–Jun recipients (disposition_cd=1 = a dispatched send, not a decisioning row).
- `cpc_landing_allsw` — 246,674 rows: all-switch No/blank CPC events for the unsub cohort, CHG_TMSTMP ≥ 2025-07-01 (the only unfiltered-switch pull; feeds T1).
- `no1002_email_card` — distinct consumer_id_hashed per 1002=No client (feeds T4).
- **Grain:** CPC = client × pref × time (CLNT_NO; **no email address exists in CPC**). Vendor = email-address (consumer_id_hashed) → CLNT_NO.

### C. Findings by thread
- **T1 — landing / blind-spot.** Only **889 clients (0.278%)** of 319,733 unsubscribers get ANY CPC No/blank event within 90d of unsubbing (refreshes old D1's 0.33%/90d) → no automated bridge. Of the 873 that land on switches we don't watch, **550 land on 1016 (a sharing switch) written by 7001 (branch staff)** + ~127 via 7033/7003 → a small HUMAN trickle to sharing prefs, not an email pipe. [T1b cluster-shape still unshot — confirmatory only.]
- **T2 — multi-switch journeys / umbrella.** 1014=No = 3,452,289 clients; contains 94.5% of 1002=No but is 70× bigger; only 1.4% of 1014=No are also 1002=No. Crosstab: **3,384,693 (98% of all 1014=No) are 1002=Yes = fully marketable** → 1014=No is the onboarding-default majority. T2c: 93% of clients No on both set both the same day (bundled write, not sequential).
- **T3 — semantics (the decider).** 1014=No got email **61.4%** (HIGHER than 1014=Yes 32.3%) → **1014 does NOT gate email.** 1002=No got email **19.2%** (vs 1002=Yes 58.9%) → **1002 IS the email gate** (the No≪Yes signature). Cross-check: 1002=No 19.2% and n=49,446 reproduce museum E4 exactly. → Email/opt-out logic anchors on **1002**; blank=No on 1014/1015 is a **sharing** standing fact only, never email suppression.
- **T4 — granularity guard.** **100% of 1002=No clients have exactly 1 email address** (15,016/15,016 single) → the 19% is a genuine same-address leak, NOT a new-email artifact. **Denominator finding:** only **15,016 of the 49,446** do-not-solicit clients appear in the vendor send system at all; **34,430 (70%) have no vendor record** (never targeted). So 19% (over all 49,446) = a blend of 0% on the 34,430 not-in-system + **63% on the 15,016 in-system** (9,498). **Mechanism: do-not-solicit is an UPSTREAM filter (70% kept out of sends) with NO downstream recheck — 63% of the do-not-solicit clients who reach the vendor get emailed.**

### D. Mechanics — how to interpret the data (rules)
- **Consent codes:** 5001=Yes, 5002=No, 5003=blank, 5004=other. Current state = latest row per (CLNT_NO, PREF_ID) by CHG_TMSTMP.
- **Blank rule:** blank=No ONLY on 1014/1015 (dictionary). On those two, "No/opted-out" = **5002 + blank as ONE population, never split**. Elsewhere (1002/1012) blank=Yes → "No" = 5002 only.
- **Switch meanings (dictionary §16):** 1002 = RBC Royal Bank do-not-solicit = the marketing/email gate (regulatory-must-honor). 1012 = channel (email) consent. **1014 = Share-for-Marketing across RBC = CROSS-ENTITY sharing, NOT a same-entity email gate.** 1015 = Share-for-Services. 1016 = Bank & Credit Bureau (sharing-side). 1036 = Share-for-Online-Personalization. **1006 = group UNCONFIRMED (product-pref vs consent).**
- **Writers (APP_SYS_CD):** 7999 = onboarding/default batch (writes the blank millions — NOT client decisions). 7001 = Sales Platform/branch. 7003 = Royal Direct/call centre. 7020 = SFMC. 7033/7053 = undocumented, at volume.
- **Grain rule:** CPC is client-level; vendor is email-address-level but consumer_id_hashed is **~1:1 with CLNT_NO** (T4) → client-grain joins are valid.
- **Number-reporting rule:** report the do-not-solicit leak as a **COUNT with explicit denominator** (~9,500 of 15,016 in-system), never a bare 19% (its denominator folds in 34,430 clients who were never in the send system).
- **DEFAULT stream:** TREATMENT_ID='DEFAULT' = mail outside campaign taxonomy (service + broken-template + UNTAGGED MARKETING e.g. Mydoh/Edge) → invisible to MNE-based suppression. Governance finding.

### E. Settled vs open
- **Settled:** no automated unsub→CPC bridge on ANY switch; 1014 ≠ email gate; 1002 = email gate; no email multiplicity (1:1); DEFAULT stream exists and bypasses MNE suppression.
- **Open:** T1b cluster time-shape of the 1016 trickle; 1006's group; CLNT_NO alignment CPC↔vendor (strong evidence OK — leaker counts matched T3, worth one confirm query); effective-dating of standing; red-team v2 process items (Legal consult, delivery-vs-decisioning definition).

### F. Red-team v2 status after the archaeology (round-2 fable file; none previously dispositioned)
- **#4 email-vs-client grain — RESOLVED** by T4 (consumer_id_hashed 1:1 with CLNT_NO; no multiplicity).
- **#7 1002 email-applicability / unconfirmed 1012 — RESOLVED** by T3 (1002 empirically IS the email gate No≪Yes; 1014 isn't).
- **#2 1014-as-breach / cross-program mail — RESOLVED** by dictionary §16 + T3 (1014 = sharing; purge 1014-breach language).
- **#1 "received email" undefined — IMPROVED:** got_email now = disposition_cd=1 (dispatched send), not a decisioning row. **#3 re-consent / #11 CASL** — handled in E8 exclusions. **#10 pipe false-precision / #12 behavior-vs-failure** — dissolved by the "two worlds / no enforcement loop" framing (no "failure" claim). **#15 standing/selection** — T1 now scans ALL switches, standing validated, dictionary-backed.
- **Deck-fixable:** #5 dedupe (use distinct-client counts), #6 send-volume normalization, #9 0.4%/0.04% diagram fix, #8 SUBSTR/named-campaign (DEFAULT-stream finding).
- **NOT deck-fixable (Andre action):** #13 Legal/Compliance consult, #14 definitions meeting with system owners → defuse by framing the deck as an internal analytic read + questions for owners, not a compliance allegation.

---

# §20. VERIFIED 2026-07-31 — vendor table facts, proven not believed

Everything below was measured against the live tables on 2026-07-31. Queries in
`spotlight/preflight.sql`, `preflight2.sql`, `preflight3.sql`, `preflight4.sql`.
Results in `spotlight/RUN_2026-07-31_preflight.sql` and `RUN_2026-07-31_scope_test.sql`.
**These supersede the earlier "believed / not yet verified" entries in §4 and §5.**

## 20.1 VENDOR_FEEDBACK_MASTER — the real 29 columns (HELP TABLE, live)

CONSUMER_ID_HASHED, SRVC_PROVDR_NM, LEGAL_ENTITY_CD, SOURCE_EVNT_ID, EMAIL_ADDR,
EMAIL_SUBJ_LINE, EMAIL_LANG_CD, CNTCT_EVNT_INITIATOR, CNTCT_MTHD_TYP, CLIENT_TYPE,
EMAIL_URL, TREATMENT_ID, CATEGORY_CD, SUB_CATEGORY_CD, PRODUCT_CODE, TREATMENT_EXP_DT,
APP_PRODUCT_TYP_CODE, CARD_ISSUE_NO, ACCESS_CARD_NO, CARD_TYPE_CD, CHANNEL_TYPE_CD,
PRODUCT_SUB_TYPE, SYS_APP_CD, SYSTEM_OF_RECORD, CONTACT_PURPS_TYP, PRIORITY_SCORE,
CLNT_NO, CARD_NO, LOAD_TM

Three that were never being used and all three matter:
- **CARD_NO / CARD_ISSUE_NO / ACCESS_CARD_NO** — explain the duplication (20.2).
- **SOURCE_EVNT_ID** — a per-send identifier. Cheaper than DISTINCT if a true send key is needed.
- **TREATMENT_EXP_DT** — treatment expiry date. It exists. Do not assume a response window.

## 20.2 MASTER grain — NOT one row per (consumer_id_hashed, TREATMENT_ID)

Measured: **1.123 rows per key**, max **95,014**, over 385,665,736 keys / 432,925,102 rows.
93.4% of keys are clean; the excess is 47,259,366 rows (**10.9% inflation**).

**97.5%** of keys map to exactly one CLNT_NO, so it is duplication, not ambiguity. The
duplication is almost certainly **card-level** — one email to a client holding three cards
writes three rows.

**Rule:** join MASTER as
`SELECT DISTINCT consumer_id_hashed, TREATMENT_ID, CLNT_NO ... WHERE CLNT_NO IS NOT NULL`.
Never join it raw. Every COUNT and SUM inflates ~11% if you do.

**16,519,813 rows (3.8%) carry a NULL CLNT_NO.** Zero negative. Exclude nulls explicitly —
`MOD(NULL, n)` matches no bite, so they vanish from bitten pulls while unbitten ones still
count them, and the outputs stop reconciling.

## 20.3 load_tm is a LOAD timestamp, not a send date

Filtering `m.load_tm >= window_start` with no margin **deletes in-window unsubs** whose MASTER
row loaded earlier. Measured: **26,782 distinct clients** dropped (8.7% of the annual
unsubscriber population) on a 12-month window.

**Rule:** floor MASTER at least 3 months before the event window. Pack 19, pack 20 and
`museum/20_lookback_cards.sql` all do this; anything that does not is undercounting.

## 20.4 TREATMENT_ID comes in two shapes — filter to real tactic ids

- **Dated (real):** 10 chars, `YYYY` + Julian day + 3-char program. `2024313BBP` → `SUBSTR(x,8,3)`
  = `BBP`, sent on day 313 of 2024. One day, one program, one send per client. **Andre's rule
  holds for these.**
- **Not dated (junk):** `DEFAULT`, `CABVRSN1`, vendor-internal codes. `SUBSTR(x,8,3)` yields a
  meaningless MNE, and because they are not date-bound a whole year of email collapses onto one
  key. This alone produced the 3,029,598 pairs that appeared to send "30+ days apart" — no
  campaign sent twice.

**Rule (Andre, 2026-07-31):** exclude non-dated ids from all unsub analysis.
```sql
AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
AND SUBSTR(TRIM(TREATMENT_ID), 1, 4) BETWEEN '2020' AND '2030'
AND SUBSTR(TRIM(TREATMENT_ID), 5, 3) BETWEEN '001' AND '366'
```
One filter fixes a correctness problem, a spool problem and an attribution problem at once.

**Corrects earlier canon:** "TACTIC_ID unique per deployment" is true only for dated ids.

## 20.5 The unsubscribe is PER-LIST, not a global opt-out

Of 23,801 unsubscribers whose unsub touches 2+ treatments, **~97% carry a distinct timestamp per
treatment**. Global fan-out is 169 clients. **69%** of unsubscribers touch exactly one treatment.

Per-campaign attribution is real. Numbers mean what they appear to mean; no "last straw, not
cause" caveat is needed.

Pack 20's "86% same-day" never showed a mechanism — same day with different timestamps is one
person working through an inbox.

**Re-proven at time-SPAN grain 2026-08-05** (`spotlight/diag_unsub_fanout_timestamps.sql`,
Jan–Apr 2026): distinct-timestamp counting alone could not exclude a batch writer staggering
rows by ms, so the test was redone on the span between first and last unsub row of each
multi-treatment client-day. Result: **91.8% of cross-campaign days spread over 1 min–hours**
(24,061 client-days at 1–60 min + 5,710 over 1 hour, of 32,422); batch-like writes (0s or
<1 min) are 8.2% cross-campaign, 4.3% single-campaign. Timestamps are event-time, not load
stamps (they spread within days). Separate deliberate clicks per list is the mechanism;
per-campaign attribution and the repeat-unsub ("suppression gap") reading stand. For
precision-critical multi-campaign counts, the ≤1-min slice (~8%) is the conservative shave.

**Mechanism confirmed via client journeys + the live unsub page 2026-08-05**
(`spotlight/diag_unsub_client_journeys.sql` + Andre's screenshots of the RBC unsubscribe
page): the unsub link lands on a preference page with per-list radio options (e.g.
"E-Newsletter – Rewards", "Accounts & Packages", "promotional emails from RBC Royal Bank").
Three consequences:
1. **MNE-level attribution is valid** — each list unsub is a deliberate per-list choice
   (observed: two campaigns unsubbed 19 seconds apart = one page visit, two choices; so even
   sub-minute cross-campaign pairs are partly human, and the 8.2% shave is an upper bound).
2. **Treatment/wave-level attribution is LOOSE** — the unsub is logged against the list's
   recent treatment(s), not necessarily an email the client opened: observed unsub tagged to
   a 2-month-old send with no open row, and one list unsub writing to 2 treatments of that
   list. Do not build wave-level unsub joins; send-to-unsub lag (§20.6) inherits this noise.
3. **Per-list is behavioral fact**: sampled clients kept receiving (and opening) other lists
   after unsubscribing — which is exactly why repeat unsubs exist.

**Independent production confirmation (2026-08-05, internal `EmailMetricsWriter` Scala job
found via Confluence):** the bank's own metrics pipeline maps disposition 1–6 exactly as our
canon (incl. 5=hard_bounce, 6=complaint) and joins EVENT↔MASTER on the same keys
(consumer_id_hashed + treatment_id, hash cast to string both sides). Two new facts:
(1) monthly-partitioned **HDFS parquet copies of both tables** exist in the dig metrics
platform (`ParquetUtils.load("VENDOR_FEEDBACK_MASTER", "yyyymm*")`; disposition_dt_tm stored
as epoch millis there) — an alternate, possibly faster substrate; (2) that job does **NOT
dedupe MASTER's card-grain duplication** before the join — internal email-activity metrics
built on it may run ~11% hot vs our deduped counts; check before reconciling our numbers
against any internal dashboard. It documents nothing about what triggers a disposition 4.

**Disposition reliability, verified per-signal 2026-08-05 (Andre self-record + trails):**
1 (sent) = reliable, same-day capture, load stamp corroborates. 2 (opened) = UNTRUSTWORTHY
BOTH DIRECTIONS — machine/proxy prefetch writes opens no human did (observed: open 20s after
a 21:55 send), and real human opens can be absorbed by provider image caches (observed:
Andre's real AUS open absent while his other opens exist). NEVER build behavior claims on
disposition 2. 3 (clicked) = unsub links typically excluded from click tracking; untested
for ordinary links. 4 (unsub) = completed, submitted opt-outs only — an abandoned unsub-page
visit (link clicked, options shown, no submit) writes NOTHING (Andre n=1, week-old, feed
proven same-day so latency excluded; consistent with span diagnostic).

**Campaign-side suppression basis (2026-08-05, CRV tech spec, Andre's screenshot):** email
eligibility for campaign selection = DTZTAU.CIDM_CHANNEL_ELIG_EM_DTL flags
(deliverable_em_addr_ind='Y', cpc1012_ind='N', email_kill_clnt_ind='N',
valid_em_addr_ind='Y', SPAM_COMPLAINT_EM_IND='N', CFS_CLNT_IND='N', %que_EMContactEligible)
AND NOT in DG6V01.CPC_CLNT_PREF_CHC with CLNT_CONSENT_TYP=5002 on PREF_ID IN (1002,1006)
(red-text later addition). READ: selection consults CPC (1012 flag + 1002/1006 direct) and
spam complaints (≈disposition 6) — NO visible ingestion of SFMC list unsubs (disposition 4).
If email_kill_clnt_ind doesn't carry them, the suppression gap is ARCHITECTURAL: list unsubs
never reach campaign eligibility, so every new campaign re-selects the client. UNKNOWN: who
feeds email_kill_clnt_ind; what PREF_ID 1006 is (blank=Yes per standards). Testable: flag
rates on CIDM_CHANNEL_ELIG_EM_DTL for unsubbed vs non-unsubbed clients. clicking the
email's unsubscribe link and viewing the options page WITHOUT submitting writes nothing
(no 3, no 4 — week-old event, same-day feed proven, latency excluded). So a 4 requires
completing the flow. STILL OPEN: which list gets written when the page offers 2+ options
(chosen vs source email's), and whether the broad "promotional emails from RBC" option
fans to multiple lists — channel-owner question or a deliberate submit test.

## 20.6 Send-to-unsub lag — set the window from this, do not guess

Per day, not per bucket: 23,118 (same day) → 8,233 → 4,052 → 3,243 → 2,620 → 2,298 → 476 (30+).
A **30-day window captures 71.5%** of logged unsub events. Defensible; report the coverage rather
than implying completeness.

## 20.7 Population anchors (12 months, Aug 2025 – Jul 2026, deduped)

- distinct unsub events: **753,608**
- distinct unsubscribers: **308,104**

Cross-validates the independently catalogued 319,733 (RESULTS_CATALOG.md, Jul25–Jun26) to within
3.6%. Andre's domain anchor: 0.2–0.6% unsub rate per deployment, under ~5,000 even at peak — use
it to sanity-check any new number before reporting it.

## 20.8 Engine gotchas hit this session

**Teradata**
- `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY c)` is a **GROUP BY aggregate**. The Oracle
  `... OVER (PARTITION BY ...)` form throws 3706.
- Ordered analytics (QUALIFY, ROW_NUMBER) are **not allowed inside subqueries** — 3706.
- `consumer_id_hashed` is **BYTE**; `||` against it throws 3662.
- `MOD(-7, 10) = -7`, matching no bite in `range(n)`. Use `MOD(ABS(x), n)`.
- `COUNT(DISTINCT ...)` over a 400M-row window throws 2646. It is never a "cheap probe".
- A bite predicate in the **outer WHERE** does not reduce spool — the join and its inputs spool
  in full first. Push it **inside** the subquery being joined.

**PySpark / pandas**
- pandas 2.0 removed `DataFrame.iteritems`; PySpark <3.4 calls it inside `createDataFrame`.
  Shim it at import.
- `createDataFrame` on a large pandas frame exceeds `spark.rpc.message.maxSize` (128 MB) around
  9M narrow rows. Write in chunks with an **explicit schema** — inferred schemas drift between
  chunks when a column is all-null in one of them.
- `df.write.csv("file:///...")` writes on the executors. Only `toPandas().to_csv(...)` reaches
  the driver pod.
- A cache guard that checks path existence, not columns or row count, will happily reuse stale
  parquet from a previous design. Version the path and write a row-count marker.

## 20.9 TREATMENT_ID shapes — measured (preflight4, 2026-07-31)

| | distinct ids | send rows | distinct clients |
|---|---|---|---|
| DATED (real tactic id) | 11,607 | 289,029,506 | 13,270,898 |
| NOT DATED (junk) | **29** | 21,817,167 | 6,950,976 |

Only **29** non-dated ids exist, and two of them are 84% of that volume:
`DEFAULT` (13,215,751 sends / 5,357,580 clients) and `CABVRSN1` (5,155,879 / 1,587,667).
Others: `COI`, `ESPTVER2`. Non-dated is **7.0% of send volume** — quote that figure when
reporting excluded email.

**Year floor is 2015, not 2020.** `2018319KVM` is a live 2018-vintage tactic id with 3,080,273
sends to 1,119,884 clients still running in 2025–26. A 2020 floor discards it, plus
`2019105THA`, `2018116KBC`, `2019350MTG` and others. Watch for ids like `21010AOT4B` and
`2019RMT350` — 10 chars but not year-first; the year-range test catches them.

## 20.10 A tactic id is one day by CONSTRUCTION, not by USE

`TACTIC_ID` = `YYYY` + Julian day + program, so it encodes a single day. It does **not** follow
that a client receives only one send under it.

Measured: **2,290,008** dated (client, tactic id) pairs have sends on more than one day —
574,165 within 1–7 days, 942,232 within 8–30, **773,611 more than 30 days apart**. Roughly 1% of
dated pairs.

Cause: evergreen and triggered campaigns reuse a single tactic id for years. `2018319KVM` was
still sending in 2026.

**Consequence:** collapse the event grain to `(client, treatment, DAY)`, never to
`(client, treatment)`. Collapsing to the treatment deletes real emails. This is not an artifact of
the junk ids — dated ids do it too.

## 20.11 TREATMENT_ID = TACTIC_ID — VERIFIED 2026-07-31 (was never tested before)

Measured over Aug 2025 – Jul 2026 (`spotlight/preflight5.sql` Q19):

| | ids |
|---|---|
| in both vendor and tactic tables | **10,893** |
| vendor only | 743 |
| tactic only | 12,446 |

**93.6% of vendor TREATMENT_IDs match a TACTIC_ID.** Same namespace. The
`EVENT.(consumer_id_hashed, TREATMENT_ID) → MASTER → TACTIC_EVNT_IP_AR_H60M.TACTIC_ID` chain is
sound and `SUBSTR(TREATMENT_ID, 8, 3)` is extracting a real MNE.

This closes §9 Open Questions item 1. `archaeology/03_tactic_join_channel_validation.sql` was
written to prove this and its output was never reviewed; the assumption held, but it was an
assumption under every number in this folder for months.

The 12,446 tactic-only ids are deployments with no email component — other channels.

## 20.12 The old-vintage TREATMENT_IDs are vendor residue, not live campaigns

`preflight5.sql` Q21 returned **zero rows**: `2018319KVM`, `CABVRSN1`, `DEFAULT`, `2019105THA`
and `21010AOT4B` have **no row in DTZV01.TACTIC_EVNT_IP_AR_H60M at all**, at any date.

The bank is not still mailing 2018 campaigns. Those ids exist only vendor-side with no deployment
behind them, which is why `2018319KVM` appeared to send 3,080,273 emails to 1,119,884 clients
across 2025–26.

**Rule: scope the campaign universe by joining to TACTIC_EVNT_IP_AR_H60M on deployments whose
`TREATMT_STRT_DT` falls in the window. Do NOT parse TREATMENT_ID strings for a year and a Julian
day — that heuristic discards live tactic ids and keeps residue, in both directions.**

## 20.13 Campaign universe, Aug 2025 – Jul 2026

`preflight5.sql` Q22: **23,339 deployments**, **431 distinct mnemonics**, first start 2025-08-01,
last start 2026-07-31. That is the whitelist any bank-wide unsub analysis should be scoped to.

## 20.14 Unmatched send volume — 14.3% under a start-date whitelist

`preflight5.sql` Q20, Aug 2025 – Jul 2026:

| | ids | send rows |
|---|---|---|
| matches a deployment started in window | 10,893 | 266,400,263 |
| NO matching deployment | 743 | 44,446,410 (**14.3%**) |

Only 29 of the 743 are junk ids (§20.9, ~21.8M sends). The other **714 are properly formed tactic
ids carrying ~22.6M sends** — too many to dismiss.

Probable cause: `TREATMT_STRT_DT` inside the window excludes a deployment that launched before it
and kept mailing through it. The correct membership test is **active during** the window:
```sql
TREATMT_STRT_DT <  WIN_CEIL
AND (TREATMT_END_DT >= WIN_FLOOR OR TREATMT_END_DT IS NULL)
```
Q23 measures the difference; Q24 names whatever is still unmatched under it. Do not scope the
analysis until one of the two tests explains the residual, and quote the final excluded-volume
percentage wherever campaign counts are reported.

## 20.15 SCOPE RULE (decided 2026-07-31) — shape test, send-date anchor, no tactic whitelist

**`DTZV01.TACTIC_EVNT_IP_AR_H60M` is NOT a bank-wide campaign registry.** `preflight5.sql` Q24:

| TREATMENT_ID | sends | in tactic table? |
|---|---|---|
| DEFAULT | 13,215,751 | no (junk) |
| CABVRSN1 | 5,155,879 | no (junk) |
| 2021342KFI | 4,451,621 | **no** |
| 2026084QCF | 3,967,358 | **no** — dated inside the window |
| 2026085QCF | 3,348,443 | **no** — dated inside the window |
| 2018319KVM | 3,080,273 | no |
| 2025270ERI | 2,952,301 | **no** — dated inside the window |

Whitelisting against it removes real programs. Andre's read: those are ODS-deployed campaigns
living in a different template. Active-during-window instead of started-during-window barely
helped — 14.3% → 13.4% unmatched.

**THE RULE:**

1. **Scope by SHAPE only.** 10 chars, first 7 numeric, last 3 the mnemonic:
   ```sql
   AND CHARACTER_LENGTH(TRIM(TREATMENT_ID)) = 10
   AND SUBSTR(TRIM(TREATMENT_ID), 1, 7) BETWEEN '0000000' AND '9999999'
   ```
   Excludes the 29 non-conforming ids (~6% of volume, 18.4M of it DEFAULT + CABVRSN1). **No year
   range** — it was wrong in both directions, discarding live ids and keeping residue.

2. **Anchor every date on `disposition_dt_tm`, never on the Julian day in the id.** The Julian day
   records when the ID was minted, not when the email went out — `2018319KVM` was still sending in
   2026, eight years apart. Only the send date is a fact about the client. This is what makes the
   year range unnecessary: vintage stops mattering once the id is read for its mnemonic alone.

3. **Group by MNE, not by full TACTIC_ID.** Cost, stated: two waves of one mnemonic in a month
   collapse into one row. The brief asks for unsubs by campaign, not by wave. Per-wave questions
   would need the tactic table back — and it is incomplete, so that is a separate problem.

## 21. Pack 54 v3.1 (2026-09-04): email-decisioned send funnel, Cards MNEs, 2025-01+

**Arms come from the channel slot, not TST_GRP_CD (Andre 2026-09-04, operational fact).** Holdout/
control cells carry NO channel label — the slot reads `XX`. Action cells carry the channel code
(`EM` for email). `TST_GRP_CD` values are plain numbered test groups (TG1, TG4, ...) with no
consistent Action/Control convention across MNEs (PCD 81 distinct codes, PCL 53, AUH 42, PCQ 40,
CRV 3) — **never use TST_GRP_CD to derive an arm.**

**XX = holdout, CONFIRMED empirically.** HOLDOUT_XX: 2,772,862 decisions (AUH 381,656; PCD
2,374,218; PCL 16,988; CRV/PCQ none) — **0 in MASTER, 0 sent**, zero across all 2.77M. EMAIL_ACTION:
34,629,979 decisions / 4,931,448 clients / 529 tactics, 31,217,277 in MASTER and 31,217,277 sent —
**in_master == sent again** (Pack 17's finding, reproduced at 30M-decision scale).

Per-MNE EMAIL_ACTION sent/decisioned:

| MNE | Sent | Decisions |
|---|---|---|
| AUH | 642,542 | 756,437 |
| CRV | 688,736 | 1,157,152 |
| PCD | 7,878,271 | 8,573,932 |
| PCL | 11,837,350 | 13,224,069 |
| PCQ | 10,170,378 | 10,918,389 |

**Zero-send months exist and must be excluded before reading non-send as client-level
suppression.** CRV 202502-202505 and PCL 202607 show zero sends across the whole month — an
operational/vendor-side gap, not a suppression signal. Rule: exclude mne x cohort_month pairs
with zero sends before attributing any client's non-send to unsub/consent suppression (applied
in Pack 57).

**Spool lesson: VOLATILE TABLEs live in the user's spool, not a separate scratch area.** A
110M-row Step A (all channel arms, full 2024+ floor) alone exhausted spool before Steps B/C
could run — `TREATMENT_ID IN (SELECT ... FROM <110M-row table>)` then forced a dedupe-and-
compare against every MASTER/EVENT row scanned on top of that. Fix, now standard in Packs 54
v3.1 / 56 / 57: (1) filter to only the arms/window actually needed before creating any volatile
table; (2) materialize a small DISTINCT tactic-id "driver" table and INNER JOIN to it instead
of an IN-subquery for every downstream MASTER/EVENT restriction.

Files: `archaeology/54_email_decisioned_send_funnel.sql` (v3.1),
`archaeology/56_xx_holdout_channel_slot_check.sql`, `archaeology/57_prior_unsub_send_split.sql`.

## 22. Pack 57 (2026-09-04): prior SFMC unsub vs send

**Prior unsub (any program, bank-wide) does NOT stop the email — it reduces send rate, doesn't
zero it.** Email-action decisions, Cards MNEs, 2025-01+, zero-send months excluded:

| MNE | NO_PRIOR_UNSUB sent/decisions | PRIOR_UNSUB sent/decisions |
|---|---|---|
| AUH | 629,332 / 716,613 (88%) | 13,210 / 39,824 (33%) |
| CRV | 654,274 / 876,935 (75%) | 34,462 / 49,359 (70%) |
| PCD | 7,732,049 / 8,153,301 (95%) | 146,222 / 420,631 (35%) |
| PCL | 11,416,399 / 12,039,061 (95%) | 420,951 / 506,953 (83%) |
| PCQ | 9,962,770 / 10,447,249 (95%) | 207,608 / 471,140 (44%) |
| TOTAL | 30,394,824 / 32,233,159 (94%) | 822,453 / 1,487,907 (55%) |

**Four readings:** (1) prior unsub drops send from 94% to 55% — a real effect, not a
suppression gate (a gate would read 0%). (2) PCL is the outlier at 83% sent despite prior
unsub — closest to re-mailing regardless. (3) prior-unsub clients are only ~5% of the
email-action population (261,400 of 4,976,263 distinct clients) — small population, large
effect. (4) prior unsubs explain ~665K of ~2.49M total non-sends (about a quarter) — most
non-send has another cause, still unidentified.

**Caveats:** "prior unsub" = disposition_cd=4 on ANY program, bank-wide, since 2024-01-01 only
(no visibility earlier); no re-subscribe/re-consent modelling — a re-opted-in client still
flags PRIOR_UNSUB; zero-send months (CRV 202502-202505, PCL 202607) excluded from the table
above — Pack 57 Block 2 (all months) shows a slightly larger population once included.

**Next = Pack 58**, splitting PRIOR_UNSUB by same-MNE vs other-MNE: is a still-sent decision a
compliance problem (the program re-mails its own unsubscribers) or expected behaviour (a
different program's opt-out shouldn't apply here)?

## 23. Pack 58 (2026-09-04): same-MNE vs other-MNE prior unsub

**Same-program unsub is honoured for ~90 days, then decays.** Pack 57's PRIOR_UNSUB split by
whether the earlier unsub was on the SAME MNE or a DIFFERENT one, zero-send months excluded:

| MNE | NO_PRIOR sent/dec | OTHER_MNE_ONLY sent/dec | SAME_MNE sent/dec |
|---|---|---|---|
| AUH | 629,332/716,613 | 13,210/39,824 | (no SAME rows) |
| CRV | 654,274/876,935 | 34,422/48,910 | 40/449 |
| PCD | 7,732,049/8,153,301 | 142,221/390,826 | 4,001/29,805 |
| PCL | 11,416,399/12,039,061 | 416,256/453,537 | 4,695/53,416 |
| PCQ | 9,962,770/10,447,249 | 178,654/365,901 | 28,954/105,239 |
| TOTAL | 30,394,824/32,233,159 | 784,763/1,298,998 | 37,690/188,909 |

**Days-since-unsub for same-MNE re-sends (decisions_sent):** CRV 91-365:5, 365+:35; PCD
0-7:4, 8-30:59, 31-90:331, 91-365:2026, 365+:1581; PCL 0-7:1, 8-30:8, 31-90:63, 91-365:1807,
365+:2816; PCQ 0-7:118, 8-30:829, 31-90:2327, 91-365:13150, 365+:12530. **~90% of all same-MNE
re-sends land at 91+ days.**

**Four readings:** (1) same-program unsub is honoured ~90 days, then decays — a list-freshness
problem, not a standing ignore. (2) hypothesis: the selection list rebuilds from a CPC/CIDM
feed that never learned of the SFMC unsub (list-rebuild-from-CPC, not re-consent) — untested,
Pack 59 tests it. (3) OTHER_MNE_ONLY sent rate is ~60% overall but ranges 33% (AUH) to 92%
(PCL) — open question on suppression-list specificity per MNE. (4) same-MNE dead weight is
~2% of the email-action population per arm — small share, real repeat-mail-to-opt-out.

**Next = Pack 59**, overlaying CPC state (1002/1012/1014) as of each decision date onto the
same-MNE population, split by sent vs not-sent — tests reading (2) directly.

## §24 (2026-09-04) CPC_RB_PREF_LOG exposure audit

Trigger: pack 61 v1 read `DDWV01.CPC_RB_PREF_LOG` for 7020-origin writes and got 324 (1012) / 70 (1046) clients, May-2025..Jun-2026; `DDWV01.CPC_RB_PREF` holds 50,660 / 17,126 for the same filters. The log carries about 1% of email-origin writes. Andre banned it (LOCKED FACT 1). Every headline number was traced to its CPC source:

| Number | Source file | CPC table | Status |
|---|---|---|---|
| Waterfall v3: 99.7% of SF unsubs no CPC write; 84% blind | 45_audit_queries.sql Q3b/Q5 | CPC_RB_PREF_MTHLY + CPC_RB_PREF | SAFE |
| 7020 writes collapse Mar-2026 (343-753/mo vs 3-9K) | 45_audit_queries.sql Q2 | CPC_RB_PREF | SAFE |
| 251,177 CPC 1012 flips | 32_cpc_1012_last_email_gap.py v2 | CPC_RB_PREF | SAFE |
| §0: 649,885 unsub clients, 0.06% CPC change in 7d, 0.33% in 90d | 06_cpc_pref_log_eda.sql | CPC_RB_PREF_LOG | EXPOSED |
| §17-C T1: 319,733 unsubs, 0.278% CPC No/blank in 90d | museum/cpc_reservoir_extract.py -> 23_cpc_landscape.py | CPC_RB_PREF_LOG | EXPOSED |
| §17-C T3/T4: 1002=No emailed 19.2% vs 58.9%; 1014=No emailed 61.4% ("1014 does not gate email") | museum/cpc_evidence.sql E4 and 23_cpc_landscape.py T3 | CPC_RB_PREF_LOG (both) | EXPOSED |
| museum/cpc_evidence.sql E1-E8 | itself | CPC_RB_PREF_LOG | EXPOSED |
| "96.6% of 251,177 flips have no prior vendor unsub" (pack 34b) | not found as one result anywhere in the repo | - | UNKNOWN, do not cite |

Consequences:
- The structural gap conclusion (SF unsubs never reach CPC) stands on the SAFE lineage (waterfall v3, pack 60).
- "1014 is not the email gate" (T3/T4) is UNPROVEN until rerun on CPC_RB_PREF. reference_cpc_1014_decisioning_parameter memory depends on it.
- §17-C's "T3 reproduces museum E4 exactly" was two pipelines on the same broken table, not a cross-check.
- Scope caveat: the ~1% coverage is proven for APP_SYS_CD 7020 writes only; coverage for branch / call-centre origins was not measured. The ban applies regardless.
- Banners added to every file that reads the log (06-14, 21a/b, 22, 30-33, 59 fixed, 61 fixed, museum/cpc_evidence.sql, museum/cpc_reservoir_extract.py).

## §25 (2026-09-04) Packs 61-63: what the email page writes to CPC (the "80/20" question)

Director (JP) reported, from MarTech's extract (proven to replicate CPC): of 17,013 Avion (1046) closures May-2025..25-Jun-2026, 19.2% also closed 1012 same time, 80.8% Avion-only. Andre's concern: the split starts from 1046 closures, so it only sees RBC-wide choices that also wrote 1046.

Pack 61 v2 (CPC_RB_PREF, APP_SYS_CD 7020): 17,126 Avion closures, 3,293 same-day 1012 (reproduces JP exactly, monthly too). Forward: 50,660 1012 revocations via the page; 46,813 (92%) never get a 1046; 3,293 same day.
Pack 63: of the 50,660, 46,514 (92%) close NOTHING else the same day; 4,119 close exactly one other pref; the companion is the page's program pref (1046 3,293; 1006 385; 1004 359; 1025 104; 1026 10), all origin 7020. No cascade to the consent set.

Settled: 1012 does not trickle down. The page writes 1012 alone, or 1012 plus one program pref.
Open: whether the 3,293 are "RBC-wide chosen on the Avion page, page writes both" (then 80/20 valid for Avion visitors; the 46,514 come from pages with no program pref) or "clients who chose both" (then RBC-wide is undercounted). CPC carries no page. Resolution = MarTech page table: of all 1012 selections, how many on the Avion page. SF join (pack 62) PARKED: 99.7% of SF unsubs never reach CPC, so any rate from matched pairs would mislead.
Headline for JP: Avion visitors are at most 16,854 of 67,514 page revocations in the window; 46,514 RBC-wide choices are invisible to a 1046-based split.
Pack 61 v1 read CPC_RB_PREF_LOG and returned 324/70 -> the LOG ban (LOCKED FACT 1, §24).

## §26 (2026-09-04) Unsubscribe page design, from the 2022 test-case workbook (Personal_RBC_Clients_Test_Cases_MM_V2/V4)

Two test clients, one visit per page variant (one page per CPC code, EN and FR), before/after CPC snapshots:
- Program option: each page writes ITS OWN code to 5002 and nothing else. Verified on 1002, 1004, 1012, 1023, 1024, 1025, 1026, 1044, 1045, 1046 (one write each, 4:03-4:10 PM 7/20/2022).
- RBC-wide option (bottom radio "unsubscribe from promotional emails from RBC Royal Bank"): writes 1012 to 5002 and nothing else (8/2/2022; one week later only the batch timestamp changed, 4:24 AM).
- In every tested case a page wrote exactly one code. The data (packs 61-63) and the test workbook do not show a cascade from 1012 to any program code. On that evidence the 3,293 same-day 1012+1046 pairs (pack 61) look like two submissions, and the director's 19.2% "also RBC-wide" should be read as a floor: a RBC-wide choice made from an Avion email would write 1012 only and be invisible to a 1046-based dataset. Phrase it as "the data does not show a cascade", not as a claim about the page.
- Residue: CPC does not refresh the timestamp when a value is rewritten unchanged, so the workbook cannot literally show "Rewards page + RBC-wide = 1046 untouched". Ask Digital Messaging (Matt) to confirm; otherwise the template evidence stands.
- Details (people, hashed ids, timestamps) in local sfmc_unsub_blueprint_notes.md §9 (gitignored).

## §27 (2026-09-04) Pack 60: SF-primary + CPC-only union, monthly (the director's reporting ask)

Window Aug-2024..Jul-2026, active-personal universe, SF = first disposition-4 per client-month, CPC = 1012 -> 5002 any origin (CPC_RB_PREF), match on CLNT_NO with the write within [-1,+14] days of the unsub.
- SF_ONLY 564,367 | SF_AND_CPC 4,428 | CPC_ONLY 291,152 client-months.
- If Salesforce is primary, CPC email-consent revocations add 291,152 on 568,795 = +51% (the director guessed +20%). Overlap 0.8%: the two sources are near-disjoint; the combined view is close to the sum.
- CPC_ONLY by writer: 7001 144,821; 7020 100,847; 7003 33,547; 7053 9,705; 7006 2,232. The 100,847 written by the email page (7020) with no SF disposition-4 within 14 days is the REVERSE gap - open, probed by pack 64.
- By LOB (Andre's pivot of Block 3): LOYALTY 371,270 / OTHER 153,826 / CARDS 39,271 SF-only; 2,453 / 1,600 / 375 matched. VRE alone 281,440.
- RECONCILIATION RESOLVED: v1 counted each client once per MONTH (568,795); Q1 and the director's table count each client once in the WINDOW (506,646). Everything else identical (spine, filters, dates). Pack 60 v2 dedups per client in the window on both sides; v1 numbers above are client-months and must not sit next to 506,646. v2 NOT YET RUN.
- Results: archaeology/results/60_*.tsv.

## §28 (2026-09-04) Pack 64: the reverse gap - two unsubscribe paths, each recorded in one system

Population: the 100,847 CPC 1012 revocations written by the email page (APP_SYS_CD 7020) with no SF disposition-4 within [-1,+14] days (pack 60, Aug-2024..Jul-2026).
- 100,649 of them are in VENDOR_FEEDBACK_MASTER (key lines up); 198 are not.
- Nearest SF disposition-4 at ANY distance: 1-14 days 337; 15-90 days 1,654; 91+ days 5,710; NEVER 92,948 (92%). The match window is not the cause.
- Uniform across quarters: 2024-Q3 13,991 / 492 matched ... 2026-Q2 3,754 / 322. About 95% of email-page CPC writes have no SF unsub in every quarter.

Reading, combined with the forward gap (99% of SF disposition-4 unsubs have no CPC write, §21-§27, waterfall v3):
- Path 1: one-click / list-unsubscribe from the mail client -> disposition 4 in Salesforce -> no CPC write.
- Path 2: the CloudPages unsubscribe form -> CPC via the 7020 backfeed -> no disposition 4.
- The two paths are nearly disjoint by mechanism. Neither system is complete; the union (pack 60) is the only complete count. The hypothesis in sfmc_unsub_blueprint_notes §7 is now measured from both sides.
- Open: pack 64 Block 2 (were these clients sent an email in the prior 90 days) not yet recorded; if most were not, some are preference-centre visits without an email.
- Results: archaeology/results/64_*.tsv.
