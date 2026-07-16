# Unsub Tracking — Knowledge Base (migration doc)

Everything learned building the bank-wide (NBA-wide) email unsubscription deep-dive.
Written 2026-07-14 for migration to a new environment. Repo folder: `unsub_tracking/`.

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
| **SHARING-ONLY per dictionary — ENFORCEMENT UNVERIFIED** | 1014 share-for-marketing · 1015 share-for-service · 1036 personalization · 1057 DI SfM · 1016 credit bureau | **1014/1015: NO**; others allowed | Dictionary says NO (data usage only) — but team lore says 1014=N = "out of all marketing". Which switches actually stop email = `12_switch_enforcement_test.sql` (state-before-window × received-email cross-tab, 1007 as negative control). Do not present the sharing-only classification as fact until 12 runs. |
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
| `08_reachability_overlap.sql` | Cross-tab unsub × 1002 × 1012 × 1014 flags — overlap/union of exit mechanisms |
| `09_cpc_switch_independence.sql` | Bundle sizes, same-timestamp pair matrix, contradiction census |
| `10_cpc_writes_by_system.sql` | APP_SYS_CD overlay: volume/bundle-shape/first-touch by system + Exact Target profile |
| `11_cpc_master_cube.sql` | THE cube extract: switch × position × system × save-shape (in-env pivot base) |
| `12_switch_enforcement_test.sql` | Which switch ACTUALLY stops email — state-before-window × received-email, 1007 negative control (settles 1014 dictionary-vs-lore) |
| `13_unsub_value_spine.sql` | Value spine: S1 first-unsub per client (in-env extract; embedded verbatim in 15 — never needs a standalone run) + S2 tracked-MNE league table |
| `14_cpc_optout_campaign_proximity.sql` | Did campaign sends precede CPC 1002 opt-outs? Backward proximity with base-rate control |
| `15_unsub_value_enrichment.py` | Spark/UCP (allowed .py — Lumina side): spine → TIBC×age segment matrix by trigger MNE + PROF_TOT_ANNUAL vetting |
| `16_population_lost_trend.sql` | Month × MNE, ALL MNEs, long format: em_clients_sent (disposition 1) + clients_first_unsub + tracked flag — Excel-pivot extract |
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
