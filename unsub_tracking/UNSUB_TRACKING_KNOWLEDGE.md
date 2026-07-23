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
