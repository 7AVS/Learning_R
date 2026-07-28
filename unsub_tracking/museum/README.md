# CPC consent evidence - museum

## The claim

Email consent lives in two systems that don't talk: 99.6% of unsubscribers remain "contactable" in CPC, and ~47% of clients who explicitly opted out of banking email received campaign email within a quarter.

## Definitions & sequencing

- Universe: NBA campaign email (SFMC vendor feed) vs the bank's CPC consent gate.
- The 3 switches: 1002 (entity do-not-solicit), 1012 (banking email), 1014 (marketing sharing) - the email-relevant slice of ~40 preference codes.
- E1: two independent volume streams over the same months; no client-level link implied.
- E2: consent standing = latest answer at any point in time, before or after the unsub (most generous read for CPC). Now split before-unsub vs after-unsub, counting a multi-switch client once on their EARLIEST qualifying opt-out - output is 5 rows, the original 3 plus `optout_recorded_before_unsub` / `optout_recorded_after_unsub`.
- E3: matches CPC flips against any prior unsub, no lookback cap.
- E4: sequenced - opted-out standing before Apr 1, 2026 -> campaign email received Apr-Jun 2026.
- E5: sequenced - unsubscribed before Apr 1, 2026 (cohort = `vt_unsub_first`) -> any campaign email Apr-Jun 2026 (reuses `vt_q2_sends` as-is, no new send scan).
- E6: cohort = latest 1002 (entity do-not-solicit) standing before Apr 1, 2026, mirroring E4's `cpc_gate` narrowed to 1002 only; sends = campaign email Apr-Jun 2026 broken out by mne = `SUBSTR(TREATMENT_ID, 8, 3)`.
- E7 (red-team round 2): breadth test - same Option-A standing logic as E4/E6, run across 4 switches (1002/1012/1014/1006) instead of 1 or 3, cohort = `vt_gate_cohorts`; sends = `vt_q2_sends_mne` (same bounds as `vt_q2_sends`, carries mne). 1006 (credit-card product content) is added as our own topic-gate control - blank/no-record = implicit YES for 1006, only an explicit No qualifies. Top 12 mne per pref_id via QUALIFY. Supersedes E6 for breadth; E6 is left as-is (simpler shape, already catalogued).
- E8 (red-team round 2): the 10.4%/25,721 waterfall - `vt_unsub_first` extended to carry `unsub_mne = SUBSTR(TREATMENT_ID, 8, 3)` (the unsub-triggering campaign) via a minimal extension to `vt_unsub_base` (now also selects `TREATMENT_ID`) and the dedup step. Cohort = pre-Apr unsub clients. New volatile `vt_postunsub_sends` (client, mne, disposition_cd, disposition_dt_tm) covers disp IN (1,5) Apr-Jun, restricted to the cohort. Exclusions applied cumulatively, in this order: (1) sends within 14 calendar days of unsub (CASL 10-business-day proxy), (2) clients whose every send hardbounced (mne-grain proxy for same TREATMENT_ID, not literal TREATMENT_ID match), (3) CPC-side re-consent (5001 flip on 1002/1012 after unsub_tm - catches CPC-side only, SFMC-side resubscribes are not visible in this feed). The residual is then split same-campaign (mne = unsub_mne) vs cross-campaign, using only the sends that survived exclusions 1-2. Order matters and is fixed as documented above - reordering the exclusions changes the residual.

## Evidence map

Primary read: run the file, read the last table - that's the story; Evidence 1-8 are the proofs behind each row. The SUMMARY statement (final SELECT, before the seven DROPs) is one table carrying all the key figures - 12 rows, columns `what | time_window | clients | of_population`. E2's full-history standing logic, E5's pre-Apr cohort, and E8's waterfall chain are copied verbatim into SUMMARY's own WITH clause (not re-derived), so the outputs can never disagree.

| # | Claim it supports | Expected output shape | Key expected magnitude |
|---|---|---|---|
| E1 | Two consent worlds exist as separate volume streams | rows: consent_world x month_yyyymm; cols: consent_world, month_yyyymm, clients | email unsubs outnumber CPC opt-outs ~35x |
| E2 | The blind gate - CPC mostly doesn't know a client unsubscribed | 3 rows (unsub_clients_total / with_explicit_cpc_optout / without_explicit_cpc_optout) x clients | ~99.6% of unsubscribers have no explicit CPC opt-out (blind) |
| E3 | No automated bridge from email unsub to CPC | rows: PREF_ID x APP_SYS_CD x had_prior_unsub(Y/N); cols: + flips | no-prior dominates every row; only real pipe is SFMC on 1012, ~15/yr against 7020/1012 flips total |
| E4 | The leaking gate - flagged-out clients still receive campaign email | rows: pref_id x exclusivity(only_this_flag/multi_flag) + ALL_SWITCHES; cols: optout_clients, got_email_apr_jun | ~47% leak on 1012-only exclusivity cut; ~19% on entity-DNS (1002) main cut |
| E5 | The SFMC suppression test - does the vendor itself honor unsubscribes, independent of CPC | 2 rows (unsub_before_apr_clients / got_email_apr_jun); cols: metric, clients | Expected LOW if SFMC suppression works - stating either result is informative |
| E6 | What mail reaches do-not-solicit clients - rules out "it's just transactional" | rows: mne x clients x send_rows, TOP 20 by clients desc | Expected mostly recognizable campaign MNEs - the transactional-escape test |
| E7 | Does the leaking-gate pattern hold across switches, including our own product preference | rows: pref_id x mne x clients, top 12 mne per pref_id (QUALIFY); <=48 rows | Expected read: pattern holds across all 4 gates incl. 1006 - if 1006 (our own topic gate) behaves differently from 1002/1012/1014, that's the story |
| E8 | The 10.4%/25,721 waterfall - how much survives CASL lag, hardbounce, and CPC re-consent | ~7 rows, stepwise ladder, cols: step, clients | Expected read: the number slide 4 is allowed to say. Five-figure in-program residual = finding stands; if it collapses to low figures, the slide changes |

Note: counts drift slightly day to day (warehouse load timing); rates are stable across reruns.

Consent-standing scans (E2's blind-gate flag, E4's leaking-gate flag, E6's 1002-only cohort, E7's 4-switch `vt_gate_cohorts`) read full history, no 2024 floor. Deliberate exception: CPC_RB_PREF_LOG is a small table, full-history scans are CPU-safe on it (proven in archaeology/21a). All send-table scans (VENDOR_FEEDBACK_EVENT/MASTER, and E1/E3's CPC volume/flip windows, E5/E6's Apr-Jun 2026 send lookups, E7's `vt_q2_sends_mne`, and E8's `vt_postunsub_sends`) remain floored at 2024 or later (Apr-Jun 2026 EVENT + Mar-Aug 2026 MASTER load_tm, the proven-safe bounds). E8's reconsent CPC scan is bounded >= Jul 2025, < Jul 2026 (not full-history - it doesn't need to be, the re-consent has to postdate the unsub).

Expected magnitudes, full-history basis:
- E2: ~99.6% of unsubscribers still have no explicit CPC opt-out (~1.3K flagged of ~312K total unsubscribers).
- E4: cohorts run ~33K-79K clients per switch (pref_id x exclusivity cut); leak rate ~47% on the 1012-only cut, ~19-30% on the others. Counts drift day to day, rates hold across reruns.

## Run notes

- Engine: Teradata-direct.
- Run top to bottom, one session, one statement at a time.
- Seven volatile tables (`vt_unsub_base`, `vt_unsub_first`, `vt_q2_sends`, `vt_dns_1002`, `vt_gate_cohorts`, `vt_q2_sends_mne`, `vt_postunsub_sends`) are dropped at the end, in reverse creation order.
- Final statement before the seven DROPs is SUMMARY - one table, 12 rows, the primary read (see Evidence map above).
- Rerun after a failure: run the seven DROPs first, then rerun from the top.

## Provenance

- Exploratory work: `../archaeology/` (packs 01-22).
- Full audit trail: `cpc_evidence_dossier.html`.
- Canon record: `../UNSUB_TRACKING_KNOWLEDGE.md` section 14.
- Numbers verified by independent rerun 2026-07-24 (Evidence 1-6, SUMMARY rows 1-10). Evidence 7-8 and SUMMARY rows 11-12 are newly authored (red-team round 2 response) and not yet run.
