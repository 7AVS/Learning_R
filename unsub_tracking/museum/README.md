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

## Evidence map

| # | Claim it supports | Expected output shape | Key expected magnitude |
|---|---|---|---|
| E1 | Two consent worlds exist as separate volume streams | rows: consent_world x month_yyyymm; cols: consent_world, month_yyyymm, clients | email unsubs outnumber CPC opt-outs ~35x |
| E2 | The blind gate - CPC mostly doesn't know a client unsubscribed | 3 rows (unsub_clients_total / with_explicit_cpc_optout / without_explicit_cpc_optout) x clients | ~99.6% of unsubscribers have no explicit CPC opt-out (blind) |
| E3 | No automated bridge from email unsub to CPC | rows: PREF_ID x APP_SYS_CD x had_prior_unsub(Y/N); cols: + flips | no-prior dominates every row; only real pipe is SFMC on 1012, ~15/yr against 7020/1012 flips total |
| E4 | The leaking gate - flagged-out clients still receive campaign email | rows: pref_id x exclusivity(only_this_flag/multi_flag) + ALL_SWITCHES; cols: optout_clients, got_email_apr_jun | ~47% leak on 1012-only exclusivity cut; ~19% on entity-DNS (1002) main cut |

Note: counts drift slightly day to day (warehouse load timing); rates are stable across reruns.

Consent-standing scans (E2's blind-gate flag, E4's leaking-gate flag) read full history, no 2024 floor. Deliberate exception: CPC_RB_PREF_LOG is a small table, full-history scans are CPU-safe on it (proven in archaeology/21a). All send-table scans (VENDOR_FEEDBACK_EVENT/MASTER, and E1/E3's CPC volume/flip windows) remain floored at 2024 or later.

Expected magnitudes, full-history basis:
- E2: ~99.6% of unsubscribers still have no explicit CPC opt-out (~1.3K flagged of ~312K total unsubscribers).
- E4: cohorts run ~33K-79K clients per switch (pref_id x exclusivity cut); leak rate ~47% on the 1012-only cut, ~19-30% on the others. Counts drift day to day, rates hold across reruns.

## Run notes

- Engine: Teradata-direct.
- Run top to bottom, one session, one statement at a time.
- Three volatile tables (`vt_unsub_base`, `vt_unsub_first`, `vt_q2_sends`) are dropped at the end.
- Rerun after a failure: run the three DROPs first, then rerun from the top.

## Provenance

- Exploratory work: `../archaeology/` (packs 01-22).
- Full audit trail: `cpc_evidence_dossier.html`.
- Canon record: `../UNSUB_TRACKING_KNOWLEDGE.md` section 14.
- Numbers verified by independent rerun 2026-07-24.
