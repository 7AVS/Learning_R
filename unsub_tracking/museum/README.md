# CPC consent evidence — museum

## The claim

Email consent lives in two systems that don't talk: 99.6% of unsubscribers remain "contactable" in CPC, and ~47% of clients who explicitly opted out of banking email received campaign email within a quarter.

## Evidence map

| # | Claim it supports | Expected output shape | Key expected magnitude |
|---|---|---|---|
| E1 | Two consent worlds exist as separate volume streams | rows: consent_world × month_yyyymm; cols: consent_world, month_yyyymm, clients | email unsubs outnumber CPC opt-outs ~35x |
| E2 | The blind gate — CPC mostly doesn't know a client unsubscribed | 3 rows (unsub_clients_total / with_explicit_cpc_optout / without_explicit_cpc_optout) × clients | ~99.6% of unsubscribers have no explicit CPC opt-out (blind) |
| E3 | No automated bridge from email unsub to CPC | rows: PREF_ID × APP_SYS_CD × had_prior_unsub(Y/N); cols: + flips | no-prior dominates every row; only real pipe is SFMC on 1012, ~15/yr against 7020/1012 flips total |
| E4 | The leaking gate — flagged-out clients still receive campaign email | rows: pref_id × exclusivity(only_this_flag/multi_flag) + ALL_SWITCHES; cols: flagged_clients, received_campaign_email | ~47% leak on 1012-only exclusivity cut; ~19% on entity-DNS (1002) main cut |

Note: counts drift slightly day to day (warehouse load timing); rates are stable across reruns.

## Run notes

- Engine: Teradata-direct.
- Run top to bottom, one session.
- Two volatile tables (`vt_unsub_base`, `vt_unsub_first`) are dropped at the end.
- Rerun after a failure: run the two DROPs first, then rerun from the top.

## Provenance

- Exploratory work: `../archaeology/` (packs 01–22).
- Full audit trail: `cpc_evidence_dossier.html`.
- Canon record: `../UNSUB_TRACKING_KNOWLEDGE.md` §14.
- Numbers verified by independent rerun 2026-07-24.
