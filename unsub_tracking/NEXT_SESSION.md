# NEXT SESSION — start here

Workstream: **"who unsubscribes"** = `unsub_tracking/museum/`.
**`unsub_before_after_jul25/` is a DIFFERENT workstream with its own session. Do not touch it.**
Commits prefixed `museum:` are ours. `jul25:` are not.

Read in this order: this file → `POWER_PACK_BRIEF.md` → `museum/RUN_2026-07-30_L10_L13.md`.

---

## 1. The reset Andre gave at the end of 2026-07-30

These override the current page's structure. The page was built bank-wide-first and that was wrong.

1. **CARDS IS THE PRIMARY LENS.** Bank-wide is a baseline bar next to each cards chart, not its own section.
2. **The window has no past.** Everything measured so far is Mar–May 2026 only. The brief's frequency questions are about the **12 months prior**. This is the root cause of every missed brief item.
3. **§3 "we lose the people who were still reading" is MISLEADING — must change.** Unsubscribing requires opening and clicking, so engagement is a *precondition* of the action, not just a correlate. Replace with: *did the client engage with campaigns BEFORE the window?*
4. **The L13 pair table is DROPPED.** 3c is still live, but as **overlap/concurrency** — how many distinct campaigns touched a client in the same period, and does the unsub rate rise with that.
5. **Exclude regulatory campaigns everywhere.**
6. **Less prose, more plot.** Caveats live in dropdowns. If the plot cannot tell the story, fix the plot.

---

## 2. THE JOB: adapt and run pack 19. Do not build from scratch.

`archaeology/19_unsub_journey_lookback.sql` (230 lines) is the 12-month-prior query. It has **never
run** — v4→v7 were all SQL fixes, no result exists. It already has the right grain, the right
lookback, and leavers-vs-stayers built in.

**It also already fixes point 3 above.** Lines 156-158 classify engagement from `lookback_clicks` /
`lookback_opens` — the 12 months *before* the index date, not the unsubscribe email. That is exactly
the replacement Andre asked for, and it is already written.

### What it produces now

`cohort_group` (unsub/stayed) × `engagement` (clicked/opened/dark) × `cohort_month`, with
`avg_contacts`, `avg_mnes`, bands `contacts_0 … contacts_15p` and `mnes_0 … mnes_5p`.
Lookback join is `19_unsub_journey_lookback.sql:150-151` — 12 months back from each client's own
`index_dt` (unsub date for leavers, last-send date for stayers).

### The three edits it needs

**(a) Exclude regulatory — line 134, in the `events` CTE `WHERE`:**
```sql
  AND SUBSTR(e.TREATMENT_ID, 8, 3) NOT IN (
      'AFD','BPU','BUK','CFR','EOE','FNE','FSA','FSO','FXR','GAF','HFC',
      'HPN','IOO','NST','OTC','PUK','ROP','TWI','VMF','VOA','ZDC','ZHX')
```
The 22 are canon in `museum/RUN_2026-07-30_REGULATORY.md`.

**(b) Add cards-only counterparts — after line 146, inside the `lookback` CTE:**
```sql
  COUNT(DISTINCT CASE WHEN s.disposition_cd = 1
        AND s.mne IN ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV')
        THEN s.TREATMENT_ID END) AS lookback_contacts_cards,
  COUNT(DISTINCT CASE WHEN s.disposition_cd = 1
        AND s.mne IN ('PCQ','PCL','PCD','AUH','CLI','MVP','CRV')
        THEN s.mne END)          AS lookback_mnes_cards,
```
Then band them in the final SELECT the same way as lines 166-180. **Do not filter the whole query to
cards** — the bank-wide columns are the baseline and must stay on the same row.

**(c) OVERLAP, for brief 3c** — add a concurrency measure. Distinct MNEs per client per *month* in
the lookback, then the max or mean across months. `lookback_mnes` counts distinct campaigns over the
whole 12 months, which is breadth; overlap is how many landed in the *same* month.

### Then run it

Teradata-direct. It is the only new run required. Watch for spool — v4 was already staged into
volatile tables after two spool failures, and there are 4 DROPs at EOF so a rerun is clean.

---

## 3. Queued behind that

| Item | Cell | Needs |
|---|---|---|
| Cards angle: H1 ratios, **H2 contact control per campaign**, H3 engagement, H4 populations | `[20h]` line 1488 | nothing — analysis only |
| Is the 427,079 a leak or regulatory mail | `[20i]` line 1584 | nothing — analysis only |
| Send cadence per campaign (brief 3a) | `[5b]` line 435 | EDW connection |
| §1/§2 cards view for PCL, PCQ, PCD | — | `hdfs dfs -getmerge .../csv_l9_per_campaign_ratios` |

**Analysis-only run path:** cell `[1]` at **line 79**, then **line 637 → 1639**. Skips every EDW cell.
At line 972 expect `stage banded - BUILT (stale - missing band_v3)`. If it says `REUSED`, stop.

---

## 4. Open questions for Andre

1. **Which table holds `ACTION_TYPE`, joinable on TACTIC_ID or MNE?** That retires the hardcoded
   22-mnemonic list permanently instead of hand-maintaining it.
2. `unsub_before_after_jul25.py:787` still carries the retired "listeners/deaf" phrasing — his other
   session owns that file.

---

## 5. Traps that already cost time. Do not re-learn these.

- **`red-team-deck` skill has the CPC deck HARDCODED** and silently ignores the target passed in args.
  All 7 agents reviewed the wrong artifact. **Spawn reviewers directly with the Agent tool.**
- **Run `museum/colcheck.py` before every push.** It replays the analysis cells against a mock Spark
  and catches ambiguous joins, missing columns and unionByName mismatches in two seconds. It is
  verified by a negative control — if you change the mock, re-run that control.
- **Two `COUNT(DISTINCT)` in one Teradata `GROUP BY` over this window does not finish.** Hung `[5b]`.
- **`WHERE MOD(...) = 0 OR x IN (subquery)`** can drop to a product join. Use two branches `UNION`ed.
- **`stage()` only checks column NAMES**, so redefining a band silently reuses stale data. That is
  what `BAND_VERSION` / `BAND_STAMP` exists for — bump it when a cut point changes.
- **Constants read by analysis cells must live in `[1]`**, not in a pull cell. `SAMPLE_MOD` in `[7b]`
  killed the analysis-only path with a `NameError`.
- **Grep proving a result is absent from the repo means "not transcribed", NOT "not run."** Ask.

---

## 6. Findings that survived scrutiny — safe to build on

- **Age survives a contact control** in all five bands (1.25–1.63). **Tenure largely does not** —
  collapses to 1.04 at 4–6 sends. Do not state them with equal confidence.
- **Frequency is non-monotonic.** Unsub rate peaks at 7–12 emails (0.887%) and falls to 0.526% at
  13+. Likely survivorship. Derived from L10b; the five bands reconcile to L7's 62,658 / 9,072,977
  exactly.
- **Cards loses high-potential far harder than the bank:** CRV 1.62, AUH 1.54 vs 1.15.
- **AUH and CRV point opposite directions on single-product:** 1.32 vs 0.78. Bank-wide 0.91 hides both.
- **Co-occurrence is ~nothing:** max cards lift 1.43 on 152 leavers; PCL+QCF 1.03 on 919k clients.
- **Value is flat.** Top profit quintile is *protective* (0.84). But it is not independent of age —
  `PROF_TOT_ANNUAL` is current-year contribution and rises with tenure.
