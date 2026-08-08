# Session bridge — 2026-08-07 (spotlight / Power Pack Q3)

## READ FIRST — what is true right now

Notebook state = commit `cb6b42c` (pushed). Three artifacts are live:
- `spotlight/unsub_analysis_notebook.py|.ipynb` — the EXPLORATION notebook (all Q/D cells).
- `spotlight/spotlight_deck.py|.ipynb` — NEW. Presentation-only, 11 cells, 3 purpose-built
  exhibits. Reads existing caches ONLY, no new pulls. Never run yet.
- `spotlight/story_2slides_2026-08-07.md` — the 2-slide NARRATIVE (not slides).
- `spotlight/results_2026-08-07_full_notebook_run.md` — every number from the 2026-08-07 run,
  transcribed from 18 screenshots. This is the provenance for the story.

## THE STORY (locked with Andre 2026-08-07, supersedes 2026-07-22 "Anatomy of an Unsub")

Cards barely bleeds email consent (0.18%/send vs 0.37% Loyalty); the one spike was FIFA, an
event. Where we DO bleed it is NOT over-contacting — it is shallow relationships: acquisition
audiences (PCQ 0.58%), the youngest AND oldest clients, and re-contact after a gap. An unsub
is a lost CHANNEL to a client whose spend is already flattening, not a lost client.

Slide 1 = Exhibit A (landscape). Slide 2 = Exhibit C (risk multiples ladder) + Exhibit D
(value strip). Charts carry the story; text is a nudge only.

## TWO NEW FINDINGS FROM THIS RUN (both change prior beliefs)

1. **Q4 v2 — "one email and they leave" is DEAD.** With honest 7-month bands (Oct25-Apr26
   total Cards emails), the risk peak MOVES from 1-2 to **3-5 emails: 1.00%**, 2x every other
   band; 6-10 is 0.24%. Unsubbers 43% in 3-5 vs stayers 18%. Saturation still dead — heaviest
   bands are safest. Risk zone = low-but-repeated contact.
   Caveat printed on chart: unsub stops the email count, so leavers tilt low-band partly by
   construction.
2. **Q5a v3 — over-contacting RULED OUT for age; age story is U-SHAPED.** Contact intensity is
   flat (4.4-4.7 emails/client, every band). Per-email propensity: <25 1.37, 25-34 1.13,
   35-49 0.81, 50-64 0.76, **65+ 1.11** per 1k emails (overall 0.96). Q5a v1/v2 showed 65+ as
   below-average — that was a DENOMINATOR ARTIFACT (Cards series denominated on all
   RBC-mailed). **Do not ship Q5a v1 or v2. v3 only.**

## THE NEXT SESSION'S JOB, in order

1. **Run `spotlight_deck.ipynb` in the work env** (`git pull` first). It has never executed.
   It prints drift checks (`expect(...)`) against the 2026-08-07 numbers — if any says DRIFT,
   the story needs re-checking before slides. Expect possible small breaks in Exhibit D's
   spend panel (column-name sniffing on `b_delta_summary.csv` is defensive but unproven).
2. **Red-team the story — IT HAS NOT BEEN REVIEWED.** See failure note below.
3. Only then build the actual 2 slides.

## RED TEAM — FAILED TWICE, STORY NEVER REVIEWED (do this right next time)

The `red-team-deck` workflow was invoked twice against the story. Both times it reviewed
`museum/cpc_evidence_deck.html` instead.
- ROOT CAUSE: the script does `const A = args || {}; const DECK = A.deck || '<cpc default>'`.
  Run 1 passed args as plain prose; run 2 passed a JSON **string**. In both cases `A.deck` is
  undefined, so it silently fell back to the CPC default. **FIX: pass `args` as a real JSON
  object in the Workflow call, not a string** — e.g.
  `args: {deck: "unsub_tracking/spotlight/story_2slides_2026-08-07.md", round: "1_spotlight_story"}`.
  Verify the run's first log line says the right path before letting it finish.
- COLLATERAL: run 1's synthesis OVERWROTE `museum/red_team_review_round5.md`. The previous
  occupant (2026-07-31, 20 objections, conflicting RT5-nn IDs) is GONE — .md is gitignored so
  there is no git copy. `red_team_review_round5_andre.md` survives. If a copy exists in the
  closed env, restore it and reconcile numbering before appending ledger rows.
- NOT WASTED: the file now at `museum/red_team_review_round5.md` (2026-08-07 11:12) is a
  legitimate fresh CPC round — 20 objections, 6 Blockers, 7 High, verdict REBUILD, with a
  consolidated data ask. Useful when CPC resumes. Run 2 wrote nothing (synthesis died on the
  session limit).

## OPEN DECISION FOR ANDRE (asked, not answered)

Extend the frequency lookback from 3 months to 12, and add a bank-wide (non-Cards) pre-window
count, in the SAME Q4-LB v2 pull? Adds `pre_cnt_12m` + `pre_any_cnt_12m`; ~30 min of edits and
one 10-bite re-pull. Buys: "new to Cards mail" becomes near-bulletproof, and first-contact
clients split into "never mailed by RBC" vs "new to Cards, seasoned elsewhere".
Claude's caveat on record: "new to mail" is NOT random — it bundles newly-acquired, newly
-consented, and re-subscribers. Deeper lookback sharpens the descriptive read; causal
first-contact impact still needs campaign randomization. Recommendation was: do it, then stop
(a standing new-to-campaign indicator is over-engineering until a decision needs it).

## OTHER OPEN ITEMS

- `evidence_repeat_unsub.sql` — still pending Andre's run (repeat-unsub distribution).
- t12 endpoint — rerun after 2026-09-01 (Aug 2026 not closed).
- Engagement-class finding ("losing the listeners, keeping the deaf") is museum-era, NOT
  rebuilt on the unified pipeline. Candidate 3rd angle if red team asks who unsubbers are.
- Per-email propensity by TENURE not built (only age).
- `NEXT_SESSION.md` and `POWER_PACK_BRIEF.md` at repo root still have UNCOMMITTED rewrites
  (they are .md = gitignored; they live only on this disk).

## FOLDER REORG (done 2026-08-07)

16 files archived to `unsub_tracking/_archive/` with mapping in
`_archive/ARCHIVE_LOG_2026-08-07.md`: 3 generations of superseded pipelines
(`spotlight.py`, `spotlight2.py`, `unsub_before_after_jul25/`, downloaders, lineage HTML),
3 withdrawn prompt files, 1 superseded brief, 3 stray .pyc. `museum/` and `archaeology/`
deliberately untouched (cited evidence trail). 11 files flagged UNSURE and left in place —
mostly the preflight/scope-test SQL chain that settled the per-list-unsub grain, plus
`diag_a1_dupes.py` and `unsub_unified_onepass_16gb.py` (still-open caveats).
Convention proposed: date one-offs (`diag_YYYYMMDD_<question>.sql`); `git mv` a superseded
pipeline into `_archive/` the SAME DAY its replacement lands.
