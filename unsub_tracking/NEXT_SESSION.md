# NEXT SESSION — unsub unified pipeline (updated 2026-08-03)

Read in this order: this file → `UNIFIED_BRIEF.md` → `spotlight/AUDIT_2026-08-02.md`.
All .py pushed to GitHub (`6410cb3`); .md files live in this local repo.
(Previous version of this file = the 2026-07-30 museum-era plan; superseded —
its resets, incl. "Cards is the primary lens", are absorbed into UNIFIED_BRIEF.md.
See git history if needed.)

## The pivot that happened 2026-08-02 (the bridge)

1. **Two stakeholder briefings were merged into ONE brief: `UNIFIED_BRIEF.md`.**
   It SUPERSEDES the raw wording of both `POWER_PACK_BRIEF.md` (Maya email,
   spotlights) and `WORKSTREAM_2_BRIEF.md` (LOB/MNE + Cards profit — transcribed
   from the second briefing photo, was never in the repo before). Andre's verbal
   refinements are locked inside it — do not re-ask settled questions.
   Core structure: Cards is the subject, enterprise = comparator; THREE time
   axes: **A** = cross-LOB profiling, in-window Jan 1–Apr 30 2026 (frequency
   lives here, 3-month logic, no 12m lookback — left-truncation); **B** =
   before/after, anchor 2025-08-31 HARDCODED, remeasure +12m (2026-08-31),
   Cards-mailed cohort, leaver = any-list unsub by anchor, cards_unsub carried
   as slicing column; **C** = trailing-12m monthly unsub curve by EVENT month
   (q_trend's entry-cohort month does NOT answer this).
2. **One pipeline file implements all of it: `spotlight/unsub_unified.py`**
   (cell-structured, ONE Run All, brain-local PySpark kernel — NEVER YARN,
   Andre hard requirement; teradatasql direct connector for EDW, never Trino).
   Replaces `spotlight.py` + `spotlight2.py` for this deliverable (kept as
   history; spotlight2's temporal core condemned by audit: floating anchor,
   contaminated leaver flag). `spotlight/unsub_unified_onepass_16gb.py` =
   one-pass variant, A/C playground ONLY — its Piece B has two known bugs,
   never ship B from it.
3. **The process (now standing practice; /analyze skill encodes it):**
   brief frozen → build → BLIND red team vs brief (different model, zero
   context) → patch → full blind re-review → coverage table → SMOKE run
   (bite 0) → CSV diff vs prior smoke (drift check = arithmetic, not trust)
   → full run. Two red-team rounds caught 4 blockers before any full-scale
   number existed.

## Run state (session end 2026-08-03)

- **Banked on HDFS (`/user/427966379/unsub_unified/`):** A1 client grain
  (10 bites, 10.44M rows), A2, C (all bites), UCP enrichment. Rerun = SKIP.
- **Full-scale A-side numbers already produced** (mislabeled `out_smoke/`):
  109,431 unique enterprise unsub clients Jan–Apr; rows a1=179, a2=178, a3=18,
  a4=456, c=1858. PRELIMINARY until the SMOKE=False run relabels into `out/`.
- **Piece B: bite-0 only, partly from the buggy one-pass version.** The
  corrected file AUTO-RE-PULLS unverifiable B bites (regime-flag guard prints
  "no regime flag found … forcing re-pull"). NO manual HDFS wipe needed.
- **FULL RUN COMPLETED 2026-08-03 (build 2026-08-03c, SMOKE=False): all six
  deliverables [OK], zero errors.** Enterprise unique unsubbers Jan-Apr =
  109,431; Cohort B = 4,522,763. B_DFP landed all 10 bites clean — NO spool
  error (the 2646 fear is dead; ~328k rows/bite, 13s each). CSVs:
  `/home/jovyan/unsub_unified_out/` + HDFS `out/`. a2 has 177 rows vs 178 in
  an earlier read — verify which MNE dropped when pivoting (likely zero-count
  edge). xlsx still needs `pip install openpyxl` (CSV fallback fine).
  Along the way: HDFS NAMESPACE quota (file count 13,107) was hit — dead dirs
  deleted; keep an eye on file counts, `hdfs dfs -count -q` shows both quotas.
- **t12 empty until September:** Aug-2026 not closed. Pre-close runs band t12
  `untiered` (NULL, not fake zeros) and print loudly. RERUN AFTER 2026-09-01 —
  the regime flag forces the t12 re-pull automatically.

## Process rules (binding — from the 2026-08-03 post-mortem, ~6 failed runs)

1. A patch is cleared ONLY by a full-file smoke that re-executes past it —
   review + compile never suffice. 2. Two crashes in unrun code → static sweep
   of the whole region, one combined patch. 3. Build stamp governs every
   diagnosis — never assume a screenshot matches the latest push. 4. Asserts
   must hold across the mode matrix (SMOKE×full, pre/post-Sept). 5. Sidecar
   files stay out of data-glob namespaces. Full doctrine in memory:
   feedback_execution_gap_lessons.

## Standing gotchas (learned the hard way 2026-08-02/03)

- 10 NULL-clnt_no rows per full A1 pull (landing conversion artifact) — dropped
  with WARN. A NULL key joins to nothing and masquerades as "1 duplicate
  client" in distinct counts (empty join lookups on a finished run = NULL key).
- Full-scale Spark joins on a 4GB pod = OOM kernel death; file is bite-looped
  (no single join exceeds ~1.1M clients). 16GB pod comfortable either way.
- `pip install openpyxl` in pod for the single xlsx; else 6 CSVs + zip
  fallback (fine, pivot-ready). Zip may report 0.0 MB — CSVs are tiny, fine.
- Outputs: HDFS `out/` (full) vs `out_smoke/` (bite-0); pod-local
  `~/unsub_unified_out[_smoke]/`; `unsub_unified_cubes.zip` = the one download.
- A4 pivots: held_t/i/b/c ∈ {1, 0, −1}; −1 = no UCP match — its OWN bucket,
  never folded into 0. UCP match rate 90.8% (floor 70%).
- Andre's binding operational rules: ONE Run All (no cell-by-cell); no YARN;
  CSVs must land on the pod; counts not rates; he slices cubes himself —
  every proposed story ships with its pivot recipe (rows/cols/filter).

## September build (03e) — accumulate here, ONE build when B re-pulls for t12

1. t12 endpoint (automatic via regime flag) → Q5 becomes the migration/delta
   view (tier/segment at anchor vs +12m; "value that walked away").
2. ADD CAMPAIGN DIMENSION to the B cube (per-MNE Piece B cuts — PCQ/PCL/PCD
   highlights; Andre ask 2026-08-03, not derivable today).
3. Remove provenance columns from CSVs → single _provenance.txt sidecar.
4. Logic fingerprint (SQL hash) in landing markers → auto-invalidation on
   any parameter change + auto-retirement of stale versions.
NOTE 2026-08-03: HTML exploration page WITHDRAWN by Andre — the notebook
plots are the deliverable. Q5 parked until t12.

## Narrative state (2026-08-03 end of day)

- Matched-tier check (CSV-only, b_before_after_cube) CONFIRMED mean
  reversion: leavers hold starting tier same/better than stayers (High
  77.6% vs 76.1%; Mid 54.9% vs 54.5%). **"Spend stalls after unsub" claim
  is DEAD - never resurrect without new evidence.**
- Final sound bite (survived marketing-director red team + kill test):
  "Unsubscribing doesn't mark declining customers - their spend, tiers,
  products and profitability hold. What we lose is the CHANNEL: we can no
  longer talk to some of our best card spenders."
- Decisions that stand: first-contact discipline on high-value lists;
  suppression-gap fix (standalone compliance ticket); pre-launch unsub
  guardrail for 1M+ blasts (FIFA = 11,476 unsubs @0.39%, 3x share spike).
- OPEN: revolver-de-revolve (27.1% vs 22.1%) is an UNMATCHED comparison -
  same critique class; match it or footnote it before any deck.
- 2-slide deck: S1 landscape (curve+FIFA+timing, first-contact, rates);
  S2 who leavers are + matched-tier honesty exhibit + access-loss framing.

## THE NEXT SESSION'S JOB: the Cards-pod PowerPoint (2 slides max)

Everything analytical is DONE and verified. Build the deck from what's saved:

1. **Deck rules:** `unsub_tracking/DECK_STANDARDS.md` is binding - copy the
   house shell `museum/cpc_evidence_deck.html` (its :root/.slide/.eyebrow/
   h1/.hrule/.body/.read/footer/.nav/.mark classes), values labeled on
   marks, date range on every chart, prints to PDF, no internal machinery
   in reader-facing text. Rule 5: every number must trace to a results_*.md
   transcription - WRITE `results_2026-08-04_spotlight_final.md` FIRST,
   transcribing the verified numbers below (+ the evidence-query output and
   exact repeat-unsub distribution once Andre runs them).
2. **Structure (agreed):** S1 "We're causing it" - three mechanisms:
   first-contact (48% of cards unsubscribers had received only 1-2 cards
   emails vs 27% of stayers), FIFA (11,476 unsubs @0.39% of 2.9M; share of
   monthly unsub events peaked ~17.6-18% vs ~5-9% baseline; unsubs
   followed send waves 0-1 month lag), suppression gap (146,706 campaign-
   distinct unsub clients vs 109,431 unique people = 37,275 counted in 2+
   campaigns; evidence trails: spotlight/evidence_repeat_unsub.sql -
   PENDING Andre running it for the receipt rows + exact distribution).
   S2 "What it costs" - who goes deaf: unsubscribers skew high-spend tier
   (41% High among tiered leavers vs ~33% stayers), tenured (cards peak
   4-7yr), Cards LOB = 20.5% of the bank's unique unsubscribers
   (a1_lob_dedup); the matched-tier null as the honesty exhibit (leavers
   hold tier same/better: High 77.6% vs 76.1%, Mid 54.9% vs 54.5% -
   "the gap is WHO unsubscribes, not what happens after"); three
   decisions: first-contact discipline, suppression fix (compliance
   ticket), 1M+ blast unsub-cost guardrail.
3. **Sound bite (survived marketing-director red team + kill test):**
   "Unsubscribes aren't customers leaving - they're customers going deaf
   to us. Spending doesn't change; we lose the CHANNEL to some of our
   best card spenders - and we cause most of it ourselves (first touch,
   mega-blasts, suppression that doesn't stick)."
4. **Dead claims - never resurrect:** "spend stalls after unsub" (killed
   by matched-tier check); de-revolve differential (dissolved when
   tier-matched); PCQ +$245 is acquisition lifecycle, not recovery.
5. Notebook state: env LLM applied the final story; D7 matched-tier
   exhibit exists; final_story_prompt.py consolidation may still be
   mid-application - verify before pulling numbers from the notebook.
6. All prompts/inputs pushed and current: cube_analyst_prompt,
   plot_revision_prompt, delta_update_prompt, final_story_prompt,
   matched_check_prompt (CSV version), evidence_repeat_unsub.sql.
   8 CSVs on the share; pipeline build 03e; September items in the 03e
   backlog section above.
