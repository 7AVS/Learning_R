# Archive Log — 2026-08-07

Reorg of `unsub_tracking/` clutter. ARCHIVE only, nothing deleted. `museum/` and
`archaeology/` left untouched — both are cited provenance/evidence trails
(`museum/README.md` Provenance section cites `../archaeology/` packs 01-22;
`UNSUB_TRACKING_KNOWLEDGE.md` header describes both folders as the intended
layout, not clutter). Working tree NOT committed — staged renames only, for
Andre's review.

## superseded_pipelines/ — replaced by `spotlight/unsub_unified.py` (2026-08-02/03 pivot)

| File | From | Reason |
|---|---|---|
| `unsub_before_after_jul25/` (whole folder: `RUN_THIS_dfp_only.py`, `SESSION_STATE.md`, `unsub_before_after_exploration.html`, `unsub_before_after_jul25.py`) | `unsub_tracking/unsub_before_after_jul25/` | Explicitly "RETIRED for Piece B" in `spotlight/AUDIT_2026-08-02.md` — no mnemonic filter, wrong question, wrong tool. |
| `spotlight.py` | `unsub_tracking/spotlight/` | NEXT_SESSION.md: replaced by `unsub_unified.py`, "kept as history." |
| `spotlight2.py` | `unsub_tracking/spotlight/` | AUDIT_2026-08-02.md VERDICT: REBUILD — "condemned by audit: floating anchor, contaminated leaver flag." |
| `spotlight_pipeline.html` | `unsub_tracking/spotlight/` | ETL lineage diagram for `spotlight.py`; `PIPELINE_MAP.md` says it "supersedes the old spotlight.py map." Current diagram is `unsub_unified_pipeline.html` (kept). |
| `download_cubes.py` | `unsub_tracking/spotlight/` | Header: "Run it after spotlight.py finishes" — reads the retired pipeline's HDFS output path. |
| `download_parquet.py` | `unsub_tracking/spotlight/` | Header: pulls parquet landed by `spotlight.py` Cells [1]/[4] — retired path. |

## superseded_prompts/ — earlier LLM-handoff prompts, not in NEXT_SESSION's "current" list

| File | From | Reason |
|---|---|---|
| `exploration_page_prompt.py` | `unsub_tracking/spotlight/` | Builds the standalone HTML exploration page — NEXT_SESSION.md: "WITHDRAWN by Andre — the notebook plots are the deliverable." `q345_focus_prompt.py` (kept in place, recent) independently confirms: "Do NOT build an HTML page — that request is withdrawn." |
| `analysis_prompt.py` | `unsub_tracking/spotlight/` | Describes the pre-unified `spotlight_parquet/` duckdb workflow, superseded when the pipeline was rebuilt into `unsub_unified.py`; not in NEXT_SESSION's current-prompts list (`cube_analyst_prompt`, `plot_revision_prompt`, `delta_update_prompt`, `final_story_prompt`, `matched_check_prompt`). |
| `q345_focus_prompt.py` | `unsub_tracking/spotlight/` | Self-describes as superseding prior revision prompts for its pass; itself absent from NEXT_SESSION's current-prompts list — an intermediate revision step, not the current one. |

## superseded_briefs/

| File | From | Reason |
|---|---|---|
| `WORKSTREAM_2_BRIEF.md` | `unsub_tracking/` | NEXT_SESSION.md: `UNIFIED_BRIEF.md` "SUPERSEDES the raw wording of both `POWER_PACK_BRIEF.md`... and `WORKSTREAM_2_BRIEF.md`." (`POWER_PACK_BRIEF.md` stays — it's on the explicit keep list.) |

## build_artifacts/ — orphaned bytecode

| File | From | Reason |
|---|---|---|
| `01_vendor_feedback_eda.cpython-311.pyc`, `02_campaign_unsub_tracker.cpython-311.pyc`, `15_unsub_value_enrichment.cpython-311.pyc` | `unsub_tracking/__pycache__/` | Compiled bytecode for `archaeology/` scripts, accidentally tracked in git (not covered by the repo's `__pycache__/` gitignore rule at this path). No source content of its own; source `.py` files are untouched in `archaeology/`. |

---

## Not moved, worth a second look (see report for full "unsure" list)

`spotlight/diag_a1_dupes.py`, `spotlight/env_probe.py`, and the `spotlight/preflight*.sql` /
`RUN_2026-07-31_*.sql` / `unsub_scope_test.sql` chain — all feed facts still cited in
canon (e.g., per-list-unsub settled by `unsub_scope_test.sql`) or fall inside the
7-day recency guard. Left in place; flagged for Andre to close out once confirmed
truly done.
