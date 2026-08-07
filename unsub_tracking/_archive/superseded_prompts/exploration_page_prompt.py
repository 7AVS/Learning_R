# exploration_page_prompt.py - second handoff for the work-env LLM: build the peer-facing
# EXPLORATION HTML from the six cubes. Load cube_analyst_prompt.py's PROMPT FIRST (schemas,
# traps, denominators), then this one. print(PROMPT) to read.

PROMPT = """
# BUILD THE EXPLORATION PAGE - unsub unified findings, peer review draft

You already know the six cubes (cube analyst brief). Now: ONE self-contained
HTML file that tells the story the data supports. This is an EXPLORATION page
for peers - not a deck, no recommendation, no sign-off ask, no exec framing.
It answers the brief by showing findings and open questions.

## PHASE 1 - EXPLORE FIRST. No HTML until this is done.

Run queries for each brief question and RECORD exact numbers:
1. Cards vs enterprise: 11,252 of 109,431 unique unsubbers (10.3%) - then
   per-MNE: top 10 by unsubs_attributed AND by rate per 1,000 senders (a2).
   Note which campaigns lead on volume vs on rate - they will differ.
2. Frequency gradient (a3): unsub rate (leavers/clients_total) by
   n_emails_all_bucket, and cards view by n_emails_cards_bucket. Is the
   gradient monotonic? Where does it steepen?
3. Profile (a4): leaver rate by age_band, by tenure_band, by prod_cat_cnt,
   and the TIBC mix of leavers vs stayers (held_t/i/b/c combos). Keep the
   no_ucp_match bucket visible in every cut.
4. The curve (c): monthly unsubs enterprise + cards-only. Seasonality?
   Spikes? Do cards spikes align with known send months?
5. Piece B t0 only (b, t_offset=0): the 3x4 template - spend_tier x
   usg_bhvr_seg x stayers/leavers. What share of leavers are High-spend?
   Revolvers? That is "value at risk". t12 IS EMBARGOED until September.

THEN, for each candidate headline you draft: try to KILL it before it goes
on the page. The kill tests:
- Does the complicating cut reverse it? (e.g., volume-leader vs rate-leader;
  a gradient that flattens when cards-only)
- Is it a composition artifact? (e.g., depth story = which campaigns mail
  whom, not an independent effect - check within-campaign if possible)
- Does the n survive scrutiny? (tiny buckets make loud rates - flag any
  claim resting on <1,000 clients)
- Is it just the no_ucp_match bucket leaking? (profile claims)
A headline that survives goes on the page WITH the number that tried to
kill it shown nearby. One that dies becomes an open question, not a claim.

## PHASE 2 - BUILD. One HTML file, self-contained, works from file://.

STRUCTURE (top to bottom, exactly):
- kicker line: "Cards pod - exploration draft - not for circulation"
- h1: the question, phrased AS A QUESTION (e.g., "Who unsubscribes, and is
  it us?")
- sub: one sentence - what data, what window, why it's new
- 3-5 stat tiles: the numbers the reader must hold (e.g., 109,431 / 11,252 /
  1.05% / the steepest-gradient number)
- one .note: the ONE caveat governing everything (window Jan-Apr 2026;
  attribution by TREATMENT_ID; UCP match 90.8%)
- then AT MOST 5 findings sections, separated by <hr>, in this order:
  S1 strongest finding - chart, callout, one open question
  S2 the finding that COMPLICATES S1
  S3 operational cut (per campaign / per segment)
  S4 "is it them or is it us" (frequency/contact-load evidence)
  S5 the thing nobody asked - callout only, no chart
- close: "What I'd test next" - exactly 3 items, each with a named cost
  (e.g., "needs a new client x campaign pull, ~1h wire")
- final .note source line: "Source: unsub_unified.py, run 2026-08-03,
  Jan-Apr 2026 window, 10,439,796 mailed clients. Counts deduped at client
  level where stated. t12 pending September rerun."

VOICE (non-negotiable):
1. Every heading is a CLAIM with a number, never a topic. "Cards drive 10.3%
   of enterprise unsubs" not "Cards analysis".
2. Lead each section with the number; bold the claim sentence; no build-up.
3. If the data complicates your headline, SAY SO in the same section.
4. Every rate carries its n.
5. Exactly ONE open question per section, marked visually (.q style).
6. No hedging adverbs (notably, interestingly, importantly). No marketing.
7. Findings, not recommendations. No CALL, no "we should".
8. Piece B language: "value at risk" / "what walked away" - composition
   words. NEVER "unsubscribing cost us X" - no causal claims.
9. t12/trajectory: one sentence saying it lands in September - nothing more.

DESIGN (self-contained, no libraries, no network):
- Pure div+CSS charts: horizontal bars for comparisons (position, shared
  baseline, zero-based), a simple monthly bar strip for the curve. NO pie,
  NO dual axes, no chart libraries, no external fonts.
- System sans 15px; max-width 1040px centered; headings 26/19px;
  font-variant-numeric: tabular-nums on ALL numbers.
- Light AND dark: use prefers-color-scheme media query; pick one accent
  color for "leavers/lost" (orange family) and one for "base/mailed" (blue
  family); neutral grays elsewhere.
- Data arrays HARDCODED inline in the page's JS/HTML, pasted from YOUR OWN
  Phase-1 query results - never retyped approximations, never re-derived.
  Tables and charts read the same array so they cannot disagree.
- Each section: chart first, then a short callout div, then the .q line,
  then a collapsed <details> with the underlying table.

## HARD RULES carried over (violations = rebuild)
- Denominators: unique-client totals ONLY from a1's two total rows. Rates
  you compute get their denominator named on the page.
- attributed vs exposed: label which one every campaign number uses.
- held_* = -1 / no_ucp_match: own bucket, visible, never folded into 0.
- Cards is the subject; enterprise is the comparator bar next to it.
- If a brief question cannot be answered from the cubes, it appears in
  "What I'd test next" with its cost - never silently missing.

Deliver: the complete HTML in one file, plus (separately, not in the page)
the list of headlines you killed in Phase 1 and what killed them.
"""

if __name__ == "__main__":
    print(PROMPT)
