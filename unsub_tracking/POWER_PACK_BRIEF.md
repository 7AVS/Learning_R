# Power Pack Q3 — stakeholder brief for the unsub spotlights

Source: email from Maya to Andre, cc Daniel. Follow-up flagged "start by / due by Mon 20 Jul 2026".
Transcribed from photo 2026-07-29. Names first-name-only per repo convention.

---

## The ask, verbatim

**1. PCL sales modal** — continue reporting as is, also include interim results
- Funnel — distinct client views, clicks, conversion (benchmark to PCL)
- No additional insights required for the power pack

**2. Async** — continue reporting as is

**3. Spotlight 1** — "We have about **2.5 weeks** now for spotlights, so managing time will be critical."
- a. Unsubs by cards campaigns, absolute number and rates. Frequency per campaign to understand if
  weekly, bi-weekly, monthly.
- b. Unsubs by number of campaigns client contacted by and frequency
- c. Once we've determined which campaigns drive the highest unsubs. Are certain campaigns causing
  unsubscribes alone, or do unsubscribes spike when specific cam[**CUT OFF at frame edge —
  sentence continues beyond the photo. Almost certainly "when specific campaigns overlap/combine".
  CONFIRM WITH MAYA before building 3c.**]

**4. Spotlight 2** — Unsubs by Product depth, bucket by number of products 1 through 4
- a. Amongst the unsub culprits, what are their product standing? Meaning what is the depth of their
  relationship with RBC (TIBC)

---

## Timeline, verbatim

| Phase | Activity | Duration | Dates |
|---|---|---|---|
| Phase 1: Spotlight Analysis | 1-2 spotlights | ~~4 weeks~~ **2.5 weeks** | July 7 – August 2 |
| Phase 2: Data & Deck Work | Vintage work begins | 1 week | August 3 – August 9 |
| | Data pull | 1 week | August 10 – August 16 |
| | Strategy team meeting (commentary) | 1 day | 17-Aug |
| | Deck creation | 1 week | August 18 – August 24 |
| Phase 3: Reviews | Kabir review meeting | 1 day | 21-Aug |
| | Krishna review meeting | 1 day | 28-Aug |
| Phase 4: Final | Final meeting | 1 day | 2-Sep-26 |

Table may be cut off at the bottom of the frame — rows below "Phase 4" not visible.

Note the internal inconsistency in the source: deck creation runs Aug 18–24 but the Kabir review is
dated 21-Aug, i.e. mid-deck-creation. Not our error — flag to Maya if it matters.

---

## Coverage against `museum/unsub_value_museum.py` (as of run 2026-07-29 08:29)

| Ask | Status | Where |
|---|---|---|
| 1a — unsubs by cards campaign, **absolute + rate** | **HAVE** ⚠ blocked by the L6/L9 defect | L6 |
| 1a — **send frequency** per campaign (weekly/bi-weekly/monthly) | **NOT BUILT** | — |
| 3b — unsubs by **number of campaigns contacted by** | **BUILT, NOT RUN** | L10 (`contact_band`, `mean_distinct_campaigns`) |
| 3c — campaign **combinations** / do unsubs spike on overlap | **NOT BUILT** | pack 20 had an uncapped MNE-pair block |
| 4 — product depth, buckets 1–4 | **HAVE**, but bands are 0/1/2/3-4/5+ not 1/2/3/4 | L7 `prod_band` |
| 4a — **TIBC** depth of relationship | **IN THE LANDED CUBE, NEVER REPORTED** | `tibc_mix` — museum script L734, in `cube`/`cards_cube`/`client_spine`; absent from the L7 output |

### The numbers we already have for Spotlight 1a

L6, attribution basis (unsub event carries that campaign's TREATMENT_ID), Mar–May 2026:

| mne | senders | unsubs | unsub per 1,000 |
|---|---:|---:|---:|
| PCL | 1,004,155 | 1,177 | 1.17 |
| PCQ | 675,097 | 1,884 | **2.79** |
| PCD | 660,662 | 698 | 1.06 |
| AUH | 555,974 | 323 | **0.58** |
| CRV | not in top-25 by senders | — | — |

4.8× spread inside Cards. Bank-wide worst is VRE at 5.69/1,000 (81× QCF).

**Blocker:** L6 `unsubs` (attribution) and L9 `mne_leavers` (exposure) differ by up to 4.6× on the
same campaign — AUH 323 vs 1,491. Both legitimate, identically labelled. Must be renamed
`unsubs_attributed` / `leavers_exposed` and defined on the slide before either ships.

### The Spotlight 2 problem — flag early, do not wait until Aug 2

Bank-wide, product depth **does not separate leavers from stayers** (L7 ratios):

| prod_band | ratio |
|---|---:|
| 0 | 1.00 |
| 1 | **0.91** |
| 2 | 0.97 |
| 3-4 | 1.13 |
| 5+ | 0.96 |

Single-product clients are *under*-represented among leavers. As specified, Spotlight 2 lands on a
null result bank-wide.

It is **not** null inside Cards, and that is the reconciliation to build:
- Cards leavers are 30.5% single-product vs 17.4% for non-cards (L1)
- PCQ senders are 52.7% single-product, median tenure 5, median 1 product (L6)

So the depth story is real but it is a *composition* story — it reflects which campaign mails whom,
not an independent effect of holding fewer products. TIBC (`tibc_mix`) is the axis that may separate
them and it has never been reported.

---

## Open items this brief creates

1. Confirm the cut-off text of item 3c with Maya before building it.
2. `tibc_mix` cross-tab — derivable from the **landed cube**, no rerun needed.
3. Send-cadence per campaign (1a) — new pull, not in any existing pack.
4. Campaign-combination analysis (3c) — new work, largest unknown in the brief.
5. Phase 1 ends **2-Aug**. Four working days from 2026-07-29.
