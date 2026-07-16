# PCL Sales Modal — Results Catalogue

> Every result screenshot logged here: source pic, the query that produced it, the transcribed
> numbers, and the finding. Local reference (not pushed — .md off the allow-list).

---

## PXL_20260708_185908823.jpg — P9 summary pivot (sales-modal, VCL id i_308392)
- **Catalogued:** 2026-07-08
- **Source query:** `p9_vcl_full_measurement.sql` (challenger WMS + champion NMS, first deployment, May–Jun 2026)
- **What it is:** Excel PivotTable. Rows nest cohort_month → strategy (BAU/NTC) → arm (challenger/champion)
  → numeric bin 0/1/2/3/4/5+ (**exposure_bin** — inferred; no field header rendered in the shot).
  Columns: "Sum of clients" and "Sum of converted_clients", each split by engagement
  (not_exposed / dismissed / exposed_not_dismissed).

### Arm × strategy × cohort — totals (clients / converted / conv rate)
| Cell | Challenger | Champion | Lift |
|---|---|---|---|
| 2026-05 BAU | 17,853 / 125,297 = 14.2% | 5,788 / 53,292 = 10.9% | +3.4pp |
| 2026-05 NTC | 7,121 / 13,352 = 53.3% | 2,649 / 5,575 = 47.5% | +5.8pp |
| 2026-06 BAU | 15,576 / 84,694 = 18.4% | 5,133 / 36,492 = 14.1% | +4.3pp |
| 2026-06 NTC | 4,715 / 9,435 = 50.0% | 1,715 / 3,997 = 42.9% | +7.1pp |

### Challenger engagement-level totals (clients ; converted)
- **2026-05 BAU:** not_exposed 30,773 ; 2,787 · dismissed 63,033 ; 4,831 · exposed_not_dismissed 31,491 ; 10,235
- **2026-05 NTC:** not_exposed 3,173 ; 1,343 · dismissed 4,619 ; 1,562 · exposed_not_dismissed 5,560 ; 4,216
- **2026-06 BAU:** not_exposed 19,377 ; 2,512 · dismissed 41,390 ; 2,869 · exposed_not_dismissed 23,927 ; 10,195
- **2026-06 NTC:** not_exposed 2,037 ; 856 · dismissed 3,133 ; 667 · exposed_not_dismissed 4,265 ; 3,192
- Champion dismissed/exposed ≈ 0 in every cell (clean holdout; confirms id i_308392).

### Challenger, exposed_not_dismissed — per exposure_bin (clients ; converted ; rate)
| bin | May BAU | May NTC | Jun BAU | Jun NTC |
|---|---|---|---|---|
| 0 | 12 ; 2 | — | 7 ; 2 | — |
| 1 | 9,948 ; 5,671 ; 57% | 3,014 ; 2,721 ; 90% | 8,632 ; 6,532 ; 76% | 2,368 ; 2,184 ; 92% |
| 2 | 3,970 ; 1,684 ; 42% | 802 ; 639 ; 80% | 3,080 ; 1,545 ; 50% | 604 ; 474 ; 79% |
| 3 | 2,613 ; 826 ; 32% | 433 ; 292 ; 67% | 1,934 ; 676 ; 35% | 266 ; 163 ; 61% |
| 4 | 1,880 ; 453 ; 24% | 240 ; 146 ; 61% | 1,384 ; 348 ; 25% | 166 ; 88 ; 53% |
| 5+ | 13,068 ; 1,599 ; 12% | 1,071 ; 418 ; 39% | 8,890 ; 1,092 ; 12% | 861 ; 283 ; 33% |

### Findings
1. **Positive, significant lift in all 4 cells** (+3.4 to +7.1pp); ~9,350 incremental responses; z = 7.3–20.3.
2. **Dismissal is a first-view reflex:** ~63% of viewers dismiss (viewer-based denominator); 78% of one-view clients dismiss, falling to 39% at 5+.
3. **Among non-dismissers, conversion DECLINES with views** (57–92% at 1 view → 12–39% at 5+) — converters act on first sight; high-view non-dismissers are passive. Caveat: reverse causation possible (converting may stop re-serving). **First impression is decisive.**
4. Data-quality: a few `exposed_not_dismissed` clients in bin 0 (0 views but tagged exposed) — immaterial, classification edge.

---

## PXL_20260708_211247840.jpg — Slide 1 mockup (conversion)
- **Catalogued:** 2026-07-08
- **What it is:** Draft PPT slide. Title "PCL Sales Modal outperforming BAU — New-to-Campaign reaches 50%+".
  Two conversion curves (left = all clients, right = NTC), Challenger vs BAU/Champion, cohort May/June,
  end-labelled with abs deltas (+3.7/+4.6 left; +5.8/+7.1 right). Copy drafted in `slide1_conversion_narrative.md`.
- **Note:** left/right plot titles were duplicated placeholder text; an empty third chart object sat between them.

---

## PXL_20260708_231638341.jpg — Slide 2 mockup (engagement)
- **Catalogued:** 2026-07-08
- **What it is:** Draft PPT slide 2 (PowerPoint edit mode). Title placeholder "PCL New titles"; subtitle
  is slide-1 conversion text duplicated (wrong for this slide). Three plots, no plot titles.
- **Left — reach curve:** 4 lines by cohort × segment; end values ~78% (May-Total), 77% (June-Total),
  74–75% (NTC lines). Legend table: May-Total 138K/103K viewers · May-NTC 13K/10K · June-Total 94K/72K ·
  June-NTC 9K/7K. (103K/138K≈75%, consistent with the ~78% curve endpoint — no DQ issue; matches calc
  of ~104,703 exposed May challenger.)
- **Top-right — dismiss rate by # views** (All navy vs NTC teal): 1v 76%/44% · 2v 71%/50% · 3v 64%/46% ·
  4v 60%/48% · 5+ 38%/35%.
- **Bottom-right — conversion rate by # views** (NTC teal vs All black): 1v 51%/16% · 2v 40%/13% ·
  3v 35%/12% · 4v 30%/10% · 5+ 24%/8%. (Denominator = all viewers at that view-count, incl. dismissers.)
- **Findings:** both dismiss and conversion decline with view count for both segments. Series are
  All (Total) vs NTC — NOT "BAU" (BAU=control, not on this slide). Narrative: `slide1_conversion_narrative.md` (page 2).

---

## PXL_20260708_234143372.jpg — Slide 2 v2 (engagement, with title)
- **Catalogued:** 2026-07-08
- **Title:** "PCL Sales Modal — Both dismissal and conversion peak at the first view". Right plots now
  labelled **Non-NTC vs NTC** (left reach plot still "Total/NTC" — inconsistent).
- **ERRORS FOUND (fix before ship):** (1) May-Total viewers **130K** of 138K = 94%, contradicts the
  74–78% curve and other cohorts (~77%) — should be **~103K** (P9: 104,703 exposed May challenger); total
  viewers ≈175K not 203K. (2) Subtitle reach "~70%" should be ~76%. (3) Subtitle claims dismissal+conversion
  decline "for both alike" but **NTC dismissal is non-monotonic (44/50/46/48/35)** — softened in narrative.
  (4) Bottom-right conversion chart has no legend.
- Dismiss by views (Non-NTC / NTC): 1v 76/44 · 2v 71/50 · 3v 64/46 · 4v 60/48 · 5+ 38/35.
- Conversion by views (Non-NTC / NTC): 1v 16/51 · 2v 13/40 · 3v 12/35 · 4v 10/30 · 5+ 8/24.
