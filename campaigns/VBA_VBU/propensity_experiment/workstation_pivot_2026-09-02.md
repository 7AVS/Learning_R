# Workstation pivot — VBU response by wave × offer × decile × arm (2026-09-02)

Source: Andre's workstation Excel pivot (screenshot in `C:\Users\andre\.claude\uploads\4d8f7ddf-2ec7-40aa-bb74-e157100e7786\d2b07adb-image.jpg`).
Filter: `group_cd = (All)`. Values: Sum of clnts / Sum of resp_target / Sum of RR.
Row grain: Model Based → offer → decile (1=best, 1–9 shown); Rule Based = rollup only.
Waves: Jun / Jul / Aug (Aug immature at read). Arms: COMM / NOT_COMM.

Cross-check vs repo b4 v2 (pooled 3 waves): NR d1 resp 89+42+5=136 = b4's 136 exact;
R_55 d1 resp 141+100+71=312 vs b4 311; volumes within ~1% (his NR d1 pooled 4,990 vs b4 4,936).
Independent build, same read → b4 certified against the workstation dataset.

## Model Based — AIB_25K_NR

| Decile | Jun COMM clnts | Jun NC clnts | Jul COMM | Jul NC | Aug COMM | Aug NC | Jun COMM resp | Jul COMM resp | Aug COMM resp | Jun RR | Jul RR | Aug RR |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2,588 | 234 | 1,624 | 102 | 778 | 56 | 89 | 42 | 5 | 3.44% | 2.59% | 0.64% |
| 2 | 2,533 | 180 | 2,068 | 104 | 1,139 | 67 | 62 | 44 | 11 | 2.45% | 2.13% | 0.97% |
| 3 | 2,273 | 123 | 2,078 | 96 | 1,538 | 72 | 43 | 33 | 5 | 1.89% | 1.59% | 0.33% |
| 4 | 2,120 | 89 | 2,053 | 74 | 1,921 | 75 | 26 | 28 | 8 | 1.23% | 1.36% | 0.42% |
| 5 | 635 | 30 | 48 | 1 | 225 | 15 | 4 | 1 | 0 | 0.63% | 2.08% | 0.00% |
| 6 | 966 | 39 | 71 | 2 | 342 | 19 | 10 | 1 | 0 | 1.04% | 1.41% | 0.00% |
| 7 | 1,055 | 43 | 116 | 2 | 455 | 29 | 5 | 3 | 1 | 0.47% | 2.59% | 0.22% |
| 8 | 1,224 | 60 | 418 | 13 | 571 | 45 | 1 | 1 | 1 | 0.08% | 0.24% | 0.18% |
| 9 | 14 | — | 64 | — | 95 | 2 | 0 | 0 | 0 | 0.00% | 0.00% | 0.00% |

NOT_COMM resp_target = 0 in every NR cell (RR 0.00%; #DIV/0! where NC clnts blank).

## Model Based — AIB_25K_R_55

| Decile | Jun COMM clnts | Jun NC clnts | Jul COMM | Jul NC | Aug COMM | Aug NC | Jun COMM resp | Jul COMM resp | Aug COMM resp | Jun RR | Jul RR | Aug RR |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,788 | 111 | 1,660 | 72 | 1,351 | 53 | 141 | 100 | 71 | 7.89% | 6.02% | 5.26% |
| 2 | 1,631 | 97 | 1,535 | 59 | 1,215 | 64 | 49 | 40 | 18 | 3.00% | 2.61% | 1.48% |
| 3 | 1,855 | 93 | 1,736 | 90 | 1,571 | 92 | 38 | 26 | 18 | 2.05% | 1.50% | 1.15% |
| 4 | 1,981 | 103 | 1,959 | 81 | 2,055 | 104 | 30 | 17 | 6 | 1.51% | 0.87% | 0.29% |
| 5 | 775 | 43 | 54 | 2 | 319 | 12 | 8 | 0 | 2 | 1.03% | 0.00% | 0.63% |
| 6 | 1,218 | 55 | 97 | 2 | 471 | 31 | 11 | 0 | 2 | 0.90% | 0.00% | 0.42% |
| 7 | 1,403 | 60 | 148 | 6 | 581 | 34 | 11 | 1 | 1 | 0.78% | 0.68% | 0.17% |
| 8 | 1,480 | 57 | 482 | 22 | 656 | 40 | 5 | 0 | 0 | 0.34% | 0.00% | 0.00% |
| 9 | 31 | — | 81 | 1 | 86 | — | 0 | 2 | 1 | 0.00% | 2.47% | 1.16% |

NOT_COMM resp_target = 0 in every R_55 cell.

## Rule Based (rollup only — no decile split; includes MCB ~50/50 holdout)

| Wave | COMM clnts | NOT_COMM clnts | COMM resp | NC resp | COMM RR | NC RR |
|---|---|---|---|---|---|---|
| Jun | 14,500 | 10,605 | 99 | 6 | 0.68% | 0.06% |
| Jul | 12,578 | 10,278 | 58 | 2 | 0.46% | 0.02% |
| Aug | 12,826 | 10,251 | 26 | 1 | 0.20% | 0.01% |

Rule Based NOT_COMM shows small but NONZERO conversions (6/2/1) — unlike Model Based
NOT_COMM which is 0 everywhere. Stray cells top of sheet: 4990 (= NR d1 pooled clnts),
1663.3333 (= /3, avg per wave).

## Notes
- The "3.4%" Andre quoted = Jun NR decile-1 COMM RR (3.44%).
- Deciles shown 1–9 (repo b4 used 1–10; his d9 likely pools our d9+d10 or scale differs — minor).
- Design direction stated 2026-09-02: 50/50 COMM/holdout within the Model Based population,
  read at score-band level. Client-level random 50/50 + stored scores (t1: @21,8 verified)
  makes band-level reads valid without band-stratified randomization.
