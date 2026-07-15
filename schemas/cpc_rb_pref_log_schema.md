# DDWV01.CPC_RB_PREF_LOG — Client Preference Log (AUTHORITATIVE)

Source of truth for client contact consent — all campaigns must abide by it.
Transcribed from S0 run + EDW data-dictionary pages, 2026-07-15.
Source pics: `pics/PXL_20260715_1823*/1824*.jpg`. Companion monthly snapshot table: `CPC_RB_PREF_MTHLY`.

## Size / grain

- 91,415,764 rows (2026-07-15). Loaded DAILY.
- **Grain = change log:** one row per client × preference × change event. NOT client-level — a client carries many PREF_IDs, each with its own history.
- Current state = latest row per `(CLNT_NO, PREF_ID)` by `CHG_TMSTMP`; a pair with no rows at all = blank default (see consent semantics).
- Layer model: client → preference (PREF_ID) → history (CHG_TMSTMP). Contactability = reconstruction across several preferences, not one lookup.

## Columns (confirmed via S0 screenshot + dictionary)

| Column | Notes |
|---|---|
| CLNT_NO | client |
| PREF_ID | preference identifier (integer; catalog below) |
| CLNT_CONSENT_TYP | integer; 5001=Yes, 5002=No, 5003=blank/never answered, 5004=Yes CB-pull w/o SIN (1016 only, added 2018) |
| SLCT_RESTR_TYP | solicitation restriction type, smallint 1-9 (1=General, 2=Loans/Mtg, 3=Credit Card, 4=Dep-Txn, 5=Dep-Inv, 6=Reg Plans, 7=Direct Mail, 8=Telemarketing, 9=Online Banking). ⚠ dictionary page ambiguous SLCT_ vs SLOT_ spelling; S0a column alignment also suspect — re-verify before using |
| CHG_TMSTMP | change timestamp (timestamp) |
| APP_SYS_CD | writing system (useful: identifies email-vendor-driven changes) |
| EMP_ID | mostly masked in samples |
| CLNT_TYP_CD | 1 = personal (presumed) |
| SYS_FUNC_CD | 6xxx codes, undecoded |

⚠ S0a screenshot showed one more value than headers — re-screenshot a wide S0a before relying on SLCT_RESTR_TYP / trailing columns.

## Consent semantics (CRITICAL for denominators)

**Blank (5003 or no row) = YES for all preferences EXCEPT:**
- **1014 Share for Marketing across RBC — blank = NO**
- **1015 Share for Service across RBC — blank = NO**

So the reachable base for share-for-marketing purposes = explicit 5001 only. Any "population lost" metric must state which PREF_ID it counts and honor the blank rule for that PREF_ID.

## PREF_ID catalog (dictionary, 2007-vintage page + later additions)

**1) Entity consents (opt out = Do Not Solicit for that entity):**
1001 RBC Direct Investing · 1002 RBC Royal Bank · 1016 Banking Credit Bureau (5004 special yes)

**Information usage:** 1014 Banking Share for Marketing · 1015 Share for Service across RBC · 1036 Share for Online Personalization (auto-Yes at OLB enrol; un-enrol → No; OLB table monthly vs CPC daily = sync drift) · 1057 DI Share for Marketing

**2) Communication channels:**
Banking: 1007 Direct Mail · 1008 Telephone · 1009 RBC Online · **1012 ⚠ CONFLICT** · 1013 Face-to-Face · 1048 ATM (2011+)
Direct Investing: 1037 DM · 1038 Telephone · 1039 Online · **1040 E-Mail** · 1041 F2F
**⚠ 1012 conflict (unresolved):** 2007-vintage dictionary page (`pics/PXL_20260715_182439530.jpg`) says 1012 = Banking **Mobile** and no Banking-E-Mail code exists; a newer reference catalog (`pics/PXL_20260715_223246054*.jpg`, source TBD) says 1012 = Banking **E-Mail** and lists no Mobile. Likely a post-2007 relabel; confirm via current dictionary page for 1012 or team before naming the code on any slide. Either way, the empirical finding stands: email unsubs do NOT write CPC (D1 showed only 65 clients setting 1012=No within 7d of unsub, out of 649,885).

**3) Product preferences:** 1006 Credit Cards · 1024 Investments Non-Reg · 1025 Loans & LOC · 1026 Mortgages · personal-only: 1004 Accounts & Packages, 1010 Creditor Ins, 1023 Investments Reg, 1044 Travel Health · business-only: 1027, 1028 (BLIP), 1030, 1031, 1034

**4) Service preferences:** 1021 Maturity Call Non-Reg · 1042 Banking Surveys · 1043 DI Surveys · 1045 E-Newsletter Banking · 1046 E-Newsletter Rewards · 1047 E-Newsletter DI · personal: 1020, 1022 · business: 1032, 1033

## Gating model (HYPOTHESIS — confirm with team/suppression rules)

Entity consent (1002) gates all Banking marketing → 1014 gates cross-entity lists → channel consent gates the medium → product preference gates the offer type. Team statement (2026-07-15): "CPC = N for 1014 → out of all marketing for RBC entity (super harsh)" — dictionary describes 1014 as cross-entity SHARING consent; scope discrepancy unresolved, verify which code means "out of all marketing" (1002 vs 1014).

## Known uses

- Unsub validation: business partners (Avion) distrust vendor-feedback unsub counts (double counting — journey-log grain). CPC flag change after a code-4 event = deduplicated, undeniable. Linkage queries: `unsub_tracking/06_cpc_pref_log_eda.sql` S4/S5.
- Population-lost-to-campaign metric anchors here, sourced by our unsub attribution chain.
