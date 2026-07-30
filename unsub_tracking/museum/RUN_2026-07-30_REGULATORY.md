# REGULATORY MNEMONICS — governed list, 2026-07-30

Source: pivot over the send data filtered on **`ACTION_TYPE = 'Regulatory'`**. Transcribed from a
screenshot, 2026-07-30. **Supersedes** the four-from-memory list of the same morning
(FXR/OTC/VMF/VOA) — that was a strict subset, so it understated regulatory volume and would have
overstated the suppression leak.

## The 22

| mne | description | LOB |
|---|---|---|
| AFD | Auto Finance new loan confirmation | PFP |
| BPU | Business Client Profile Update | BFS |
| BUK | Unclaimed certified cheques notification | BFS |
| CFR | Client Focused Reform | PSI |
| EOE | Interac E-Transfer | BFS |
| FNE | Transfer out investment notification | PSI |
| **FSA** | **Advantage Account Fee Charge Notification** | **EDB** |
| FSO | Financial Stress Outreach | PFP |
| FXR | FX Private Banking Remediation | CPS |
| GAF | RBC Group Advantage fund merger update email | PSI |
| HFC | RCL Healthcare Financial Check-up | PFP |
| HPN | HomeProtector Notification | INS |
| IOO | Investment Offer Qualification Reminder | PSI |
| NST | Annual NSF rebate notification | EDB |
| **OTC** | **Regulatory notification on changes to program's T&Cs** | **CPS** |
| PUK | Unclaimed certified cheques notification | PFP |
| ROP | HomeProtector ROP Email | INS |
| TWI | TFSA Digital Open Welcome Interest | PSI |
| VMF | Credit card clients who earned on merchant offer statements | CPS |
| VOA | Avion Rewards Program, formerly RBC Rewards — Personal | CPS |
| ZDC | Regulatory Documents Compliance | PSI |
| ZHX | Mutual Fund Closure Outcome Review | PSI |

---

## Cross-check against every table already on the exploration page

| table | regulatory mnemonics present |
|---|---|
| L12c — campaigns clients left *through*, top 25 | **none** |
| L6 — top 25 campaigns by senders | **none** |
| L13 — campaign-pair table | **FSA** |

### 1. The leak is very likely still a marketing leak

No regulatory mnemonic appears in L12c's top 25, which covers the bulk of the 427,079. That is
consistent with the already-out population having genuinely been mailed marketing after opting out —
but it is **not proof**, because L12c names the campaign each client *left through*, never the
campaigns that mailed them *afterwards*. Cell `[20i]` builds that second set and has not run. Until
it does, §6 stays "the size of a question".

### 2. FSA in the L13 pair table is a defect

`FSA + FWC` sits in the bank-wide co-occurrence top 25 at **lift 1.49** (5,154 clients, 74 leavers).
FSA is regulatory — an Advantage Account fee-charge notification. Clients cannot opt out of it, so:

- FSA's **solo rate is not a marketing unsubscribe rate** and is not comparable to one.
- Any **pair** containing FSA compares a marketing rate against a regulatory one.

The row is not wrong arithmetic; it is a category error. `[20g]` should exclude regulatory mnemonics
from the pair universe, or label them, before that table is shown. **Cards pairs are unaffected** —
no regulatory mnemonic appears in L13b.

---

## The number that is worth more than the list

**`ACTION_TYPE` exists as a field.** A hardcoded set of 22 goes stale the moment a campaign is added
or reclassified, and it will go stale silently. If `ACTION_TYPE` can be reached from
`VENDOR_FEEDBACK_MASTER` or the tactic tables, every cell here should join to it instead.

**OPEN QUESTION FOR ANDRE — which table holds `ACTION_TYPE`, and can it be joined on TACTIC_ID/MNE?**
That single answer replaces `REGULATORY_MNES` permanently.

---

## Volumes: transcribed but NOT usable

The screenshot's "Sum of sent" column is broken by a spreadsheet formatting fault — most rows render
as percentages of the underlying value (`AFD 7086900.00%`, i.e. 70,869) while four render as plain
numbers (`FXR 8,458`, `OTC 9,413,066`, `VMF 29,651`, `VOA 516,965`). Grand total shows
`1129797800.00%`.

**Do not quote any send volume from this screenshot.** The unsub-rate column is unaffected in
appearance — OTC 0.92%, ZDC 0.19%, FXR 0.14%, NST 0.14%, VMF 0.14%, AFD 0.13%, TWI 0.13%, CFR 0.12%,
IOO 0.12%, HFC 0.09%, ZHX 0.08%, VOA 0.03%, BPU 0.01%, FSO 0.01%, grand total 0.78% — but its
denominator comes from the same broken column, so treat those as unverified too until re-exported.

**OTC at 0.92% is the interesting one if it survives re-export:** a regulatory notification with an
unsubscribe rate above the bank-wide marketing figure of 0.68%. Clients cannot opt out of regulatory
mail, so whatever that 0.92% is counting, it is not consent withdrawal working as designed.
