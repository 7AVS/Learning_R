# Channel Code Dictionary — Universal Reference

These codes appear across Cards-pod campaigns (PCD, PCL, CRV, VBA/VBU, CTU/O2P) in
`channels_deployed` strings, per-channel flag column suffixes (`channel_deploy_im`,
`chnl_mb`, etc.), and in `TACTIC_DECISN_VRB_INFO` substrings.

Source: Andre's reference sheet, transcribed 2026-04-28, captured in
`schemas/nbo_vba_rbol_combined.md`. Moved here as standalone universal reference 2026-06-18.

---

## Top-level buckets + specific channel codes

**Code** = top-level bucket. **Channel Code** = specific code that appears on deployments/responses.
**P/R** = Proactive (we initiate) vs Reactive (client encounters channel).

| Code | Channel Name | Channel Code | Description | P/R |
|---|---|---|---|---|
| BR | Branch | AM | Branch | R |
| BR | Branch | AM | PLL-Proactive Leads Calling | P |
| BR | Branch | HA | Home Advisor | P |
| BR | Branch | CA | SYNERGY | R |
| BR | Branch | CA | Commercial Advisor | P |
| BR | Branch | AL | #N/A | R |
| BR | Branch | BO | Offer for Business | R |
| BR | Branch | IR | Investment Retirement Planner | P |
| BR | Branch | MM | Mortgage Sales Manager | P |
| BR | Branch | TB | #N/A | #N/A |
| AC | Advice Centre | RD | Contact centre | P |
| AC | Advice Centre | IC | Insurance Calling | P |
| AC | Advice Centre | EC | External Calling | P |
| MB | Mobile | MB | Mobile | R |
| OB | Online Banner | OLB | Online Banking | R |
| OB | Online Banner | IN | Online Banking eOffer | R |
| OB | Online Banner | IM | Internet Message | R |
| OB | Online Banner | IU | Intercept Campaign | P |
| O&O | Offers & Opportunities | DO | Display Offer | R |
| O&O | Offers & Opportunities | OP | Opportunity (Personal) | R |
| EM | Email | EM | E-Mail | P |
| DM | Direct Mail | DM | Direct Mail | P |
| OTH | Other | CM | Cardholder Messaging | R |
| OTH | Other | SM | Statement Message | P |
| OTH | Other | OB | Opportunity (Business) | R |
| OTH | Other | NA | #N/A | #N/A |
| OTH | Other | AD | ATM Display | R |
| OTH | Other | AT | ATM Message | R |
| OTH | Other | FX | Fax | P |
| OTH | Other | IP | Invest By Phone | P |
| OTH | Other | IS | RBC Insurance | P |
| OTH | Other | KS | SST-Kiosk | R |
| OTH | Other | SI | Statement Insert | P |

---

## Critical disambiguation

**IM vs MB — NOT the same channel:**
- `IM` (Internet Message) = **online banner** placement. Top-level bucket: OB (Online Banner).
- `MB` (Mobile) = **mobile in-app banner**. Top-level bucket: MB.

> **CONFLICT FLAG:** The top-level bucket code for online banner group is `OB` in this dictionary,
> but the specific channel code deployed is `IM`. Memory note (2026-05-15, confirmed by Andre)
> says IM = online banner and MB = mobile banner — this is consistent with the dictionary.
> The potential confusion: `OB` as a top-level group is distinct from `OB` listed under OTH
> (which maps to "Opportunity (Business)" — a different code reuse collision in the dictionary itself).
> When writing about channels: say "IM online banner" or "MB mobile banner" using the channel code,
> not the top-level bucket, to avoid ambiguity.

---

## Known gaps (not in dictionary)

These codes appear in curated table columns but have no entry in the dictionary above:

| Code | Appears in | Status |
|---|---|---|
| `IV` | `chnl_iv` (nbo_vba_rbol_combined, nbo_pba_upgrade) | Unknown — ask Andre |
| `OM` | `chnl_om` (nbo_vba_rbol_combined) | Unknown — ask Andre |
| `ZZ` | `chnl_zz` (nbo_vba_rbol_combined) | Unknown — ask Andre |
| `LVR` | `channel_deploy_lvr` (cards_bizups_vbu_descresp_clnt) | Possibly Live Voice Response — unconfirmed |
| `AV`, `XX` | CRV/PCL deployments seen 2026-05-15 | Unconfirmed — do not name without confirmation |

---

## Non-unique codes (disambiguation required)

- `AM` maps to both *Branch* (R) and *PLL-Proactive Leads Calling* (P) — disambiguate via P/R flag or context.
- `CA` maps to both *SYNERGY* (R) and *Commercial Advisor* (P) — same disambiguation required.
- `OB` as a channel code means *Opportunity (Business)* (under OTH bucket), but `OB` as a top-level bucket means Online Banner — read context carefully.
