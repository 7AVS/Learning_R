/*
Email reply to colleague re: O2P / CTU control validity.
Not a query — text carried in a .sql so it can be pulled into the work environment.
Context: colleague flagged that the O2P control (champion/challenger: async+BAU vs BAU)
isn't responding like recent BAU waves, and CTU looks like it has no proper control.
Grounded in: o2p_success_validation.sql (TG4=TEST / TG7=CONTROL, rpt_grp allowlist,
no channel filter) and async_banner_summary_success.sql (CTU block: no T/C split, flag='ALL').

------------------------------------------------------------------
Subject: O2P / CTU control — you're right, here's what's going on

Hi [Name],

You're onto something, and the code backs it up.

CTU: there's no control arm coded at all - the measurement runs everything as one
group. So "no proper control" is literally the case, not a data quirk.

O2P: a control does exist (TG4 = test, TG7 = control), but it's a whole-population
holdout with no channel attached - control was never deployed, so it carries no
mobile flag. The async cohort, by contrast, is defined by the mobile channel. That
means there's no mobile-only control to compare against: if the mobile test response
is being held up against the full TG7 holdout, we're comparing a mobile-engaged slice
of test to the entire control population - different audiences, so the control won't
track recent mobile waves the way you'd expect.

The comparison we can stand behind is TG4 vs TG7 overall on conversion. What we can't
do as deployed is isolate a mobile-matched control.

One thing to confirm so we're looking at the same thing: in the numbers you're seeing,
is the control the full TG7 holdout, or is it being compared against a mobile-filtered
test? That tells us whether it's the mismatch above.

Happy to grab 15 minutes.

Thanks,
Andre
------------------------------------------------------------------
*/
