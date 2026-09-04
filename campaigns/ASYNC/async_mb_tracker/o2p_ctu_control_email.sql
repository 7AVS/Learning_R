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

You're onto something, and it comes down to how the two groups are identified.

How I pull them:
 - Async (mobile) group = a CHANNEL filter. For O2P that's the mobile cell code
   (TACTIC_CELL_CD LIKE '%IM%'); for CTU it's the MB deploy flag in the decision
   info. Then I join to the GA4 banner view/click events for that creative.
 - Test vs control = the test-group code, NOT channel. TG4 = test, TG7 = control,
   scoped by the report-group allowlist. No channel filter on either arm.

The catch: control (TG7) is never deployed, so it has no channel - it can't satisfy
the mobile filter. So the async/mobile cohort is test-only by construction, and the
only control I can form is the whole-arm TG7 holdout, not a mobile-matched one.

That's why the control doesn't track recent mobile waves: if the mobile test response
is held up against the full TG7 holdout, we're comparing a mobile-engaged slice of
test to the entire control population - different audiences.

CTU is worse - there's no test/control split coded at all; it runs as one group. So
"no proper control" is literally the case there.

The comparison we can stand behind is TG4 vs TG7 overall on conversion. What we can't
do as deployed is isolate a mobile-matched control.

One thing to confirm so we're looking at the same thing: in the numbers you're seeing,
is the control the full TG7 holdout, or is it being compared against a mobile-filtered
test?

Happy to grab 15 minutes.

Thanks,
Andre
------------------------------------------------------------------
*/
