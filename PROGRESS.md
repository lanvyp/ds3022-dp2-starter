# Progress log

Add a new entry at the end of every session (and whenever you stop working).

## Mon 05 Oct (Lesson 9)
- Done: Set up venv (fixed a macOS SSL certificate error during pip install), smoke test passes (Emulator OK). Wrote scatter + collect: long-polls the queue, logs the three counters, lands each fragment in raw.fragments before deleting it. First fast run collected all 21 in 59s.
- Next: dbt models and tests, then the submit step.
- Blocked: Nothing.

## Thu 08 Oct (Lesson 10)
- Done: Added the dbt models and tests (stg_fragments, schema.yml, singular test, assembled_phrase). Those changes got lost before I committed them, so I had to redo them later.
- Next: Re-add the dbt files, add the submit step, commit as I go.
- Blocked: Nothing.

## Sun 18 Oct
- Done: Re-added the dbt models and tests, then added the dbt quality gate and the submit step to the flow. Made a test fail on purpose (deleted fragment 7), then passed. Full-delay run (DP2_FAST=0) completed in 14m 21s, receipt C21AD53E68. Added handling for malformed and duplicate messages ahead of the chaos round. README, screenshots and DAG sketch pushed.
- Next: Lesson 11 chaos round (DP2_CHAOS=1, two consecutive PASS runs), fill in the README Chaos round section, submit the repo link and RUBRIC.md by 23:59.
- Blocked: Nothing.

## Mon 19 Oct (Lesson 11)
- Done: Turned on chaos mode (DP2_CHAOS=1). Ran two chaos runs in a row (academic-grebe, huge-spoonbill), both PASS. Each had 24 messages: 2 duplicates counted once, 1 malformed message moved to raw.rejected and deleted, queue counters at 0. Wrote the README Chaos round section with screenshots, filled in RUBRIC.md.
- Next: Evidence run in Lesson 11 if asked, then submit the repo link and rubric by 23:59.
- Blocked: Nothing.