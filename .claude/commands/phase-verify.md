---
description: Run a roadmap phase's exit criterion on the verification VM
argument-hint: <phase number>
allowed-tools: Bash(bash scripts/vm.sh:*), Bash(scripts/vm.sh:*), Bash(make check), Bash(git status:*), Bash(git log:*), Read, Grep, Glob, Edit
---

Verify **phase $ARGUMENTS** against its exit criterion, on the VM.

A phase is not done because the tests pass. Phase 5 passed strict mypy, the
whole unit suite and six CI jobs, and still carried seven bugs — every one an
integration-boundary failure that only a real run could find. This procedure is
the actual exit gate.

## Procedure

1. **Read the criterion.** Find the phase's row in the roadmap table in
   `README.md` and quote its exit criterion. Everything below is in service of
   answering it. If the criterion is vague, say so before starting rather than
   declaring success against your own interpretation.

2. **Check the VM will run what you think it will.** `git status` and confirm
   the working tree is clean and the branch is pushed. If it is not, stop and
   tell the user — the VM builds from its own `git pull`.

3. **Local gate first**, because it is cheap: `make check`. Do not go to the VM
   with a failing local suite.

4. **Sync and health-check:** `scripts/vm.sh sync`, then
   `scripts/vm.sh make doctor`. All backing services must be reachable before
   anything else means anything.

5. **Run the phase's pipelines** with `scripts/vm.sh make <target>` — typically
   `migrate`, `seed`, `run-all`, `weather`, and whatever the phase added.
   Capture row counts and timings.

6. **Run it a second time.** Idempotence is the property most likely to be
   broken and least likely to be tested: snapshot re-runs must produce identical
   counts, and an incremental re-run with the watermark caught up must extract
   nothing while leaving the partition populated.

7. **Assert against the warehouse, not the logs.** `scripts/vm.sh ch '<sql>'`
   for row counts, null rates, and the specific numbers the criterion names.
   A pipeline reporting success while writing nothing is a failure mode this
   platform has already seen.

8. **Integration and DAG suites:** `scripts/vm.sh make test-integration` and
   `scripts/vm.sh make test-dags`. Report the pass counts.

9. **Check the quarantine zone.** `scripts/vm.sh exec 'make hdfs-ls ZONE=quarantine'`
   — rejected rows are expected to be few and explainable. An unexpected
   quarantine is a finding, not noise.

## Reporting

State plainly whether the criterion is **met or not met**, with the measured
numbers. If anything failed, report it with the output — do not summarise a
failure as a caveat.

Only if it is fully met: update that phase's row in the `README.md` roadmap to
**done**, and add the measured numbers to the verification-status section in
the style of the existing entries. If any bug was found and fixed during the
run, add a row to the bug table saying why nothing caught it — that table is
the argument for this procedure existing.

Do not mark a phase done on a partial pass. Report what passed, what did not,
and leave the roadmap alone.
