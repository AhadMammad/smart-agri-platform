---
description: Run something on the verification VM via scripts/vm.sh
argument-hint: <what to do on the VM>
allowed-tools: Bash(bash scripts/vm.sh:*), Bash(scripts/vm.sh:*), Read, Grep, Glob
---

Do this on the verification VM: **$ARGUMENTS**

Rules:

1. Reach the VM **only** through `scripts/vm.sh`. Never construct an `ssh`
   command — the connection details live in the gitignored `.env.vm` and the
   script already holds the options, the remote repo path and the container
   prefix. Run `scripts/vm.sh help` if you need the subcommand list.

2. **Run `scripts/vm.sh sync` first** for anything that depends on ETL code.
   The VM runs the image built from its last `git pull`, so an un-pushed or
   un-committed local edit is invisible there. `scripts/vm.sh make <target>`
   syncs automatically; `scripts/vm.sh exec` does not.

3. If the local branch has commits that are not pushed, say so and ask before
   syncing — `git pull --ff-only` on the VM will not pick them up, and the run
   would silently test stale code.

4. Query the databases with `scripts/vm.sh ch '<sql>'` and
   `scripts/vm.sh psql '<sql>'`. Read container logs with
   `scripts/vm.sh logs <service> [lines]`. Do not try `make logs`, `make ch`,
   `make psql` or `make clean` on the VM — they block on stdin or never exit,
   and `vm.sh make` refuses them.

5. Report what the VM actually printed. Counts, timings and errors verbatim —
   the whole point of running there is that the host cannot tell you these.

If `vm.sh` reports a missing variable, tell the user which one and point them
at `.env.vm.example`. Do not guess a hostname, username or key path.
