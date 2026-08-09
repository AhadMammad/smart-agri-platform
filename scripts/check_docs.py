"""Check the documentation still describes the repository that exists.

Phase 8's exit criterion is a *documented* path to dashboards, and documentation
rots in two specific ways that are both mechanical to catch:

* a `make <target>` in the README that the Makefile no longer has, so the
  documented path stops halfway with "No rule to make target"
* a relative link to a file that has been renamed or moved

Both look fine in review and fail only for the person following the instructions
for the first time — which is exactly the person least able to work around it.

Run with `make check-docs`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

#: Files whose instructions a newcomer is expected to follow.
DOCS = ["README.md", "CLAUDE.md", "etl/README.md", "etl/CLAUDE.md", *("docs/*.md",)]

#: Code only — a fenced block or an inline span. "make" is also an ordinary
#: English verb, and prose like "make partition-level replacement worth it"
#: would otherwise be read as a command and reported as a missing target.
FENCED = re.compile(r"```[a-z]*\n(.*?)```", re.DOTALL)
INLINE = re.compile(r"`([^`\n]+)`")

#: The target is the first word; `VAR=value` arguments after it are ignored.
#: `[ \t]+` rather than `\s+` so a match cannot span two code spans: joining
#: `make` and `make check` would otherwise read as the target "make".
MAKE_CALL = re.compile(r"(?:^|[ \t(&;|])make[ \t]+([a-z][a-z0-9-]*)", re.MULTILINE)

#: Markdown links to a path in the repo. Anchors, URLs and mailto are skipped.
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def documents() -> list[Path]:
    found: list[Path] = []
    for entry in DOCS:
        if "*" in entry:
            found.extend(sorted(ROOT.glob(entry)))
        elif (ROOT / entry).is_file():
            found.append(ROOT / entry)
    return found


def make_targets() -> set[str]:
    """Every target the Makefile declares, from its `.PHONY` lines and rules."""
    text = (ROOT / "Makefile").read_text()
    targets = set(re.findall(r"^\.PHONY:\s*(.+)$", text, re.MULTILINE))
    names = {name for line in targets for name in line.split()}
    names |= set(re.findall(r"^([a-zA-Z][a-zA-Z0-9_-]*):", text, re.MULTILINE))
    return names


def code_in(text: str) -> str:
    """Just the code from a Markdown document, prose discarded."""
    return "\n".join(FENCED.findall(text) + INLINE.findall(text))


def check_make_calls(paths: list[Path], targets: set[str]) -> list[str]:
    problems = []
    for path in paths:
        code = code_in(path.read_text())
        for target in sorted(set(MAKE_CALL.findall(code))):
            if target in targets:
                continue
            problems.append(
                f"{path.relative_to(ROOT)}: documents `make {target}`, "
                f"which the Makefile does not define"
            )
    return problems


def check_links(paths: list[Path]) -> list[str]:
    problems = []
    for path in paths:
        for target in sorted(set(LINK.findall(path.read_text()))):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            # Strip an anchor; the file is what matters here.
            relative = target.split("#", 1)[0]
            if not relative:
                continue
            resolved = (path.parent / relative).resolve()
            if not resolved.exists():
                problems.append(f"{path.relative_to(ROOT)}: link to missing {target}")
    return problems


def main() -> int:
    paths = documents()
    if not paths:
        print("no documentation found", file=sys.stderr)
        return 1

    targets = make_targets()
    problems = check_make_calls(paths, targets) + check_links(paths)

    for problem in problems:
        print(f"  {problem}")

    checked = ", ".join(str(p.relative_to(ROOT)) for p in paths)
    if problems:
        print(f"\n{len(problems)} documentation problem(s) across {len(paths)} file(s).")
        return 1

    print(f"documentation is consistent with the repo ({checked})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
