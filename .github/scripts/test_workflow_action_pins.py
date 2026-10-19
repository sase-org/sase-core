#!/usr/bin/env python3
"""Require setup-uv workflow references to use exact release tags or commit SHAs.

setup-uv stopped publishing floating ``vN`` and ``vN.M`` tags after v7. A
floating ref such as ``@v10`` fails at "Set up job" before any step runs, which
kept ``release workflow scripts`` red from ``9876b3ae`` on. This rule is scoped
to setup-uv because the other actions in this repo still publish floating majors.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
SETUP_UV = re.compile(
    r"^\s*-\s*uses:\s*astral-sh/setup-uv@([^\s#]+)(?:\s*#.*)?\s*$",
    re.MULTILINE,
)
VALID_REF = re.compile(r"(?:^v\d+\.\d+\.\d+$|^[0-9a-f]{40}$)")


class WorkflowActionPinTests(unittest.TestCase):
    def test_setup_uv_refs_are_exact_release_tags_or_full_shas(self) -> None:
        hits: list[tuple[Path, int, str]] = []
        workflow_files = sorted(
            path
            for pattern in ("*.yml", "*.yaml")
            for path in WORKFLOWS.glob(pattern)
        )

        for workflow in workflow_files:
            text = workflow.read_text()
            for match in SETUP_UV.finditer(text):
                line = text.count("\n", 0, match.start()) + 1
                hits.append((workflow, line, match.group(1)))

        self.assertTrue(hits, "No astral-sh/setup-uv workflow references found")
        for workflow, line, ref in hits:
            with self.subTest(file=workflow.relative_to(REPO_ROOT), line=line):
                self.assertRegex(
                    ref,
                    VALID_REF,
                    f"{workflow.relative_to(REPO_ROOT)}:{line}: bad setup-uv ref {ref!r}",
                )


if __name__ == "__main__":
    unittest.main()
