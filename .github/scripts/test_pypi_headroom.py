#!/usr/bin/env python3
"""Contract checks for the weekly sase-core-rs PyPI headroom detector.

GitHub Actions cannot be run here, so these checks read the workflow text
(stdlib only, no YAML parser) and pin the detector-not-deleter decisions.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "pypi-headroom.yml"


def workflow_text() -> str:
    return WORKFLOW.read_text()


class HeadroomWorkflowTests(unittest.TestCase):
    def test_workflow_file_exists(self) -> None:
        self.assertTrue(WORKFLOW.is_file(), WORKFLOW)

    def test_weekly_schedule_and_manual_dispatch(self) -> None:
        text = workflow_text()
        crons = re.findall(r'^    - cron: "([^"]+)"$', text, re.MULTILINE)
        self.assertEqual(len(crons), 1, crons)
        minute, hour, _dom, _month, dow = crons[0].split()
        self.assertTrue(minute.isdigit() and hour.isdigit(), crons[0])
        self.assertNotEqual(dow, "*", crons[0])
        self.assertRegex(text, r"(?m)^  workflow_dispatch:$")
        self.assertIn("min_runway_days", text)

    def test_runs_verify_with_min_runway_days(self) -> None:
        text = workflow_text()
        self.assertIn("pypi_retention.py verify", text)
        self.assertIn("--min-runway-days", text)
        self.assertIn("astral-sh/setup-uv", text)

    def test_never_deletes_or_proposes_deletion(self) -> None:
        text = workflow_text()
        self.assertNotIn("pypi_delete.py", text)
        self.assertNotIn("pypi-cleanup", text)
        self.assertNotIn("pypi_retention.py plan", text)
        self.assertRegex(text, r"never deletes", re.IGNORECASE)

    def test_mirrors_one_sase_issue_with_release_token(self) -> None:
        text = workflow_text()
        self.assertIn("sase-org/sase", text)
        self.assertIn("SASE_RELEASE_TOKEN", text)
        self.assertIn("pypi-headroom", text)
        self.assertIn("astral-sh/setup-uv@v10.3.0", text)
        self.assertIn("[pypi-headroom] sase-core-rs runway below threshold", text)
        self.assertIn("sase-core-rs PyPI storage runway below threshold", text)
        self.assertIn("below the ${MIN_RUNWAY_DAYS}-day threshold", text)

    def test_red_run_is_the_guaranteed_alert(self) -> None:
        text = workflow_text()
        self.assertIn("the red run is the alert", text)
        self.assertIn("ci_watch", text)
        self.assertIn("github.repository_owner == 'sase-org'", text)


if __name__ == "__main__":
    unittest.main()
