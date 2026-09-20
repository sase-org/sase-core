#!/usr/bin/env python3
"""Unit checks for the Release-plz workflow's release cadence wiring.

The workflow cuts a release once a day instead of merging the release PR on every
master push. GitHub Actions cannot be run here, so these checks read the workflow
text (stdlib only, no YAML parser) and pin the pieces that keep that decision from
drifting: the daily cron, the merge job's trigger gate, the untouched heal path, and
the merge safety guards. Run with ``python3 -m unittest discover -s .github/scripts``.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release-plz.yml"

HEAL_CRON = "23 */6 * * *"


def workflow_text() -> str:
    return WORKFLOW.read_text()


def header(text: str) -> str:
    """The comment block above ``on:``, where the release-flow decisions live."""
    return text.split("\non:\n", 1)[0]


def schedule_crons(text: str) -> list[str]:
    block = re.search(r"^  schedule:\n((?:    .*\n)+)", text, re.MULTILINE)
    assert block is not None, "on.schedule block not found"
    return re.findall(r'^    - cron: "([^"]+)"$', block.group(1), re.MULTILINE)


def job_block(text: str, job: str) -> str:
    """One job's text: from its ``  <job>:`` key to the next job key."""
    match = re.search(
        rf"^  {re.escape(job)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"job {job!r} not found"
    return match.group(1)


def job_if(text: str, job: str) -> str:
    match = re.search(r"^    if: (.+)$", job_block(text, job), re.MULTILINE)
    assert match is not None, f"job {job!r} has no `if:`"
    return match.group(1)


class ScheduleTests(unittest.TestCase):
    def test_heal_cron_is_kept_and_a_distinct_daily_cron_is_added(self) -> None:
        crons = schedule_crons(workflow_text())
        self.assertIn(HEAL_CRON, crons)
        self.assertEqual(len(crons), 2, crons)
        self.assertEqual(len(set(crons)), 2, crons)

    def test_daily_cron_fires_once_a_day(self) -> None:
        (daily,) = [c for c in schedule_crons(workflow_text()) if c != HEAL_CRON]
        minute, hour, dom, month, dow = daily.split()
        self.assertTrue(minute.isdigit() and hour.isdigit(), daily)
        self.assertEqual((dom, month, dow), ("*", "*", "*"), daily)

    def test_push_and_manual_dispatch_triggers_remain(self) -> None:
        text = workflow_text()
        self.assertRegex(text, r"(?m)^  push:\n    branches: \[master\]$")
        self.assertRegex(text, r"(?m)^  workflow_dispatch:$")
        self.assertRegex(text, r"(?m)^      dry_run:\n(?:        .*\n)*?        default: true$")


class MergeGateTests(unittest.TestCase):
    def daily_cron(self) -> str:
        (daily,) = [c for c in schedule_crons(workflow_text()) if c != HEAL_CRON]
        return daily

    def test_merge_runs_only_for_the_daily_cron_or_a_live_dispatch(self) -> None:
        gate = job_if(workflow_text(), "release-plz-merge")
        self.assertIn(f"github.event.schedule == '{self.daily_cron()}'", gate)
        self.assertIn("github.event_name == 'workflow_dispatch' && !inputs.dry_run", gate)

    def test_merge_does_not_run_for_pushes_or_the_heal_cron(self) -> None:
        gate = job_if(workflow_text(), "release-plz-merge")
        self.assertNotIn("push", gate)
        self.assertNotIn(HEAL_CRON, gate)
        # A `!=` form ("anything but a dry-run dispatch") would re-admit pushes.
        self.assertNotIn("!=", gate)

    def test_daily_cron_is_in_the_schedule_the_gate_and_the_group_only(self) -> None:
        text = workflow_text()
        self.assertEqual(text.count(f'"{self.daily_cron()}"'), 1)  # on.schedule
        self.assertEqual(text.count(f"'{self.daily_cron()}'"), 2)  # merge `if` + group

    def test_concurrency_group_isolates_runs_that_do_not_cut(self) -> None:
        text = workflow_text()
        gate = job_if(text, "release-plz-merge")
        cut = re.search(r" && (\(.*\)) \}\}$", gate)
        self.assertIsNotNone(cut, gate)
        assert cut is not None
        group = re.search(
            r"^      group: (release-plz-merge-.+)$",
            job_block(text, "release-plz-merge"),
            re.MULTILINE,
        )
        self.assertIsNotNone(group)
        assert group is not None
        self.assertIn(f"{cut.group(1)} && 'cut' || github.run_id", group.group(1))

    def test_merge_still_cancels_a_predecessor_cut(self) -> None:
        block = job_block(workflow_text(), "release-plz-merge")
        self.assertIn("cancel-in-progress: true", block)


class HealPathTests(unittest.TestCase):
    """The self-healing publish path must not depend on the daily cut."""

    def test_publish_path_jobs_have_no_schedule_or_push_gate(self) -> None:
        text = workflow_text()
        for job in ("release-plz-release", "publish-plan", "release-plz-pr"):
            gate = job_if(text, job)
            self.assertNotIn("github.event.schedule", gate, job)
            self.assertNotIn("push", gate, job)

    def test_publish_plan_still_feeds_the_build_and_publish_jobs(self) -> None:
        text = workflow_text()
        for job in ("linux", "macos", "windows", "sdist", "metadata-check", "publish"):
            gate = job_if(text, job)
            self.assertIn("needs.publish-plan.outputs.needs_publish == 'true'", gate, job)
            self.assertIn("github.event_name != 'workflow_dispatch'", gate, job)
            self.assertNotIn("github.event.schedule", gate, job)

    def test_release_pr_is_still_updated_by_every_run(self) -> None:
        # release-plz-pr is what lets the release PR accumulate between cuts, so
        # it may only be skipped by a dry-run dispatch.
        self.assertEqual(
            job_if(workflow_text(), "release-plz-pr"),
            "${{ github.repository_owner == 'sase-org' && "
            "!(github.event_name == 'workflow_dispatch' && inputs.dry_run) }}",
        )


class MergeSafetyGuardTests(unittest.TestCase):
    """sase-core master is unprotected, so none of these guards may be relaxed."""

    def test_merge_only_touches_the_guarded_release_plz_pr(self) -> None:
        block = job_block(workflow_text(), "release-plz-merge")
        for needle in (
            "--base master --state open --author \"${release_actor}\"",
            r'test("^release-plz-[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}-[0-9]{2}-[0-9]{2}Z$")',
            r'test("^chore: release v[0-9]+\\.[0-9]+\\.[0-9]+$")',
            'contains("generated with [release-plz]")',
            "gh pr checks \"${PR_NUMBER}\" --watch --fail-fast",
            'gh pr merge "${PR_NUMBER}" --squash --delete-branch',
        ):
            self.assertIn(needle, block)

    def test_merge_still_follows_the_release_pr_job(self) -> None:
        block = job_block(workflow_text(), "release-plz-merge")
        self.assertRegex(block, r"(?m)^    needs: release-plz-pr$")
        self.assertIn("github.repository_owner == 'sase-org'", job_if(workflow_text(), "release-plz-merge"))


class DecisionRecordTests(unittest.TestCase):
    def test_header_records_the_daily_cadence_and_the_escape_hatch(self) -> None:
        text = header(workflow_text())
        self.assertIn("once a day", text)
        self.assertIn("Do not restore merge-on-push", text)
        self.assertIn("dry_run=false", text)


if __name__ == "__main__":
    unittest.main()
