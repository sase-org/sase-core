#!/usr/bin/env python3
"""Unit checks for the runner-starvation retry helper and its workflow.

Covers the "rerun only when every red job never got a runner, once per run"
rule from the v0.42.7 incident (Release-plz run 40346929251) plus the workflow
text pins that keep the trigger, gate, and token scoped. Run with
``python3 -m unittest discover -s .github/scripts -t .github/scripts``.
"""

from __future__ import annotations

import os
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

import rerun_starved_jobs as retry


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "runner-starvation-retry.yml"
WORKFLOWS = REPO_ROOT / ".github" / "workflows"


def starved_job(name: str = "macos universal2") -> dict[str, Any]:
    """The real v0.42.7 shape: cancelled, no runner, no steps."""
    return {
        "name": name,
        "conclusion": "cancelled",
        "runner_id": 0,
        "runner_name": "",
        "steps": [],
    }


def failed_job(name: str = "linux x86_64") -> dict[str, Any]:
    return {
        "name": name,
        "conclusion": "failure",
        "runner_id": 1000210200,
        "runner_name": "GitHub Actions 1000210200",
        "steps": [{"name": "Build wheel", "conclusion": "failure"}],
    }


def setup_failure_job(name: str = "release workflow scripts") -> dict[str, Any]:
    """A "Set up job" failure had a runner and steps, so it is not starved."""
    return {
        "name": name,
        "conclusion": "failure",
        "runner_id": 1000210233,
        "runner_name": "GitHub Actions 1000210233",
        "steps": [{"name": "Set up job", "conclusion": "failure"}],
    }


def skipped_job(name: str = "twine check") -> dict[str, Any]:
    return {
        "name": name,
        "conclusion": "skipped",
        "runner_id": None,
        "runner_name": None,
        "steps": [],
    }


def success_job(name: str = "linux x86_64") -> dict[str, Any]:
    return {
        "name": name,
        "conclusion": "success",
        "runner_id": 1000210200,
        "runner_name": "GitHub Actions 1000210200",
        "steps": [{"name": "Build wheel", "conclusion": "success"}],
    }


def config(
    *,
    conclusion: str = "failure",
    attempt: int = 1,
    run_id: str = "40346929251",
) -> retry.StarvationConfig:
    return retry.StarvationConfig(
        repo="sase-org/sase-core",
        run_id=run_id,
        run_attempt=attempt,
        run_conclusion=conclusion,
    )


class FakeGitHub:
    def __init__(self, jobs: list[dict[str, Any]], *, fail_rerun: bool = False) -> None:
        self._jobs = jobs
        self.fail_rerun = fail_rerun
        self.reruns: list[str] = []

    def list_jobs(self, run_id: str, attempt: int) -> list[dict[str, Any]]:  # noqa: ARG002
        return self._jobs

    def rerun_failed(self, run_id: str) -> None:
        if self.fail_rerun:
            raise retry.StarvationError("could not rerun failed jobs")
        self.reruns.append(run_id)


class DecideTests(unittest.TestCase):
    def test_starved_only_attempt_1_reruns(self) -> None:
        github = FakeGitHub([starved_job(), success_job(), skipped_job()])
        self.assertEqual(retry.run(github, config()), 0)
        self.assertEqual(github.reruns, ["40346929251"])

    def test_starved_plus_real_failure_does_not_rerun(self) -> None:
        github = FakeGitHub([starved_job(), failed_job()])
        self.assertEqual(retry.run(github, config()), 0)
        self.assertEqual(github.reruns, [])
        decision = retry.decide(
            [starved_job(), failed_job()],
            run_conclusion="failure",
            run_attempt=1,
        )
        self.assertFalse(decision.should_rerun)
        self.assertIn("linux x86_64", decision.reason)

    def test_real_failure_only_does_not_rerun(self) -> None:
        github = FakeGitHub([failed_job(), success_job(), skipped_job()])
        self.assertEqual(retry.run(github, config()), 0)
        self.assertEqual(github.reruns, [])

    def test_setup_job_failure_is_not_starved(self) -> None:
        job = setup_failure_job()
        self.assertFalse(retry.is_starved(job))
        github = FakeGitHub([job])
        self.assertEqual(retry.run(github, config()), 0)
        self.assertEqual(github.reruns, [])

    def test_second_attempt_does_not_rerun(self) -> None:
        github = FakeGitHub([starved_job()])
        self.assertEqual(retry.run(github, config(attempt=retry.MAX_ATTEMPTS)), 0)
        self.assertEqual(github.reruns, [])

    def test_cancelled_run_does_not_rerun(self) -> None:
        github = FakeGitHub([starved_job()])
        self.assertEqual(retry.run(github, config(conclusion="cancelled")), 0)
        self.assertEqual(github.reruns, [])

    def test_skipped_dependents_next_to_starved_still_rerun(self) -> None:
        github = FakeGitHub([starved_job(), skipped_job("twine check"), skipped_job("publish to PyPI")])
        self.assertEqual(retry.run(github, config()), 0)
        self.assertEqual(github.reruns, ["40346929251"])

    def test_no_red_jobs_skips(self) -> None:
        github = FakeGitHub([success_job(), skipped_job()])
        self.assertEqual(retry.run(github, config()), 0)
        self.assertEqual(github.reruns, [])

    def test_rerun_failure_exits_1_with_error(self) -> None:
        github = FakeGitHub([starved_job()], fail_rerun=True)
        self.assertEqual(retry.run(github, config()), 1)

    def test_summary_line_is_written(self) -> None:
        github = FakeGitHub([starved_job()])
        with tempfile.TemporaryDirectory() as tmp:
            summary = Path(tmp) / "summary.md"
            os.environ["GITHUB_STEP_SUMMARY"] = str(summary)
            try:
                self.assertEqual(retry.run(github, config()), 0)
            finally:
                del os.environ["GITHUB_STEP_SUMMARY"]
            line = summary.read_text().strip()
            self.assertIn("rerun", line)
            self.assertIn("macos universal2", line)
            self.assertIn("https://github.com/sase-org/sase-core/actions/runs/40346929251", line)


class ConfigTests(unittest.TestCase):
    def base_env(self) -> dict[str, str]:
        return {
            "GH_REPO": "sase-org/sase-core",
            "GH_TOKEN": "token",
            "RUN_ID": "40346929251",
            "RUN_ATTEMPT": "1",
            "RUN_CONCLUSION": "failure",
        }

    def test_missing_repo_is_an_error(self) -> None:
        env = self.base_env()
        del env["GH_REPO"]
        with self.assertRaises(retry.StarvationError):
            retry.read_config(env)

    def test_missing_token_is_an_error(self) -> None:
        env = self.base_env()
        del env["GH_TOKEN"]
        with self.assertRaises(retry.StarvationError):
            retry.read_config(env)

    def test_non_integer_attempt_is_an_error(self) -> None:
        env = self.base_env()
        env["RUN_ATTEMPT"] = "first"
        with self.assertRaises(retry.StarvationError):
            retry.read_config(env)

    def test_max_attempts_caps_reruns_at_two(self) -> None:
        self.assertEqual(retry.MAX_ATTEMPTS, 2)


def workflow_text() -> str:
    return WORKFLOW.read_text()


def job_block(text: str, job: str) -> str:
    match = re.search(
        rf"^  {re.escape(job)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match is not None, f"job {job!r} not found"
    return match.group(1)


class RetryWorkflowTests(unittest.TestCase):
    def test_triggers_on_completed_workflow_runs(self) -> None:
        text = workflow_text()
        self.assertIn("workflow_run:", text)
        self.assertRegex(text, r"(?m)^    types: \[completed\]$")

    def test_every_watched_name_matches_an_existing_workflow(self) -> None:
        text = workflow_text()
        match = re.search(r"(?m)^    workflows: \[(.+)\]$", text)
        assert match is not None, "workflows: list not found"
        watched = [name.strip() for name in match.group(1).split(",")]
        existing = set()
        for path in (*WORKFLOWS.glob("*.yml"), *WORKFLOWS.glob("*.yaml")):
            name_match = re.search(r"(?m)^name: (.+)$", path.read_text())
            if name_match:
                existing.add(name_match.group(1).strip())
        self.assertTrue(watched, "no watched workflows")
        for name in watched:
            with self.subTest(workflow=name):
                self.assertIn(name, existing)

    def test_watches_release_plz_and_ci(self) -> None:
        text = workflow_text()
        self.assertRegex(text, r"(?m)^    workflows: \[Release-plz, CI\]$")

    def test_job_gates_on_failure_and_holds_actions_write(self) -> None:
        text = workflow_text()
        block = job_block(text, "rerun-starved")
        self.assertIn("github.event.workflow_run.conclusion", block)
        self.assertIn("'failure'", block.replace('"', "'").replace(" ", ""))
        self.assertIn("actions: write", block)

    def test_top_level_permissions_are_empty(self) -> None:
        text = workflow_text()
        self.assertRegex(text, r"(?m)^permissions: \{\}$")

    def test_job_runs_the_starvation_helper(self) -> None:
        block = job_block(workflow_text(), "rerun-starved")
        self.assertIn("python3 .github/scripts/rerun_starved_jobs.py", block)

    def test_token_is_least_privilege_github_token(self) -> None:
        block = job_block(workflow_text(), "rerun-starved")
        self.assertIn("GH_TOKEN: ${{ github.token }}", block)
        self.assertNotIn("SASE_RELEASE_TOKEN", block)


if __name__ == "__main__":
    unittest.main()
