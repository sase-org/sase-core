"""Offline acceptance tests for the release-plz merge step."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import release_plz_merge as merge  # noqa: E402


def check(bucket: str, *, name: str = "CI / tests", run_id: str = "123") -> dict[str, str]:
    return {
        "bucket": bucket,
        "state": bucket.upper(),
        "name": name,
        "link": f"https://github.com/sase-org/sase-core/actions/runs/{run_id}/job/456",
    }


class FakeGitHub:
    def __init__(
        self,
        check_rounds: list[list[dict[str, str]]],
        *,
        pr: dict[str, Any] | None = None,
        run_infos: dict[str, dict[str, Any] | list[dict[str, Any]]] | None = None,
    ) -> None:
        self.check_rounds = check_rounds
        self.pr = pr if pr is not None else {"number": 42}
        self.run_infos = run_infos or {
            "123": [
                {
                    "attempt": 1,
                    "conclusion": "failure",
                    "status": "completed",
                    "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                },
                {
                    "attempt": 2,
                    "conclusion": "success",
                    "status": "completed",
                    "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                },
            ]
        }
        self.check_calls = 0
        self.run_info_calls: dict[str, int] = {}
        self.reruns: list[str] = []
        self.merges: list[int] = []

    def find_release_pr(self) -> dict[str, Any] | None:
        return self.pr

    def checks(self, pr_number: int) -> list[dict[str, Any]]:
        self.check_calls += 1
        index = min(self.check_calls - 1, len(self.check_rounds) - 1)
        return self.check_rounds[index]

    def run_info(self, run_id: str) -> dict[str, Any]:
        self.run_info_calls[run_id] = self.run_info_calls.get(run_id, 0) + 1
        value = self.run_infos[run_id]
        if isinstance(value, list):
            index = min(self.run_info_calls[run_id] - 1, len(value) - 1)
            return value[index]
        return value

    def rerun_failed(self, run_id: str) -> None:
        self.reruns.append(run_id)

    def merge(self, pr_number: int) -> None:
        self.merges.append(pr_number)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class ReleasePlzMergeTests(unittest.TestCase):
    def run_merge(self, github: FakeGitHub, *, timeout: float = 1) -> merge.MergeResult:
        clock = FakeClock()
        return merge.merge_release_pr(
            github,
            register_timeout=timeout,
            check_timeout=timeout,
            poll_interval=0.1,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    def test_first_failed_run_is_rerun_once_then_merge_succeeds(self) -> None:
        github = FakeGitHub([[check("fail")], [check("pass")]])
        result = self.run_merge(github)
        self.assertEqual(result.outcome, "MERGED")
        self.assertEqual(github.reruns, ["123"])
        self.assertEqual(github.merges, [42])

    def test_waits_for_the_new_attempt_before_reading_check_results_again(self) -> None:
        github = FakeGitHub(
            [[check("fail")], [check("pass")]],
            run_infos={
                "123": [
                    {
                        "attempt": 1,
                        "conclusion": "failure",
                        "status": "completed",
                        "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                    },
                    {
                        "attempt": 1,
                        "conclusion": "failure",
                        "status": "completed",
                        "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                    },
                    {
                        "attempt": 2,
                        "conclusion": "success",
                        "status": "completed",
                        "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                    },
                ]
            },
        )
        result = self.run_merge(github)
        self.assertEqual(result.outcome, "MERGED")
        self.assertEqual(github.run_info_calls["123"], 3)

    def test_second_failure_refuses_with_typed_core_cut_red(self) -> None:
        github = FakeGitHub(
            [[check("fail")], [check("fail")]],
            run_infos={
                "123": [
                    {
                        "attempt": 1,
                        "conclusion": "failure",
                        "status": "completed",
                        "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                    },
                    {
                        "attempt": 2,
                        "conclusion": "failure",
                        "status": "completed",
                        "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                    },
                ]
            },
        )
        result = self.run_merge(github)
        self.assertEqual(result.outcome, "CORE_CUT_RED")
        self.assertEqual(result.failed_check, "CI / tests")
        self.assertIn("CORE_CUT_RED check=CI_/_tests", result.typed_line)
        self.assertIn("attempt=2", result.typed_line)
        self.assertEqual(github.reruns, ["123"])
        self.assertEqual(github.merges, [])

    def test_checks_never_registered_refuses_without_rerun(self) -> None:
        github = FakeGitHub([[]])
        result = self.run_merge(github, timeout=0.2)
        self.assertEqual(result.outcome, "CHECKS_NEVER_REGISTERED")
        self.assertEqual(github.reruns, [])
        self.assertEqual(github.merges, [])

    def test_no_release_pr_is_a_successful_notice_case(self) -> None:
        github = FakeGitHub([])
        github.pr = None
        result = self.run_merge(github)
        self.assertEqual(result.outcome, "NO_RELEASE_PR")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(github.check_calls, 0)
        self.assertEqual(github.merges, [])

    def test_cancelled_run_is_never_rerun(self) -> None:
        github = FakeGitHub(
            [[check("cancel")]],
            run_infos={
                "123": {
                    "attempt": 1,
                    "conclusion": "cancelled",
                    "status": "completed",
                    "url": "https://github.com/sase-org/sase-core/actions/runs/123",
                }
            },
        )
        result = self.run_merge(github)
        self.assertEqual(result.outcome, "CORE_CUT_RED")
        self.assertEqual(github.reruns, [])
        self.assertEqual(github.merges, [])


if __name__ == "__main__":
    unittest.main()
