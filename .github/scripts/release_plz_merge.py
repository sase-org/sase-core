#!/usr/bin/env python3
"""Wait for the guarded release-plz PR, rerun failed jobs once, then merge."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol


PR_FILTER = (
    '[.[] | select(.headRefName | test("^release-plz-[0-9]{4}-[0-9]{2}-[0-9]{2}T'
    '[0-9]{2}-[0-9]{2}-[0-9]{2}Z$")) '
    '| select(.title | test("^chore: release v[0-9]+[.][0-9]+[.][0-9]+$")) '
    '| select(.body | contains("generated with [release-plz]"))] | .[0] // empty'
)
CHECK_FIELDS = "bucket,state,link,name"
REGISTER_TIMEOUT_SECONDS = 300
CHECK_TIMEOUT_SECONDS = 2400
POLL_INTERVAL_SECONDS = 10


class MergeError(Exception):
    """A GitHub CLI call or response could not be trusted."""


class MergeGitHub(Protocol):
    def find_release_pr(self) -> dict[str, Any] | None: ...

    def checks(self, pr_number: int) -> list[dict[str, Any]]: ...

    def run_info(self, run_id: str) -> dict[str, Any]: ...

    def rerun_failed(self, run_id: str) -> None: ...

    def merge(self, pr_number: int) -> None: ...


class GitHubCLI:
    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                ["gh", *args],
                capture_output=True,
                text=True,
                check=False,
            )
        except (OSError, UnicodeError) as exc:
            raise MergeError(f"could not run gh {' '.join(args)}: {exc}") from exc

    def _json(self, args: list[str], *, allow_pending: bool = False) -> Any:
        result = self._run(args)
        if result.returncode != 0 and not (allow_pending and result.returncode == 8):
            detail = result.stderr.strip() or result.stdout.strip() or f"gh exited {result.returncode}"
            if "no checks reported" in detail.lower():
                return []
            raise MergeError(f"gh {' '.join(args)} failed: {detail}")
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise MergeError(f"gh {' '.join(args)} returned malformed JSON") from exc

    def find_release_pr(self) -> dict[str, Any] | None:
        actor_result = self._run(["api", "user", "--jq", ".login"])
        if actor_result.returncode != 0 or not actor_result.stdout.strip():
            detail = actor_result.stderr.strip() or "could not identify authenticated GitHub user"
            raise MergeError(detail)
        result = self._run(
            [
                "pr",
                "list",
                "--base",
                "master",
                "--state",
                "open",
                "--author",
                actor_result.stdout.strip(),
                "--json",
                "number,title,headRefName,body",
                "--jq",
                PR_FILTER,
            ]
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or "could not query open release-plz PRs"
            raise MergeError(detail)
        if not result.stdout.strip():
            return None
        try:
            pr = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise MergeError("gh pr list returned malformed JSON") from exc
        if not isinstance(pr, dict) or not isinstance(pr.get("number"), int):
            raise MergeError("gh pr list returned an invalid release PR")
        return pr

    def checks(self, pr_number: int) -> list[dict[str, Any]]:
        checks = self._json(
            ["pr", "checks", str(pr_number), "--json", CHECK_FIELDS], allow_pending=True
        )
        if not isinstance(checks, list) or any(not isinstance(check, dict) for check in checks):
            raise MergeError("gh pr checks returned malformed check data")
        return checks

    def run_info(self, run_id: str) -> dict[str, Any]:
        info = self._json(
            ["run", "view", run_id, "--json", "attempt,conclusion,status,url"]
        )
        if not isinstance(info, dict):
            raise MergeError(f"gh run view {run_id} returned malformed run data")
        return info

    def rerun_failed(self, run_id: str) -> None:
        result = self._run(["run", "rerun", run_id, "--failed"])
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"gh exited {result.returncode}"
            raise MergeError(f"could not rerun failed jobs in run {run_id}: {detail}")

    def merge(self, pr_number: int) -> None:
        result = self._run(["pr", "merge", str(pr_number), "--squash", "--delete-branch"])
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"gh exited {result.returncode}"
            raise MergeError(f"could not merge release PR #{pr_number}: {detail}")


@dataclass(frozen=True)
class ChecksWait:
    status: str
    checks: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class MergeResult:
    outcome: str
    failed_check: str = ""
    run_url: str = ""
    attempt: int = 0
    pr_number: int | None = None
    detail: str = ""

    @property
    def exit_code(self) -> int:
        return 0 if self.outcome in {"MERGED", "NO_RELEASE_PR"} else 1

    @property
    def typed_line(self) -> str:
        if self.outcome == "CORE_CUT_RED":
            return (
                f"CORE_CUT_RED check={clean_field(self.failed_check or 'unknown')} "
                f"run={clean_field(self.run_url or 'unknown')} attempt={self.attempt}"
            )
        if self.outcome == "CHECKS_NEVER_REGISTERED":
            return "CHECKS_NEVER_REGISTERED check=checks run=unknown attempt=0"
        return self.outcome


def clean_field(value: str) -> str:
    return re.sub(r"[\r\n\t ]+", "_", value).replace("%", "%25")


def bucket(check: dict[str, Any]) -> str:
    value = check.get("bucket")
    if isinstance(value, str) and value:
        return value.lower()
    state = check.get("state")
    return state.lower() if isinstance(state, str) else "unknown"


def check_name(check: dict[str, Any]) -> str:
    name = check.get("name")
    return name if isinstance(name, str) and name else "unknown"


def check_link(check: dict[str, Any]) -> str:
    link = check.get("link")
    return link if isinstance(link, str) and link else ""


def run_id_from_link(link: str) -> str | None:
    match = re.search(r"/actions/runs/(\d+)(?:/|$)", link)
    return match.group(1) if match else None


def wait_for_checks(
    github: MergeGitHub,
    pr_number: int,
    *,
    register_timeout: float = REGISTER_TIMEOUT_SECONDS,
    check_timeout: float = CHECK_TIMEOUT_SECONDS,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    already_registered: bool = False,
    deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ChecksWait:
    started = monotonic()
    check_deadline = deadline if deadline is not None else started + check_timeout
    registered = already_registered
    latest: tuple[dict[str, Any], ...] = ()
    while True:
        try:
            rows = github.checks(pr_number)
        except MergeError:
            rows = []
        if rows:
            registered = True
            latest = tuple(rows)
            pending = any(bucket(check) == "pending" for check in rows)
            if not pending:
                return ChecksWait("complete", latest)
        now = monotonic()
        elapsed = now - started
        if not registered and elapsed >= register_timeout:
            return ChecksWait("never_registered", latest)
        if now >= check_deadline:
            return ChecksWait("timeout" if registered else "never_registered", latest)
        remaining = max(0.0, (register_timeout - elapsed) if not registered else (check_deadline - now))
        sleep(min(poll_interval, remaining))


def run_context(github: MergeGitHub, check: dict[str, Any]) -> tuple[str | None, str, int, str]:
    link = check_link(check)
    run_id = run_id_from_link(link)
    if run_id is None:
        return None, link, 0, "check link does not identify a GitHub Actions run"
    try:
        info = github.run_info(run_id)
    except MergeError as exc:
        return run_id, link, 0, str(exc)
    attempt = info.get("attempt")
    attempt_number = attempt if isinstance(attempt, int) and attempt > 0 else 0
    url = info.get("url") if isinstance(info.get("url"), str) else link
    status = str(info.get("status", "")).lower()
    conclusion = str(info.get("conclusion", "")).lower()
    if conclusion == "cancelled" or status == "cancelled":
        return run_id, url, attempt_number, "cancelled runs are not rerun"
    if status != "completed":
        return run_id, url, attempt_number, f"run is not complete (status={status or 'unknown'})"
    if conclusion in {"success", "skipped", "neutral"}:
        return run_id, url, attempt_number, f"check is red but run concluded {conclusion}"
    return run_id, url, attempt_number, ""


def first_red_context(
    github: MergeGitHub,
    checks: tuple[dict[str, Any], ...],
    *,
    default_attempt: int,
) -> tuple[str, str, int]:
    for check in checks:
        if bucket(check) in {"fail", "cancel"}:
            _run_id, url, attempt, _problem = run_context(github, check)
            return check_name(check), url, attempt or default_attempt
    for check in checks:
        if bucket(check) not in {"pass", "skipping", "skip", "success"}:
            _run_id, url, attempt, _problem = run_context(github, check)
            return check_name(check), url, attempt or default_attempt
    return "unknown", "", default_attempt


def wait_for_reruns(
    github: MergeGitHub,
    reruns: dict[str, tuple[str, str, int]],
    *,
    deadline: float,
    poll_interval: float,
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[str, str, int] | None:
    """Wait until every requested rerun attempt has completed."""
    pending = dict(reruns)
    while pending:
        for run_id, context in tuple(pending.items()):
            try:
                info = github.run_info(run_id)
            except MergeError:
                continue
            attempt = info.get("attempt")
            status = str(info.get("status", "")).lower()
            if isinstance(attempt, int) and attempt > context[2] and status == "completed":
                del pending[run_id]
        if not pending:
            return None
        remaining = deadline - monotonic()
        if remaining <= 0:
            return next(iter(pending.values()))
        sleep(min(poll_interval, remaining))
    return None


def merge_release_pr(
    github: MergeGitHub,
    *,
    register_timeout: float = REGISTER_TIMEOUT_SECONDS,
    check_timeout: float = CHECK_TIMEOUT_SECONDS,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> MergeResult:
    pr = github.find_release_pr()
    if pr is None:
        return MergeResult("NO_RELEASE_PR")
    number = pr["number"]
    check_deadline = monotonic() + check_timeout

    first = wait_for_checks(
        github,
        number,
        register_timeout=register_timeout,
        check_timeout=check_timeout,
        poll_interval=poll_interval,
        deadline=check_deadline,
        monotonic=monotonic,
        sleep=sleep,
    )
    if first.status == "never_registered":
        return MergeResult("CHECKS_NEVER_REGISTERED", pr_number=number)
    if first.status == "timeout":
        name, url, attempt = first_red_context(github, first.checks, default_attempt=1)
        return MergeResult("CORE_CUT_RED", name or "checks", url, attempt, number, "check timeout")

    red = [check for check in first.checks if bucket(check) in {"fail", "cancel"}]
    unknown = [
        check
        for check in first.checks
        if bucket(check) not in {"pass", "skipping", "skip", "success", "fail", "cancel"}
    ]
    if not red and not unknown:
        github.merge(number)
        return MergeResult("MERGED", pr_number=number)

    first_failure: tuple[str, str, int] | None = None
    rerunnable: dict[str, tuple[str, str, int]] = {}
    refusal: tuple[str, str, int, str] | None = None
    for check in [*red, *unknown]:
        name = check_name(check)
        run_id, url, attempt, problem = run_context(github, check)
        context = (name, url, attempt or 1)
        if first_failure is None:
            first_failure = context
        if bucket(check) == "cancel":
            if refusal is None:
                refusal = (*context, "cancelled checks are not rerun")
            continue
        if problem:
            if refusal is None:
                refusal = (name, url, attempt or 1, problem)
            continue
        if run_id is None:
            if refusal is None:
                refusal = (name, url, attempt or 1, "check has no rerunnable workflow run")
            continue
        rerunnable.setdefault(run_id, context)

    # One check can report multiple failed jobs from the same workflow. Rerun each
    # workflow attempt once, which reruns only its failed jobs and dependencies.
    requested_reruns: dict[str, tuple[str, str, int]] = {}
    for run_id, context in rerunnable.items():
        try:
            github.rerun_failed(run_id)
            requested_reruns[run_id] = context
        except MergeError as exc:
            if refusal is None:
                refusal = (*context, str(exc))

    if not requested_reruns and refusal is not None:
        name, url, attempt, detail = refusal
        return MergeResult("CORE_CUT_RED", name, url, attempt, number, detail)

    pending_rerun = wait_for_reruns(
        github,
        requested_reruns,
        deadline=check_deadline,
        poll_interval=poll_interval,
        monotonic=monotonic,
        sleep=sleep,
    )
    if pending_rerun is not None:
        name, url, attempt = pending_rerun
        return MergeResult(
            "CORE_CUT_RED", name, url, attempt, number, "rerun attempt did not complete before the deadline"
        )

    second = wait_for_checks(
        github,
        number,
        register_timeout=0,
        check_timeout=check_timeout,
        poll_interval=poll_interval,
        already_registered=True,
        deadline=check_deadline,
        monotonic=monotonic,
        sleep=sleep,
    )
    if second.status == "complete" and all(
        bucket(check) in {"pass", "skipping", "skip", "success"} for check in second.checks
    ) and refusal is None:
        name, url, attempt = first_failure or ("", "", 0)
        github.merge(number)
        return MergeResult("MERGED", name, url, attempt, number, "failed checks recovered")

    name, url, attempt = first_red_context(github, second.checks or first.checks, default_attempt=2)
    detail = refusal[3] if refusal is not None else "checks remained red after one rerun"
    if refusal is not None:
        name, url, attempt = refusal[:3]
    return MergeResult("CORE_CUT_RED", name, url, attempt or 2, number, detail)


def write_result(result: MergeResult) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"outcome={result.outcome}\n")
            output.write(f"failed_check={clean_field(result.failed_check)}\n")
            output.write(f"run_url={clean_field(result.run_url)}\n")

    line = result.typed_line
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(f"{line}\n")
    if result.outcome == "NO_RELEASE_PR":
        print("::notice::no open release-plz release PR found; nothing to merge")
    elif result.outcome in {"CORE_CUT_RED", "CHECKS_NEVER_REGISTERED"}:
        print(f"::error::{line}")
        if result.detail:
            print(result.detail, file=sys.stderr)
    elif result.outcome == "MERGED":
        print(f"merged release PR #{result.pr_number}")


def main() -> int:
    try:
        result = merge_release_pr(GitHubCLI())
    except MergeError as exc:
        result = MergeResult("CORE_CUT_RED", "release-plz", "", 1, detail=str(exc))
    write_result(result)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
