#!/usr/bin/env python3
"""Rerun jobs GitHub never assigned a runner to, once per workflow run.

A workflow run whose only red jobs never got a hosted runner is always safe to
rerun: no step of those jobs ran, so nothing was uploaded, tagged, or
published. This is what happened to Release-plz run 40346929251 for v0.42.7:
its only red job (``macos universal2``) was queued at 15:12:31Z and cancelled
at 15:27:35Z without ever starting (``runner_id: 0``, ``runner_name: ""``,
``steps: []``), while the other four builds succeeded. The run concluded
``failure``, ``twine check`` and ``publish to PyPI`` were skipped, and the
version stayed unpublished for 4.5+ hours until an unrelated push healed it.
The publish gate's next-push/six-hourly heal remains the backstop; this script
just shortens that wait to minutes for the infrastructure-only case.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Protocol


# Only attempt 1 is ever rerun. The cap prevents rerun loops: a rerun that is
# itself starved is left for the scheduled heal, never rerun a second time.
MAX_ATTEMPTS = 2

RED_CONCLUSIONS = frozenset({"failure", "cancelled", "timed_out"})


class StarvationError(Exception):
    """A GitHub CLI call, response, or environment value could not be trusted."""


class StarvationGitHub(Protocol):
    def list_jobs(self, run_id: str, attempt: int) -> list[dict[str, Any]]: ...

    def rerun_failed(self, run_id: str) -> None: ...


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
            raise StarvationError(f"could not run gh {' '.join(args)}: {exc}") from exc

    def list_jobs(self, run_id: str, attempt: int) -> list[dict[str, Any]]:
        result = self._run(
            [
                "api",
                "--paginate",
                f"repos/{os.environ.get('GH_REPO', '')}"
                f"/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100",
                "--jq",
                ".jobs[]",
            ]
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"gh exited {result.returncode}"
            raise StarvationError(f"could not list jobs for run {run_id} attempt {attempt}: {detail}")
        jobs: list[dict[str, Any]] = []
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            try:
                job = json.loads(line)
            except json.JSONDecodeError as exc:
                raise StarvationError(
                    f"gh api returned malformed job JSON for run {run_id} attempt {attempt}"
                ) from exc
            if not isinstance(job, dict):
                raise StarvationError(
                    f"gh api returned a non-object job for run {run_id} attempt {attempt}"
                )
            jobs.append(job)
        return jobs

    def rerun_failed(self, run_id: str) -> None:
        result = self._run(["run", "rerun", run_id, "--failed"])
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"gh exited {result.returncode}"
            raise StarvationError(f"could not rerun failed jobs in run {run_id}: {detail}")


def is_starved(job: dict[str, Any]) -> bool:
    """A job is starved only when every signal says no runner ever picked it up."""
    if str(job.get("conclusion", "")).lower() not in RED_CONCLUSIONS:
        return False
    if job.get("steps"):
        return False
    if job.get("runner_id"):
        return False
    return not job.get("runner_name")


@dataclass(frozen=True)
class StarvationDecision:
    action: str  # "rerun" or "skip"
    reason: str
    starved: tuple[str, ...] = ()

    @property
    def should_rerun(self) -> bool:
        return self.action == "rerun"


def job_name(job: dict[str, Any]) -> str:
    name = job.get("name")
    return name if isinstance(name, str) and name else "unknown"


def decide(
    jobs: list[dict[str, Any]],
    *,
    run_conclusion: str,
    run_attempt: int,
) -> StarvationDecision:
    if run_conclusion != "failure":
        return StarvationDecision(
            "skip",
            f"run concluded {run_conclusion or 'unknown'}; "
            "a cancelled run means a person or a concurrency group cancelled it",
        )
    if run_attempt >= MAX_ATTEMPTS:
        return StarvationDecision(
            "skip",
            f"run attempt {run_attempt} is at the retry cap "
            f"(MAX_ATTEMPTS={MAX_ATTEMPTS}); the scheduled heal stays the backstop",
        )
    red = [job for job in jobs if str(job.get("conclusion", "")).lower() in RED_CONCLUSIONS]
    if not red:
        return StarvationDecision("skip", "no red jobs in this attempt")
    starved = [job for job in red if is_starved(job)]
    if not starved:
        return StarvationDecision("skip", "no red job was starved of a runner")
    real = [job for job in red if not is_starved(job)]
    if real:
        names = ", ".join(sorted(job_name(job) for job in real))
        return StarvationDecision(
            "skip",
            f"not infrastructure-only: {names} ran and failed, "
            "and a rerun would also rerun that real failure",
        )
    names = tuple(job_name(job) for job in starved)
    return StarvationDecision(
        "rerun",
        f"every red job never got a runner ({len(names)} starved)",
        names,
    )


@dataclass(frozen=True)
class StarvationConfig:
    repo: str
    run_id: str
    run_attempt: int
    run_conclusion: str

    @property
    def run_url(self) -> str:
        return f"https://github.com/{self.repo}/actions/runs/{self.run_id}"


def read_config(env: dict[str, str] | None = None) -> StarvationConfig:
    source = env if env is not None else os.environ
    repo = (source.get("GH_REPO") or "").strip()
    if not repo or "/" not in repo:
        raise StarvationError("GH_REPO must be set to owner/repo")
    if not (source.get("GH_TOKEN") or "").strip():
        raise StarvationError("GH_TOKEN must be set for gh authentication")
    run_id = (source.get("RUN_ID") or "").strip()
    if not run_id.isdigit():
        raise StarvationError("RUN_ID must be set to a numeric workflow run id")
    raw_attempt = (source.get("RUN_ATTEMPT") or "").strip()
    try:
        run_attempt = int(raw_attempt)
    except ValueError as exc:
        raise StarvationError("RUN_ATTEMPT must be set to an integer run attempt") from exc
    if run_attempt < 1:
        raise StarvationError("RUN_ATTEMPT must be a positive integer")
    run_conclusion = (source.get("RUN_CONCLUSION") or "").strip().lower()
    if not run_conclusion:
        raise StarvationError("RUN_CONCLUSION must be set to the workflow run conclusion")
    return StarvationConfig(
        repo=repo,
        run_id=run_id,
        run_attempt=run_attempt,
        run_conclusion=run_conclusion,
    )


def summarize(config: StarvationConfig, decision: StarvationDecision) -> str:
    if decision.starved:
        jobs = f"; starved jobs: {', '.join(decision.starved)}"
    else:
        jobs = ""
    return f"{decision.action}: {decision.reason}{jobs}; run: {config.run_url}"


def write_summary(line: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(f"{line}\n")


def run(github: StarvationGitHub, config: StarvationConfig) -> int:
    try:
        jobs = github.list_jobs(config.run_id, config.run_attempt)
    except StarvationError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    decision = decide(
        jobs,
        run_conclusion=config.run_conclusion,
        run_attempt=config.run_attempt,
    )
    write_summary(summarize(config, decision))
    if not decision.should_rerun:
        print(summarize(config, decision))
        return 0
    try:
        github.rerun_failed(config.run_id)
    except StarvationError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    print(f"::notice::reran run {config.run_id}: {decision.reason}")
    return 0


def main() -> int:
    try:
        config = read_config()
    except StarvationError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    return run(GitHubCLI(), config)


if __name__ == "__main__":
    raise SystemExit(main())
