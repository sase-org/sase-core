#!/usr/bin/env python3
"""Unit checks for ``pypi_quota.py``.

Fixtures use the PyPI project JSON shape (``releases`` -> version -> files with
``filename`` and ``size``). Run with
``python3 -m unittest discover -s .github/scripts``.
"""

from __future__ import annotations

import contextlib
import io
import os
import re
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pypi_quota as quota  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release-plz.yml"
RETENTION = REPO_ROOT / ".github" / "scripts" / "pypi_retention.py"

MB = 1_000_000
LIMIT = 1000 * MB


def release_files(version: str, sizes: list[int]) -> list[dict[str, object]]:
    return [
        {"filename": f"sase_core_rs-{version}-{i}.whl", "size": size}
        for i, size in enumerate(sizes)
    ]


def project(used_mb: int, versions: int = 4) -> dict[str, object]:
    """A project of ``versions`` equal releases totalling ``used_mb`` MB."""
    each = used_mb * MB // versions
    return {
        "releases": {
            f"0.1.{n}": release_files(f"0.1.{n}", [each // 2, each - each // 2])
            for n in range(versions)
        }
    }


def dist_of(size_mb: int, version: str = "0.2.0") -> dict[str, int]:
    return {f"sase_core_rs-{version}-a.whl": size_mb * MB}


class MeasureTests(unittest.TestCase):
    def test_used_sums_every_file_and_average_uses_recent_releases(self) -> None:
        q = quota.measure(project(400), dist_of(75), LIMIT)
        self.assertEqual(q.used, 400 * MB)
        self.assertEqual(q.incoming, 75 * MB)
        self.assertEqual(q.average_release, 100 * MB)
        self.assertEqual(q.projected, 475 * MB)
        self.assertEqual(q.overflow, 0)
        self.assertEqual(q.releases_remaining, 5)

    def test_files_pypi_already_holds_are_not_incoming(self) -> None:
        proj = project(400)
        held = release_files("0.2.0", [30 * MB])
        proj["releases"]["0.2.0"] = held  # type: ignore[index]
        dist = {held[0]["filename"]: 30 * MB, "sase_core_rs-0.2.0-new.whl": 20 * MB}
        q = quota.measure(proj, dist, LIMIT)  # type: ignore[arg-type]
        self.assertEqual(q.incoming, 20 * MB)

    def test_overflow_is_the_excess_over_the_limit(self) -> None:
        q = quota.measure(project(980), dist_of(75), LIMIT)
        self.assertEqual(q.overflow, 55 * MB)
        self.assertEqual(q.releases_remaining, 0)

    def test_exactly_at_the_limit_fits(self) -> None:
        q = quota.measure(project(925), dist_of(75), LIMIT)
        self.assertEqual(q.overflow, 0)
        self.assertEqual(q.free_after, 0)

    def test_empty_project_has_no_average(self) -> None:
        q = quota.measure({"releases": {"0.1.0": []}}, dist_of(75), LIMIT)
        self.assertEqual(q.used, 0)
        self.assertIsNone(q.releases_remaining)


class MessageTests(unittest.TestCase):
    def test_overflow_message_names_numbers_and_remedy(self) -> None:
        q = quota.measure(project(980), dist_of(75), LIMIT)
        text = quota.overflow_message(q)
        for expected in (
            f"{q.used:,}",
            f"{q.incoming:,}",
            f"{q.limit:,}",
            f"{q.overflow:,}",
            "docs/pypi-retention.md",
        ):
            self.assertIn(expected, text)


class CheckCommandTests(unittest.TestCase):
    def run_check(
        self,
        proj: object,
        dist_mb: int,
        *,
        limit: str = str(LIMIT),
        summary: bool = True,
    ) -> tuple[int, str, str, str]:
        with tempfile.TemporaryDirectory() as tmp:
            dist = Path(tmp) / "dist"
            dist.mkdir()
            (dist / "sase_core_rs-0.2.0-a.whl").write_bytes(b"x" * (dist_mb * 1024))
            summary_path = Path(tmp) / "summary.md"
            env = {quota.LIMIT_ENV: limit}
            if summary:
                env[quota.SUMMARY_ENV] = str(summary_path)
            out, err = io.StringIO(), io.StringIO()
            fetch = (
                mock.Mock(side_effect=proj)
                if isinstance(proj, BaseException)
                else mock.Mock(return_value=proj)
            )
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch.object(quota, "fetch_project", fetch),
                contextlib.redirect_stdout(out),
                contextlib.redirect_stderr(err),
            ):
                code = quota.main(["check", str(dist), "--version", "0.2.0"])
            text = summary_path.read_text() if summary and summary_path.exists() else ""
            return code, out.getvalue(), err.getvalue(), text

    def test_over_quota_fails_with_numbers_before_upload(self) -> None:
        # 10 MiB incoming (10240 KiB) into a project 1 MB under the limit.
        code, _, err, summary = self.run_check(project(999), 10 * 1024, limit=str(LIMIT))
        self.assertEqual(code, 1)
        self.assertIn("::error::", err)
        self.assertIn("overflow", err)
        self.assertIn("Upload refused", summary)

    def test_normal_publish_writes_headroom_to_summary(self) -> None:
        code, out, _, summary = self.run_check(project(400), 75 * 1024)
        self.assertEqual(code, 0)
        self.assertIn("PyPI quota ok", out)
        self.assertIn("Free after upload", summary)
        self.assertIn("more releases fit", summary)
        self.assertNotIn("refused", summary)

    def test_pypi_api_error_is_not_fatal(self) -> None:
        code, _, err, summary = self.run_check(urllib.error.URLError("boom"), 75 * 1024)
        self.assertEqual(code, 0)
        self.assertIn("::warning::", err)
        self.assertEqual(summary, "")

    def test_missing_limit_is_an_error(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(SystemExit):
                quota.limit_from_env()

    def test_non_integer_limit_is_an_error(self) -> None:
        with mock.patch.dict(os.environ, {quota.LIMIT_ENV: "10GB"}, clear=True):
            with self.assertRaises(SystemExit):
                quota.limit_from_env()


class WorkflowWiringTests(unittest.TestCase):
    def workflow_limit(self) -> int:
        match = re.search(
            r'^  PYPI_PROJECT_LIMIT_BYTES: "(\d+)"$', WORKFLOW.read_text(), re.MULTILINE
        )
        self.assertIsNotNone(match, "PYPI_PROJECT_LIMIT_BYTES not found in workflow")
        assert match is not None
        return int(match.group(1))

    def test_workflow_limit_is_ten_gib(self) -> None:
        self.assertEqual(self.workflow_limit(), 10 * 1024**3)

    def test_retention_tool_agrees_with_workflow_limit(self) -> None:
        # pypi_retention.py needs `packaging`, so read its constant from source.
        match = re.search(
            r"^PROJECT_LIMIT_BYTES = (.+)$", RETENTION.read_text(), re.MULTILINE
        )
        self.assertIsNotNone(match)
        assert match is not None
        self.assertEqual(eval(match.group(1), {}), self.workflow_limit())

    def test_guard_runs_before_publish_step(self) -> None:
        text = WORKFLOW.read_text()
        guard = text.index("pypi_quota.py check dist")
        publish = text.index("uses: pypa/gh-action-pypi-publish")
        self.assertLess(guard, publish)


if __name__ == "__main__":
    unittest.main()
