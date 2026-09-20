#!/usr/bin/env python3
"""Unit checks for ``pypi_release_files.py``.

The fixtures mirror the live 0.34.48 partial release (three wheels, no ``win_amd64``
wheel and no sdist) and a complete release, in the shape of the PyPI per-version JSON
API's ``urls`` list. Run with ``python3 -m unittest discover -s .github/scripts``.
"""

from __future__ import annotations

import contextlib
import io
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pypi_release_files as gate  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release-plz.yml"

SUFFIXES = [
    "manylinux_2_28_x86_64.whl",
    "manylinux_2_28_aarch64.whl",
    "universal2.whl",
    "win_amd64.whl",
    ".tar.gz",
]

VERSION = "0.34.48"

LINUX_X86 = f"sase_core_rs-{VERSION}-cp312-abi3-manylinux_2_28_x86_64.whl"
LINUX_ARM = f"sase_core_rs-{VERSION}-cp312-abi3-manylinux_2_28_aarch64.whl"
MACOS = (
    f"sase_core_rs-{VERSION}-cp312-abi3-"
    "macosx_10_12_x86_64.macosx_11_0_arm64.macosx_10_12_universal2.whl"
)
WINDOWS = f"sase_core_rs-{VERSION}-cp312-abi3-win_amd64.whl"
SDIST = f"sase_core_rs-{VERSION}.tar.gz"

COMPLETE_NAMES = [LINUX_X86, LINUX_ARM, MACOS, WINDOWS, SDIST]
# What 0.34.48 held after the quota tripped mid-upload.
PARTIAL_NAMES = [MACOS, LINUX_ARM, LINUX_X86]


def urls(names: list[str], *, yanked: tuple[str, ...] = ()) -> list[dict[str, object]]:
    return [{"filename": name, "yanked": name in yanked} for name in names]


class ClassifyTests(unittest.TestCase):
    def classify(self, files: list[dict[str, object]]) -> gate.ReleaseFiles:
        return gate.classify(VERSION, files, SUFFIXES)

    def test_full_set_is_complete(self) -> None:
        result = self.classify(urls(COMPLETE_NAMES))
        self.assertEqual(result, gate.ReleaseFiles(gate.COMPLETE, (), ()))

    def test_partial_release_is_not_published(self) -> None:
        # The bug this gate exists for: the version exists (HTTP 200), so the old
        # existence check called it published and never healed it.
        result = self.classify(urls(PARTIAL_NAMES))
        self.assertEqual(result.state, gate.PARTIAL)
        self.assertEqual(result.missing, ("win_amd64.whl", ".tar.gz"))
        self.assertEqual(result.yanked, ())

    def test_missing_sdist_alone_is_partial(self) -> None:
        result = self.classify(urls([n for n in COMPLETE_NAMES if n != SDIST]))
        self.assertEqual(result.state, gate.PARTIAL)
        self.assertEqual(result.missing, (".tar.gz",))

    def test_single_file_is_partial(self) -> None:
        result = self.classify(urls([LINUX_X86]))
        self.assertEqual(result.state, gate.PARTIAL)
        self.assertEqual(len(result.missing), 4)

    def test_release_without_files_is_absent(self) -> None:
        result = self.classify([])
        self.assertEqual(result.state, gate.ABSENT)
        self.assertEqual(result.missing, tuple(SUFFIXES))

    def test_yanked_file_does_not_count(self) -> None:
        result = self.classify(urls(COMPLETE_NAMES, yanked=(WINDOWS,)))
        self.assertEqual(result.state, gate.PARTIAL)
        self.assertEqual(result.missing, ())
        self.assertEqual(result.yanked, ("win_amd64.whl",))

    def test_extra_files_do_not_break_completeness(self) -> None:
        extra = f"sase_core_rs-{VERSION}-cp312-abi3-musllinux_1_2_x86_64.whl"
        result = self.classify(urls([*COMPLETE_NAMES, extra]))
        self.assertEqual(result.state, gate.COMPLETE)

    def test_other_versions_files_do_not_count(self) -> None:
        # Guards the suffix match against a file list that names another version.
        stray = [n.replace(VERSION, "0.34.47") for n in COMPLETE_NAMES]
        result = self.classify(urls(stray))
        self.assertEqual(result.state, gate.PARTIAL)
        self.assertEqual(len(result.missing), len(SUFFIXES))

    def test_macos_tag_drift_still_matches_universal2(self) -> None:
        drifted = MACOS.replace("macosx_10_12_universal2", "macosx_10_13_universal2")
        names = [LINUX_X86, LINUX_ARM, drifted, WINDOWS, SDIST]
        self.assertEqual(self.classify(urls(names)).state, gate.COMPLETE)


class ParseSuffixesTests(unittest.TestCase):
    def test_reads_whitespace_separated_entries_in_order(self) -> None:
        text = "\n  a.whl\n  b.whl  \n\n  .tar.gz\n"
        self.assertEqual(gate.parse_suffixes(text), ["a.whl", "b.whl", ".tar.gz"])

    def test_empty_set_is_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            gate.parse_suffixes("  \n ")

    def test_duplicate_entry_is_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            gate.parse_suffixes("a.whl b.whl a.whl")

    def test_unset_environment_is_rejected(self) -> None:
        with mock.patch.dict(os.environ, clear=True):
            with self.assertRaises(SystemExit):
                gate.suffixes_from_env()


class DistTests(unittest.TestCase):
    def problems(self, names: list[str]) -> list[str]:
        return gate.dist_problems(sorted(names), VERSION, SUFFIXES)

    def test_exact_set_has_no_problems(self) -> None:
        self.assertEqual(self.problems(COMPLETE_NAMES), [])

    def test_missing_build_is_reported(self) -> None:
        problems = self.problems([n for n in COMPLETE_NAMES if n != WINDOWS])
        self.assertEqual(problems, ["no file matches *win_amd64.whl"])

    def test_unlisted_build_is_reported(self) -> None:
        # A matrix entry added without updating the expected set.
        extra = f"sase_core_rs-{VERSION}-cp312-abi3-musllinux_1_2_x86_64.whl"
        problems = self.problems([*COMPLETE_NAMES, extra])
        self.assertEqual(problems, [f"{extra} matches no expected entry"])

    def test_duplicate_match_is_reported(self) -> None:
        second = LINUX_X86.replace("cp312-abi3", "cp313-abi3")
        problems = self.problems([*COMPLETE_NAMES, second])
        self.assertEqual(len(problems), 1)
        self.assertIn("2 files match *manylinux_2_28_x86_64.whl", problems[0])

    def test_cli_reports_and_fails_on_incomplete_dist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for name in PARTIAL_NAMES:
                (Path(tmp) / name).touch()
            stderr = io.StringIO()
            env = {gate.SUFFIXES_ENV: " ".join(SUFFIXES)}
            with mock.patch.dict(os.environ, env):
                with contextlib.redirect_stderr(stderr):
                    code = gate.main(["dist", tmp, VERSION])
        self.assertEqual(code, 1)
        self.assertIn("no file matches *win_amd64.whl", stderr.getvalue())
        self.assertIn("no file matches *.tar.gz", stderr.getvalue())


class StatusCliTests(unittest.TestCase):
    def run_status(self, files: list[dict[str, object]] | None) -> tuple[str, str]:
        stdout, stderr = io.StringIO(), io.StringIO()
        env = {gate.SUFFIXES_ENV: " ".join(SUFFIXES)}
        with mock.patch.dict(os.environ, env):
            with mock.patch.object(gate, "fetch_release_files", return_value=files):
                with contextlib.redirect_stdout(stdout):
                    with contextlib.redirect_stderr(stderr):
                        self.assertEqual(gate.main(["status", VERSION]), 0)
        return stdout.getvalue(), stderr.getvalue()

    def test_partial_prints_state_and_names_missing_files(self) -> None:
        out, err = self.run_status(urls(PARTIAL_NAMES))
        self.assertEqual(out, "partial\n")
        self.assertIn("missing *win_amd64.whl", err)
        self.assertIn("missing *.tar.gz", err)

    def test_complete_prints_only_the_state(self) -> None:
        out, err = self.run_status(urls(COMPLETE_NAMES))
        self.assertEqual((out, err), ("complete\n", ""))

    def test_unknown_version_prints_absent(self) -> None:
        out, _ = self.run_status(None)
        self.assertEqual(out, "absent\n")


class WorkflowWiringTests(unittest.TestCase):
    """The set the workflow really declares classifies the real 0.34.48 shape."""

    def workflow_suffixes(self) -> list[str]:
        text = WORKFLOW.read_text()
        match = re.search(
            r"^  EXPECTED_DIST_SUFFIXES: \|\n((?:    \S.*\n)+)", text, re.MULTILINE
        )
        self.assertIsNotNone(match, "EXPECTED_DIST_SUFFIXES block not found")
        assert match is not None
        return gate.parse_suffixes(match.group(1))

    def test_workflow_set_rejects_partial_and_accepts_complete(self) -> None:
        suffixes = self.workflow_suffixes()
        self.assertEqual(len(suffixes), 5)
        partial = gate.classify(VERSION, urls(PARTIAL_NAMES), suffixes)
        complete = gate.classify(VERSION, urls(COMPLETE_NAMES), suffixes)
        self.assertEqual(partial.state, gate.PARTIAL)
        self.assertEqual(complete.state, gate.COMPLETE)

    def test_workflow_no_longer_gates_on_version_existence(self) -> None:
        text = WORKFLOW.read_text()
        self.assertNotIn('print("published")', text)
        self.assertIn("pypi_release_files.py status", text)


if __name__ == "__main__":
    unittest.main()
