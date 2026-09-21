"""Console-script entry for the reviewed sudo runner.

The PyO3 binding relaunches detached hops as
`<sys.executable> -I -m sase_core_rs.sudo_runner --internal-root-*`.
Isolated mode keeps a lookalike package in the reviewed working directory,
`PYTHONPATH`, or the user site from being imported.

Detached execution requires Linux procfs and is unavailable on other
platforms, where `--capabilities` reports an empty capability list and
detach requests fail with an explicit unsupported-platform error.
Synchronous manifest execution works everywhere.
"""

from __future__ import annotations

import sys

from . import sudo_runner_main


def main() -> int:
    sudo_runner_main(sys.argv[1:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
