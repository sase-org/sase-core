"""Console-script entry for the reviewed sudo runner.

The PyO3 binding relaunches detached hops as
`<sys.executable> -I -m sase_core_rs.sudo_runner --internal-root-*`.
Isolated mode keeps a lookalike package in the reviewed working directory,
`PYTHONPATH`, or the user site from being imported.
"""

from __future__ import annotations

import sys

from . import sudo_runner_main


def main() -> int:
    sudo_runner_main(sys.argv[1:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
