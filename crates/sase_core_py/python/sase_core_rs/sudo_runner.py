from __future__ import annotations

import sys

from . import sudo_runner_main


def main() -> int:
    sudo_runner_main(sys.argv[1:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
