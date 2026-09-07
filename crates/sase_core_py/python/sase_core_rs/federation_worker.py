from __future__ import annotations

import sys

from . import federation_worker_main


def main() -> int:
    federation_worker_main(sys.argv[1:])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
