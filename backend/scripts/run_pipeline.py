#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from job_search.pipeline import run_pipeline


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        sys.exit(130)
