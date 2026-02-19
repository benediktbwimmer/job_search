#!/usr/bin/env python3
import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run(cmd: list[str]) -> int:
    cp = subprocess.run(cmd, cwd=str(ROOT))
    return int(cp.returncode)


def main():
    parser = argparse.ArgumentParser(description="Run operational scheduler loop for pipeline + weekly digest")
    parser.add_argument("--interval-hours", type=int, default=24, help="Pipeline run interval in hours")
    parser.add_argument("--weekly-interval-hours", type=int, default=168, help="Weekly digest interval in hours")
    parser.add_argument("--sleep-seconds", type=int, default=30, help="Loop poll interval in seconds")
    args = parser.parse_args()

    interval_seconds = max(1, int(args.interval_hours)) * 3600
    weekly_seconds = max(1, int(args.weekly_interval_hours)) * 3600
    sleep_seconds = max(5, int(args.sleep_seconds))

    last_pipeline_at = 0.0
    last_weekly_at = 0.0
    while True:
        now = time.time()
        if now - last_pipeline_at >= interval_seconds:
            print(f"[{datetime.now(timezone.utc).isoformat()}] running pipeline")
            _run(["python3", str(ROOT / "scripts" / "run_pipeline.py")])
            last_pipeline_at = now
        if now - last_weekly_at >= weekly_seconds:
            print(f"[{datetime.now(timezone.utc).isoformat()}] generating weekly ops digest")
            _run(["python3", str(ROOT / "scripts" / "run_weekly_ops.py"), "--skip-pipeline"])
            last_weekly_at = now
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
