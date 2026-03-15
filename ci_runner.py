#!/usr/bin/env python3
"""
ci_runner.py
============
Lightweight CI test runner with toggles for Redis, Docker, and load stage.
Fully cross-platform: Linux, macOS, Windows.

USAGE
-----
    # Minimal CI (no Redis, no Docker, smoke run only)
    python ci_runner.py

    # With Redis integration tests
    python ci_runner.py --redis

    # With Docker integration tests
    python ci_runner.py --docker

    # Full suite + 10k smoke run
    python ci_runner.py --redis --docker --smoke-stage=10k

    # Skip slow timeout tests (recommended for fast CI)
    python ci_runner.py --no-slow

EXIT CODES
----------
    0 = all tests passed + smoke run within thresholds
    1 = test failures or threshold violations

CROSS-PLATFORM NOTES
---------------------
- All subprocess commands use sys.executable so the runner works on Windows
  where 'python3' may not be in PATH.
- Platform-specific tests (rlimit, SIGKILL, forkserver) are automatically
  skipped on Windows via @unittest.skipIf / pytest.mark.skipif decorators
  in the test files themselves -- no special handling needed here.
- The run_tests.py zero-dependency runner is preferred for environments
  without pytest; it covers the same test surface.

RESOURCE-LIMITED CI GUIDANCE
------------------------------
- Use --no-slow to skip timeout tests (each sleeps 1-2 s per kill cycle)
- Use --no-smoke to skip the load harness entirely
- Full 10M runs require >= 16 GB RAM and dedicated perf environments
- Scale workers to CI resources: --workers=4 for 2-core runners

EXPECTED OUTPUTS
----------------
All tests pass (pytest path):
    ===== N passed in Xs =====

Smoke run:
    [PASS] rss_mb < HWM: 45.2 MB < 2048 MB
    [PASS] log_queue < 80%: 0 < 80000
    [PASS] throughput > 1000 tps: XXXX tps > 1000
    [PASS] no exceptions: exceptions=0
    READY-FOR-REVIEW: All thresholds passed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# Detect platform for informational banner only; test skips are
# handled per-test via platform_compat flags.
_IS_WINDOWS = sys.platform == "win32"
_PLATFORM_LABEL = "Windows" if _IS_WINDOWS else "POSIX"


def run(cmd: list[str], check: bool = True) -> int:
    """Run a command, print it, and return the exit code."""
    # Display the command with the executable shortened for readability
    display = [Path(cmd[0]).name if cmd[0] == sys.executable else cmd[0]] + cmd[1:]
    print(f"\n$ {' '.join(display)}")
    result = subprocess.run(cmd, cwd=str(Path(__file__).parent))
    if check and result.returncode != 0:
        print(f"FAILED (exit {result.returncode})")
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Aequitas CI runner (cross-platform)")
    parser.add_argument("--redis",     action="store_true", help="Run Redis integration tests")
    parser.add_argument("--docker",    action="store_true", help="Run Docker integration tests")
    parser.add_argument("--no-slow",   action="store_true", help="Skip slow timeout tests")
    parser.add_argument("--no-smoke",  action="store_true", help="Skip smoke load run")
    parser.add_argument("--use-run-tests", action="store_true",
                        help="Use run_tests.py (zero-dep) instead of pytest")
    parser.add_argument("--smoke-stage", default="10k",
                        choices=["10k", "100k", "1m", "10m"])
    parser.add_argument("--workers", type=int, default=8,
                        help="Worker count for smoke run")
    args = parser.parse_args()

    print("=" * 62)
    print(f"Aequitas CI Runner  [{_PLATFORM_LABEL}]")
    print(f"  Python   : {sys.version.split()[0]}  ({sys.executable})")
    print(f"  redis    : {args.redis}   docker : {args.docker}")
    print(f"  slow     : {not args.no_slow}   smoke  : {not args.no_smoke} ({args.smoke_stage})")
    print(f"  workers  : {args.workers}")
    print("=" * 62)

    failures = []

    # -----------------------------------------------------------------------
    # Step 1: Core unit tests
    # -----------------------------------------------------------------------
    print("\n[1/4] Core unit tests (no external dependencies)")

    if args.use_run_tests:
        # Zero-dependency path — works everywhere without pytest installed
        cmd = [sys.executable, "run_tests.py", "-v"]
        rc = run(cmd, check=False)
        if rc != 0:
            failures.append(f"run_tests.py failed (exit {rc})")
    else:
        markers = "not redis and not docker and not integration"
        if args.no_slow:
            markers += " and not slow"

        cmd = [
            sys.executable, "-m", "pytest",
            "tests/test_config.py",
            "tests/test_dispatcher.py",
            "tests/test_escalation.py",
            "tests/test_logging_backpressure.py",
            "tests/test_memory_gc.py",
            "tests/test_subprocess_wrapper.py",
            f"-m={markers}",
            "-v",
        ]
        rc = run(cmd, check=False)
        if rc != 0:
            failures.append(f"Core unit tests failed (exit {rc})")

    # -----------------------------------------------------------------------
    # Step 2: Container pool tests (mocked Docker)
    # -----------------------------------------------------------------------
    if not args.use_run_tests:
        print("\n[2/4] Container pool tests (mocked)")
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/test_container_pool.py",
            "-m=not docker",
            "-v",
        ]
        rc = run(cmd, check=False)
        if rc != 0:
            failures.append(f"Container pool tests failed (exit {rc})")
    else:
        print("\n[2/4] Container pool tests — skipped (run_tests.py mode)")

    # -----------------------------------------------------------------------
    # Step 3: Integration tests (optional)
    # -----------------------------------------------------------------------
    if args.redis and not args.use_run_tests:
        print("\n[3a/4] Redis integration tests")
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/test_escalation.py",
            "-m=redis",
            "-v",
        ]
        rc = run(cmd, check=False)
        if rc != 0:
            failures.append(f"Redis integration tests failed (exit {rc})")

    if args.docker and not args.use_run_tests:
        print("\n[3b/4] Docker integration tests")
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/test_container_pool.py",
            "-m=docker",
            "-v",
        ]
        rc = run(cmd, check=False)
        if rc != 0:
            failures.append(f"Docker integration tests failed (exit {rc})")

    # -----------------------------------------------------------------------
    # Step 4: Smoke load run
    # -----------------------------------------------------------------------
    if not args.no_smoke:
        print(f"\n[4/4] Smoke load run — stage={args.smoke_stage} [{_PLATFORM_LABEL}]")
        cmd = [
            sys.executable, "-m", "harness.run",
            f"--stage={args.smoke_stage}",
            "--no-redis",
            "--no-docker",
            f"--workers={args.workers}",
            "--task-sleep-ms=0",
        ]
        rc = run(cmd, check=False)
        if rc != 0:
            failures.append(f"Smoke run ({args.smoke_stage}) failed (exit {rc})")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print("\n" + "=" * 62)
    if failures:
        print("CI FAILED:")
        for f in failures:
            print(f"   - {f}")
        sys.exit(1)
    else:
        print("CI PASSED — all checks green")
        sys.exit(0)


if __name__ == "__main__":
    main()
