"""
aequitas/subprocess_wrapper.py
==============================
Secure Popen wrapper enforcing soft/hard timeouts, output streaming,
and per-process resource limits.

CROSS-PLATFORM DESIGN
----------------------
This module runs identically on Linux, macOS, and Windows.  All
platform-specific behaviour is isolated behind guards imported from
`platform_compat`:

  HAVE_RESOURCE   -> resource.setrlimit calls (Linux/macOS only)
  HAVE_FORK       -> preexec_fn is only valid when the OS can fork(2)
  HAVE_SIGKILL    -> SIGKILL integer (POSIX); Windows uses TerminateProcess
  IS_WINDOWS      -> close_fds semantics, signal names in log messages

SECURITY PRINCIPLES
--------------------
1. shell=False everywhere -- eliminates shell injection surface.
2. Command must be a list[str] -- no string interpolation.
3. Environment is a pre-sanitised template + per-task patches only
   (NOT os.environ.copy() per task).
4. Resource limits via resource.setrlimit on POSIX; graceful no-op on Windows.
5. File descriptor cleanup: stdout/stderr temp files closed promptly after read.

TIMEOUT ENFORCEMENT
--------------------
All platforms:
  soft kill at soft_kill_timeout_s -> proc.terminate()
  hard kill at hard_kill_timeout_s -> proc.kill()

POSIX: SIGTERM / SIGKILL.
Windows: both map to TerminateProcess() (unconditional).
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from .platform_compat import (
    HAVE_FORK,
    HAVE_RESOURCE,
    HAVE_SIGKILL,
    IS_WINDOWS,
    SIGKILL,
    SIGTERM,
    resource_mod,
    safe_fsync,
)

logger = logging.getLogger(__name__)

# Platform-appropriate sentinel values recorded in SubprocessResult.kill_signal.
# Callers inspect these rather than importing signal.SIGKILL directly.
_SOFT_KILL_SENTINEL: int | str = SIGTERM if SIGTERM is not None else "SIGTERM"
_HARD_KILL_SENTINEL: int | str = SIGKILL if SIGKILL is not None else "SIGKILL"


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class SubprocessResult:
    """
    Outcome of a subprocess execution.

    Fields
    ------
    returncode   : Process exit code (None if Popen itself failed).
    stdout_bytes : stdout content if <= stream_threshold_bytes, else None.
    stderr_bytes : stderr content if <= stream_threshold_bytes, else None.
    stdout_path  : Path to tempfile if stdout exceeded threshold.
    stderr_path  : Path to tempfile if stderr exceeded threshold.
    wall_time_s  : Total elapsed wall-clock time in seconds.
    timed_out    : True if process was killed by soft or hard timeout.
    kill_signal  : Signal number (POSIX) or sentinel string (Windows).
    platform     : "posix" or "windows" -- for audit log enrichment.
    """
    returncode: int | None
    stdout_bytes: bytes | None
    stderr_bytes: bytes | None
    stdout_path: Path | None
    stderr_path: Path | None
    wall_time_s: float
    timed_out: bool = False
    kill_signal: int | str | None = None
    platform: str = field(default_factory=lambda: "windows" if IS_WINDOWS else "posix")


# ---------------------------------------------------------------------------
# Wrapper
# ---------------------------------------------------------------------------

class SubprocessWrapper:
    """
    Secure, resource-limited subprocess executor.  Linux / macOS / Windows.

    Thread-safe: stateless after construction; run() may be called
    concurrently from any number of threads.
    """

    def __init__(self, cfg_subprocess, env_template: dict[str, str]) -> None:
        self._cfg = cfg_subprocess
        self._env_template = env_template  # shared read-only; never mutate

    def run(
        self,
        cmd: list[str],
        task_env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> SubprocessResult:
        """
        Execute cmd with full lifecycle management.

        cmd must be a non-empty list[str].  shell=False is enforced unconditionally.
        """
        assert isinstance(cmd, list) and cmd, "cmd must be a non-empty list[str]"
        assert not any(
            isinstance(c, str) and " " in c and c.startswith("$") for c in cmd
        ), "Potential shell substitution in cmd"

        env = dict(self._env_template)
        if task_env:
            env.update(task_env)

        start = time.monotonic()
        stdout_tf = tempfile.TemporaryFile(prefix="aq_stdout_")
        stderr_tf = tempfile.TemporaryFile(prefix="aq_stderr_")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=stdout_tf,
                stderr=stderr_tf,
                env=env,
                cwd=cwd,
                close_fds=True,
                shell=False,           # NEVER shell=True
                # preexec_fn is POSIX-only; passing it on Windows raises ValueError.
                preexec_fn=self._make_preexec() if HAVE_FORK else None,
            )
        except (OSError, ValueError) as exc:
            stdout_tf.close()
            stderr_tf.close()
            logger.warning("Popen failed cmd=%s: %s", cmd[0], exc)
            return SubprocessResult(
                returncode=None,
                stdout_bytes=None,
                stderr_bytes=None,
                stdout_path=None,
                stderr_path=None,
                wall_time_s=time.monotonic() - start,
            )

        # Windows: attempt Job Object limits post-CreateProcess
        if IS_WINDOWS:
            self._apply_windows_limits(proc)

        kill_result: dict[str, object] = {"signal": None, "timed_out": False}
        watchdog = threading.Thread(
            target=self._timeout_watchdog,
            args=(proc, kill_result),
            daemon=True,
        )
        watchdog.start()
        proc.wait()
        watchdog.join(timeout=1.0)

        wall_time = time.monotonic() - start
        stdout_bytes, stdout_path = self._read_output(stdout_tf, "stdout")
        stderr_bytes, stderr_path = self._read_output(stderr_tf, "stderr")

        for tf in (stdout_tf, stderr_tf):
            try:
                tf.close()
            except Exception:  # noqa: BLE001
                pass

        return SubprocessResult(
            returncode=proc.returncode,
            stdout_bytes=stdout_bytes,
            stderr_bytes=stderr_bytes,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            wall_time_s=wall_time,
            timed_out=kill_result["timed_out"],
            kill_signal=kill_result["signal"],
        )

    # ------------------------------------------------------------------
    # Timeout watchdog
    # ------------------------------------------------------------------

    def _timeout_watchdog(self, proc: subprocess.Popen, result: dict) -> None:
        """
        Background thread enforcing soft and hard kill timeouts.

        POSIX: SIGTERM (soft) -> SIGKILL (hard).
        Windows: TerminateProcess for both (no SIGTERM equivalent in Win32 API).
        """
        soft = self._cfg.soft_kill_timeout_s
        hard = self._cfg.hard_kill_timeout_s
        deadline_soft = time.monotonic() + soft
        deadline_hard = time.monotonic() + hard

        while True:
            if proc.poll() is not None:
                return

            now = time.monotonic()

            if now >= deadline_hard:
                try:
                    proc.kill()
                    result["signal"] = _HARD_KILL_SENTINEL
                    result["timed_out"] = True
                    logger.warning(
                        "Hard kill sent to pid=%d (%s, timeout=%ds)",
                        proc.pid,
                        "SIGKILL" if HAVE_SIGKILL else "TerminateProcess",
                        hard,
                    )
                except ProcessLookupError:
                    pass
                return

            if now >= deadline_soft and result["signal"] is None:
                try:
                    proc.terminate()
                    result["signal"] = _SOFT_KILL_SENTINEL
                    result["timed_out"] = True
                    logger.warning(
                        "Soft kill sent to pid=%d (%s, timeout=%ds)",
                        proc.pid,
                        "SIGTERM" if not IS_WINDOWS else "TerminateProcess",
                        soft,
                    )
                except ProcessLookupError:
                    return

            time.sleep(0.1)

    # ------------------------------------------------------------------
    # Resource limits — POSIX
    # ------------------------------------------------------------------

    def _make_preexec(self):
        """
        Return a preexec_fn for POSIX resource limits (called after fork, before exec).

        Returns a no-op lambda when resource module is unavailable (rare, but
        guards against stripped Python builds).

        NOTE: preexec_fn must NEVER be passed on Windows; the caller conditionally
        passes `self._make_preexec() if HAVE_FORK else None`.
        """
        if not HAVE_RESOURCE:
            return lambda: None  # no-op: best effort on stripped POSIX builds

        cpu_s: int = self._cfg.rlimit_cpu_s
        as_bytes: int = self._cfg.rlimit_as_mb * 1024 * 1024

        def _preexec() -> None:
            resource_mod.setrlimit(resource_mod.RLIMIT_CPU, (cpu_s, cpu_s + 5))
            resource_mod.setrlimit(resource_mod.RLIMIT_AS,  (as_bytes, as_bytes))
            resource_mod.setrlimit(resource_mod.RLIMIT_CORE, (0, 0))

        return _preexec

    # ------------------------------------------------------------------
    # Resource limits — Windows
    # ------------------------------------------------------------------

    def _apply_windows_limits(self, proc: subprocess.Popen) -> None:
        """
        Apply resource limits to a Windows process via Job Objects (stub).

        TODO: implement ctypes-based Job Object assignment.
        Sketch:
            job = ctypes.windll.kernel32.CreateJobObjectW(None, None)
            # set JOBOBJECT_EXTENDED_LIMIT_INFORMATION with ProcessMemoryLimit
            ctypes.windll.kernel32.AssignProcessToJobObject(job, proc._handle)
        See: https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects
        """
        logger.debug(
            "Windows Job Object limits not yet implemented; "
            "watchdog provides wall-clock enforcement for pid=%d", proc.pid
        )

    # ------------------------------------------------------------------
    # Output handling
    # ------------------------------------------------------------------

    def _read_output(self, tf, name: str) -> tuple[bytes | None, Path | None]:
        """
        Read output from temp file.  Small outputs returned as bytes;
        large outputs persisted to a named temp file and path returned.
        """
        threshold = self._cfg.stream_threshold_bytes
        try:
            tf.seek(0, 2)
            size = tf.tell()
            tf.seek(0)

            if size <= threshold:
                return tf.read(), None

            named = tempfile.NamedTemporaryFile(
                prefix=f"aq_{name}_", suffix=".bin", delete=False
            )
            while True:
                chunk = tf.read(65_536)
                if not chunk:
                    break
                named.write(chunk)
            named.flush()
            safe_fsync(named.fileno())
            named.close()
            return None, Path(named.name)

        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read %s output: %s", name, exc)
            return None, None
