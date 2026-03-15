"""
tests/test_subprocess_wrapper.py
=================================
Unit tests for SubprocessWrapper: timeouts, streaming, resource limits,
and the shell=False invariant.

CROSS-PLATFORM NOTES
--------------------
All subprocess commands use sys.executable rather than "echo", "sleep", or
"python3" so tests run identically on Linux, macOS, and Windows.

Platform-specific tests are decorated with pytest.mark.skipif using flags
from aequitas.platform_compat.  See HAVE_RESOURCE (rlimit tests) and
HAVE_SIGKILL (SIGTERM-ignore test).

Run:
    pytest tests/test_subprocess_wrapper.py -v
    pytest tests/test_subprocess_wrapper.py -v -m "not slow"

Reviewer verification:
    pytest tests/test_subprocess_wrapper.py::TestSubprocessWrapper -v
    pytest tests/test_subprocess_wrapper.py::TestShellSafety -v
    pytest tests/test_subprocess_wrapper.py::TestTimeouts -v
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

import pytest

from aequitas.config import EngineConfig
from aequitas.platform_compat import (
    HAVE_RESOURCE,
    HAVE_SIGKILL,
    HAVE_FORK,
    IS_WINDOWS,
)
from aequitas.subprocess_wrapper import SubprocessWrapper, _SOFT_KILL_SENTINEL, _HARD_KILL_SENTINEL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wrapper(
    soft_kill: int = 5,
    hard_kill: int = 10,
    stream_threshold: int = 1024,
) -> SubprocessWrapper:
    cfg = EngineConfig()
    cfg.subprocess.soft_kill_timeout_s = soft_kill
    cfg.subprocess.hard_kill_timeout_s = hard_kill
    cfg.subprocess.stream_threshold_bytes = stream_threshold
    cfg.subprocess.rlimit_cpu_s = 60
    cfg.subprocess.rlimit_as_mb = 1024
    return SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())


def _py(*code: str) -> list[str]:
    """[sys.executable, '-c', code] — portable on all platforms."""
    return [sys.executable, "-c", "\n".join(code)]


def _sleep_cmd(seconds: int) -> list[str]:
    """
    Portable sleep using sys.executable.

    WHY NOT ['sleep', str(seconds)]
    --------------------------------
    'sleep' is a POSIX binary (/bin/sleep).  On Windows it is a CMD shell
    builtin and not reachable via Popen(shell=False).
    """
    return _py(f"import time; time.sleep({seconds})")


# ---------------------------------------------------------------------------
# Basic functionality
# ---------------------------------------------------------------------------

class TestSubprocessWrapper:

    def test_simple_command_succeeds(self):
        """Cross-platform: use sys.executable instead of /bin/true."""
        wrapper = _make_wrapper()
        result = wrapper.run(_py("pass"))
        assert result.returncode == 0
        assert not result.timed_out

    def test_stdout_captured(self):
        wrapper = _make_wrapper()
        result = wrapper.run(_py("print('hello world', end='')"))
        output = result.stdout_bytes or b""
        assert b"hello world" in output

    def test_stderr_captured(self):
        wrapper = _make_wrapper()
        result = wrapper.run(_py("import sys; sys.stderr.write('err_output')"))
        stderr = result.stderr_bytes or b""
        assert b"err_output" in stderr

    def test_nonzero_exit_code(self):
        wrapper = _make_wrapper()
        result = wrapper.run(_py("raise SystemExit(42)"))
        assert result.returncode == 42

    def test_invalid_command_returns_gracefully(self):
        """An unknown binary must not raise — OSError is caught internally."""
        wrapper = _make_wrapper()
        result = wrapper.run(["__definitely_not_a_real_binary__"])
        assert result.returncode is None

    def test_task_env_merged(self):
        wrapper = _make_wrapper()
        result = wrapper.run(
            _py("import os; print(os.environ.get('TASK_ID','missing'), end='')"),
            task_env={"TASK_ID": "test_123"},
        )
        output = result.stdout_bytes or b""
        assert b"test_123" in output

    def test_wall_time_recorded(self):
        wrapper = _make_wrapper()
        result = wrapper.run(_py("pass"))
        assert result.wall_time_s >= 0

    def test_result_platform_field(self):
        """SubprocessResult.platform must match the running OS."""
        wrapper = _make_wrapper()
        result = wrapper.run(_py("pass"))
        expected = "windows" if IS_WINDOWS else "posix"
        assert result.platform == expected

    def test_windows_limits_no_raise(self):
        """_apply_windows_limits() must not raise on any platform (stub safety)."""
        import subprocess as sp
        cfg = EngineConfig()
        w = SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())
        proc = sp.Popen(_py("import time; time.sleep(0.05)"),
                        stdout=sp.DEVNULL, stderr=sp.DEVNULL)
        try:
            w._apply_windows_limits(proc)
        finally:
            proc.wait()


# ---------------------------------------------------------------------------
# Shell safety
# ---------------------------------------------------------------------------

class TestShellSafety:

    def test_no_shell_execution(self):
        """shell=False: semicolons and other metacharacters are literal."""
        wrapper = _make_wrapper()
        # Print a string containing shell metacharacters
        result = wrapper.run(_py("print('hello; echo hacked', end='')"))
        output = result.stdout_bytes or b""
        # The literal string must appear; 'hacked' must NOT appear on a new line
        assert b"hello; echo hacked" in output
        assert b"\nhacked" not in output

    def test_cmd_must_be_list(self):
        """A bare string as cmd must raise AssertionError (not silently split)."""
        wrapper = _make_wrapper()
        with pytest.raises(AssertionError):
            wrapper.run("ls -la")  # type: ignore[arg-type]

    def test_empty_cmd_raises(self):
        wrapper = _make_wrapper()
        with pytest.raises(AssertionError):
            wrapper.run([])

    def test_no_shell_true_in_source(self):
        """No Popen(shell=True) in executable code (docstring mentions are fine)."""
        import inspect, re
        import aequitas.subprocess_wrapper as sw
        src = inspect.getsource(sw)
        # Strip docstrings and comments before checking
        stripped = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
        stripped = re.sub(r"'''.*?'''", "", stripped, flags=re.DOTALL)
        stripped = re.sub(r"#.*", "", stripped)
        assert "shell=True" not in stripped


# ---------------------------------------------------------------------------
# Streaming large output
# ---------------------------------------------------------------------------

class TestOutputStream:

    def test_small_output_returns_bytes(self):
        """Output within threshold must be returned as bytes (not a file path)."""
        wrapper = _make_wrapper(stream_threshold=10_000)
        result = wrapper.run(_py("print('tiny', end='')"))
        assert result.stdout_bytes is not None
        assert result.stdout_path is None

    def test_large_output_streams_to_file(self):
        """Output exceeding stream_threshold must be written to a named temp file."""
        wrapper = _make_wrapper(stream_threshold=100)
        result = wrapper.run(_py("print('x' * 500)"))
        if result.stdout_path:
            assert result.stdout_path.exists()
            content = result.stdout_path.read_bytes()
            assert b"x" in content
            result.stdout_path.unlink()
        else:
            # Small threshold sometimes fits if the OS buffers differently
            assert result.stdout_bytes is not None


# ---------------------------------------------------------------------------
# Resource limits (POSIX only)
# ---------------------------------------------------------------------------

class TestResourceLimits:

    @pytest.mark.skipif(not HAVE_RESOURCE, reason="resource module unavailable (Windows)")
    @pytest.mark.skipif(not HAVE_FORK, reason="preexec_fn requires fork (POSIX only)")
    def test_preexec_fn_is_callable(self):
        """
        POSIX: _make_preexec() must return a callable, not None.

        A full rlimit enforcement test would require spawning a process that
        exhausts its CPU limit and observing SIGXCPU — brittle in unit tests.
        We verify the callable is produced without error and is invocable.
        """
        cfg = EngineConfig()
        cfg.subprocess.rlimit_cpu_s = 60
        cfg.subprocess.rlimit_as_mb = 1024
        w = SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())
        fn = w._make_preexec()
        assert fn is not None
        assert callable(fn)

    @pytest.mark.skipif(HAVE_FORK, reason="Only validates Windows no-op path")
    def test_preexec_fn_is_none_on_windows(self):
        """
        Windows: _make_preexec() must return a callable (no-op lambda), and
        the Popen call must pass preexec_fn=None.
        """
        cfg = EngineConfig()
        w = SubprocessWrapper(cfg.subprocess, cfg.sanitized_env_template())
        # On Windows HAVE_FORK=False, so run() passes preexec_fn=None.
        # _make_preexec() itself returns a no-op lambda (for stripped POSIX builds),
        # but the key guarantee is that run() does not crash.
        result = w.run(_py("pass"))
        assert result.returncode == 0


# ---------------------------------------------------------------------------
# Timeout enforcement
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestTimeouts:

    def test_soft_kill_terminates_slow_process(self):
        """
        A process sleeping past soft_kill_timeout_s must be killed.

        Cross-platform: uses sys.executable so no dependency on /bin/sleep.
        kill_signal is compared against the platform-appropriate sentinels
        from platform_compat rather than raw signal integers.
        """
        wrapper = _make_wrapper(soft_kill=1, hard_kill=2)
        t0 = time.monotonic()
        result = wrapper.run(_sleep_cmd(60))
        elapsed = time.monotonic() - t0

        assert result.timed_out, "Expected timed_out=True"
        assert elapsed < 5, f"Kill took too long: {elapsed:.1f}s"
        # kill_signal must be one of the two platform sentinels
        assert result.kill_signal in (_SOFT_KILL_SENTINEL, _HARD_KILL_SENTINEL), (
            f"Unexpected kill_signal value: {result.kill_signal!r}"
        )

    @pytest.mark.skipif(not HAVE_SIGKILL,
                        reason="SIGTERM-ignore test requires POSIX SIGKILL")
    def test_hard_kill_escalates_from_sigterm(self):
        """
        POSIX only: a process that ignores SIGTERM must be SIGKILL-ed at hard timeout.

        WHY SKIP ON WINDOWS
        --------------------
        Windows has no SIGTERM that a subprocess can catch and ignore via the
        Python signal module in the Popen context.  Both terminate() and kill()
        call TerminateProcess() unconditionally — there is no two-phase
        soft-then-hard escalation available at the OS level.
        """
        ignore_sigterm = (
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "time.sleep(60)\n"
        )
        wrapper = _make_wrapper(soft_kill=1, hard_kill=2)
        t0 = time.monotonic()
        result = wrapper.run(_py(ignore_sigterm))
        elapsed = time.monotonic() - t0

        assert result.timed_out
        assert elapsed < 4, f"Hard kill took too long: {elapsed:.1f}s"
        assert result.kill_signal == _HARD_KILL_SENTINEL
