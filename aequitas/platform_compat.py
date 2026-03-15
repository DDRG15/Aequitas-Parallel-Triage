"""
aequitas/platform_compat.py
============================
Central platform-detection module for cross-platform compatibility.

WHY A SINGLE MODULE
--------------------
Scattering `sys.platform == "win32"` checks across every module creates
divergence and makes future platform additions (e.g., OpenBSD, AIX) a
search-and-replace exercise.  All conditional imports and capability flags
live here.  Every other module imports from this one.

SUPPORTED PLATFORMS
-------------------
- Linux   (primary target for 10M+ production workloads)
- macOS   (development / CI)
- Windows (developer workstations, Windows Server deployments)

CAPABILITY FLAGS
----------------
HAVE_RESOURCE     True on POSIX; False on Windows.
                  Guards resource.setrlimit calls.

HAVE_FORK         True on Linux/macOS; False on Windows.
                  Guards forkserver / fork multiprocessing contexts and
                  preexec_fn usage in subprocess.Popen.

HAVE_SIGKILL      True on POSIX (signal.SIGKILL = 9); False on Windows.
                  On Windows proc.kill() calls TerminateProcess() which
                  is equivalent but there is no integer signal number.

HAVE_FSYNC        True everywhere (os.fsync exists on Windows), but wrapped
                  in _safe_fsync() to suppress OSError on network-backed
                  files where fsync is not supported.

IS_WINDOWS        True on win32; False everywhere else.
IS_POSIX          True on Linux/macOS; False on Windows.
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------
# Primary platform flag
# ---------------------------------------------------------------------------

IS_WINDOWS: bool = sys.platform == "win32"
IS_POSIX:   bool = not IS_WINDOWS

# ---------------------------------------------------------------------------
# Capability: resource limits (resource module)
# ---------------------------------------------------------------------------

try:
    import resource as _resource_mod
    HAVE_RESOURCE = True
except ImportError:
    _resource_mod = None  # type: ignore[assignment]
    HAVE_RESOURCE = False

# Expose the module directly so callers can do `from platform_compat import resource_mod`
resource_mod = _resource_mod

# ---------------------------------------------------------------------------
# Capability: fork-based multiprocessing contexts
# ---------------------------------------------------------------------------

HAVE_FORK: bool = IS_POSIX  # forkserver / fork contexts require fork(2)

# ---------------------------------------------------------------------------
# Capability: SIGKILL signal number
# ---------------------------------------------------------------------------

import signal as _signal_mod

# signal.SIGKILL is defined on POSIX; on Windows only SIGTERM and SIGBREAK exist.
HAVE_SIGKILL: bool = hasattr(_signal_mod, "SIGKILL")
SIGKILL  = getattr(_signal_mod, "SIGKILL",  None)   # int | None
SIGTERM  = getattr(_signal_mod, "SIGTERM",  None)   # int | None — exists on all platforms
SIGBREAK = getattr(_signal_mod, "SIGBREAK", None)   # int | None — Windows only (CTRL+BREAK)

# ---------------------------------------------------------------------------
# Safe fsync
# ---------------------------------------------------------------------------

def safe_fsync(fd: int) -> None:
    """
    Call os.fsync(fd) and silently absorb OSError.

    WHY NOT BARE os.fsync()
    -----------------------
    On Windows, fsync on a file that lives on a network share or a virtual
    filesystem (e.g., Windows Subsystem for Linux mounts) raises OSError
    errno 22 (EINVAL) or errno 9 (EBADF).  Suppressing those errors is
    correct — the write() and flush() have already landed in the OS page
    cache; we lose only the durability guarantee, which is acceptable for
    an audit log that tolerates bounded data loss.

    On Linux, the only common errno we may see is EROFS (30) for read-only
    filesystems, which we also absorb.
    """
    try:
        os.fsync(fd)
    except OSError:
        pass  # best-effort; write + flush already completed


# ---------------------------------------------------------------------------
# Multiprocessing start method
# ---------------------------------------------------------------------------

def best_mp_context():
    """
    Return the safest multiprocessing context available on this platform.

    - Linux/macOS: "forkserver" — avoids lock-corruption bugs from bare fork()
                   in multi-threaded processes.
    - Windows:     "spawn"      — only option; forkserver/fork are not available.

    WHY NOT "fork" ON POSIX
    -----------------------
    bare fork() copies all threads' lock state into the child.  If a lock was
    held at fork time, the child deadlocks on first lock acquisition.
    forkserver starts a clean helper process before any threads are spawned,
    then communicates over a pipe.  spawn is equivalent but slower to start.
    """
    import multiprocessing
    if HAVE_FORK:
        return multiprocessing.get_context("forkserver")
    return multiprocessing.get_context("spawn")


# ---------------------------------------------------------------------------
# Env template allowlist
# ---------------------------------------------------------------------------

# Variables present on POSIX but not Windows
_POSIX_ENV = {"HOME", "LANG", "LC_ALL", "TMPDIR", "USER", "SHELL"}
# Variables present on Windows but not POSIX
_WINDOWS_ENV = {"USERPROFILE", "TEMP", "TMP", "USERNAME", "COMSPEC", "SYSTEMROOT",
                "SYSTEMDRIVE", "APPDATA", "LOCALAPPDATA", "WINDIR"}
# Variables present on both
_COMMON_ENV = {"PATH"}

ENV_ALLOWLIST: frozenset[str] = frozenset(
    _COMMON_ENV | (_WINDOWS_ENV if IS_WINDOWS else _POSIX_ENV)
)

# ---------------------------------------------------------------------------
# Default log directory (platform-appropriate)
# ---------------------------------------------------------------------------

import tempfile as _tempfile

def default_log_dir() -> str:
    """
    Return a writable log directory appropriate for the current platform.

    Production deployments should override this via config YAML.
    The default is intentionally a temp path so tests always pass without
    root privileges.

    - Linux/macOS production: /var/log/aequitas  (set in YAML)
    - Windows production:     C:\\ProgramData\\aequitas\\logs  (set in YAML)
    - Fallback (tests/dev):   tempfile.gettempdir() / "aequitas_logs"
    """
    if IS_POSIX:
        candidate = "/var/log/aequitas"
        if os.access(os.path.dirname(candidate), os.W_OK):
            return candidate
    else:
        import pathlib
        candidate = str(pathlib.Path(os.environ.get("ProgramData", "C:\\ProgramData"))
                        / "aequitas" / "logs")
        try:
            pathlib.Path(candidate).mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            pass

    # Fallback: temp dir (always writable)
    fallback = os.path.join(_tempfile.gettempdir(), "aequitas_logs")
    return fallback
