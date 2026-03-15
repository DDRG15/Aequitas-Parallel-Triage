"""
aequitas/config.py
==================
Configuration loader for Aequitas-Parallel-Triage.

WHY THIS EXISTS
---------------
All tunable knobs live in one place with documented safe defaults.  Workers,
the dispatcher, and the log writer all import EngineConfig rather than reading
os.environ or hard-coded literals.  This makes overrides testable and prevents
accidental divergence between modules.

CROSS-PLATFORM NOTES
---------------------
- log_dir default: /var/log/aequitas on POSIX, %ProgramData%/aequitas/logs on
  Windows.  Both are overridden by config YAML.  If neither is writable, falls
  back to tempfile.gettempdir()/aequitas_logs.
- sanitized_env_template(): the allowlist is platform-specific so the child
  process always receives a minimal but functional environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml  # PyYAML

from .platform_compat import ENV_ALLOWLIST, default_log_dir


# ---------------------------------------------------------------------------
# Sub-configs (dataclasses mirror the YAML structure)
# ---------------------------------------------------------------------------


@dataclass
class EngineSettings:
    executor_mode: Literal["threaded", "process", "async-hybrid", "auto"] = "auto"
    max_workers: int = 200
    batch_size: int = 10_000
    task_timeout_s: int = 120


@dataclass
class DispatcherSettings:
    token_bucket_rate: float = 5_000.0   # tasks/s
    token_bucket_burst: int = 10_000
    ingestion_queue_maxsize: int = 20_000


@dataclass
class LoggingSettings:
    log_queue_size: int = 100_000
    flush_interval_ms: int = 500
    max_batch_bytes: int = 4_194_304      # 4 MiB
    log_dir: str = field(default_factory=default_log_dir)
    max_bytes_per_file: int = 104_857_600  # 100 MiB
    backup_count: int = 10
    backpressure_sample_rate: float = 0.1


@dataclass
class MemorySettings:
    memory_high_water_mb: int = 2_048
    gc_check_interval_batches: int = 10


@dataclass
class SubprocessSettings:
    soft_kill_timeout_s: int = 30
    hard_kill_timeout_s: int = 60
    stream_threshold_bytes: int = 1_048_576  # 1 MiB
    rlimit_cpu_s: int = 60
    rlimit_as_mb: int = 1_024


@dataclass
class EscalationSettings:
    backend: Literal["redis", "in_process", "lock_sharded"] = "redis"
    redis_url: str = "redis://localhost:6379/0"
    window_seconds: int = 60
    bucket_count: int = 12
    bucket_ttl_s: int = 120
    error_threshold: int = 500
    circuit_breaker_base_delay_s: float = 1.0
    circuit_breaker_max_delay_s: float = 60.0
    circuit_breaker_jitter: float = 0.3


@dataclass
class ContainerPoolSettings:
    mode: Literal["process", "pooled", "ephemeral", "microvm"] = "pooled"
    pool_size: int = 20
    image: str = "ubuntu:22.04"
    overlay_reset: bool = True
    docker_create_timeout_ms: int = 200
    checkout_timeout_s: int = 10


@dataclass
class MetricsSettings:
    enabled: bool = True
    backend: Literal["prometheus", "statsd", "noop"] = "prometheus"
    push_gateway: str = "http://localhost:9091"
    push_interval_s: int = 15


# ---------------------------------------------------------------------------
# Root config
# ---------------------------------------------------------------------------


@dataclass
class EngineConfig:
    """
    Root configuration object.  Instantiate with safe defaults or load from
    a YAML file.

    WHY DATACLASS + YAML
    --------------------
    Dataclasses give free __repr__, type hints, and IDE autocompletion.
    YAML keeps config human-readable and version-controllable.  We avoid
    pydantic to minimise dependency weight on worker processes.
    """

    engine: EngineSettings = field(default_factory=EngineSettings)
    dispatcher: DispatcherSettings = field(default_factory=DispatcherSettings)
    logging: LoggingSettings = field(default_factory=LoggingSettings)
    memory: MemorySettings = field(default_factory=MemorySettings)
    subprocess: SubprocessSettings = field(default_factory=SubprocessSettings)
    escalation: EscalationSettings = field(default_factory=EscalationSettings)
    container_pool: ContainerPoolSettings = field(default_factory=ContainerPoolSettings)
    metrics: MetricsSettings = field(default_factory=MetricsSettings)

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str | Path) -> "EngineConfig":
        """
        Load config from a YAML file and merge with safe defaults.

        Missing keys fall back to dataclass defaults — no KeyError on
        partially-specified override files.
        """
        raw = yaml.safe_load(Path(path).read_text())
        return cls._from_dict(raw or {})

    @classmethod
    def _from_dict(cls, d: dict) -> "EngineConfig":
        """Recursive merge: dataclass fields win over missing YAML keys."""

        def _merge(dc_cls, section: dict):
            # Build kwargs only for fields present in YAML
            import dataclasses
            kwargs = {}
            for f in dataclasses.fields(dc_cls):
                if f.name in section:
                    kwargs[f.name] = section[f.name]
            return dc_cls(**kwargs)

        cfg = cls()
        if "engine" in d:
            cfg.engine = _merge(EngineSettings, d["engine"])
        if "dispatcher" in d:
            cfg.dispatcher = _merge(DispatcherSettings, d["dispatcher"])
        if "logging" in d:
            cfg.logging = _merge(LoggingSettings, d["logging"])
        if "memory" in d:
            cfg.memory = _merge(MemorySettings, d["memory"])
        if "subprocess" in d:
            cfg.subprocess = _merge(SubprocessSettings, d["subprocess"])
        if "escalation" in d:
            cfg.escalation = _merge(EscalationSettings, d["escalation"])
        if "container_pool" in d:
            cfg.container_pool = _merge(ContainerPoolSettings, d["container_pool"])
        if "metrics" in d:
            cfg.metrics = _merge(MetricsSettings, d["metrics"])
        return cfg

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def resolve_executor_mode(self, task_profile: str = "cpu") -> str:
        """
        Auto-select executor mode from task profile when mode == 'auto'.

        Rules (TODO: tune per benchmark):
        - io-bound tasks  → threaded
        - cpu-bound tasks → process
        - mixed           → async-hybrid
        """
        if self.engine.executor_mode != "auto":
            return self.engine.executor_mode
        mapping = {
            "io": "threaded",
            "cpu": "process",
            "mixed": "async-hybrid",
        }
        return mapping.get(task_profile, "threaded")

    def sanitized_env_template(self) -> dict:
        """
        Return a minimal sanitized environment dict for subprocess execution.

        WHY: os.environ.copy() per task is O(env-size) and creates a new dict
        for every one of the 10M tasks.  Build ONE template here; workers
        patch only per-task keys onto a shallow copy.

        CROSS-PLATFORM: ENV_ALLOWLIST from platform_compat selects the right
        variable names for the current OS.  POSIX vars (HOME, SHELL, ...) are
        excluded on Windows and vice-versa.
        """
        return {k: v for k, v in os.environ.items() if k in ENV_ALLOWLIST}
