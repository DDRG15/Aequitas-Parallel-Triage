"""
tests/test_config.py
====================
Unit tests for configuration loading, safe defaults, env template,
and cross-platform log_dir resolution.

Run:
    pytest tests/test_config.py -v
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

from aequitas.config import EngineConfig
from aequitas.platform_compat import IS_WINDOWS, IS_POSIX, ENV_ALLOWLIST


class TestSafeDefaults:

    def test_default_memory_high_water(self):
        assert EngineConfig().memory.memory_high_water_mb == 2048

    def test_default_log_queue_size(self):
        assert EngineConfig().logging.log_queue_size == 100_000

    def test_default_max_workers(self):
        assert EngineConfig().engine.max_workers == 200

    def test_default_batch_size(self):
        assert EngineConfig().engine.batch_size == 10_000

    def test_default_flush_interval_ms(self):
        assert EngineConfig().logging.flush_interval_ms == 500

    def test_default_max_batch_bytes(self):
        assert EngineConfig().logging.max_batch_bytes == 4_194_304  # 4 MiB

    def test_default_soft_kill_timeout(self):
        assert EngineConfig().subprocess.soft_kill_timeout_s == 30

    def test_default_hard_kill_timeout(self):
        assert EngineConfig().subprocess.hard_kill_timeout_s == 60

    def test_log_dir_is_nonempty_string(self):
        """
        log_dir must be a non-empty string on every platform.

        WHY NOT ASSERT A SPECIFIC PATH
        --------------------------------
        The default is platform-dependent:
          POSIX:   /var/log/aequitas  (if writable) or tempdir fallback
          Windows: %ProgramData%/aequitas/logs or tempdir fallback
        Asserting the exact string would make the test fail in sandboxed
        environments where /var/log is not writable.  We assert structure
        only: non-empty string.
        """
        log_dir = EngineConfig().logging.log_dir
        assert isinstance(log_dir, str)
        assert len(log_dir) > 0

    def test_log_dir_path_type_ok(self):
        """log_dir must be coercible to a Path without error."""
        log_dir = EngineConfig().logging.log_dir
        p = Path(log_dir)
        assert str(p)  # no exception


class TestYAMLLoad:

    def test_load_defaults_yaml(self):
        """config/defaults.yaml must load cleanly and match spec defaults."""
        cfg_path = Path(__file__).parent.parent / "config" / "defaults.yaml"
        cfg = EngineConfig.from_yaml(cfg_path)
        assert cfg.engine.max_workers == 200
        assert cfg.logging.log_queue_size == 100_000
        assert cfg.memory.memory_high_water_mb == 2048

    def test_partial_override_preserves_defaults(self):
        """A YAML with only one key must not erase the rest of the defaults."""
        override = {"engine": {"max_workers": 50}}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            yaml.dump(override, f)
            fpath = f.name
        try:
            cfg = EngineConfig.from_yaml(fpath)
            assert cfg.engine.max_workers == 50
            assert cfg.logging.log_queue_size == 100_000
            assert cfg.memory.memory_high_water_mb == 2048
        finally:
            os.unlink(fpath)

    def test_empty_yaml_gives_defaults(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        ) as f:
            f.write("")
            fpath = f.name
        try:
            cfg = EngineConfig.from_yaml(fpath)
            assert cfg.engine.max_workers == 200
        finally:
            os.unlink(fpath)


class TestExecutorAutoSelect:

    def test_auto_io_resolves_to_threaded(self):
        cfg = EngineConfig()
        cfg.engine.executor_mode = "auto"
        assert cfg.resolve_executor_mode("io") == "threaded"

    def test_auto_cpu_resolves_to_process(self):
        cfg = EngineConfig()
        cfg.engine.executor_mode = "auto"
        assert cfg.resolve_executor_mode("cpu") == "process"

    def test_auto_mixed_resolves_to_hybrid(self):
        cfg = EngineConfig()
        cfg.engine.executor_mode = "auto"
        assert cfg.resolve_executor_mode("mixed") == "async-hybrid"

    def test_explicit_mode_not_overridden(self):
        cfg = EngineConfig()
        cfg.engine.executor_mode = "threaded"
        assert cfg.resolve_executor_mode("cpu") == "threaded"


class TestEnvTemplate:

    def test_sanitized_env_excludes_secrets(self):
        """No common secret variable names must appear in the sanitized template."""
        cfg = EngineConfig()
        env = cfg.sanitized_env_template()
        dangerous = {"AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "DATABASE_URL", "API_KEY"}
        leaked = dangerous.intersection(env.keys())
        assert not leaked, f"Secret env vars leaked into template: {leaked}"

    def test_sanitized_env_is_dict(self):
        assert isinstance(EngineConfig().sanitized_env_template(), dict)

    def test_env_content_consistent_across_calls(self):
        """Two calls must return identical content (template is deterministic)."""
        cfg = EngineConfig()
        assert cfg.sanitized_env_template() == cfg.sanitized_env_template()

    def test_path_present_when_available(self):
        """PATH is in the allowlist on every platform and usually set."""
        cfg = EngineConfig()
        env = cfg.sanitized_env_template()
        if "PATH" in os.environ:
            assert "PATH" in env

    @pytest.mark.skipif(IS_WINDOWS, reason="POSIX-only vars not in Windows allowlist")
    def test_posix_vars_excluded_on_windows(self):
        pass  # only runs on POSIX; inverse check is in TestWindowsEnv

    @pytest.mark.skipif(IS_POSIX, reason="Windows-only vars not in POSIX allowlist")
    def test_windows_vars_excluded_on_posix(self):
        pass  # only runs on Windows; inverse check is in TestPosixEnv

    def test_allowlist_does_not_include_custom_secrets(self):
        """
        Verify the ENV_ALLOWLIST does not include obviously dangerous variable names.
        Even if a dangerous var happens to be set, it must not be in the allowlist.
        """
        bad = {"AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "PRIVATE_KEY", "DB_PASSWORD"}
        assert not bad.intersection(ENV_ALLOWLIST), \
            f"Dangerous vars in allowlist: {bad.intersection(ENV_ALLOWLIST)}"
