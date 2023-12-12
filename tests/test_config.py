# ruff: noqa: N806
import logging

import pytest

from harvester.config import configure_logger, configure_sentry


def test_configure_logger_not_verbose():
    logger = logging.getLogger(__name__)
    result = configure_logger(logger, verbose=False)
    info_log_level = 20
    assert logger.getEffectiveLevel() == info_log_level
    assert result == "Logger 'tests.test_config' configured with level=INFO"


def test_configure_logger_verbose():
    logger = logging.getLogger(__name__)
    result = configure_logger(logger, verbose=True)
    debug_log_level = 10
    assert logger.getEffectiveLevel() == debug_log_level
    assert result == "Logger 'tests.test_config' configured with level=DEBUG"


def test_configure_sentry_no_env_variable(monkeypatch):
    monkeypatch.delenv("SENTRY_DSN", raising=False)
    result = configure_sentry()
    assert result == "No Sentry DSN found, exceptions will not be sent to Sentry"


def test_configure_sentry_env_variable_is_none(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    result = configure_sentry()
    assert result == "No Sentry DSN found, exceptions will not be sent to Sentry"


def test_configure_sentry_env_variable_is_dsn(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "https://1234567890@00000.ingest.sentry.io/123456")
    result = configure_sentry()
    assert result == "Sentry DSN found, exceptions will be sent to Sentry with env=test"


def test_config_check_required_env_vars_success(config_instance):
    config_instance.check_required_env_vars()


def test_config_check_required_env_vars_error(monkeypatch, config_instance):
    monkeypatch.delenv("S3_RESTRICTED_CDN_ROOT")
    with pytest.raises(OSError, match="Missing required environment variables"):
        config_instance.check_required_env_vars()


def test_config_env_var_access_success(config_instance):
    assert config_instance.WORKSPACE == "test"


def test_config_env_var_access_error(config_instance):
    with pytest.raises(AttributeError):
        _ = config_instance.DOES_NOT_EXIST


def test_config_cdn_root(config_instance):
    assert config_instance.http_cdn_root == "https://cdn.dev.mitlibrary.net/geo"
