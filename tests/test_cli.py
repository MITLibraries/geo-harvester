from time import perf_counter
from unittest.mock import patch

import pytest

from harvester.cli import main
from harvester.config import Config

MISSING_CLICK_ARG_RESULT_CODE = 2


def test_cli_no_options_success(caplog, runner):
    result = runner.invoke(main, ["ping"], obj={"START_TIME": perf_counter()})
    assert result.exit_code == 0
    assert "Logger 'root' configured with level=INFO" in caplog.text
    assert "Running process" in caplog.text
    assert "Total elapsed" in caplog.text


def test_cli_all_options_success(caplog, runner):
    result = runner.invoke(
        main, ["--verbose", "ping"], obj={"START_TIME": perf_counter()}
    )
    assert result.exit_code == 0
    assert "Logger 'root' configured with level=DEBUG" in caplog.text
    assert "Running process" in caplog.text
    assert "Total elapsed" in caplog.text


def test_cli_harvest_no_options_success(runner):
    result = runner.invoke(
        main, ["--verbose", "harvest"], obj={"START_TIME": perf_counter()}
    )
    assert result.exit_code == 0
    assert "Harvest command with sub-commands for different sources." in result.stdout


@pytest.mark.usefixtures("_unset_s3_cdn_env_vars")
def test_cli_harvest_mit_no_options_raise_error(runner):
    with patch.object(Config, "check_required_env_vars") as mocked_env_vars_check:
        mocked_env_vars_check.return_value = True
        result = runner.invoke(
            main, ["--verbose", "harvest", "mit"], obj={"START_TIME": perf_counter()}
        )
        assert result.exit_code == MISSING_CLICK_ARG_RESULT_CODE
        assert "Missing option '-i' / '--input-files'." in result.stdout


@pytest.mark.usefixtures("_mocked_harvester_harvest")
def test_cli_harvest_mit_full_legacy_single_success(
    runner,
    mocked_sqs_topic_name,
    sqs_client_message_count_zero,
):
    result = runner.invoke(
        main,
        [
            "--verbose",
            "harvest",
            "mit",
            "--input-files",
            "tests/fixtures/s3_cdn_restricted_legacy_single",
            "--sqs-topic-name",
            "mocked-geo-harvester-input",
        ],
        obj={"START_TIME": perf_counter()},
    )
    assert result.exit_code == 0


def test_cli_harvest_ogm_no_options_success(runner, mocked_ogm_harvester):
    result = runner.invoke(
        main, ["--verbose", "harvest", "ogm"], obj={"START_TIME": perf_counter()}
    )
    assert result.exit_code == 0


def test_cli_harvest_ogm_include_repositories_success(runner, mocked_ogm_harvester):
    _result = runner.invoke(
        main,
        [
            "--verbose",
            "harvest",
            "ogm",
            "--include-repositories",
            "repo1,repo2,  repo3",  # testing whitespace stripping
        ],
    )
    args, kwargs = mocked_ogm_harvester.call_args
    assert kwargs["include_repositories"] == ["repo1", "repo2", "repo3"]


def test_cli_harvest_ogm_exclude_repositories_success(runner, mocked_ogm_harvester):
    _result = runner.invoke(
        main,
        [
            "--verbose",
            "harvest",
            "ogm",
            "--exclude-repositories",
            "  repo1,repo2   ,repo3",  # testing whitespace stripping
        ],
    )
    args, kwargs = mocked_ogm_harvester.call_args
    assert kwargs["exclude_repositories"] == ["repo1", "repo2", "repo3"]
