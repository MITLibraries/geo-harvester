from time import perf_counter

import pytest

from harvester.cli import main

MISSING_CLICK_ARG_RESULT_CODE = 2


def test_cli_no_options(caplog, runner):
    result = runner.invoke(main, ["ping"], obj={"START_TIME": perf_counter()})
    assert result.exit_code == 0
    assert "Logger 'root' configured with level=INFO" in caplog.text
    assert "Running process" in caplog.text
    assert "Total elapsed" in caplog.text


def test_cli_all_options(caplog, runner):
    result = runner.invoke(
        main, ["--verbose", "ping"], obj={"START_TIME": perf_counter()}
    )
    assert result.exit_code == 0
    assert "Logger 'root' configured with level=DEBUG" in caplog.text
    assert "Running process" in caplog.text
    assert "Total elapsed" in caplog.text


def test_cli_harvest_no_options(runner):
    result = runner.invoke(
        main, ["--verbose", "harvest"], obj={"START_TIME": perf_counter()}
    )
    assert result.exit_code == 0
    assert "Harvest command with sub-commands for different sources." in result.stdout


def test_cli_harvest_mit_no_options(runner):
    result = runner.invoke(
        main, ["--verbose", "harvest", "mit"], obj={"START_TIME": perf_counter()}
    )
    assert result.exit_code == MISSING_CLICK_ARG_RESULT_CODE
    assert "Missing option '-i' / '--input-files'." in result.stdout


@pytest.mark.usefixtures("_mocked_harvester_harvest")
def test_cli_harvest_mit_full_legacy_single(
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
