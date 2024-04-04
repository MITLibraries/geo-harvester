# ruff: noqa: SLF001, N818

import datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest
from dateutil.parser import ParserError
from dateutil.tz import tzutc

from harvester.harvest.mit import MITFGDC
from harvester.records import MITAardvark, Record


def test_harvester_bad_harvest_type_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(
        harvest_type="bad_type",
    )
    with pytest.raises(ValueError, match="harvest type: 'bad_type' not recognized"):
        harvester.harvest()


def test_harvester_from_until_date_parsing_success(generic_harvester_class):
    from_date = "2000-01-01"
    until_date = "2050-12-31"
    harvester = generic_harvester_class(
        from_date=from_date,
        until_date=until_date,
    )
    assert harvester.from_datetime_object == datetime.datetime(2000, 1, 1).astimezone(
        tzutc()
    )
    assert harvester.until_datetime_object == datetime.datetime(2050, 12, 31).astimezone(
        tzutc()
    )


def test_harvester_from_until_date_parsing_bad_date_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(
        from_date="watermelon",
    )
    with pytest.raises(ParserError):
        _ = harvester.from_datetime_object
    assert harvester.until_datetime_object is None


def test_harvester_from_until_date_parsing_none_date_returns_none(
    generic_harvester_class,
):
    harvester = generic_harvester_class()
    assert harvester.from_datetime_object is None
    assert harvester.until_datetime_object is None


def test_harvester_harvest_type_selector_full_success(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="full")
    harvester.full_harvest_get_source_records = MagicMock()
    harvester.harvest()
    harvester.full_harvest_get_source_records.assert_called()


def test_harvester_harvest_type_selector_incremental_success(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="incremental")
    harvester.incremental_harvest_get_source_records = MagicMock()
    harvester.harvest()
    harvester.incremental_harvest_get_source_records.assert_called()


def test_harvester_harvest_type_selector_bad_type_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="invalid-type")
    harvester.incremental_harvest_get_source_records = MagicMock()
    with pytest.raises(ValueError, match="harvest type: 'invalid-type' not recognized"):
        harvester.harvest()


def test_harvester_records_with_error_filtered_out(generic_harvester_class):
    records = [
        Record(
            identifier="abc123",
            source_record=MITFGDC(
                origin="mit",
                identifier="abc123",
                data=b"",
                zip_file_location="/path/to/file1.zip",
                event="created",
            ),
        ),
        Record(
            identifier="abc123",
            source_record=MITFGDC(
                origin="mit",
                identifier="abc123",
                data=b"",
                zip_file_location="/path/to/file2.zip",
                event="created",
            ),
            exception_stage="get_source_records",
            exception=Exception("I have an error"),
        ),
    ]
    harvester = generic_harvester_class(harvest_type="full")
    assert len(list(harvester.filter_failed_records(records))) == 1
    assert len(harvester.failed_records) == 1
    failed_record = harvester.failed_records[0]
    assert failed_record["record_identifier"] == "abc123"
    assert failed_record["harvest_step"] == "get_source_records"
    assert str(failed_record["exception"]) == "I have an error"


def test_harvester_step_get_source_records(caplog, generic_harvester_class):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(harvest_type="full")
    with patch.object(
        harvester, "full_harvest_get_source_records"
    ) as mocked_full_harvest_get_source_records:
        mocked_full_harvest_get_source_records.return_value = records = [
            Record(
                identifier="abc123",
                source_record=MITFGDC(
                    origin="mit",
                    identifier="abc123",
                    data=b"",
                    zip_file_location="/path/to/file1.zip",
                    event="created",
                ),
            )
        ]
        records = harvester.get_source_records()
        _record = next(records)
        assert "Record abc123: retrieved source record" in caplog.text
        assert harvester.processed_records_count == 1


def test_harvester_step_normalize_source_records_deleted_record_set_gbl_suppressed(
    caplog, generic_harvester_class, records_for_normalize
):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(harvest_type="full")
    records_for_normalize[0].source_record.event = "deleted"
    records = harvester.normalize_source_records(records_for_normalize)
    record = next(records)
    assert record.normalized_record.gbl_suppressed_b


def test_harvester_step_normalize_source_records_created_record_normalized_success(
    caplog, generic_harvester_class, records_for_normalize
):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(harvest_type="full")
    records = harvester.normalize_source_records(records_for_normalize)
    record = next(records)
    assert isinstance(record.normalized_record, MITAardvark)


def test_harvester_step_normalize_source_records_stores_exception_stage_and_object(
    caplog, generic_harvester_class, records_for_normalize
):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(harvest_type="full")
    with patch("harvester.records.record.SourceRecord.normalize") as mocked_normalize:

        class MyCustomException(Exception):
            pass

        mocked_normalize.side_effect = MyCustomException("Error during normalization.")
        records = harvester.normalize_source_records(records_for_normalize)
        record = next(records)

    assert record.exception_stage == "normalize_source_records"
    assert isinstance(record.exception, MyCustomException)


def test_harvester_step_write_combined_normalized_success(
    caplog,
    generic_harvester_class,
    records_for_writing,
):
    output_file = "output/combined_normalized.jsonl"
    harvester = generic_harvester_class(harvest_type="full", output_file=output_file)
    mocked_open = mock_open()
    with patch("harvester.harvest.smart_open.open", mocked_open):
        _ = list(harvester.write_combined_normalized(records_for_writing))
    mocked_open.assert_called_with(output_file, "w")


def test_harvester_step_write_combined_normalized_write_error_log_and_continue(
    caplog,
    generic_harvester_class,
    records_for_writing,
):
    output_file = "output/combined_normalized.jsonl"
    harvester = generic_harvester_class(harvest_type="full", output_file=output_file)
    mocked_open = mock_open()
    mocked_writer = MagicMock()
    exception_message = "Error during write!"
    mocked_writer.write.side_effect = Exception(exception_message)
    with patch("harvester.harvest.smart_open.open", mocked_open), patch(
        "jsonlines.Writer", return_value=mocked_writer
    ):
        output_record = next(harvester.write_combined_normalized(records_for_writing))
    mocked_open.assert_called_with(output_file, "w")
    assert output_record.exception_stage == "write_combined_normalized"
    assert str(output_record.exception) == exception_message


def test_harvester_get_source_records_two_records_pipeline_completes(
    generic_harvester_class, records_for_writing
):
    output_file = "output/combined_normalized.jsonl"
    harvester = generic_harvester_class(harvest_type="full", output_file=output_file)
    with patch.object(harvester, "get_source_records") as mocked_get_source_records:
        mocked_get_source_records.return_value = iter(records_for_writing)
        result = harvester.harvest()
        assert result["successful_records"] == 1


def test_harvester_get_source_records_empty_iterator_graceful_exit_early(
    caplog, generic_harvester_class
):
    caplog.set_level("INFO")
    output_file = "output/combined_normalized.jsonl"
    harvester = generic_harvester_class(harvest_type="full", output_file=output_file)
    with patch.object(harvester, "get_source_records") as mocked_get_source_records:
        mocked_get_source_records.return_value = iter(())
        with patch.object(
            harvester, "normalize_source_records"
        ) as mocked_normalize_records:
            _result = harvester.harvest()
            mocked_normalize_records.assert_not_called()
    assert "No source records found for harvest parameters, exiting." in caplog.text
