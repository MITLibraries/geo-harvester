# ruff: noqa: SLF001

import datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest
from dateutil.parser import ParserError
from dateutil.tz import tzutc

from harvester.records import FGDC, MITAardvark, Record
from harvester.records.exceptions import FieldMethodError


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
    assert harvester.from_datetime_object == datetime.datetime(
        2000, 1, 1, 5, 0, tzinfo=tzutc()
    )
    assert harvester.until_datetime_object == datetime.datetime(
        2050, 12, 31, 5, 0, tzinfo=tzutc()
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
            source_record=FGDC(
                origin="mit",
                identifier="abc123",
                data=b"",
                zip_file_location="/path/to/file1.zip",
                event="created",
            ),
        ),
        Record(
            identifier="abc123",
            source_record=FGDC(
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


def test_harvester_step_get_source_records(caplog, generic_harvester_class):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(harvest_type="full")
    with patch.object(
        harvester, "full_harvest_get_source_records"
    ) as mocked_full_harvest_get_source_records:
        mocked_full_harvest_get_source_records.return_value = records = [
            Record(
                identifier="abc123",
                source_record=FGDC(
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


def test_harvester_step_normalize_source_records_stores_exception(
    caplog, generic_harvester_class, records_for_normalize
):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(harvest_type="full")
    records_for_normalize[0].source_record.metadata_format = "bad_format"
    records = harvester.normalize_source_records(records_for_normalize)
    record = next(records)
    assert record.exception_stage == "normalize_source_records"
    assert isinstance(record.exception, FieldMethodError)
    assert isinstance(record.exception.original_exception, KeyError)


def test_harvester_step_write_source_and_normalized_both_success(
    caplog,
    generic_harvester_class,
    records_for_writing,
    mocked_source_writer,
    mocked_normalized_writer,
):
    caplog.set_level("DEBUG")
    harvester = generic_harvester_class(
        harvest_type="full",
        output_source_directory="output",
        output_normalized_directory="output",
    )
    output_record = next(harvester.write_source_and_normalized(records_for_writing))
    mocked_source_writer.assert_called_once_with(output_record)
    mocked_normalized_writer.assert_called_once_with(output_record)


def test_harvester_step_write_source_and_normalized_source_exception_log_and_yield(
    caplog,
    generic_harvester_class,
    records_for_writing,
    mocked_source_writer,
    mocked_normalized_writer,
):
    caplog.set_level("DEBUG")
    mocked_source_writer.side_effect = Exception("source write error!")

    harvester = generic_harvester_class(
        harvest_type="full",
        output_source_directory="output",
        output_normalized_directory="output",
    )
    output_record = next(harvester.write_source_and_normalized(records_for_writing))

    mocked_source_writer.assert_called_once_with(output_record)
    mocked_normalized_writer.assert_not_called()
    assert output_record.exception_stage == "write_metadata.source"
    assert str(output_record.exception) == "source write error!"


def test_harvester_step_write_source_and_normalized_normalized_exception_log_and_yield(
    caplog,
    generic_harvester_class,
    records_for_writing,
    mocked_source_writer,
    mocked_normalized_writer,
):
    caplog.set_level("DEBUG")
    mocked_normalized_writer.side_effect = Exception("normalized write error!")

    harvester = generic_harvester_class(
        harvest_type="full",
        output_source_directory="output",
        output_normalized_directory="output",
    )
    output_record = next(harvester.write_source_and_normalized(records_for_writing))

    mocked_source_writer.assert_called_once_with(output_record)
    mocked_source_writer.assert_called_once_with(output_record)
    assert output_record.exception_stage == "write_metadata.normalized"
    assert str(output_record.exception) == "normalized write error!"


def test_harvester_write_source_metadata_success(
    generic_harvester_class, records_for_writing
):
    harvester = generic_harvester_class(
        harvest_type="full",
        output_source_directory="output",
    )
    record = records_for_writing[0]
    mocked_open = mock_open()
    with patch("harvester.harvest.smart_open.open", mocked_open):
        harvester._write_source_metadata(record)
    mocked_open.assert_called_with(
        f"output/{record.source_record.source_metadata_filename}", "wb"
    )
    file_obj = mocked_open()
    file_obj.write.assert_called_once_with(record.source_record.data)


def test_harvester_write_normalized_metadata_success(
    generic_harvester_class, records_for_writing
):
    harvester = generic_harvester_class(
        harvest_type="full",
        output_normalized_directory="output",
    )
    record = records_for_writing[0]
    mocked_open = mock_open()
    with patch("harvester.harvest.smart_open.open", mocked_open):
        harvester._write_normalized_metadata(record)
    mocked_open.assert_called_with(
        f"output/{record.source_record.normalized_metadata_filename}", "w"
    )
    file_obj = mocked_open()
    file_obj.write.assert_called_once_with(
        record.source_record.normalize().to_json(pretty=False)
    )


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
