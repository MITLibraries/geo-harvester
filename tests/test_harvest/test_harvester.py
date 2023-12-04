import datetime
from unittest.mock import MagicMock, patch

import pytest
from dateutil.parser import ParserError
from dateutil.tz import tzutc

from harvester.records import FGDC, Record


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
            source_record=FGDC(zip_file_location="/path/to/file1.zip", event="created"),
        ),
        Record(
            identifier="abc123",
            source_record=FGDC(zip_file_location="/path/to/file2.zip", event="created"),
            error_message="I have an error",
            error_stage="get_source_records",
        ),
    ]
    harvester = generic_harvester_class(harvest_type="full")
    assert len(list(harvester.filter_failed_records(records))) == 1
    assert len(harvester.failed_records) == 1


def test_harvester_get_records(caplog, generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="full")
    with patch.object(
        harvester, "full_harvest_get_source_records"
    ) as mocked_full_harvest_get_source_records:
        mocked_full_harvest_get_source_records.return_value = records = [
            Record(
                identifier="abc123",
                source_record=FGDC(
                    zip_file_location="/path/to/file1.zip", event="created"
                ),
            )
        ]
        records = harvester.get_source_records()
        _record = next(records)
        assert "Record abc123: retrieved source record" in caplog.text
        assert harvester.processed_records_count == 1
