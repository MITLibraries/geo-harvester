# ruff: noqa: SLF001
from unittest import mock

import pytest

from harvester.harvest.mit import MITHarvester
from harvester.records import FGDC


def test_mit_harvester_list_local_files_equals_one():
    harvester = MITHarvester(input_files="tests/fixtures/s3_cdn_restricted_legacy_single")
    zip_files = harvester._list_zip_files()
    assert len(zip_files) == 1


def test_mit_harvester_list_s3_files_equals_one(
    mocked_restricted_bucket_one_legacy_fgdc_zip,
):
    harvester = MITHarvester(input_files="s3://mocked_cdn_restricted/cdn/geo/restricted/")
    zip_files = harvester._list_zip_files()
    assert len(zip_files) == 1


def test_mit_harvester_list_local_files_date_filter_equals_zero():
    harvester = MITHarvester(
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        from_date="2000-01-01",
        until_date="2000-12-31",
    )
    zip_files = harvester._list_zip_files()
    assert len(zip_files) == 0


def test_mit_harvester_list_s3_files_date_filter_equals_zero(
    mocked_restricted_bucket_one_legacy_fgdc_zip,
):
    harvester = MITHarvester(
        input_files="s3://mocked_cdn_restricted/cdn/geo/restricted/",
        from_date="2000-01-01",
        until_date="2000-12-31",
    )
    zip_files = harvester._list_zip_files()
    assert len(zip_files) == 0


def test_mit_harvester_full_harvest_non_empty_sqs_queue_raise_error(
    mocked_sqs_topic_name, sqs_client_message_count_ten
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="/local/does/not/matter",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    assert not harvester.skip_sqs_check
    assert not harvester._sqs_queue_is_empty()
    with pytest.raises(
        RuntimeError,
        match="Cannot perform full harvest when SQS queue has unprocessed messages",
    ):
        list(harvester.full_harvest_get_source_records())


def test_mit_harvester_full_harvest_bad_input_files_path_local_raise_error(
    mocked_restricted_bucket_empty, mocked_sqs_topic_name, sqs_client_message_count_zero
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/does_not_exist",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    with pytest.raises(ValueError, match="Invalid input files path"):
        harvester._list_local_zip_files()


def test_mit_harvester_full_harvest_bad_bucket_input_files_path_s3_raise_error(
    mocked_restricted_bucket_empty, mocked_sqs_topic_name, sqs_client_message_count_zero
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="s3://bad-bucket/prefix/okay/though",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    with pytest.raises(
        ValueError,
        match="Could not list objects for: 's3://bad-bucket/prefix/okay/though'",
    ):
        harvester._list_s3_zip_files()


def test_mit_harvester_full_harvest_zero_zip_files_returned(
    caplog,
    mocked_restricted_bucket_empty,
    mocked_sqs_topic_name,
    sqs_client_message_count_zero,
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_empty",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    records = harvester.full_harvest_get_source_records()
    assert len(list(records)) == 0


def test_mit_harvester_full_harvest_one_zip_files_returned(
    caplog,
    mocked_restricted_bucket_empty,
    mocked_sqs_topic_name,
    sqs_client_message_count_zero,
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    records = harvester.full_harvest_get_source_records()
    assert len(list(records)) == 1


def test_mit_harvester_incremental_harvest_two_zip_files_returned(
    caplog,
    mocked_sqs_topic_name,
    mock_boto3_sqs_client,
    invalid_sqs_message_dict,
    valid_sqs_message_deleted_dict,
    valid_sqs_message_created_dict,
):
    mock_boto3_sqs_client.receive_message.side_effect = [
        {"Messages": [valid_sqs_message_created_dict]},  # zip file created in s3
        {"Messages": [invalid_sqs_message_dict]},  # invalid message skipped
        {"Messages": [valid_sqs_message_deleted_dict]},  # zip file deleted from s3
        {},
    ]
    harvester = MITHarvester(
        harvest_type="incremental",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_multiple",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    with mock.patch(
        "harvester.harvest.mit.MITHarvester._identify_and_read_metadata_file"
    ) as mock_metadata_extract:
        mock_metadata_extract.return_value = ("fgdc", FGDC(data="", event="created"))
        records = harvester.incremental_harvest_get_source_records()
        assert len(list(records)) == 2  # noqa: PLR2004


def test_mit_harvester_source_record_has_expected_values(caplog):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    records = harvester.full_harvest_get_source_records()
    record = next(records)

    assert record.identifier == "SDE_DATA_AE_A8GNS_2003"
    assert (
        record.source_record.zip_file_location
        == "tests/fixtures/s3_cdn_restricted_legacy_single/SDE_DATA_AE_A8GNS_2003.zip"
    )
    assert record.source_record.event == "created"
    assert record.exception is None


def test_mit_harvester_find_metadata_file_missing_file_error():
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    with mock.patch("harvester.harvest.mit.zipfile.ZipFile.namelist") as mocked_namelist:
        mocked_namelist.return_value = ["abc123.shp"]
    with pytest.raises(FileNotFoundError):
        harvester._identify_and_read_metadata_file(
            "abc123",
            "tests/fixtures/s3_cdn_restricted_legacy_single/SDE_DATA_AE_A8GNS_2003.zip",
        )
