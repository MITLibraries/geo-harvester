# ruff: noqa: SLF001, D212, D200, ARG002, TRY002, TRY003, EM101
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
        mock_metadata_extract.return_value = (
            "fgdc",
            FGDC(origin="mit", identifier="abc123", data="", event="created"),
        )
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


def test_mit_harvester_metadata_file_use_skip_list_success():
    """
    NOTE: this zip file contains EG_CAIRO_A25TOPO_1972.aux.xml which should be skipped
    """
    source_record = MITHarvester.create_source_record_from_zip_file(
        identifier="EG_CAIRO_A25TOPO_1972",
        event="created",
        zip_file="tests/fixtures/zip_files/EG_CAIRO_A25TOPO_1972.zip",
    )
    assert source_record.metadata_format == "fgdc"
    assert (
        source_record.zip_file_location
        == "tests/fixtures/zip_files/EG_CAIRO_A25TOPO_1972.zip"
    )


def test_mit_harvester_harvester_specific_steps_success(records_for_mit_steps):
    class MockMITHarvester(MITHarvester):
        def send_eventbridge_event(self, records):
            yield from records

        def delete_sqs_messages(self, records):
            yield from records

    harvester = MockMITHarvester(
        harvest_type="incremental",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    output_records = list(harvester.harvester_specific_steps(records_for_mit_steps))
    assert output_records == records_for_mit_steps


def test_mit_harvester_send_eventbridge_event_success(caplog, records_for_mit_steps):
    caplog.set_level("DEBUG")

    class MockMITHarvester(MITHarvester):
        def _prepare_payload_and_send_event(self, bucket, path, record):
            return "uuid-abc123-def456"

    harvester = MockMITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    output_record = next(harvester.send_eventbridge_event(records_for_mit_steps))
    assert "sending EventBridge event" in caplog.text
    assert output_record.exception is None


def test_mit_harvester_send_eventbridge_event_error_log_and_yield(
    caplog, records_for_mit_steps
):
    caplog.set_level("DEBUG")

    class MockMITHarvester(MITHarvester):
        def _prepare_payload_and_send_event(self, bucket, path, record):
            raise Exception("Error sending event")

    harvester = MockMITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    output_record = next(harvester.send_eventbridge_event(records_for_mit_steps))
    assert "sending EventBridge event" in caplog.text
    assert str(output_record.exception) == "Error sending event"
    assert output_record.exception_stage == "send_eventbridge_event"


def test_mit_harvester_prepare_payload_and_send_event_success(records_for_mit_steps):
    record = records_for_mit_steps[0]
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    with mock.patch(
        "harvester.harvest.mit.EventBridgeClient.send_event"
    ) as mocked_send_event:
        harvester._prepare_payload_and_send_event(
            "the-bucket",
            "/path/here",
            record,
        )
        mocked_send_event.assert_called_with(
            detail={
                "bucket": "the-bucket",
                "identifier": "SDE_DATA_AE_A8GNS_2003",
                "restricted": "false",
                "deleted": "false",
                "objects": [
                    {"Key": "/path/here/SDE_DATA_AE_A8GNS_2003.source.fgdc.xml"},
                    {"Key": "/path/here/SDE_DATA_AE_A8GNS_2003.normalized.aardvark.json"},
                    {"Key": "/path/here/SDE_DATA_AE_A8GNS_2003.zip"},
                ],
            }
        )


def test_mit_harvester_delete_sqs_messages_preserve_flag_skip_step(
    caplog, records_for_mit_steps
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
        preserve_sqs_messages=True,
    )
    _output_records = list(harvester.delete_sqs_messages(records_for_mit_steps))
    assert "Flag preserve_sqs_messages set, skipping delete of SQS message" in caplog.text


def test_mit_harvester_delete_sqs_messages_success(
    caplog, records_for_mit_steps, valid_sqs_message_created_instance, mock_sqs_client
):
    caplog.set_level("DEBUG")
    records_for_mit_steps[
        0
    ].source_record.sqs_message = valid_sqs_message_created_instance
    harvester = MITHarvester(
        harvest_type="incremental",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        skip_sqs_check=True,
    )
    with mock.patch.object(harvester.sqs_client, "delete_message") as mocked_delete:
        _output_records = list(harvester.delete_sqs_messages(records_for_mit_steps))
        assert "Record SDE_DATA_AE_A8GNS_2003: deleting SQS message" in caplog.text
        mocked_delete.assert_called_with(
            valid_sqs_message_created_instance.receipt_handle
        )
