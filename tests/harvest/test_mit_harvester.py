import os

import pytest

from harvester.harvest.mit import MITHarvester


def test_mit_harvester_list_local_files_equals_one():
    harvester = MITHarvester(input_files="tests/fixtures/s3_cdn_restricted_legacy_single")
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 1


def test_mit_harvester_list_s3_files_equals_one(
    mocked_restricted_bucket_one_legacy_fgdc_zip,
):
    harvester = MITHarvester(input_files=os.environ["S3_RESTRICTED_CDN_ROOT"])
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 1


def test_mit_harvester_list_local_files_date_filter_equals_zero():
    harvester = MITHarvester(
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        from_date="2000-01-01",
        until_date="2000-12-31",
    )
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 0


def test_mit_harvester_list_s3_files_date_filter_equals_zero(
    mocked_restricted_bucket_one_legacy_fgdc_zip,
):
    harvester = MITHarvester(
        input_files=os.environ["S3_RESTRICTED_CDN_ROOT"],
        from_date="2000-01-01",
        until_date="2000-12-31",
    )
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 0


def test_mit_harvester_full_harvest_non_empty_sqs_queue_raise_error(
    mocked_sqs_topic_name, sqs_client_message_count_ten
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="/local/does/not/matter",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    with pytest.raises(RuntimeError) as exc_info:
        harvester.harvest()
    assert "Cannot perform full harvest when SQS queue has unprocessed messages" in str(
        exc_info.value
    )


def test_mit_harvester_full_harvest_bad_input_files_path_local_raise_error(
    mocked_restricted_bucket_empty, mocked_sqs_topic_name, sqs_client_message_count_zero
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/does_not_exist",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    with pytest.raises(ValueError, match="Invalid input files path"):
        harvester._list_local_zip_files()  # noqa: SLF001


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
        harvester._list_s3_zip_files()  # noqa: SLF001


def test_mit_harvester_full_harvest_zero_zip_files_found(
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
    harvester.harvest()
    assert "0 zip file(s) identified for full harvest" in caplog.text


def test_mit_harvester_full_harvest_one_zip_files_found(
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
    harvester.harvest()
    assert "1 zip file(s) identified for full harvest" in caplog.text


def test_mit_harvester_incremental_harvest_one_zip_files_found(
    caplog,
    mocked_sqs_topic_name,
    mock_boto3_sqs_client,
    invalid_sqs_message_dict,
    valid_sqs_message_dict,
):
    mock_boto3_sqs_client.receive_message.side_effect = [
        {"Messages": [valid_sqs_message_dict]},
        {"Messages": [invalid_sqs_message_dict]},
        {"Messages": [valid_sqs_message_dict]},
        {},
    ]
    harvester = MITHarvester(
        harvest_type="incremental",
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    harvester.harvest()
    assert "2 message(s) identified for incremental harvest" in caplog.text
