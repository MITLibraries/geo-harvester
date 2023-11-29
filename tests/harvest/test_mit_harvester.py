import pytest

from harvester.harvest.mit import MITHarvester


def test_mit_harvester_list_local_files():
    harvester = MITHarvester(input_files="tests/fixtures/s3_cdn_restricted_legacy_single")
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 1


def test_mit_harvester_list_s3_files(mocked_restricted_bucket_one_legacy_fgdc_zip):
    harvester = MITHarvester(input_files="s3://mocked_cdn_restricted/cdn/geo/restricted/")
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 1


def test_mit_harvester_list_local_files_date_filter():
    harvester = MITHarvester(
        input_files="tests/fixtures/s3_cdn_restricted_legacy_single",
        from_date="2000-01-01",
        until_date="2000-12-31",
    )
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 0


def test_mit_harvester_list_s3_files_date_filter(
    mocked_restricted_bucket_one_legacy_fgdc_zip,
):
    harvester = MITHarvester(
        input_files="s3://mocked_cdn_restricted/cdn/geo/restricted/",
        from_date="2000-01-01",
        until_date="2000-12-31",
    )
    zip_files = harvester.list_zip_files()
    assert len(zip_files) == 0


def test_mit_harvester_full_harvest_non_empty_sqs_queue(
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


def test_mit_harvester_full_harvest_bad_input_files_path_local(
    mocked_restricted_bucket_empty, mocked_sqs_topic_name, sqs_client_message_count_zero
):
    harvester = MITHarvester(
        harvest_type="full",
        input_files="tests/fixtures/does_not_exist",
        sqs_topic_name=mocked_sqs_topic_name,
    )
    with pytest.raises(ValueError, match="Invalid input files path"):
        harvester.harvest()


def test_mit_harvester_full_harvest_bad_input_files_path_s3():
    pass


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
